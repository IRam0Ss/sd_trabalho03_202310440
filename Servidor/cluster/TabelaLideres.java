/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: TabelaLideres
* Funcao.............: Tabela (shard -> lider ativo conhecido) com expiracao, alimentada por anuncios entre shards.
*************************************************************** */

package cluster;

/**
 * Mapa dos lideres ativos de cada shard conhecido por este no. As entradas expiram
 * se nao forem renovadas (anuncios chegam a cada ~1s), indicando shard sem lider alcancavel.
 * IDs sao os IDs internos dos nos (idReplica + 1).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class TabelaLideres {

  public static final long VALIDADE_MS = 8000;

  private static class Entrada {
    int idLider = -1;
    long epoca = -1;
    long ts = 0;
    String host = null;
  }

  private final Entrada[] entradas = new Entrada[Particionador.NUM_SHARDS];
  private final String[][] hostsReplicas = new String[Particionador.NUM_SHARDS][PortasShard.MAX_REPLICAS];

  public TabelaLideres() {
    for (int i = 0; i < entradas.length; i++) {
      entradas[i] = new Entrada();
    }
  }

  private static boolean hostValido(String h) {
    return h != null && !h.trim().isEmpty() && !"0.0.0.0".equals(h.trim());
  }

  /** Registra o IP conhecido de uma replica especifica (idNo = idReplica + 1) em outro shard. */
  public synchronized void registrarHostReplica(int shard, int idNo, String host) {
    if (shard < 0 || shard >= Particionador.NUM_SHARDS) return;
    int idx = idNo - 1;
    if (idx < 0 || idx >= PortasShard.MAX_REPLICAS || !hostValido(host)) return;
    String novoHost = host.trim();
    String atual = hostsReplicas[shard][idx];
    // Prefere IP real da LAN sobre 127.0.0.1 quando disponivel
    if (atual == null || !"127.0.0.1".equals(novoHost) || "127.0.0.1".equals(atual)) {
      hostsReplicas[shard][idx] = novoHost;
    }
  }

  /** Retorna o host conhecido para uma replica (idNo = idReplica + 1), ou hostPadrao se desconhecido. */
  public synchronized String getHostReplica(int shard, int idNo, String hostPadrao) {
    if (shard >= 0 && shard < Particionador.NUM_SHARDS) {
      int idx = idNo - 1;
      if (idx >= 0 && idx < PortasShard.MAX_REPLICAS && hostValido(hostsReplicas[shard][idx])) {
        return hostsReplicas[shard][idx];
      }
      if (hostValido(entradas[shard].host)) {
        return entradas[shard].host;
      }
      for (int r = 0; r < PortasShard.MAX_REPLICAS; r++) {
        if (hostValido(hostsReplicas[shard][r])) {
          return hostsReplicas[shard][r];
        }
      }
    }
    return hostPadrao;
  }

  /** Retorna o host conhecido do lider ativo do shard, ou hostPadrao se desconhecido. */
  public synchronized String getHostLider(int shard, String hostPadrao) {
    if (shard >= 0 && shard < Particionador.NUM_SHARDS) {
      Entrada e = entradas[shard];
      if (hostValido(e.host)) {
        return e.host;
      }
      if (e.idLider > 0) {
        return getHostReplica(shard, e.idLider, hostPadrao);
      }
    }
    return hostPadrao;
  }

  /** Retorna o ID interno do lider do shard, ou -1 se desconhecido/expirado. */
  public synchronized int getLider(int shard) {
    Entrada e = entradas[shard];
    if (e.idLider > 0 && System.currentTimeMillis() - e.ts <= VALIDADE_MS) {
      return e.idLider;
    }
    return -1;
  }

  public synchronized long getEpoca(int shard) {
    return entradas[shard].epoca;
  }

  /** Registra o lider anunciado por ele mesmo (papel ATIVO). Retorna true se o lider mudou. */
  public synchronized boolean registrarAtivo(int shard, int idLider, long epoca) {
    return registrarAtivo(shard, idLider, epoca, null);
  }

  public synchronized boolean registrarAtivo(int shard, int idLider, long epoca, String host) {
    if (hostValido(host)) {
      registrarHostReplica(shard, idLider, host);
    }
    Entrada e = entradas[shard];
    boolean expirado = System.currentTimeMillis() - e.ts > VALIDADE_MS;
    if (e.idLider == idLider || expirado || e.idLider <= 0 || epoca >= e.epoca) {
      boolean mudou = e.idLider != idLider;
      e.idLider = idLider;
      e.epoca = epoca;
      e.ts = System.currentTimeMillis();
      if (hostValido(host)) {
        e.host = host.trim();
      } else {
        e.host = getHostReplica(shard, idLider, e.host);
      }
      return mudou;
    }
    return false;
  }

  /** Registra o lider conhecido por um standby, so se nao ha informacao fresca. */
  public synchronized boolean sugerir(int shard, int idLider) {
    return sugerir(shard, idLider, null);
  }

  public synchronized boolean sugerir(int shard, int idLider, String hostSugerido) {
    if (idLider <= 0) {
      return false;
    }
    if (hostValido(hostSugerido)) {
      registrarHostReplica(shard, idLider, hostSugerido);
    }
    Entrada e = entradas[shard];
    boolean expirado = System.currentTimeMillis() - e.ts > VALIDADE_MS;
    if (expirado || e.idLider <= 0) {
      boolean mudou = e.idLider != idLider;
      e.idLider = idLider;
      e.ts = System.currentTimeMillis();
      if (hostValido(hostSugerido)) {
        e.host = hostSugerido.trim();
      } else {
        e.host = getHostReplica(shard, idLider, e.host);
      }
      return mudou;
    }
    return false;
  }

  /** Descarta a entrada se ainda apontar para o no informado (ex.: no inalcancavel). */
  public synchronized void invalidar(int shard, int idLider) {
    Entrada e = entradas[shard];
    if (e.idLider == idLider) {
      e.idLider = -1;
    }
  }

  /** Substitui a entrada do proprio shard (fonte: estado local do no). */
  public synchronized boolean definirProprio(int shard, int idLider, long epoca) {
    return definirProprio(shard, idLider, epoca, null);
  }

  public synchronized boolean definirProprio(int shard, int idLider, long epoca, String host) {
    if (hostValido(host)) {
      registrarHostReplica(shard, idLider, host);
    }
    Entrada e = entradas[shard];
    boolean mudou = e.idLider != idLider;
    e.idLider = idLider;
    e.epoca = epoca;
    e.ts = System.currentTimeMillis();
    if (hostValido(host)) {
      e.host = host.trim();
    }
    return mudou;
  }
}
