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

  public static final long VALIDADE_MS = 4000;

  private static class Entrada {
    int idLider = -1;
    long epoca = -1;
    long ts = 0;
  }

  private final Entrada[] entradas = new Entrada[Particionador.NUM_SHARDS];

  public TabelaLideres() {
    for (int i = 0; i < entradas.length; i++) {
      entradas[i] = new Entrada();
    }
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
    Entrada e = entradas[shard];
    boolean expirado = System.currentTimeMillis() - e.ts > VALIDADE_MS;
    if (e.idLider == idLider || expirado || e.idLider <= 0 || epoca >= e.epoca) {
      boolean mudou = e.idLider != idLider;
      e.idLider = idLider;
      e.epoca = epoca;
      e.ts = System.currentTimeMillis();
      return mudou;
    }
    return false;
  }

  /** Registra o lider conhecido por um standby, so se nao ha informacao fresca. */
  public synchronized boolean sugerir(int shard, int idLider) {
    if (idLider <= 0) {
      return false;
    }
    Entrada e = entradas[shard];
    boolean expirado = System.currentTimeMillis() - e.ts > VALIDADE_MS;
    if (expirado || e.idLider <= 0) {
      boolean mudou = e.idLider != idLider;
      e.idLider = idLider;
      e.ts = System.currentTimeMillis();
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
    Entrada e = entradas[shard];
    boolean mudou = e.idLider != idLider;
    e.idLider = idLider;
    e.epoca = epoca;
    e.ts = System.currentTimeMillis();
    return mudou;
  }
}
