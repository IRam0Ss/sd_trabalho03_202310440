/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 06/11/2026
* Nome...............: ConfigCluster
* Funcao.............: Topologia Zero-Config: ID automatico (menor livre), shard = ID % 3 e portas derivadas do ID.
*************************************************************** */

package cluster;

import java.io.Serializable;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import utils.Protocolo;

/**
 * Carrega e disponibiliza os metadados de conectividade de cada no dentro do seu Shard (T#03).
 * O no descobre sozinho seu ID global ao entrar na rede; dele derivam ({@link PortasShard}):
 * <ul>
 *   <li>idShard    = ID % 3  (ID 1 -> S1, ID 2 -> S2, ID 3 -> S0, ...)</li>
 *   <li>TCP        = 5000 + ID</li>
 *   <li>UDP        = 6000 + ID</li>
 *   <li>ClusterLAN = 7000 + ID</li>
 * </ul>
 * Apenas nos do MESMO shard sao registrados na tabela de consenso local, garantindo
 * que S0, S1 e S2 formem tres clusters independentes.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 4.0
 */
public class ConfigCluster implements Serializable {

  private static final long serialVersionUID = 2L;

  /**
   * Metadados de rede de um no do shard.
   */
  public static class NoInfo implements Serializable {
    private static final long serialVersionUID = 2L;

    public final int id;
    public final int idShard;
    public final int idReplica;
    public final String host;
    public final int portaCluster;
    public final int portaTcp;
    public final int portaUdp;
    public final int portaDiscovery;

    public NoInfo(int id, int idShard, int idReplica, String host, int portaCluster, int portaTcp, int portaUdp,
        int portaDiscovery) {
      this.id = id;
      this.idShard = idShard;
      this.idReplica = idReplica;
      this.host = host;
      this.portaCluster = portaCluster;
      this.portaTcp = portaTcp;
      this.portaUdp = portaUdp;
      this.portaDiscovery = portaDiscovery;
    }

    @Override
    public String toString() {
      return String.format("No[S%d/R%d (id=%d): host=%s, tcp=%d, udp=%d, cluster=%d]",
          idShard, idReplica, id, host, portaTcp, portaUdp, portaCluster);
    }
  }

  private final int meuId;
  private final int idShard;
  private final int idReplica;
  private final Map<Integer, NoInfo> nos;

  public ConfigCluster(int meuId, int idShard, int idReplica, Map<Integer, NoInfo> nos) {
    Particionador.validarShard(idShard);
    this.meuId = meuId;
    this.idShard = idShard;
    this.idReplica = idReplica;
    this.nos = new ConcurrentHashMap<>(nos);
  }

  /** Shard deste no (0, 1 ou 2). Eleicao, replicacao e heartbeats ocorrem apenas intra-shard. */
  public int getIdShard() {
    return idShard;
  }

  /** Indice (0-based) desta replica dentro do shard. */
  public int getIdReplica() {
    return idReplica;
  }

  /** ID global do no (1..99), atribuido automaticamente. Shard = ID % 3. */
  public int getMeuId() {
    return meuId;
  }

  public NoInfo getMeuNoInfo() {
    return nos.get(meuId);
  }

  public Map<Integer, NoInfo> getNos() {
    return Collections.unmodifiableMap(nos);
  }

  public List<NoInfo> getOutrosNos() {
    List<NoInfo> outros = new ArrayList<>();
    for (NoInfo n : nos.values()) {
      if (n.id != meuId && n.idShard == idShard) {
        outros.add(n);
      }
    }
    return outros;
  }

  /**
   * Registra dinamicamente um par descoberto durante a execucao, APENAS se pertencer ao mesmo shard.
   */
  public void registrarParDinamico(NoInfo info) {
    if (info == null) {
      return;
    }
    if (info.idShard != this.idShard) {
      ClusterLog.aviso("ISOLAMENTO", String.format(
          "Ignorando registro de par de outro shard (Recebido: S%d/R%d | Meu Shard: S%d).",
          info.idShard, info.idReplica, this.idShard));
      return;
    }
    // Mapeamento estrito e deterministico derivado do ID global (7000 + ID_DO_PAR)
    int portaClusterCorreta = PortasShard.clusterDoNo(info.id);
    int portaTcpCorreta = PortasShard.tcpDoNo(info.id);
    int portaUdpCorreta = PortasShard.udpDoNo(info.id);
    NoInfo infoAjustada = new NoInfo(
        info.id, info.idShard, info.idReplica, info.host,
        portaClusterCorreta, portaTcpCorreta, portaUdpCorreta, info.portaDiscovery);

    if (!nos.containsKey(info.id)) {
      nos.put(info.id, infoAjustada);
      ClusterLog.info("CONFIG", String.format(
          "Nova replica integrada ao Shard S%d: Replica %d (No %d: %s, cluster=%d, tcp=%d)",
          infoAjustada.idShard, infoAjustada.idReplica, infoAjustada.id, infoAjustada.host,
          infoAjustada.portaCluster, infoAjustada.portaTcp));
    }
  }

  /**
   * Testa se uma porta TCP esta livre para bind no host informado.
   */
  public static boolean isPortaDisponivel(String host, int porta) {
    try (ServerSocket ss = new ServerSocket()) {
      ss.setReuseAddress(true);
      ss.bind(new InetSocketAddress(host, porta));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Testa se ha algum servico TCP escutando na porta informada.
   */
  public static boolean isPortaEmUso(String host, int porta) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, porta), 80);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  // ===========================================================================
  // INICIALIZACAO AUTONOMA (ZERO-CONFIG): ID automatico -> shard = ID % 3
  // ===========================================================================

  /** Socket do canal de cluster reservado durante a escolha do ID (evita corrida entre nos simultaneos). */
  private static ServerSocket reservaCluster;

  /**
   * Entrega ao {@link CanalCluster} o ServerSocket ja vinculado durante a escolha do ID, se houver.
   * Assim a porta 7000+ID nunca fica livre entre a eleicao do ID e a abertura do canal.
   */
  public static synchronized ServerSocket assumirReservaCluster(int porta) {
    ServerSocket s = reservaCluster;
    if (s != null && !s.isClosed() && s.getLocalPort() == porta) {
      reservaCluster = null;
      return s;
    }
    return null;
  }

  private static ServerSocket tentarReservar(int porta) {
    try {
      ServerSocket ss = new ServerSocket();
      ss.setReuseAddress(true);
      ss.bind(new InetSocketAddress(porta)); // 0.0.0.0: vale para loopback e LAN
      return ss;
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean portaUdpLivre(int porta) {
    try (DatagramSocket ds = new DatagramSocket(null)) {
      ds.bind(new InetSocketAddress(porta));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean isIdTotalmenteDisponivel(int k) {
    if (isPortaEmUso("127.0.0.1", PortasShard.clusterDoNo(k))) {
      return false;
    }
    if (!isPortaDisponivel("0.0.0.0", PortasShard.tcpDoNo(k))) {
      return false;
    }
    if (!portaUdpLivre(PortasShard.udpDoNo(k))) {
      return false;
    }
    return true;
  }

  /**
   * Descobre automaticamente o proprio ID ao entrar na rede com priorizacao inteligente de Shards:
   * <ol>
   *   <li>Sonda a rede (farol UDP global) e a maquina local (portas 7000+k) para listar IDs e Shards ocupados;</li>
   *   <li><b>Prioridade 1:</b> Garante a inicializacao de novos shards em ordem crescente (S0 [A..I], depois S1 [J..R], depois S2 [S..Z]);</li>
   *   <li><b>Prioridade 2:</b> Somente quando todos os 3 shards tiverem nos ativos, aloca novos processos como STANDBY / REPLICA balanceados;</li>
   *   <li>{@code idShard = (ID - 1) % 3}, {@code idReplica = (ID - 1) / 3};</li>
   *   <li>Portas: TCP=5000+ID, UDP=6000+ID, Cluster=7000+ID (bind em 0.0.0.0).</li>
   * </ol>
   *
   * @param forcarLoopback se true anuncia 127.0.0.1 (testes isolados); caso contrario anuncia o IP da LAN.
   */
  public static synchronized ConfigCluster descobrirEConfigurarAutomatico(boolean forcarLoopback) {
    return descobrirEConfigurarAutomatico(null, null, null, forcarLoopback);
  }

  public static synchronized ConfigCluster descobrirEConfigurarAutomatico(Integer idForcado, boolean forcarLoopback) {
    return descobrirEConfigurarAutomatico(idForcado, null, null, forcarLoopback);
  }

  /**
   * Descobre automaticamente o proprio ID ao entrar na rede com priorizacao inteligente de Shards:
   *
   * @param idForcado      ID especifico opcional (1..99).
   * @param shardForcado   Shard especifico opcional (0=S0, 1=S1, 2=S2).
   * @param replicaForcada Replica especifica opcional (0=Lider Ativo, 1=Standby).
   * @param forcarLoopback se true anuncia 127.0.0.1 (testes isolados); caso contrario anuncia o IP da LAN.
   */
  public static synchronized ConfigCluster descobrirEConfigurarAutomatico(
      Integer idForcado, Integer shardForcado, Integer replicaForcada, boolean forcarLoopback) {
    String ipLan = BeaconClusterLAN.obterIpLocalLAN();
    String meuHost = forcarLoopback ? "127.0.0.1" : ipLan;

    ClusterLog.info("CONFIG:AUTO", "Sondando nos existentes (LAN + localhost) para atribuicao automatica de ID...");
    Map<Integer, NoInfo> nosRede = BeaconClusterLAN.sondarTodosOsNos(1500);

    Set<Integer> ocupados = new TreeSet<>(nosRede.keySet());
    for (int k = 1; k <= PortasShard.MAX_ID; k++) {
      if (!ocupados.contains(k) && isPortaEmUso("127.0.0.1", PortasShard.clusterDoNo(k))) {
        ocupados.add(k);
      }
    }

    // Identifica quais shards ja possuem nos vivos e conta replicas por shard
    Set<Integer> shardsCobertos = new HashSet<>();
    int[] contagemPorShard = new int[Particionador.NUM_SHARDS];
    for (int k : ocupados) {
      int s = PortasShard.shardDoNo(k);
      if (s >= 0 && s < Particionador.NUM_SHARDS) {
        shardsCobertos.add(s);
        contagemPorShard[s]++;
      }
    }

    int idEscolhido = -1;
    if (idForcado != null && idForcado > 0) {
      idEscolhido = idForcado;
      ServerSocket reserva = tentarReservar(PortasShard.clusterDoNo(idEscolhido));
      if (reservaCluster != null) {
        try {
          reservaCluster.close();
        } catch (Exception ignored) {
        }
      }
      reservaCluster = reserva;
    } else if (shardForcado != null && shardForcado >= 0 && shardForcado < Particionador.NUM_SHARDS) {
      // 1. Shard explicitamente solicitado via CLI (ex: --shard 0 [--standby / --replica 1])
      int repAlvo = (replicaForcada != null && replicaForcada >= 0)
          ? replicaForcada
          : (shardsCobertos.contains(shardForcado) ? 1 : 0);

      int candId = PortasShard.idDoNo(shardForcado, repAlvo);
      // Se a replica alvo ja estiver ocupada localmente, procura a proxima replica livre deste shard
      if (ocupados.contains(candId) || !isIdTotalmenteDisponivel(candId)) {
        for (int r = 0; r < PortasShard.MAX_REPLICAS; r++) {
          int testId = PortasShard.idDoNo(shardForcado, r);
          if (!ocupados.contains(testId) && isIdTotalmenteDisponivel(testId)) {
            candId = testId;
            repAlvo = r;
            break;
          }
        }
      }
      idEscolhido = candId;
      ServerSocket reserva = tentarReservar(PortasShard.clusterDoNo(idEscolhido));
      if (reservaCluster != null) {
        try {
          reservaCluster.close();
        } catch (Exception ignored) {
        }
      }
      reservaCluster = reserva;
      ClusterLog.info("CONFIG:CLI", String.format(
          "[SHARD FORCADO] Shard S%d (%s) | Replica %d -> ID %d (%s)",
          shardForcado, Particionador.nome(shardForcado), repAlvo, idEscolhido,
          repAlvo == 0 ? "LIDER ATIVO" : "STANDBY"));
    } else {
      // PRIORIDADE 1: Percorre os shards em ordem estritamente crescente (S0 -> S1 -> S2).
      // Se algum shard AINDA NAO TEM no ativo, ele DEVE ser inicializado com prioridade maxima!
      for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
        if (!shardsCobertos.contains(s)) {
          for (int rep = 0; rep < PortasShard.MAX_REPLICAS; rep++) {
            int candId = PortasShard.idDoNo(s, rep);
            if (!ocupados.contains(candId) && isIdTotalmenteDisponivel(candId)) {
              ServerSocket reserva = tentarReservar(PortasShard.clusterDoNo(candId));
              if (reserva != null) {
                if (reservaCluster != null) {
                  try {
                    reservaCluster.close();
                  } catch (Exception ignored) {
                  }
                }
                reservaCluster = reserva;
                idEscolhido = candId;
                ClusterLog.info("CONFIG:AUTO", String.format(
                    "[PRIORIDADE-1: NOVO SHARD] Shard S%d (%s) sem lider ativo! Assumindo este shard (ID %d).",
                    s, Particionador.nome(s), idEscolhido));
                break;
              }
            }
          }
          if (idEscolhido > 0) break;
        }
      }

      // PRIORIDADE 2: Se todos os 3 shards ja possuem nos ativos (S0, S1 e S2 estao no ar),
      // este novo no entra como STANDBY / REPLICA no shard com menor numero de replicas (round-robin ordenado).
      if (idEscolhido <= 0) {
        int menorQtd = Integer.MAX_VALUE;
        int shardAlvoStandby = 0;
        for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
          if (contagemPorShard[s] < menorQtd) {
            menorQtd = contagemPorShard[s];
            shardAlvoStandby = s;
          }
        }

        for (int rep = 1; rep < PortasShard.MAX_REPLICAS; rep++) {
          int candId = PortasShard.idDoNo(shardAlvoStandby, rep);
          if (!ocupados.contains(candId) && isIdTotalmenteDisponivel(candId)) {
            ServerSocket reserva = tentarReservar(PortasShard.clusterDoNo(candId));
            if (reserva != null) {
              if (reservaCluster != null) {
                try {
                  reservaCluster.close();
                } catch (Exception ignored) {
                }
              }
              reservaCluster = reserva;
              idEscolhido = candId;
              ClusterLog.info("CONFIG:AUTO", String.format(
                  "[PRIORIDADE-2: STANDBY] Todos os shards ativos. Assumindo modo STANDBY para Shard S%d (%s) | Replica %d (ID %d).",
                  shardAlvoStandby, Particionador.nome(shardAlvoStandby), rep, idEscolhido));
              break;
            }
          }
        }
      }

      // FALLBACK: varredura sequencial caso as prioridades tenham falhado em portas especificas
      if (idEscolhido <= 0) {
        for (int k = 1; k <= PortasShard.MAX_ID; k++) {
          if (ocupados.contains(k) || !isIdTotalmenteDisponivel(k)) {
            continue;
          }
          ServerSocket reserva = tentarReservar(PortasShard.clusterDoNo(k));
          if (reserva != null) {
            if (reservaCluster != null) {
              try {
                reservaCluster.close();
              } catch (Exception ignored) {
              }
            }
            reservaCluster = reserva;
            idEscolhido = k;
            break;
          }
        }
      }
    }
    if (idEscolhido <= 0) {
      throw new IllegalStateException("Nenhum ID livre entre 1 e " + PortasShard.MAX_ID + " (portas esgotadas).");
    }

    int idShard = PortasShard.shardDoNo(idEscolhido);
    int idReplica = PortasShard.replicaDoNo(idEscolhido);

    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    mapa.put(idEscolhido, new NoInfo(idEscolhido, idShard, idReplica, meuHost,
        PortasShard.clusterDoNo(idEscolhido), PortasShard.tcpDoNo(idEscolhido),
        PortasShard.udpDoNo(idEscolhido), Protocolo.PORTA_DISCOVERY));

    // Pares do MESMO shard entram na tabela de consenso local
    for (int k : ocupados) {
      if (k == idEscolhido || PortasShard.shardDoNo(k) != idShard) {
        continue;
      }
      NoInfo info = nosRede.get(k);
      if (info == null) {
        info = new NoInfo(k, idShard, PortasShard.replicaDoNo(k), "127.0.0.1",
            PortasShard.clusterDoNo(k), PortasShard.tcpDoNo(k), PortasShard.udpDoNo(k), Protocolo.PORTA_DISCOVERY);
      }
      mapa.put(k, info);
    }

    ClusterLog.info("CONFIG:AUTO", String.format(
        "IDs ja ocupados na rede: %s -> este no assumiu o ID %d%s",
        ocupados.isEmpty() ? "nenhum" : ocupados.toString(), idEscolhido, ocupados.isEmpty() ? " (pioneiro)" : ""));
    ClusterLog.info("CONFIG:AUTO", String.format(
        "No %d -> Shard %s (S%d) | Replica %d | Portas: TCP=%d, UDP=%d, Cluster=%d | Host anunciado: %s",
        idEscolhido, Particionador.nome(idShard), idShard, idReplica,
        PortasShard.tcpDoNo(idEscolhido), PortasShard.udpDoNo(idEscolhido), PortasShard.clusterDoNo(idEscolhido),
        meuHost));
    ClusterLog.info("CONFIG:AUTO", String.format("Pares conhecidos no Shard S%d: %d", idShard, mapa.size() - 1));

    return new ConfigCluster(idEscolhido, idShard, idReplica, mapa);
  }
}
