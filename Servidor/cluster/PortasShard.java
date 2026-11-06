/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 06/11/2026
* Nome...............: PortasShard
* Funcao.............: Derivacao de shard, replica e portas a partir do ID global (automatico) do no.
*************************************************************** */

package cluster;

/**
 * Toda a identidade de um no e derivada do seu ID global (1..MAX_ID), atribuido automaticamente
 * na entrada da rede (menor ID livre). Nao ha configuracao manual:
 * <pre>
 *   idShard    = id % 3            (ID 1 -> S1, ID 2 -> S2, ID 3 -> S0, ID 4 -> S1 ...)
 *   idReplica  = (id - 1) / 3      (indice da replica dentro do shard: 0, 1, 2 ...)
 *   TCP        = 5000 + id
 *   UDP        = 6000 + id
 *   ClusterLAN = 7000 + id
 * </pre>
 * A porta legada TCP/UDP 5000 nunca colide (id >= 1) e continua reservada ao lider do Shard 0.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 */
public final class PortasShard {

  public static final int BASE_TCP = 5000;
  public static final int BASE_UDP = 6000;
  public static final int BASE_CLUSTER = 7000;

  /** Maior ID global aceito (mantem TCP em 5001..5099, abaixo de qualquer outra porta do sistema). */
  public static final int MAX_ID = 99;

  /** Limite de replicas por shard (compatibilidade com tabelas indexadas). */
  public static final int MAX_REPLICAS = 100;

  private PortasShard() {
  }

  // ---------------------------------------------------------------------------
  // ID global <-> (shard, replica)
  // ---------------------------------------------------------------------------

  /** Shard de um no: {@code id % 3}. */
  public static int shardDoNo(int id) {
    return Math.floorMod(id, Particionador.NUM_SHARDS);
  }

  /** Indice da replica dentro do shard: {@code (id - 1) / 3}. */
  public static int replicaDoNo(int id) {
    return Math.max(0, (id - 1) / Particionador.NUM_SHARDS);
  }

  /** Inverso de {@link #shardDoNo}/{@link #replicaDoNo}: ID global da replica {@code r} do shard {@code s}. */
  public static int idDoNo(int idShard, int idReplica) {
    Particionador.validarShard(idShard);
    int base = (idShard == 0) ? Particionador.NUM_SHARDS : idShard;
    return idReplica * Particionador.NUM_SHARDS + base;
  }

  // ---------------------------------------------------------------------------
  // Portas a partir do ID global
  // ---------------------------------------------------------------------------

  public static int tcpDoNo(int id) {
    return BASE_TCP + id;
  }

  public static int udpDoNo(int id) {
    return BASE_UDP + id;
  }

  public static int clusterDoNo(int id) {
    return BASE_CLUSTER + id;
  }

  // ---------------------------------------------------------------------------
  // Atalhos por (shard, replica) - mantidos para os paineis de observabilidade
  // ---------------------------------------------------------------------------

  public static int tcp(int idShard, int idReplica) {
    return tcpDoNo(idDoNo(idShard, idReplica));
  }

  public static int udp(int idShard, int idReplica) {
    return udpDoNo(idDoNo(idShard, idReplica));
  }

  public static int cluster(int idShard, int idReplica) {
    return clusterDoNo(idDoNo(idShard, idReplica));
  }
}
