/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: PortasShard
* Funcao.............: Derivacao de shard, replica e portas a partir do ID global (automatico) do no.
*************************************************************** */

package cluster;

/**
 * Toda a identidade de um no e derivada do seu ID global (1..MAX_ID), atribuido automaticamente
 * na entrada da rede (com prioridade crescente {@code S0 -> S1 -> S2}). Nao ha configuracao manual:
 * <pre>{@code
 *   idShard    = (id - 1) % 3      (ID 1 -> S0 [A..I], ID 2 -> S1 [J..R], ID 3 -> S2 [S..Z], ID 4 -> S0 ...)
 *   idReplica  = (id - 1) / 3      (indice da replica dentro do shard: 0, 1, 2 ...)
 *   TCP        = 5000 + id
 *   UDP        = 6000 + id
 *   ClusterLAN = 7000 + id
 * }</pre>
 * A porta legada TCP/UDP 5000 nunca colide ({@code id >= 1}) e continua reservada ao lider do Shard 0.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
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

  /** Shard de um no: {@code (id - 1) % 3} (ordem crescente: {@code ID 1 -> S0, ID 2 -> S1, ID 3 -> S2}). */
  public static int shardDoNo(int id) {
    if (id <= 0) return 0;
    return Math.floorMod(id - 1, Particionador.NUM_SHARDS);
  }

  /** Indice da replica dentro do shard: {@code (id - 1) / 3}. */
  public static int replicaDoNo(int id) {
    if (id <= 0) return 0;
    return Math.max(0, (id - 1) / Particionador.NUM_SHARDS);
  }

  /** Inverso de {@link #shardDoNo}/{@link #replicaDoNo}: ID global da replica {@code r} do shard {@code s}. */
  public static int idDoNo(int idShard, int idReplica) {
    Particionador.validarShard(idShard);
    return idReplica * Particionador.NUM_SHARDS + idShard + 1;
  }

  // ---------------------------------------------------------------------------
  // Portas a partir do ID global
  // ---------------------------------------------------------------------------

  /**
   * Calcula a porta TCP dedicada de atendimento a clientes para o no informado ({@code 5000 + id}).
   *
   * @param id Identificador global do no.
   * @return Porta TCP correspondente.
   */
  public static int tcpDoNo(int id) {
    return BASE_TCP + id;
  }

  /**
   * Calcula a porta UDP dedicada de atendimento a clientes para o no informado ({@code 6000 + id}).
   *
   * @param id Identificador global do no.
   * @return Porta UDP correspondente.
   */
  public static int udpDoNo(int id) {
    return BASE_UDP + id;
  }

  /**
   * Calcula a porta TCP interna de comunicacao do cluster para o no informado ({@code 7000 + id}).
   *
   * @param id Identificador global do no.
   * @return Porta TCP do canal de cluster.
   */
  public static int clusterDoNo(int id) {
    return BASE_CLUSTER + id;
  }

  // ---------------------------------------------------------------------------
  // Atalhos por (shard, replica) - mantidos para os paineis de observabilidade
  // ---------------------------------------------------------------------------

  /**
   * Retorna a porta TCP da replica {@code idReplica} pertencente ao shard {@code idShard}.
   *
   * @param idShard   Indice do shard (0..2).
   * @param idReplica Indice da replica (0..N).
   * @return Porta TCP correspondente.
   */
  public static int tcp(int idShard, int idReplica) {
    return tcpDoNo(idDoNo(idShard, idReplica));
  }

  /**
   * Retorna a porta UDP da replica {@code idReplica} pertencente ao shard {@code idShard}.
   *
   * @param idShard   Indice do shard (0..2).
   * @param idReplica Indice da replica (0..N).
   * @return Porta UDP correspondente.
   */
  public static int udp(int idShard, int idReplica) {
    return udpDoNo(idDoNo(idShard, idReplica));
  }

  /**
   * Retorna a porta de cluster da replica {@code idReplica} pertencente ao shard {@code idShard}.
   *
   * @param idShard   Indice do shard (0..2).
   * @param idReplica Indice da replica (0..N).
   * @return Porta TCP de cluster correspondente.
   */
  public static int cluster(int idShard, int idReplica) {
    return clusterDoNo(idDoNo(idShard, idReplica));
  }
}
