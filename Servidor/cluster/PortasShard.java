/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: PortasShard
* Funcao.............: Calculo dinamico e deterministico das portas de cada no a partir de (idShard, idReplica).
*************************************************************** */

package cluster;

/**
 * Formula de portas para execucao de varios nos na mesma maquina sem colisao:
 * <pre>
 *   TCP        = 5000 + idShard*100 + idReplica
 *   UDP        = 6000 + idShard*100 + idReplica
 *   ClusterLAN = 7000 + idShard*100 + idReplica
 * </pre>
 * Shard 0 / Replica 0 assume TCP 5000 (porta legada, compativel com clientes antigos).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public final class PortasShard {

  public static final int BASE_TCP = 5000;
  public static final int BASE_UDP = 6000;
  public static final int BASE_CLUSTER = 7000;
  public static final int MAX_REPLICAS = 100;

  private PortasShard() {
  }

  private static void validar(int idShard, int idReplica) {
    Particionador.validarShard(idShard);
    if (idReplica < 0 || idReplica >= MAX_REPLICAS) {
      throw new IllegalArgumentException("ID_REPLICA invalido: " + idReplica + " (esperado 0.." + (MAX_REPLICAS - 1) + ")");
    }
  }

  public static int tcp(int idShard, int idReplica) {
    validar(idShard, idReplica);
    return BASE_TCP + idShard * 100 + idReplica;
  }

  public static int udp(int idShard, int idReplica) {
    validar(idShard, idReplica);
    return BASE_UDP + idShard * 100 + idReplica;
  }

  public static int cluster(int idShard, int idReplica) {
    validar(idShard, idReplica);
    return BASE_CLUSTER + idShard * 100 + idReplica;
  }
}

