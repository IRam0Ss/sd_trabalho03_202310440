/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: ConfigCluster
* Funcao.............: Configuracao de topologia por Shard (S0, S1, S2) e Replica com calculo deterministico de portas.
*************************************************************** */

package cluster;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import utils.Protocolo;

/**
 * Carrega e disponibiliza os metadados de conectividade de cada no dentro do seu Shard (T#03).
 * Cada instancia pertence obrigatoriamente a um {@code idShard} (0..2) e possui um {@code idReplica} (0..N),
 * utilizando a matematica deterministica de portas ({@link PortasShard}):
 * <ul>
 *   <li>TCP        = 5000 + (idShard * 100) + idReplica</li>
 *   <li>UDP        = 6000 + (idShard * 100) + idReplica</li>
 *   <li>ClusterLAN = 7000 + (idShard * 100) + idReplica</li>
 * </ul>
 * Apenas nos do MESMO shard sao registrados na tabela de consenso local, garantindo
 * que S0, S1 e S2 formem tres clusters independentes.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
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

  /** ID interno (1-based: idReplica + 1) utilizado na maquina de consenso do proprio shard. */
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
    if (!nos.containsKey(info.id)) {
      nos.put(info.id, info);
      ClusterLog.info("CONFIG", String.format(
          "Nova replica integrada ao Shard S%d: Replica %d (%s:%d)",
          info.idShard, info.idReplica, info.host, info.portaCluster));
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

  /**
   * Configura um no em modo LOCAL (127.0.0.1) para (idShard, idReplica) aplicando estritamente
   * a matematica de portas e isolando os membros por shard.
   *
   * @param idShard        Identificador do shard (0=A..I, 1=J..R, 2=S..Z).
   * @param idReplica      Identificador da replica no shard (0..N).
   * @param totalReplicas  Quantidade esperada de replicas no shard (padrao: 2).
   * @return ConfigCluster isolado para o shard informado.
   */
  public static ConfigCluster gerarConfigShard(int idShard, int idReplica, int totalReplicas) {
    Particionador.validarShard(idShard);
    if (idReplica < 0 || idReplica >= PortasShard.MAX_REPLICAS) {
      throw new IllegalArgumentException(
          "ID_REPLICA invalido: " + idReplica + " (esperado 0.." + (PortasShard.MAX_REPLICAS - 1) + ")");
    }
    if (totalReplicas < 1 || totalReplicas > PortasShard.MAX_REPLICAS) {
      throw new IllegalArgumentException("Total de replicas invalido: " + totalReplicas);
    }

    int minhaPortaTcp = PortasShard.tcp(idShard, idReplica);
    int minhaPortaUdp = PortasShard.udp(idShard, idReplica);
    int minhaPortaCluster = PortasShard.cluster(idShard, idReplica);

    if (isPortaEmUso("127.0.0.1", minhaPortaCluster)) {
      throw new IllegalStateException(String.format(
          "Ja existe uma instancia ativa para Shard S%d | Replica %d (porta Cluster %d em uso em 127.0.0.1).",
          idShard, idReplica, minhaPortaCluster));
    }

    int qtdSlots = Math.max(totalReplicas, idReplica + 1);
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    for (int r = 0; r < qtdSlots; r++) {
      int idInterno = r + 1;
      mapa.put(idInterno, new NoInfo(
          idInterno,
          idShard,
          r,
          "127.0.0.1",
          PortasShard.cluster(idShard, r),
          PortasShard.tcp(idShard, r),
          PortasShard.udp(idShard, r),
          Protocolo.PORTA_DISCOVERY));
    }

    // Descobre eventuais replicas adicionais (r >= qtdSlots) APENAS do MESMO shard em localhost
    for (int r = qtdSlots; r < 10; r++) {
      int pCluster = PortasShard.cluster(idShard, r);
      if (isPortaEmUso("127.0.0.1", pCluster)) {
        int idInterno = r + 1;
        mapa.put(idInterno, new NoInfo(
            idInterno,
            idShard,
            r,
            "127.0.0.1",
            pCluster,
            PortasShard.tcp(idShard, r),
            PortasShard.udp(idShard, r),
            Protocolo.PORTA_DISCOVERY));
      }
    }

    ClusterLog.info("CONFIG", String.format(
        "Iniciando Shard: S%d | Replica: %d | Portas: TCP=%d, UDP=%d, Cluster=%d",
        idShard, idReplica, minhaPortaTcp, minhaPortaUdp, minhaPortaCluster));

    return new ConfigCluster(idReplica + 1, idShard, idReplica, mapa);
  }

  /**
   * Configura um no em modo REDE (LAN) para (idShard, idReplica), mantendo a mesma matematica de portas
   * e sondando via broadcast UDP apenas pares que pertencam ao mesmo idShard.
   */
  public static ConfigCluster configurarRedeShard(int idShard, int idReplica) {
    Particionador.validarShard(idShard);
    if (idReplica < 0 || idReplica >= PortasShard.MAX_REPLICAS) {
      throw new IllegalArgumentException(
          "ID_REPLICA invalido: " + idReplica + " (esperado 0.." + (PortasShard.MAX_REPLICAS - 1) + ")");
    }

    String meuIp = BeaconClusterLAN.obterIpLocalLAN();
    int minhaPortaTcp = PortasShard.tcp(idShard, idReplica);
    int minhaPortaUdp = PortasShard.udp(idShard, idReplica);
    int minhaPortaCluster = PortasShard.cluster(idShard, idReplica);

    if (isPortaEmUso(meuIp, minhaPortaCluster)) {
      throw new IllegalStateException(String.format(
          "Ja existe uma instancia ativa para Shard S%d | Replica %d em %s:%d.",
          idShard, idReplica, meuIp, minhaPortaCluster));
    }

    ClusterLog.info("CONFIG", String.format(
        "Iniciando Shard: S%d | Replica: %d | Portas: TCP=%d, UDP=%d, Cluster=%d (Host LAN: %s)",
        idShard, idReplica, minhaPortaTcp, minhaPortaUdp, minhaPortaCluster, meuIp));

    Map<Integer, NoInfo> paresShard = BeaconClusterLAN.sondarParesNaRede(
        idShard, idReplica, meuIp, minhaPortaCluster, minhaPortaTcp, minhaPortaUdp);

    int meuId = idReplica + 1;
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>(paresShard);
    mapa.put(meuId, new NoInfo(
        meuId, idShard, idReplica, meuIp, minhaPortaCluster, minhaPortaTcp, minhaPortaUdp, Protocolo.PORTA_DISCOVERY));

    ClusterLog.info("CONFIG", String.format(
        "Sondagem LAN concluida para Shard S%d: %d par(es) do mesmo shard encontrado(s).",
        idShard, paresShard.size()));

    return new ConfigCluster(meuId, idShard, idReplica, mapa);
  }
}
