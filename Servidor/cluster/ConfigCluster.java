/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ConfigCluster
* Funcao.............: Gerenciador de topologia dinamica do cluster com suporte a atribuicao automatica de ID e crescimento infinito.
*************************************************************** */

package cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import utils.Protocolo;

/**
 * Carrega e disponibiliza os metadados de conectividade de cada no do cluster.
 * Suporta atribuicao automatica de IDs (sem intervencao manual do usuario),
 * cluster aberto e infinito que aceita novos membros a qualquer instante,
 * execucao local e distribuicao em rede LAN via auto-descoberta.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 */
public class ConfigCluster implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Metadados de rede de um no do cluster.
   */
  public static class NoInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int id;
    public final String host;
    public final int portaCluster;
    public final int portaTcp;
    public final int portaUdp;
    public final int portaDiscovery;
    /** Shard (0..2) ao qual o no pertence. */
    public final int idShard;
    /** Indice da replica dentro do shard (0-based). Participa da formula de portas. */
    public final int idReplica;

    /** Construtor legado (T#02): assume shard 0 e replica = id-1. */
    public NoInfo(int id, String host, int portaCluster, int portaTcp, int portaUdp, int portaDiscovery) {
      this(id, 0, Math.max(0, id - 1), host, portaCluster, portaTcp, portaUdp, portaDiscovery);
    }

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
      return String.format("No[%d: shard=%d, replica=%d, host=%s, cluster=%d, tcp=%d, udp=%d]", id, idShard, idReplica,
          host, portaCluster, portaTcp, portaUdp);
    }
  }

  private final int meuId;
  private final int idShard;
  private final Map<Integer, NoInfo> nos;

  public ConfigCluster(int meuId, Map<Integer, NoInfo> nos) {
    this(meuId, 0, nos);
  }

  public ConfigCluster(int meuId, int idShard, Map<Integer, NoInfo> nos) {
    this.meuId = meuId;
    this.idShard = idShard;
    this.nos = new ConcurrentHashMap<>(nos);
  }

  /** Shard deste no. Eleicao, replicacao e heartbeats so valem entre nos do mesmo shard. */
  public int getIdShard() {
    return idShard;
  }

  /** Indice (0-based) desta replica dentro do shard. */
  public int getIdReplica() {
    NoInfo m = getMeuNoInfo();
    return (m != null) ? m.idReplica : Math.max(0, meuId - 1);
  }

  /**
   * Gera a configuracao de um no em execucao local (127.0.0.1) para a topologia de shards.
   * O ID interno do no no cluster do shard e idReplica+1 (IDs positivos, como no T#02);
   * as portas seguem {@link PortasShard}. Cada no conhece APENAS as replicas do proprio shard,
   * o que isola eleicao e replicacao entre shards.
   *
   * @param idShard        0..2
   * @param idReplica      0..totalReplicas-1
   * @param totalReplicas  replicas por shard (demo: 2)
   */
  public static ConfigCluster gerarConfigShard(int idShard, int idReplica, int totalReplicas) {
    Particionador.validarShard(idShard);
    if (totalReplicas < 1 || totalReplicas > PortasShard.MAX_REPLICAS) {
      throw new IllegalArgumentException("Total de replicas invalido: " + totalReplicas);
    }
    if (idReplica < 0 || idReplica >= totalReplicas) {
      throw new IllegalArgumentException("ID_REPLICA " + idReplica + " fora do intervalo 0.." + (totalReplicas - 1));
    }
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    for (int r = 0; r < totalReplicas; r++) {
      mapa.put(r + 1, new NoInfo(r + 1, idShard, r, "127.0.0.1", PortasShard.cluster(idShard, r),
          PortasShard.tcp(idShard, r), PortasShard.udp(idShard, r), Protocolo.PORTA_DISCOVERY));
    }
    ClusterLog.info("CONFIG:SHARD", String.format(
        "Shard %s | Replica %d (id interno %d) | Portas: TCP=%d, UDP=%d, Cluster=%d | Replicas do shard: %d",
        Particionador.nome(idShard), idReplica, idReplica + 1, PortasShard.tcp(idShard, idReplica),
        PortasShard.udp(idShard, idReplica), PortasShard.cluster(idShard, idReplica), totalReplicas));
    return new ConfigCluster(idReplica + 1, idShard, mapa);
  }

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
      if (n.id != meuId) {
        outros.add(n);
      }
    }
    return outros;
  }

  /**
   * Registra um novo no descoberto dinamicamente durante a execucao do cluster.
   */
  public void registrarParDinamico(NoInfo info) {
    if (info != null && !nos.containsKey(info.id)) {
      nos.put(info.id, info);
      ClusterLog.info("CONFIG", "Novo no integrado a topologia: No " + info.id + " (" + info.host + ":" + info.portaCluster + ")");
    }
  }

  /**
   * Testa se uma porta TCP local esta disponivel para bind.
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
   * Testa se ha algum servico respondendo em uma porta TCP.
   */
  public static boolean isPortaEmUso(String host, int porta) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, porta), 100);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static Integer carregarIdCacheado(String sufixo) {
    try {
      File f = new File(".cluster_node_" + sufixo + ".id");
      if (f.exists() && f.isFile()) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"))) {
          String linha = br.readLine();
          if (linha != null && !linha.trim().isEmpty()) {
            int id = Integer.parseInt(linha.trim());
            if (id > 0) return id;
          }
        }
      }
    } catch (Exception ignored) {}
    return null;
  }

  private static void salvarIdCacheado(String sufixo, int id) {
    try {
      File f = new File(".cluster_node_" + sufixo + ".id");
      try (java.io.FileOutputStream fos = new java.io.FileOutputStream(f)) {
        fos.write(String.valueOf(id).getBytes("UTF-8"));
      }
    } catch (Exception ignored) {}
  }

  /**
   * Atribui automaticamente o ID local e descobre os outros nos locais ativos
   * na mesma maquina escaneando os offsets de porta.
   * 
   * @param idForcado ID opcional forcado por linha de comando (se null, detecta automaticamente).
   * @return Objeto ConfigCluster pronto para uso.
   */
  public static ConfigCluster descobrirEConfigurarLocal(Integer idForcado) {
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    int idEscolhido = -1;

    if (idForcado != null && idForcado > 0) {
      idEscolhido = idForcado;
    } else {
      // Escaneia os IDs de 1 a 100 para encontrar o primeiro disponivel
      for (int k = 1; k <= 100; k++) {
        int portaCluster = 9000 + k;
        if (!isPortaEmUso("127.0.0.1", portaCluster)) {
          idEscolhido = k;
          break;
        }
      }
      if (idEscolhido <= 0) {
        idEscolhido = 1;
      }
    }

    // Registra o proprio no
    int minhaPortaCluster = 9000 + idEscolhido;
    int minhaPortaTcp = 6789 + 10 * (idEscolhido - 1);
    int minhaPortaUdp = 7777 + 10 * (idEscolhido - 1);
    int minhaPortaDisc = Protocolo.PORTA_DISCOVERY;
    NoInfo meuInfo = new NoInfo(idEscolhido, "127.0.0.1", minhaPortaCluster, minhaPortaTcp, minhaPortaUdp, minhaPortaDisc);
    mapa.put(idEscolhido, meuInfo);

    // Detecta outros nos locais que ja estejam em execucao
    for (int j = 1; j <= 50; j++) {
      if (j == idEscolhido) continue;
      int pCluster = 9000 + j;
      if (isPortaEmUso("127.0.0.1", pCluster)) {
        int pTcp = 6789 + 10 * (j - 1);
        int pUdp = 7777 + 10 * (j - 1);
        mapa.put(j, new NoInfo(j, "127.0.0.1", pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY));
      }
    }

    ClusterLog.info("CONFIG:LOCAL", String.format("No %d atribuido | Portas: Cluster=%d, TCP=%d, UDP=%d",
        idEscolhido, minhaPortaCluster, minhaPortaTcp, minhaPortaUdp));
    if (mapa.size() > 1) {
      StringBuilder sb = new StringBuilder("Nos locais detectados: ");
      for (Integer id : mapa.keySet()) {
        if (id != idEscolhido) sb.append("No ").append(id).append(" ");
      }
      ClusterLog.info("CONFIG:LOCAL", sb.toString().trim());
    } else {
      ClusterLog.info("CONFIG:LOCAL", "Nenhum outro no local ativo. Este no sera o pioneiro do cluster.");
    }

    return new ConfigCluster(idEscolhido, mapa);
  }

  /**
   * Atribui automaticamente o ID e descobre os pares via broadcast UDP na rede local (LAN).
   * 
   * @param idForcado ID opcional forcado por linha de comando (se null, detecta automaticamente).
   * @return Objeto ConfigCluster pronto para uso em rede.
   */
  public static ConfigCluster descobrirEConfigurarRede(Integer idForcado) {
    String meuIp = BeaconClusterLAN.obterIpLocalLAN();
    int portaClusterPadrao = 9000;
    int portaTcpPadrao = Protocolo.PORTA_SERVIDOR_TCP;
    int portaUdpPadrao = Protocolo.PORTA_SERVIDOR_UDP;

    ClusterLog.info("CONFIG:REDE", "Sondando presenca de cluster existente na rede local via broadcast...");
    Map<Integer, NoInfo> paresRede = BeaconClusterLAN.sondarParesNaRede(meuIp, portaClusterPadrao, portaTcpPadrao, portaUdpPadrao);

    int idEscolhido = -1;
    if (idForcado != null && idForcado > 0) {
      idEscolhido = idForcado;
      ClusterLog.info("CONFIG:REDE", "Utilizando ID forcado por parametro: No " + idEscolhido);
    } else {
      // 1. Tenta reutilizar o ID previamente atribuido a esta maquina
      Integer idCache = carregarIdCacheado("lan");
      if (idCache != null && idCache > 0 && !paresRede.containsKey(idCache)) {
        idEscolhido = idCache;
        ClusterLog.info("CONFIG:REDE", "Reassumindo identidade anterior desta maquina: No " + idEscolhido);
      } else {
        // 2. Preenchimento de lacunas (Gap Recycling): menor inteiro positivo livre >= 1
        int candidato = 1;
        while (paresRede.containsKey(candidato)) {
          candidato++;
        }
        idEscolhido = candidato;
        ClusterLog.info("CONFIG:REDE", "Alocando menor ID livre no cluster: No " + idEscolhido);
      }
    }
    salvarIdCacheado("lan", idEscolhido);

    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>(paresRede);
    NoInfo meuInfo = new NoInfo(idEscolhido, meuIp, portaClusterPadrao, portaTcpPadrao, portaUdpPadrao, Protocolo.PORTA_DISCOVERY);
    mapa.put(idEscolhido, meuInfo);

    ClusterLog.info("CONFIG:REDE", String.format("No %d pronto (%s) | Pares encontrados na LAN: %d",
        idEscolhido, meuIp, paresRede.size()));

    return new ConfigCluster(idEscolhido, mapa);
  }

  /**
   * Metodo legado para compatibilidade com testes que informam (id, totalNos).
   */
  public static ConfigCluster gerarConfigLocal(int meuId, int totalNos) {
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    for (int k = 1; k <= totalNos; k++) {
      int portaCluster = 9000 + k;
      int portaTcp = 6789 + 10 * (k - 1);
      int portaUdp = 7777 + 10 * (k - 1);
      int portaDisc = Protocolo.PORTA_DISCOVERY;
      mapa.put(k, new NoInfo(k, "127.0.0.1", portaCluster, portaTcp, portaUdp, portaDisc));
    }
    return new ConfigCluster(meuId, mapa);
  }

  /**
   * Carrega configuracao a partir de um arquivo .properties para casos especificos.
   */
  public static ConfigCluster carregarArquivo(String caminho, int meuId) throws Exception {
    File f = new File(caminho);
    if (!f.exists()) {
      throw new IllegalArgumentException("Arquivo de configuracao do cluster nao encontrado: " + caminho);
    }

    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"))) {
      String linha;
      while ((linha = br.readLine()) != null) {
        linha = linha.trim();
        if (linha.isEmpty() || linha.startsWith("#")) {
          continue;
        }

        String[] partesIgual = linha.split("=", 2);
        if (partesIgual.length < 2) continue;

        String chave = partesIgual[0].trim();
        if (!chave.startsWith("no.")) continue;

        int id = Integer.parseInt(chave.substring(3).trim());
        String resto = partesIgual[1].trim();

        String[] tokens = resto.split(";");
        String host = tokens[0].trim();
        int portaCluster = 9000 + id;
        int portaTcp = Protocolo.PORTA_SERVIDOR_TCP;
        int portaUdp = Protocolo.PORTA_SERVIDOR_UDP;
        int portaDisc = Protocolo.PORTA_DISCOVERY;

        for (int i = 1; i < tokens.length; i++) {
          String tok = tokens[i].trim();
          if (tok.startsWith("cluster=")) {
            portaCluster = Integer.parseInt(tok.substring(8).trim());
          } else if (tok.startsWith("tcp=")) {
            portaTcp = Integer.parseInt(tok.substring(4).trim());
          } else if (tok.startsWith("udp=")) {
            portaUdp = Integer.parseInt(tok.substring(4).trim());
          } else if (tok.startsWith("discovery=")) {
            portaDisc = Integer.parseInt(tok.substring(10).trim());
          }
        }

        mapa.put(id, new NoInfo(id, host, portaCluster, portaTcp, portaUdp, portaDisc));
      }
    }

    if (!mapa.containsKey(meuId)) {
      throw new IllegalArgumentException("O ID informado (" + meuId + ") nao consta no arquivo de configuracao.");
    }

    return new ConfigCluster(meuId, mapa);
  }
}
