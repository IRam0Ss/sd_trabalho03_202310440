/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: BeaconClusterLAN
* Funcao.............: Farol UDP em broadcast com filtragem estrita por idShard para descoberta intra-shard.
*************************************************************** */

package cluster;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.Protocolo;

/**
 * Permite que servidores descubram automaticamente outros nos DO MESMO SHARD via UDP broadcast.
 * Todas as mensagens de descoberta carregam obrigatoriamente o {@code idShard} e o {@code idReplica},
 * garantindo que um no somente se conecte via {@link CanalCluster} (TCP de consenso) com pares
 * pertencentes ao seu proprio shard.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 */
public class BeaconClusterLAN {

  public interface ListenerNovoParLAN {
    void onNovoParDescoberto(ConfigCluster.NoInfo novoPar);

    default void onParSaindo(int idPar) {
    }

    /** Fase 4 (Ghost Client): disparado quando um no recebe broadcast UDP interno USUARIO_CAIU. */
    default void onUsuarioCaiu(String idEvento, String nomeUsuario) {
    }

    /** Fase 8 (Rede Real LAN): disparado quando um shard externo anuncia seu lider/IP fisico via UDP. */
    default void onAnuncioShardLAN(int idShard, int idNo, String papel, long epoca, int idLider, String host) {
    }

    /** Disparado quando outro no do MESMO shard anuncia sua presenca ou lideranca via LAN. */
    default void onAnuncioMesmoShardLAN(int idShard, int idReplica, int idNo, String papel, long epoca, int idLider, String host) {
    }

    /** Disparado quando duas maquinas fisicas diferentes na LAN colidem no mesmo ID. */
    default void onColisaoIdLAN(int idNo, int idShard, int idReplica, String hostRemoto) {
    }
  }

  private final int meuId;
  private final int meuShard;
  private final int minhaReplica;
  private final int portaBeacon;
  private final ConfigCluster.NoInfo meuInfo;
  private final ConfigCluster config;
  private final ListenerNovoParLAN listener;
  private volatile boolean executando = true;
  private DatagramSocket socketEscuta;
  private DatagramSocket socketEscutaBase;
  private final Map<Integer, String> paresConhecidosNoShard = new java.util.concurrent.ConcurrentHashMap<>();

  /** Replicas por shard alcancaveis pelo farol (portas 9876..9905 => ate 30 nos no total). */
  private static final int MAX_REPLICAS_BEACON = 10;

  /** Porta base do farol intra-shard (S0=9876, S1=9886, S2=9896). Compartilhada com SO_REUSEADDR/SO_REUSEPORT. */
  public static int portaBeaconShard(int idShard) {
    return Protocolo.PORTA_CLUSTER_BEACON + (idShard * 10);
  }

  public static int portaBeaconDe(int idShard, int idReplica) {
    return portaBeaconShard(idShard) + idReplica + 1;
  }

  public BeaconClusterLAN(ConfigCluster config, ListenerNovoParLAN listener) {
    this.config = config;
    this.meuId = config.getMeuId();
    this.meuShard = config.getIdShard();
    this.minhaReplica = config.getIdReplica();
    this.portaBeacon = portaBeaconShard(this.meuShard);
    this.meuInfo = config.getMeuNoInfo();
    this.listener = listener;

    for (ConfigCluster.NoInfo n : config.getNos().values()) {
      if (n.id != this.meuId && n.idShard == this.meuShard) {
        this.paresConhecidosNoShard.put(n.id, n.host + ":" + n.portaCluster + ":" + n.portaTcp + ":" + n.portaUdp);
      }
    }
  }

  /**
   * Envia um datagrama UDP isolando individualmente qualquer falha de roteamento de rede
   * (como Network is unreachable em 255.255.255.255), impedindo efeito cascata nos demais destinos.
   */
  private static void enviarSeguro(DatagramSocket socket, byte[] dados, InetAddress destino, int porta) {
    if (socket == null || dados == null || destino == null || porta <= 0) {
      return;
    }
    try {
      socket.send(new DatagramPacket(dados, dados.length, destino, porta));
    } catch (Exception ignored) {
    }
  }

  /**
   * Coleta todos os enderecos IPv4 de broadcast de todas as interfaces ativas (+ loopback prioritario).
   */
  public static List<InetAddress> coletarBroadcasts() {
    java.util.LinkedHashSet<InetAddress> broadcasts = new java.util.LinkedHashSet<>();
    try {
      broadcasts.add(InetAddress.getByName("127.0.0.1"));
    } catch (Exception ignored) {
    }
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface netInterface = interfaces.nextElement();
        if (netInterface.isLoopback() || !netInterface.isUp()) continue;
        for (InterfaceAddress address : netInterface.getInterfaceAddresses()) {
          InetAddress broadcast = address.getBroadcast();
          if (broadcast != null) {
            broadcasts.add(broadcast);
          }
        }
      }
    } catch (Exception ignored) {
    }
    try {
      broadcasts.add(InetAddress.getByName("255.255.255.255"));
    } catch (Exception ignored) {
    }
    return new ArrayList<>(broadcasts);
  }

  private static boolean isInterfaceVirtual(String nomeInterface) {
    if (nomeInterface == null) return false;
    String n = nomeInterface.toLowerCase();
    return n.startsWith("docker") || n.startsWith("br-") || n.startsWith("veth")
        || n.startsWith("virbr") || n.startsWith("vboxnet") || n.startsWith("vmnet")
        || n.startsWith("tun") || n.startsWith("tap") || n.startsWith("wg") || n.startsWith("tailscale");
  }

  private static String resolverHostRecebido(String hostPayload, InetAddress enderecoPacote) {
    String ipPacote = (enderecoPacote != null) ? enderecoPacote.getHostAddress() : null;
    if (hostPayload == null || hostPayload.trim().isEmpty()
        || "0.0.0.0".equals(hostPayload.trim())
        || ("127.0.0.1".equals(hostPayload.trim()) && enderecoPacote != null && !enderecoPacote.isLoopbackAddress())) {
      return (ipPacote != null && !ipPacote.isEmpty()) ? ipPacote : "127.0.0.1";
    }
    return hostPayload.trim();
  }

  /**
   * Obtem o endereco IPv4 local prioritario da maquina na rede fisica (Wi-Fi / Ethernet),
   * ignorando pontes virtuais (Docker, libvirt, VirtualBox, VPN) sempre que houver interface fisica ativa.
   */
  public static String obterIpLocalLAN() {
    String melhorIp = null;
    int melhorPontuacao = -1;
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface netInterface = interfaces.nextElement();
        if (netInterface.isLoopback() || !netInterface.isUp() || netInterface.isVirtual()) continue;
        String nome = netInterface.getName() != null ? netInterface.getName().toLowerCase() : "";
        boolean virtual = isInterfaceVirtual(nome);

        for (InterfaceAddress ia : netInterface.getInterfaceAddresses()) {
          InetAddress addr = ia.getAddress();
          if (!(addr instanceof java.net.Inet4Address) || addr.isLoopbackAddress() || addr.isLinkLocalAddress()) {
            continue;
          }
          int pontos = virtual ? 1 : 10;
          if (ia.getBroadcast() != null) pontos += 5;
          if (addr.isSiteLocalAddress()) pontos += 4;
          if (nome.startsWith("wl") || nome.startsWith("eth") || nome.startsWith("en")) pontos += 6;

          if (pontos > melhorPontuacao) {
            melhorPontuacao = pontos;
            melhorIp = addr.getHostAddress();
          }
        }
      }
    } catch (Exception ignored) {
    }
    return (melhorIp != null) ? melhorIp : "127.0.0.1";
  }

  /**
   * Realiza uma sondagem UDP na rede filtrando estritamente pelos nos que pertencem ao mesmo idShard.
   */
  public static Map<Integer, ConfigCluster.NoInfo> sondarParesNaRede(
      int idShard, int idReplica, String meuIp, int portaCluster, int portaTcp, int portaUdp) {
    Map<Integer, ConfigCluster.NoInfo> paresEncontrados = new HashMap<>();

    try (DatagramSocket socket = new DatagramSocket(null)) {
      socket.setReuseAddress(true);
      socket.setBroadcast(true);
      socket.bind(new InetSocketAddress(0));
      socket.setSoTimeout(350);

      // Formato: EDEN_CLUSTER_PROBE:<idShard>:<idReplica>:<meuIp>:<portaCluster>:<portaTcp>:<portaUdp>
      String probeMsg = String.format("EDEN_CLUSTER_PROBE:%d:%d:%s:%d:%d:%d",
          idShard, idReplica, meuIp, portaCluster, portaTcp, portaUdp);
      byte[] dados = probeMsg.getBytes(StandardCharsets.UTF_8);

      List<InetAddress> bcasts = coletarBroadcasts();
      int portaBaseShard = portaBeaconShard(idShard);
      for (InetAddress bcast : bcasts) {
        enviarSeguro(socket, dados, bcast, portaBaseShard);
      }
      for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
        if (rep == idReplica) continue;
        int portaAlvo = portaBeaconDe(idShard, rep);
        for (InetAddress bcast : bcasts) {
          enviarSeguro(socket, dados, bcast, portaAlvo);
        }
      }

      long limite = System.currentTimeMillis() + 700;
      byte[] buffer = new byte[512];

      while (System.currentTimeMillis() < limite) {
        try {
          DatagramPacket resp = new DatagramPacket(buffer, buffer.length);
          socket.receive(resp);
          String texto = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8).trim();

          // Formato: EDEN_CLUSTER_PEER:<idShard>:<idReplica>:<idNo>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
          if (texto.startsWith("EDEN_CLUSTER_PEER:")) {
            String[] p = texto.split(":");
            if (p.length >= 8) {
              int shardPar = Integer.parseInt(p[1]);
              int replicaPar = Integer.parseInt(p[2]);
              int idNoPar = Integer.parseInt(p[3]);
              String host = resolverHostRecebido(p[4], resp.getAddress());
              int pCluster = Integer.parseInt(p[5]);
              int pTcp = Integer.parseInt(p[6]);
              int pUdp = Integer.parseInt(p[7]);

              if (shardPar != idShard) {
                // Isolamento: ignora respostas de outros shards
                continue;
              }
              if (replicaPar == idReplica) {
                continue;
              }

              paresEncontrados.put(idNoPar, new ConfigCluster.NoInfo(
                  idNoPar, shardPar, replicaPar, host, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY));
            }
          }
        } catch (java.net.SocketTimeoutException ste) {
          if (System.currentTimeMillis() >= limite) break;
        } catch (Exception ignored) {
          break;
        }
      }
    } catch (Exception e) {
      ClusterLog.aviso("BEACON", "Aviso durante sondagem do Shard S" + idShard + ": " + e.getMessage());
    }
    return paresEncontrados;
  }

  /**
   * Sondagem GLOBAL multi-dialeto usada na inicializacao Zero-Config para descobrir quais IDs
   * e Shards ja estao ocupados na rede (LAN fisica e localhost).
   * Dispara multiplas ondas de probes para portas intra-shard (9876, 9886, 9896) e Discovery geral (8888, 5001),
   * coletando tanto respostas diretas quanto anuncios ambientais que estejam circulando na rede.
   *
   * @return mapa ID global -> metadados de todos os nos vivos que responderam.
   */
  public static Map<Integer, ConfigCluster.NoInfo> sondarTodosOsNos(int janelaMs) {
    Map<Integer, ConfigCluster.NoInfo> encontrados = new HashMap<>();
    int tempoJanela = Math.max(1500, janelaMs);
    try (DatagramSocket socket = new DatagramSocket(null)) {
      socket.setReuseAddress(true);
      socket.setBroadcast(true);
      socket.bind(new InetSocketAddress(0));
      socket.setSoTimeout(120);
      byte[] dadosProbeEden = "EDEN_ID_PROBE".getBytes(StandardCharsets.UTF_8);
      byte[] dadosProbeCluster = Protocolo.DISCOVER_EDEN_CLUSTER.getBytes(StandardCharsets.UTF_8);

      List<InetAddress> bcasts = coletarBroadcasts();
      long limite = System.currentTimeMillis() + tempoJanela;
      long proximoEnvio = 0;
      int onda = 0;
      byte[] buffer = new byte[1024];

      while (System.currentTimeMillis() < limite) {
        long agora = System.currentTimeMillis();
        // Dispara ate 3 ondas de probes espacadas por 350ms para compensar eventuais perdas de pacotes no Wi-Fi
        if (agora >= proximoEnvio && onda < 3) {
          onda++;
          proximoEnvio = agora + 350;
          for (int shard = 0; shard < Particionador.NUM_SHARDS; shard++) {
            int portaBaseShard = portaBeaconShard(shard);
            for (InetAddress bcast : bcasts) {
              // 1. Farol intra-shard EDEN (portas 9876, 9886, 9896)
              enviarSeguro(socket, dadosProbeEden, bcast, portaBaseShard);
              // 2. Farois por replica (9877..9886, 9887..9896, 9897..9906) - cada envio isolado!
              for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
                int pAlvo = portaBeaconDe(shard, rep);
                enviarSeguro(socket, dadosProbeEden, bcast, pAlvo);
              }
              // 3. Portas Discovery gerais (8888, 8889, 8890, 5001)
              enviarSeguro(socket, dadosProbeCluster, bcast, Protocolo.PORTA_DISCOVERY + shard);
              enviarSeguro(socket, dadosProbeCluster, bcast, Protocolo.PORTA_DISCOVERY_EDEN);
            }
          }
        }

        try {
          DatagramPacket resp = new DatagramPacket(buffer, buffer.length);
          socket.receive(resp);
          String texto = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8).trim();

          if (texto.startsWith("EDEN_CLUSTER_PEER:")) {
            // EDEN_CLUSTER_PEER:<idShard>:<idReplica>:<idNo>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
            String[] p = texto.split(":");
            if (p.length >= 8) {
              int shardPar = Integer.parseInt(p[1]);
              int replicaPar = Integer.parseInt(p[2]);
              int idNoPar = Integer.parseInt(p[3]);
              String host = resolverHostRecebido(p[4], resp.getAddress());
              encontrados.put(idNoPar, new ConfigCluster.NoInfo(idNoPar, shardPar, replicaPar, host,
                  Integer.parseInt(p[5]), Integer.parseInt(p[6]), Integer.parseInt(p[7]), Protocolo.PORTA_DISCOVERY));
            }
          } else if (texto.startsWith("EDEN_CLUSTER_JOIN:")) {
            // EDEN_CLUSTER_JOIN:<idShard>:<idReplica>:<idNo>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
            String[] p = texto.split(":");
            if (p.length >= 8) {
              int shardPar = Integer.parseInt(p[1]);
              int replicaPar = Integer.parseInt(p[2]);
              int idNoPar = Integer.parseInt(p[3]);
              String host = resolverHostRecebido(p[4], resp.getAddress());
              encontrados.put(idNoPar, new ConfigCluster.NoInfo(idNoPar, shardPar, replicaPar, host,
                  Integer.parseInt(p[5]), Integer.parseInt(p[6]), Integer.parseInt(p[7]), Protocolo.PORTA_DISCOVERY));
            }
          } else if (texto.startsWith("EDEN_SHARD_LAN:")) {
            // EDEN_SHARD_LAN:<idShard>:<idReplica>:<idNo>:<papel>:<epoca>:<idLider>:<host>
            String[] p = texto.split(":");
            if (p.length >= 8) {
              int shardPar = Integer.parseInt(p[1]);
              int replicaPar = Integer.parseInt(p[2]);
              int idNoPar = Integer.parseInt(p[3]);
              String host = resolverHostRecebido(p[7], resp.getAddress());
              encontrados.put(idNoPar, new ConfigCluster.NoInfo(idNoPar, shardPar, replicaPar, host,
                  PortasShard.clusterDoNo(idNoPar), PortasShard.tcpDoNo(idNoPar), PortasShard.udpDoNo(idNoPar), Protocolo.PORTA_DISCOVERY));
            }
          } else if (texto.startsWith("EDEN_CLUSTER:")) {
            // EDEN_CLUSTER:<idNo>:<papel>:<portaTcp>:<portaUdp>:<idShard>
            String[] p = texto.split(":");
            if (p.length >= 6) {
              int idNoPar = Integer.parseInt(p[1]);
              int pTcp = Integer.parseInt(p[3]);
              int pUdp = Integer.parseInt(p[4]);
              int shardPar = Integer.parseInt(p[5]);
              int repPar = PortasShard.replicaDoNo(idNoPar);
              String host = resp.getAddress().getHostAddress();
              encontrados.put(idNoPar, new ConfigCluster.NoInfo(idNoPar, shardPar, repPar, host,
                  PortasShard.clusterDoNo(idNoPar), pTcp, pUdp, Protocolo.PORTA_DISCOVERY));
            }
          }
        } catch (java.net.SocketTimeoutException ste) {
          // Segue recebendo ate expirar o limite da janela
        } catch (Exception ignored) {
        }
      }
    } catch (Exception e) {
      ClusterLog.aviso("BEACON", "Aviso durante sondagem global de IDs: " + e.getMessage());
    }
    return encontrados;
  }

  /**
   * Inicia o servico de escuta em segundo plano para responder apenas a pares do MESMO shard.
   * Garante alinhamento estrito de portas: toda replica escuta na sua porta dedicada
   * {@code portaBeaconDe(meuShard, minhaReplica)} (ex: 9877 para R0, 9878 para R1) e a Replica 0
   * tambem escuta na porta base do shard (ex: 9876, 9886, 9896), eliminando assimetrias LAN/localhost.
   */
  public void iniciar() {
    final int portaDedicada = portaBeaconDe(meuShard, minhaReplica);

    Thread t = new Thread(() -> {
      try {
        socketEscuta = new DatagramSocket(null);
        socketEscuta.setReuseAddress(true);
        socketEscuta.setBroadcast(true);
        socketEscuta.bind(new InetSocketAddress(portaDedicada));
        ClusterLog.info("BEACON:SHARD", "Farol intra-shard S" + meuShard + " (Replica " + minhaReplica
            + ") escutando na porta UDP " + socketEscuta.getLocalPort());

        byte[] buffer = new byte[512];
        while (executando && !socketEscuta.isClosed()) {
          try {
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);
            socketEscuta.receive(pacote);
            processarDatagramaBeacon(socketEscuta, pacote);
          } catch (Exception e) {
            if (!executando) break;
          }
        }
      } catch (Exception e) {
        if (executando) {
          ClusterLog.erro("BEACON:SHARD", "Falha no socket de beacon do Shard S" + meuShard + ": " + e.getMessage());
        }
      } finally {
        parar();
      }
    }, "Thread-BeaconClusterLAN-S" + meuShard + "-R" + minhaReplica);
    t.setDaemon(true);
    t.start();

    // Alinhamento do Lider (Replica 0): escuta simultaneamente na porta base do shard (9876, 9886, 9896)
    if (minhaReplica == 0) {
      Thread tBase = new Thread(() -> {
        try {
          socketEscutaBase = new DatagramSocket(null);
          socketEscutaBase.setReuseAddress(true);
          socketEscutaBase.setBroadcast(true);
          socketEscutaBase.bind(new InetSocketAddress(portaBeacon));
          ClusterLog.info("BEACON:SHARD", "Farol base do lider S" + meuShard
              + " alinhado tambem na porta UDP " + socketEscutaBase.getLocalPort());

          byte[] bufferBase = new byte[512];
          while (executando && !socketEscutaBase.isClosed()) {
            try {
              DatagramPacket pacote = new DatagramPacket(bufferBase, bufferBase.length);
              socketEscutaBase.receive(pacote);
              processarDatagramaBeacon(socketEscutaBase, pacote);
            } catch (Exception e) {
              if (!executando) break;
            }
          }
        } catch (Exception ignored) {
          // Se outro processo na maquina ja detiver a porta base sem SO_REUSEPORT, mantem escuta na dedicada
        }
      }, "Thread-BeaconClusterLAN-Base-S" + meuShard);
      tBase.setDaemon(true);
      tBase.start();
    }

    anunciarPresenca();
  }

  private void processarDatagramaBeacon(DatagramSocket sockResposta, DatagramPacket pacote) {
    if (sockResposta == null || pacote == null || pacote.getData() == null) {
      return;
    }
    String texto = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8).trim();

    if ("EDEN_ID_PROBE".equals(texto)) {
      // Sondagem global Zero-Config: qualquer no (de qualquer shard) informa seu ID ocupado
      String hostAnunciado = "127.0.0.1".equals(meuInfo.host) && !pacote.getAddress().isLoopbackAddress()
          ? obterIpLocalLAN()
          : meuInfo.host;
      String resposta = String.format("EDEN_CLUSTER_PEER:%d:%d:%d:%s:%d:%d:%d",
          meuShard, minhaReplica, meuId, hostAnunciado, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
      byte[] dResp = resposta.getBytes(StandardCharsets.UTF_8);
      enviarSeguro(sockResposta, dResp, pacote.getAddress(), pacote.getPort());

    } else if (texto.startsWith("EDEN_CLUSTER_PROBE:")) {
      // EDEN_CLUSTER_PROBE:<idShard>:<idReplica>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
      String[] p = texto.split(":");
      if (p.length >= 7) {
        int shardOrigem = Integer.parseInt(p[1]);
        int replicaOrigem = Integer.parseInt(p[2]);
        if (shardOrigem != meuShard || replicaOrigem == minhaReplica) {
          return;
        }

        String hostAnunciado = "127.0.0.1".equals(meuInfo.host) && !pacote.getAddress().isLoopbackAddress()
            ? obterIpLocalLAN()
            : meuInfo.host;
        String resposta = String.format("EDEN_CLUSTER_PEER:%d:%d:%d:%s:%d:%d:%d",
            meuShard, minhaReplica, meuId, hostAnunciado, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
        byte[] dResp = resposta.getBytes(StandardCharsets.UTF_8);
        enviarSeguro(sockResposta, dResp, pacote.getAddress(), pacote.getPort());
      }

    } else if (texto.startsWith("EDEN_CLUSTER_JOIN:")) {
      // EDEN_CLUSTER_JOIN:<idShard>:<idReplica>:<idNo>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
      String[] p = texto.split(":");
      if (p.length >= 8) {
        int shardEntrou = Integer.parseInt(p[1]);
        int replicaEntrou = Integer.parseInt(p[2]);
        int idEntrou = Integer.parseInt(p[3]);
        String hostEntrou = resolverHostRecebido(p[4], pacote.getAddress());
        int pCluster = PortasShard.clusterDoNo(idEntrou);
        int pTcp = PortasShard.tcpDoNo(idEntrou);
        int pUdp = PortasShard.udpDoNo(idEntrou);

        if (shardEntrou != meuShard) {
          // Isolamento estrito: jamais conecta consenso TCP a no de outro shard
          return;
        }

        boolean mesmoHost = hostEntrou.equals(meuInfo.host)
            || ("127.0.0.1".equals(hostEntrou) && "127.0.0.1".equals(meuInfo.host));
        if (idEntrou == meuId && !mesmoHost) {
          // Conflito critico: duas maquinas fisicas diferentes assumiram o mesmo ID na rede!
          if (listener != null) {
            listener.onColisaoIdLAN(idEntrou, shardEntrou, replicaEntrou, hostEntrou);
          }
          return;
        }

        if (idEntrou != meuId) {
          String assinatura = hostEntrou + ":" + pCluster + ":" + pTcp + ":" + pUdp;
          boolean eNovo;
          synchronized (paresConhecidosNoShard) {
            String assinaturaAntiga = paresConhecidosNoShard.put(idEntrou, assinatura);
            eNovo = (assinaturaAntiga == null || !assinaturaAntiga.equals(assinatura));
          }

          if (eNovo) {
            // Responde com os proprios dados para que a descoberta seja BIDIRECIONAL apenas se for NOVO
            String hostAnunciado = "127.0.0.1".equals(meuInfo.host) && !pacote.getAddress().isLoopbackAddress()
                ? obterIpLocalLAN()
                : meuInfo.host;
            String respJoin = String.format("EDEN_CLUSTER_JOIN:%d:%d:%d:%s:%d:%d:%d",
                meuShard, minhaReplica, meuId, hostAnunciado, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
            byte[] dResp = respJoin.getBytes(StandardCharsets.UTF_8);
            enviarSeguro(sockResposta, dResp, pacote.getAddress(), pacote.getPort());

            ConfigCluster.NoInfo novoNo = new ConfigCluster.NoInfo(
                idEntrou, shardEntrou, replicaEntrou, hostEntrou, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY);
            config.registrarParDinamico(novoNo);
            if (listener != null) {
              listener.onNovoParDescoberto(novoNo);
            }
          }
        }
      }

    } else if (texto.startsWith("EDEN_SHARD_LAN:")) {
      // EDEN_SHARD_LAN:<idShard>:<idReplica>:<idNo>:<papel>:<epoca>:<idLider>:<host>
      String[] p = texto.split(":");
      if (p.length >= 8 && listener != null) {
        int shardRemoto = Integer.parseInt(p[1]);
        int replicaRemota = Integer.parseInt(p[2]);
        int idNoRemoto = Integer.parseInt(p[3]);
        String papelRemoto = p[4];
        long epocaRemota = Long.parseLong(p[5]);
        int idLiderRemoto = Integer.parseInt(p[6]);
        String hostRemoto = resolverHostRecebido(p[7], pacote.getAddress());

        if (shardRemoto != meuShard && shardRemoto >= 0 && shardRemoto < Particionador.NUM_SHARDS) {
          listener.onAnuncioShardLAN(shardRemoto, idNoRemoto, papelRemoto, epocaRemota, idLiderRemoto, hostRemoto);
        } else if (shardRemoto == meuShard) {
          listener.onAnuncioMesmoShardLAN(shardRemoto, replicaRemota, idNoRemoto, papelRemoto, epocaRemota, idLiderRemoto, hostRemoto);
        }
      }

    } else if (texto.startsWith("EDEN_CLUSTER_LEAVE:")) {
      // EDEN_CLUSTER_LEAVE:<idShard>:<idNo>
      String[] p = texto.split(":");
      if (p.length >= 3) {
        int shardSaiu = Integer.parseInt(p[1]);
        int idSaiu = Integer.parseInt(p[2]);
        if (shardSaiu == meuShard && idSaiu != meuId) {
          paresConhecidosNoShard.remove(idSaiu);
          if (listener != null) {
            listener.onParSaindo(idSaiu);
          }
        }
      }

    } else if (texto.startsWith("USUARIO_CAIU:")) {
      // USUARIO_CAIU:<idEvento>:<shardOrigem>:<replicaOrigem>:<nomeUsuario>
      String[] p = texto.split(":", 5);
      if (p.length >= 5 && listener != null) {
        String idEvento = p[1].trim();
        String nomeUsuario = p[4].trim();
        if (!nomeUsuario.isEmpty()) {
          listener.onUsuarioCaiu(idEvento, nomeUsuario);
        }
      }
    }
  }

  public void anunciarPresenca() {
    new Thread(() -> {
      while (executando) {
        try (DatagramSocket s = new DatagramSocket(null)) {
          s.setReuseAddress(true);
          s.setBroadcast(true);
          s.bind(new InetSocketAddress(0));
          String hostLan = "127.0.0.1".equals(meuInfo.host) ? obterIpLocalLAN() : meuInfo.host;
          byte[] dadosLan = String.format("EDEN_CLUSTER_JOIN:%d:%d:%d:%s:%d:%d:%d",
              meuShard, minhaReplica, meuId, hostLan, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp)
              .getBytes(StandardCharsets.UTF_8);
          byte[] dadosLocal = String.format("EDEN_CLUSTER_JOIN:%d:%d:%d:%s:%d:%d:%d",
              meuShard, minhaReplica, meuId, meuInfo.host, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp)
              .getBytes(StandardCharsets.UTF_8);
          List<InetAddress> bcasts = coletarBroadcasts();

          // 1. Envia para a porta base compartilhada do shard (9876, 9886, 9896)
          int portaBase = portaBeaconShard(meuShard);
          for (InetAddress b : bcasts) {
            byte[] payload = (b.isLoopbackAddress() && "127.0.0.1".equals(meuInfo.host)) ? dadosLocal : dadosLan;
            enviarSeguro(s, payload, b, portaBase);
          }

          // 2. Envia para as portas por replica (isolando cada envio UDP)
          for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
            if (rep == minhaReplica) continue;
            int portaAlvo = portaBeaconDe(meuShard, rep);
            for (InetAddress b : bcasts) {
              byte[] payload = (b.isLoopbackAddress() && "127.0.0.1".equals(meuInfo.host)) ? dadosLocal : dadosLan;
              enviarSeguro(s, payload, b, portaAlvo);
            }
          }
        } catch (Exception ignored) {
        }

        try {
          Thread.sleep(1500);
        } catch (InterruptedException e) {
          break;
        }
      }
    }, "Thread-AnunciarPresencaLAN-S" + meuShard).start();
  }

  public void anunciarSaida() {
    try (DatagramSocket s = new DatagramSocket(null)) {
      s.setReuseAddress(true);
      s.setBroadcast(true);
      s.bind(new InetSocketAddress(0));
      String msg = String.format("EDEN_CLUSTER_LEAVE:%d:%d", meuShard, meuId);
      byte[] dados = msg.getBytes(StandardCharsets.UTF_8);
      List<InetAddress> bcasts = coletarBroadcasts();
      int portaBase = portaBeaconShard(meuShard);
      for (InetAddress b : bcasts) {
        enviarSeguro(s, dados, b, portaBase);
      }
      for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
        if (rep == minhaReplica) continue;
        int portaAlvo = portaBeaconDe(meuShard, rep);
        for (InetAddress b : bcasts) {
          enviarSeguro(s, dados, b, portaAlvo);
        }
      }
    } catch (Exception ignored) {
    }
  }

  /**
   * Dispara um broadcast UDP interno USUARIO_CAIU para todos os 3 shards e suas replicas,
   * permitindo que todos os lideres removam imediatamente o cliente fantasma de usuarios e grupos cross-shard.
   */
  public static void dispararBroadcastUsuarioCaiu(String idEvento, String nomeUsuario, int shardOrigem, int replicaOrigem) {
    if (nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return;
    }
    final String msg = String.format("USUARIO_CAIU:%s:%d:%d:%s",
        idEvento, shardOrigem, replicaOrigem, nomeUsuario.trim());
    final byte[] dados = msg.getBytes(StandardCharsets.UTF_8);

    ClusterLog.aviso("GHOST-CLIENT", String.format(
        "Queda brusca TCP de '%s' detectada! Disparando broadcast UDP interno USUARIO_CAIU [%s] para S0, S1 e S2.",
        nomeUsuario.trim(), idEvento));

    new Thread(() -> {
      try (DatagramSocket s = new DatagramSocket(null)) {
        s.setReuseAddress(true);
        s.setBroadcast(true);
        s.bind(new InetSocketAddress(0));
        List<InetAddress> bcasts = coletarBroadcasts();
        for (int rajada = 0; rajada < 2; rajada++) {
          for (int shard = 0; shard < Particionador.NUM_SHARDS; shard++) {
            int pBase = portaBeaconShard(shard);
            for (InetAddress dest : bcasts) {
              enviarSeguro(s, dados, dest, pBase);
            }
            for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
              int portaAlvo = portaBeaconDe(shard, rep);
              for (InetAddress dest : bcasts) {
                enviarSeguro(s, dados, dest, portaAlvo);
              }
            }
          }
          if (rajada == 0) {
            try {
              Thread.sleep(80);
            } catch (InterruptedException ignored) {
            }
          }
        }
      } catch (Exception e) {
        ClusterLog.aviso("GHOST-CLIENT", "Falha ao enviar broadcast UDP USUARIO_CAIU: " + e.getMessage());
      }
    }, "Thread-Broadcast-UsuarioCaiu").start();
  }

  /**
   * Anuncia via broadcast UDP o estado e o IP fisico deste no para os DEMAIS shards na LAN,
   * permitindo que particoes rodando em maquinas fisicas diferentes descubram automaticamente seus IPs
   * sem misturar os canais de consenso intra-shard.
   */
  public static void dispararAnuncioShardLAN(
      int meuShard, int minhaReplica, int meuId, String papel, long epoca, int idLider, String meuHost, int totalReplicas) {
    final String hostOriginal = (meuHost == null || meuHost.isEmpty()) ? "127.0.0.1" : meuHost;
    final String hostAnuncio = "127.0.0.1".equals(hostOriginal) ? obterIpLocalLAN() : hostOriginal;
    final byte[] dadosLan = String.format("EDEN_SHARD_LAN:%d:%d:%d:%s:%d:%d:%s",
        meuShard, minhaReplica, meuId, papel, epoca, idLider, hostAnuncio).getBytes(StandardCharsets.UTF_8);
    final byte[] dadosLocal = String.format("EDEN_SHARD_LAN:%d:%d:%d:%s:%d:%d:%s",
        meuShard, minhaReplica, meuId, papel, epoca, idLider, hostOriginal).getBytes(StandardCharsets.UTF_8);
    final int maxRep = Math.min(MAX_REPLICAS_BEACON, Math.max(2, totalReplicas));

    try (DatagramSocket s = new DatagramSocket(null)) {
      s.setReuseAddress(true);
      s.setBroadcast(true);
      s.bind(new InetSocketAddress(0));
      List<InetAddress> bcasts = coletarBroadcasts();
      for (int shard = 0; shard < Particionador.NUM_SHARDS; shard++) {
        int pBase = portaBeaconShard(shard);
        for (InetAddress dest : bcasts) {
          byte[] payload = (dest.isLoopbackAddress() && "127.0.0.1".equals(hostOriginal)) ? dadosLocal : dadosLan;
          enviarSeguro(s, payload, dest, pBase);
        }
        for (int rep = 0; rep < maxRep; rep++) {
          if (shard == meuShard && rep == minhaReplica) {
            continue;
          }
          int portaAlvo = portaBeaconDe(shard, rep);
          for (InetAddress dest : bcasts) {
            byte[] payload = (dest.isLoopbackAddress() && "127.0.0.1".equals(hostOriginal)) ? dadosLocal : dadosLan;
            enviarSeguro(s, payload, dest, portaAlvo);
          }
        }
      }
    } catch (Exception ignored) {
    }
  }

  public void parar() {
    if (this.executando) {
      anunciarSaida();
    }
    this.executando = false;
    if (socketEscuta != null && !socketEscuta.isClosed()) {
      try {
        socketEscuta.close();
      } catch (Exception ignored) {
      }
    }
    if (socketEscutaBase != null && !socketEscutaBase.isClosed()) {
      try {
        socketEscutaBase.close();
      } catch (Exception ignored) {
      }
    }
  }
}
