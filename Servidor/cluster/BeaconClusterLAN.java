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

  private static final int MAX_REPLICAS_BEACON = 6;

  public static int portaBeaconDe(int idShard, int idReplica) {
    return Protocolo.PORTA_CLUSTER_BEACON + (idShard * 10) + idReplica;
  }

  public BeaconClusterLAN(ConfigCluster config, ListenerNovoParLAN listener) {
    this.config = config;
    this.meuId = config.getMeuId();
    this.meuShard = config.getIdShard();
    this.minhaReplica = config.getIdReplica();
    this.portaBeacon = portaBeaconDe(this.meuShard, this.minhaReplica);
    this.meuInfo = config.getMeuNoInfo();
    this.listener = listener;
  }

  /**
   * Coleta todos os enderecos IPv4 de broadcast de todas as interfaces ativas (+ loopback).
   */
  public static List<InetAddress> coletarBroadcasts() {
    List<InetAddress> broadcasts = new ArrayList<>();
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
      broadcasts.add(InetAddress.getByName("127.0.0.1"));
    } catch (Exception ignored) {
    }
    return broadcasts;
  }

  /**
   * Obtem o endereco IPv4 local prioritario da maquina na rede.
   */
  public static String obterIpLocalLAN() {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface netInterface = interfaces.nextElement();
        if (netInterface.isLoopback() || !netInterface.isUp()) continue;
        for (InterfaceAddress address : netInterface.getInterfaceAddresses()) {
          InetAddress addr = address.getAddress();
          if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()) {
            return addr.getHostAddress();
          }
        }
      }
    } catch (Exception ignored) {
    }
    return "127.0.0.1";
  }

  /**
   * Realiza uma sondagem UDP na rede filtrando estritamente pelos nos que pertencem ao mesmo idShard.
   */
  public static Map<Integer, ConfigCluster.NoInfo> sondarParesNaRede(
      int idShard, int idReplica, String meuIp, int portaCluster, int portaTcp, int portaUdp) {
    Map<Integer, ConfigCluster.NoInfo> paresEncontrados = new HashMap<>();

    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setBroadcast(true);
      socket.setSoTimeout(350);

      // Formato: EDEN_CLUSTER_PROBE:<idShard>:<idReplica>:<meuIp>:<portaCluster>:<portaTcp>:<portaUdp>
      String probeMsg = String.format("EDEN_CLUSTER_PROBE:%d:%d:%s:%d:%d:%d",
          idShard, idReplica, meuIp, portaCluster, portaTcp, portaUdp);
      byte[] dados = probeMsg.getBytes(StandardCharsets.UTF_8);

      List<InetAddress> bcasts = coletarBroadcasts();
      for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
        if (rep == idReplica) continue;
        int portaAlvo = portaBeaconDe(idShard, rep);
        for (InetAddress bcast : bcasts) {
          try {
            socket.send(new DatagramPacket(dados, dados.length, bcast, portaAlvo));
          } catch (Exception ignored) {
          }
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
              String host = p[4];
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
   * Inicia o servico de escuta em segundo plano para responder apenas a pares do MESMO shard.
   */
  public void iniciar() {
    Thread t = new Thread(() -> {
      try {
        socketEscuta = new DatagramSocket(null);
        socketEscuta.setReuseAddress(true);
        socketEscuta.setBroadcast(true);
        socketEscuta.bind(new InetSocketAddress(portaBeacon));
        ClusterLog.info("BEACON:SHARD", "Farol intra-shard S" + meuShard + " escutando na porta UDP " + portaBeacon);

        byte[] buffer = new byte[512];
        while (executando && !socketEscuta.isClosed()) {
          try {
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);
            socketEscuta.receive(pacote);
            String texto = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8).trim();

            if (texto.startsWith("EDEN_CLUSTER_PROBE:")) {
              // EDEN_CLUSTER_PROBE:<idShard>:<idReplica>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
              String[] p = texto.split(":");
              if (p.length >= 7) {
                int shardOrigem = Integer.parseInt(p[1]);
                int replicaOrigem = Integer.parseInt(p[2]);
                if (shardOrigem != meuShard || replicaOrigem == minhaReplica) {
                  continue;
                }

                String resposta = String.format("EDEN_CLUSTER_PEER:%d:%d:%d:%s:%d:%d:%d",
                    meuShard, minhaReplica, meuId, meuInfo.host, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
                byte[] dResp = resposta.getBytes(StandardCharsets.UTF_8);
                socketEscuta.send(new DatagramPacket(dResp, dResp.length, pacote.getAddress(), pacote.getPort()));
              }

            } else if (texto.startsWith("EDEN_CLUSTER_JOIN:")) {
              // EDEN_CLUSTER_JOIN:<idShard>:<idReplica>:<idNo>:<host>:<portaCluster>:<portaTcp>:<portaUdp>
              String[] p = texto.split(":");
              if (p.length >= 8) {
                int shardEntrou = Integer.parseInt(p[1]);
                int replicaEntrou = Integer.parseInt(p[2]);
                int idEntrou = Integer.parseInt(p[3]);
                String hostEntrou = p[4];
                int pCluster = Integer.parseInt(p[5]);
                int pTcp = Integer.parseInt(p[6]);
                int pUdp = Integer.parseInt(p[7]);

                if (shardEntrou != meuShard) {
                  // Isolamento estrito: jamais conecta consenso TCP a no de outro shard
                  continue;
                }
                if (idEntrou != meuId) {
                  ConfigCluster.NoInfo novoNo = new ConfigCluster.NoInfo(
                      idEntrou, shardEntrou, replicaEntrou, hostEntrou, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY);
                  config.registrarParDinamico(novoNo);
                  if (listener != null) {
                    listener.onNovoParDescoberto(novoNo);
                  }
                }
              }

            } else if (texto.startsWith("EDEN_CLUSTER_LEAVE:")) {
              // EDEN_CLUSTER_LEAVE:<idShard>:<idNo>
              String[] p = texto.split(":");
              if (p.length >= 3) {
                int shardSaiu = Integer.parseInt(p[1]);
                int idSaiu = Integer.parseInt(p[2]);
                if (shardSaiu == meuShard && idSaiu != meuId && listener != null) {
                  listener.onParSaindo(idSaiu);
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
    }, "Thread-BeaconClusterLAN-S" + meuShard);
    t.setDaemon(true);
    t.start();

    anunciarPresenca();
  }

  public void anunciarPresenca() {
    new Thread(() -> {
      try (DatagramSocket s = new DatagramSocket()) {
        s.setBroadcast(true);
        String msg = String.format("EDEN_CLUSTER_JOIN:%d:%d:%d:%s:%d:%d:%d",
            meuShard, minhaReplica, meuId, meuInfo.host, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
        byte[] dados = msg.getBytes(StandardCharsets.UTF_8);
        List<InetAddress> bcasts = coletarBroadcasts();
        for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
          if (rep == minhaReplica) continue;
          int portaAlvo = portaBeaconDe(meuShard, rep);
          for (InetAddress b : bcasts) {
            try {
              s.send(new DatagramPacket(dados, dados.length, b, portaAlvo));
            } catch (Exception ignored) {
            }
          }
        }
      } catch (Exception ignored) {
      }
    }, "Thread-AnunciarPresencaLAN-S" + meuShard).start();
  }

  public void anunciarSaida() {
    try (DatagramSocket s = new DatagramSocket()) {
      s.setBroadcast(true);
      String msg = String.format("EDEN_CLUSTER_LEAVE:%d:%d", meuShard, meuId);
      byte[] dados = msg.getBytes(StandardCharsets.UTF_8);
      List<InetAddress> bcasts = coletarBroadcasts();
      for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
        if (rep == minhaReplica) continue;
        int portaAlvo = portaBeaconDe(meuShard, rep);
        for (InetAddress b : bcasts) {
          try {
            s.send(new DatagramPacket(dados, dados.length, b, portaAlvo));
          } catch (Exception ignored) {
          }
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
      try (DatagramSocket s = new DatagramSocket()) {
        s.setBroadcast(true);
        List<InetAddress> bcasts = coletarBroadcasts();
        for (int rajada = 0; rajada < 2; rajada++) {
          for (int shard = 0; shard < Particionador.NUM_SHARDS; shard++) {
            for (int rep = 0; rep < MAX_REPLICAS_BEACON; rep++) {
              int portaAlvo = portaBeaconDe(shard, rep);
              for (InetAddress dest : bcasts) {
                try {
                  s.send(new DatagramPacket(dados, dados.length, dest, portaAlvo));
                } catch (Exception ignored) {
                }
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
  }
}
