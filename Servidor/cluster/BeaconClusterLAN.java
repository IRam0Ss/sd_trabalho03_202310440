/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: BeaconClusterLAN
* Funcao.............: Farol UDP em broadcast para auto-descoberta dinamica de servidores e atribuicao de ID na rede local.
*************************************************************** */

package cluster;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utils.Protocolo;

/**
 * Permite que servidores em computadores distintos na mesma rede local se descubram
 * automaticamente via UDP broadcast sem necessidade de arquivos de configuracao ou
 * intervencao manual do usuario.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class BeaconClusterLAN {

  public interface ListenerNovoParLAN {
    void onNovoParDescoberto(ConfigCluster.NoInfo novoPar);
    default void onParSaindo(int idPar) {}
  }

  private final int meuId;
  private final ConfigCluster.NoInfo meuInfo;
  private final ConfigCluster config;
  private final ListenerNovoParLAN listener;
  private volatile boolean executando = true;
  private DatagramSocket socketEscuta;

  public BeaconClusterLAN(ConfigCluster config, ListenerNovoParLAN listener) {
    this.config = config;
    this.meuId = config.getMeuId();
    this.meuInfo = config.getMeuNoInfo();
    this.listener = listener;
  }

  /**
   * Coleta todos os enderecos IPv4 de broadcast de todas as interfaces ativas.
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
   * Realiza uma sondagem na rede e retorna todos os nos que responderem dentro de 800ms.
   */
  public static Map<Integer, ConfigCluster.NoInfo> sondarParesNaRede(String meuIp, int portaCluster, int portaTcp, int portaUdp) {
    Map<Integer, ConfigCluster.NoInfo> paresEncontrados = new HashMap<>();
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setBroadcast(true);
      socket.setSoTimeout(400);

      String probeMsg = String.format("EDEN_CLUSTER_PROBE:%s:%d:%d:%d", meuIp, portaCluster, portaTcp, portaUdp);
      byte[] dados = probeMsg.getBytes(StandardCharsets.UTF_8);

      List<InetAddress> bcasts = coletarBroadcasts();
      for (InetAddress bcast : bcasts) {
        try {
          socket.send(new DatagramPacket(dados, dados.length, bcast, Protocolo.PORTA_CLUSTER_BEACON));
        } catch (Exception ignored) {}
      }

      long limite = System.currentTimeMillis() + 800;
      byte[] buffer = new byte[512];

      while (System.currentTimeMillis() < limite) {
        try {
          DatagramPacket resp = new DatagramPacket(buffer, buffer.length);
          socket.receive(resp);
          String texto = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8).trim();

          if (texto.startsWith("EDEN_CLUSTER_PEER:")) {
            String[] p = texto.split(":");
            if (p.length >= 6) {
              int id = Integer.parseInt(p[1]);
              String host = p[2];
              int pCluster = Integer.parseInt(p[3]);
              int pTcp = Integer.parseInt(p[4]);
              int pUdp = Integer.parseInt(p[5]);
              paresEncontrados.put(id, new ConfigCluster.NoInfo(id, host, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY));
            }
          }
        } catch (java.net.SocketTimeoutException ste) {
          if (System.currentTimeMillis() >= limite) break;
        } catch (Exception ignored) {
          break;
        }
      }
    } catch (Exception e) {
      ClusterLog.aviso("BEACON", "Aviso durante sondagem inicial: " + e.getMessage());
    }
    return paresEncontrados;
  }

  /**
   * Inicia o servico de escuta em segundo plano para responder a novos nos que chegarem.
   */
  public void iniciar() {
    Thread t = new Thread(() -> {
      try {
        socketEscuta = new DatagramSocket(null);
        socketEscuta.setReuseAddress(true);
        socketEscuta.setBroadcast(true);
        socketEscuta.bind(new java.net.InetSocketAddress(Protocolo.PORTA_CLUSTER_BEACON));
        ClusterLog.info("BEACON:LAN", "Farol de cluster escutando na porta UDP " + Protocolo.PORTA_CLUSTER_BEACON);

        byte[] buffer = new byte[512];
        while (executando && !socketEscuta.isClosed()) {
          try {
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);
            socketEscuta.receive(pacote);
            String texto = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8).trim();

            if (texto.startsWith("EDEN_CLUSTER_PROBE:")) {
              String[] p = texto.split(":");
              if (p.length >= 5) {
                String hostOrigem = p[1];
                int pCluster = Integer.parseInt(p[2]);
                int pTcp = Integer.parseInt(p[3]);
                int pUdp = Integer.parseInt(p[4]);

                // Responde com meus dados para o solicitante
                String resposta = String.format("EDEN_CLUSTER_PEER:%d:%s:%d:%d:%d:OK:0",
                    meuId, meuInfo.host, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
                byte[] dResp = resposta.getBytes(StandardCharsets.UTF_8);
                socketEscuta.send(new DatagramPacket(dResp, dResp.length, pacote.getAddress(), pacote.getPort()));
              }

            } else if (texto.startsWith("EDEN_CLUSTER_JOIN:")) {
              String[] p = texto.split(":");
              if (p.length >= 6) {
                int idEntrou = Integer.parseInt(p[1]);
                String hostEntrou = p[2];
                int pCluster = Integer.parseInt(p[3]);
                int pTcp = Integer.parseInt(p[4]);
                int pUdp = Integer.parseInt(p[5]);

                if (idEntrou != meuId) {
                  ConfigCluster.NoInfo novoNo = new ConfigCluster.NoInfo(
                      idEntrou, hostEntrou, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY
                  );
                  config.registrarParDinamico(novoNo);
                  if (listener != null) {
                    listener.onNovoParDescoberto(novoNo);
                  }
                }
              }
            } else if (texto.startsWith("EDEN_CLUSTER_LEAVE:")) {
              String[] p = texto.split(":");
              if (p.length >= 2) {
                int idSaiu = Integer.parseInt(p[1]);
                if (idSaiu != meuId && listener != null) {
                  listener.onParSaindo(idSaiu);
                }
              }
            }

          } catch (Exception e) {
            if (!executando) break;
          }
        }
      } catch (Exception e) {
        if (executando) {
          ClusterLog.erro("BEACON:LAN", "Falha no socket de beacon LAN: " + e.getMessage());
        }
      } finally {
        parar();
      }
    }, "Thread-BeaconClusterLAN");
    t.setDaemon(true);
    t.start();

    // Envia aviso de JOIN para todos os outros nos na rede
    anunciarPresenca();
  }

  public void anunciarPresenca() {
    new Thread(() -> {
      try (DatagramSocket s = new DatagramSocket()) {
        s.setBroadcast(true);
        String msg = String.format("EDEN_CLUSTER_JOIN:%d:%s:%d:%d:%d",
            meuId, meuInfo.host, meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
        byte[] dados = msg.getBytes(StandardCharsets.UTF_8);
        List<InetAddress> bcasts = coletarBroadcasts();
        for (InetAddress b : bcasts) {
          try {
            s.send(new DatagramPacket(dados, dados.length, b, Protocolo.PORTA_CLUSTER_BEACON));
          } catch (Exception ignored) {}
        }
      } catch (Exception ignored) {}
    }, "Thread-AnunciarPresencaLAN").start();
  }

  public void anunciarSaida() {
    try (DatagramSocket s = new DatagramSocket()) {
      s.setBroadcast(true);
      String msg = String.format("EDEN_CLUSTER_LEAVE:%d", meuId);
      byte[] dados = msg.getBytes(StandardCharsets.UTF_8);
      List<InetAddress> bcasts = coletarBroadcasts();
      for (InetAddress b : bcasts) {
        try {
          s.send(new DatagramPacket(dados, dados.length, b, Protocolo.PORTA_CLUSTER_BEACON));
        } catch (Exception ignored) {}
      }
    } catch (Exception ignored) {}
  }

  public void parar() {
    if (this.executando) {
      anunciarSaida();
    }
    this.executando = false;
    if (socketEscuta != null && !socketEscuta.isClosed()) {
      try {
        socketEscuta.close();
      } catch (Exception ignored) {}
    }
  }
}

