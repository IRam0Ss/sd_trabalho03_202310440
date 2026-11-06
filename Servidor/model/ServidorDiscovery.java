/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ServidorDiscovery
* Funcao.............: Farol UDP para descoberta automatica do servidor local e nos do cluster.
*************************************************************** */

package model;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import utils.Protocolo;

/**
 * Thread de discovery local (farol UDP) com suporte a busca de cluster e compatibilidade legada.
 * Responde a "SERVIDOR_IP" -> "IP", "DISCOVER_EDEN" -> "EDEN_HERE" e
 * "DISCOVER_EDEN_CLUSTER" -> "EDEN_CLUSTER;id;papel;tcp;udp".
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class ServidorDiscovery implements Runnable {

  private final int porta;
  private DatagramSocket socket;
  private volatile boolean executando = true;
  private int idNo = 1;
  private int portaTcp = Protocolo.PORTA_SERVIDOR_TCP;
  private int portaUdp = Protocolo.PORTA_SERVIDOR_UDP;
  private String papel = "ATIVO";

  public ServidorDiscovery() {
    this(Protocolo.PORTA_DISCOVERY);
  }

  public ServidorDiscovery(int porta) {
    this.porta = porta;
  }

  public void configurarInfo(int idNo, String papel, int portaTcp, int portaUdp) {
    this.idNo = idNo;
    this.papel = papel;
    this.portaTcp = portaTcp;
    this.portaUdp = portaUdp;
  }

  public void parar() {
    this.executando = false;
    if (this.socket != null && !this.socket.isClosed()) {
      try {
        this.socket.close();
      } catch (Exception ignored) {
      }
    }
    System.out.println("[DISCOVERY:" + porta + "] Farol UDP encerrado.");
  }

  @Override
  public void run() {
    try {
      socket = new DatagramSocket(null);
      socket.setReuseAddress(true);
      socket.setBroadcast(true);
      socket.bind(new InetSocketAddress(porta));

      System.out.println("[DISCOVERY:" + porta + "] Farol UDP ligado na porta " + porta + " aguardando buscas...");
      byte[] buffer = new byte[256];

      while (executando && !socket.isClosed()) {
        DatagramPacket pacoteRecebido = new DatagramPacket(buffer, buffer.length);
        try {
          socket.receive(pacoteRecebido);
        } catch (Exception e) {
          if (!executando) break;
          continue;
        }

        String mensagem = new String(pacoteRecebido.getData(), 0, pacoteRecebido.getLength(),
            StandardCharsets.UTF_8).trim();

        if ("SERVIDOR_IP".equalsIgnoreCase(mensagem)) {
          byte[] dadosResposta = "IP".getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
        } else if ("DISCOVER_EDEN".equalsIgnoreCase(mensagem)) {
          byte[] dadosResposta = "EDEN_HERE".getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
        } else if (Protocolo.DISCOVER_EDEN_CLUSTER.equalsIgnoreCase(mensagem)) {
          String respostaCluster = String.format("EDEN_CLUSTER:%d:%s:%d:%d", idNo, papel, portaTcp, portaUdp);
          byte[] dadosResposta = respostaCluster.getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
        }
      }
    } catch (Exception e) {
      if (executando) {
        System.err.println("[DISCOVERY:" + porta + "] [ERROR] Falha no farol: " + e.getMessage());
      }
    } finally {
      if (socket != null && !socket.isClosed()) {
        socket.close();
      }
    }
  }
}
