/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: NotificadorClientes
* Funcao.............: Despacha notificacoes assincronas via UDP para os clientes ativos do E.D.E.N.
*************************************************************** */

package model;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Responsavel por enviar notificacoes broadcast/multicast via UDP para a lista
 * de usuarios ativos (como atualizacao da lista de online e comunicacao de troca de lider).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class NotificadorClientes {

  private static byte[] serializarAPDU(APDU apdu) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeUnshared(apdu);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      System.err.println("[NOTIFICADOR] [ERROR] Falha ao serializar APDU: " + e.getMessage());
      return new byte[0];
    }
  }

  /**
   * Dispara notificacao UDP UPDATE_USERS para todos os usuarios online fornecidos.
   * Executa em thread separada para nao bloquear o processador de comandos.
   * 
   * @param usuarios Colecao de usuarios ativos atuais.
   */
  public static void notificarAtualizacaoUsuarios(Collection<InfoUser> usuarios) {
    if (usuarios == null || usuarios.isEmpty()) {
      return;
    }
    final List<InfoUser> lista = new ArrayList<>(usuarios);

    new Thread(() -> {
      try (DatagramSocket socketUDP = new DatagramSocket()) {
        APDU apdu = new APDU(Protocolo.UPDATE_USERS, null, null, null, 0);
        byte[] dados = serializarAPDU(apdu);

        for (InfoUser u : lista) {
          try {
            InetAddress ipDest = InetAddress.getByName(u.getIp());
            DatagramPacket pacote = new DatagramPacket(dados, dados.length, ipDest, u.getPorta());
            socketUDP.send(pacote);
          } catch (Exception ignored) {
          }
        }
        System.out.println("[NOTIFICADOR] [INFO] Notificacao UPDATE_USERS disparada para " + lista.size() + " clientes.");
      } catch (Exception e) {
        System.err.println("[NOTIFICADOR] [ERROR] Falha ao disparar UPDATE_USERS: " + e.getMessage());
      }
    }, "Thread-Notificador-UpdateUsers").start();
  }

  /**
   * Envia notificacao SERVER_CHANGED para todos os clientes avisando a posse de um novo lider.
   * Dispara 3 rajadas consecutivas para compensar eventuais perdas de pacotes UDP.
   * 
   * @param usuarios Colecao de usuarios cadastrados na estrutura.
   * @param novoIp   IP de servico do novo lider.
   * @param portaTcp Porta TCP do novo lider.
   * @param portaUdp Porta UDP do novo lider.
   * @param epoca    Epoca de lideranca.
   * @param idNo     ID do no que assumiu.
   */
  public static void notificarServerChanged(Collection<InfoUser> usuarios, String novoIp, int portaTcp, int portaUdp, long epoca, int idNo) {
    if (usuarios == null || usuarios.isEmpty()) {
      return;
    }
    final List<InfoUser> lista = new ArrayList<>(usuarios);

    new Thread(() -> {
      try (DatagramSocket socketUDP = new DatagramSocket()) {
        String payload = String.format("%s;%d;%d;%d;%d", novoIp, portaTcp, portaUdp, epoca, idNo);
        APDU apdu = new APDU(Protocolo.SERVER_CHANGED, "GLOBAL", "SERVIDOR", payload, 0);
        byte[] dados = serializarAPDU(apdu);

        // Envia 3 rajadas com intervalo de 200ms para garantir recepcao
        for (int tentativa = 1; tentativa <= 3; tentativa++) {
          for (InfoUser u : lista) {
            try {
              InetAddress ipDest = InetAddress.getByName(u.getIp());
              DatagramPacket pacote = new DatagramPacket(dados, dados.length, ipDest, u.getPorta());
              socketUDP.send(pacote);
            } catch (Exception ignored) {
            }
          }
          if (tentativa < 3) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
          }
        }
        System.out.println("[NOTIFICADOR] [INFO] Notificacao SERVER_CHANGED disparada para " + lista.size() + " clientes.");
      } catch (Exception e) {
        System.err.println("[NOTIFICADOR] [ERROR] Falha ao disparar SERVER_CHANGED: " + e.getMessage());
      }
    }, "Thread-Notificador-ServerChanged").start();
  }
}

