/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ClienteUDP
* Funcao.............: Gerencia o envio e recepcao assincrona de datagramas UDP com alvo mutavel para failover.
*************************************************************** */

package model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Gerenciador UDP do cliente para mensagens em tempo real e ticks de confirmacao.
 * Permite a atualizacao dinamica do IP/porta do servidor alvo em caso de troca de lider
 * sem reiniciar o socket UDP local (preservando o endpoint do cliente).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class ClienteUDP implements Runnable {

  private volatile int portaServidor;
  private volatile InetAddress ipServidor;
  private DatagramSocket socketUDP;
  private static final int BUFFER = 4096;
  private MessageListener listener;
  private String meuNome;

  /**
   * Construtor que inicializa o socket UDP em uma porta efemera disponivel.
   */
  public ClienteUDP(String ipServidor, int portaServidor)
      throws UnknownHostException, SocketException {
    this.ipServidor = InetAddress.getByName(ipServidor);
    this.portaServidor = portaServidor;
    this.socketUDP = new DatagramSocket();
    System.out.println("[CLIENTE:UDP] [INFO] Socket vinculado a porta efemera " + socketUDP.getLocalPort());
  }

  public int getPortaLocal() {
    return socketUDP.getLocalPort();
  }

  public void setListener(MessageListener listener) {
    this.listener = listener;
  }

  public void setMeuNome(String meuNome) {
    this.meuNome = meuNome;
  }

  /**
   * Atualiza dinamicamente o destino dos datagramas UDP (usado no failover).
   */
  public void setDestino(String novoIp, int novaPorta) throws UnknownHostException {
    this.ipServidor = InetAddress.getByName(novoIp);
    this.portaServidor = novaPorta;
    System.out.println("[CLIENTE:UDP] [INFO] Alvo UDP atualizado para " + novoIp + ":" + novaPorta);
  }

  private byte[] serializarAPDU(APDU apdu) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(apdu);
    oos.flush();
    return baos.toByteArray();
  }

  public String send(String nomeGrupo, InfoUser usuario, String mensagem) throws exceptions.ConexaoException {
    APDU apdu = new APDU("SEND", nomeGrupo, usuario.getNome(), mensagem, usuario.getPorta());
    try {
      byte[] dadosEnviados = serializarAPDU(apdu);
      DatagramPacket pacoteEnvio = new DatagramPacket(dadosEnviados, dadosEnviados.length, ipServidor, portaServidor);
      socketUDP.send(pacoteEnvio);
      System.out.println("[CLIENTE:UDP] [INFO] Mensagem enviada ao servidor. ID: " + apdu.getIdMensagem());
      return apdu.getIdMensagem();
    } catch (IOException e) {
      System.out.println("[CLIENTE:UDP] [ERROR] Falha ao enviar mensagem para o servidor.");
      throw new exceptions.ConexaoException("Falha ao enviar mensagem via UDP", e);
    }
  }

  public String sendPvt(String nomeDestinatario, InfoUser usuario, String mensagem) throws exceptions.ConexaoException {
    APDU apdu = new APDU("SENDPVT", "@" + nomeDestinatario, usuario.getNome(), mensagem, usuario.getPorta(), nomeDestinatario);
    try {
      byte[] dadosEnviados = serializarAPDU(apdu);
      DatagramPacket pacoteEnvio = new DatagramPacket(dadosEnviados, dadosEnviados.length, ipServidor, portaServidor);
      socketUDP.send(pacoteEnvio);
      System.out.println("[CLIENTE:UDP] [INFO] Mensagem privada enviada ao servidor. ID: " + apdu.getIdMensagem());
      return apdu.getIdMensagem();
    } catch (IOException e) {
      System.out.println("[CLIENTE:UDP] [ERROR] Falha ao enviar mensagem privada.");
      throw new exceptions.ConexaoException("Falha ao enviar mensagem privada via UDP", e);
    }
  }

  public String sendVu(String nomeGrupo, InfoUser usuario, String mensagem) throws exceptions.ConexaoException {
    APDU apdu = new APDU("SENDVU", nomeGrupo, usuario.getNome(), mensagem, usuario.getPorta(), true);
    try {
      byte[] dadosEnviados = serializarAPDU(apdu);
      DatagramPacket pacoteEnvio = new DatagramPacket(dadosEnviados, dadosEnviados.length, ipServidor, portaServidor);
      socketUDP.send(pacoteEnvio);
      System.out.println("[CLIENTE:UDP] [INFO] Mensagem VU enviada. ID: " + apdu.getIdMensagem());
      return apdu.getIdMensagem();
    } catch (IOException e) {
      throw new exceptions.ConexaoException("Falha ao enviar mensagem SENDVU via UDP", e);
    }
  }

  public String sendPvtVu(String nomeDestinatario, InfoUser usuario, String mensagem) throws exceptions.ConexaoException {
    APDU apdu = new APDU("SENDPVT", "@" + nomeDestinatario, usuario.getNome(), mensagem, usuario.getPorta(), nomeDestinatario);
    apdu.setVisualizacaoUnica(true);
    try {
      byte[] dadosEnviados = serializarAPDU(apdu);
      DatagramPacket pacoteEnvio = new DatagramPacket(dadosEnviados, dadosEnviados.length, ipServidor, portaServidor);
      socketUDP.send(pacoteEnvio);
      System.out.println("[CLIENTE:UDP] [INFO] Mensagem privada VU enviada. ID: " + apdu.getIdMensagem());
      return apdu.getIdMensagem();
    } catch (IOException e) {
      throw new exceptions.ConexaoException("Falha ao enviar mensagem privada SENDVU via UDP", e);
    }
  }

  public void sendConfirm(String idMensagem, int status, String nomeGrupoOuDestino, String donoDaMensagem) {
    if (idMensagem == null || idMensagem.isEmpty()) {
      return;
    }
    String remetenteConfirm = (this.meuNome != null) ? this.meuNome : "Anonimo";
    APDU confirmApdu = new APDU("CONFIRM", idMensagem, status, remetenteConfirm, nomeGrupoOuDestino, donoDaMensagem);
    try {
      byte[] dadosEnviados = serializarAPDU(confirmApdu);
      DatagramPacket pacoteEnvio = new DatagramPacket(dadosEnviados, dadosEnviados.length, ipServidor, portaServidor);
      socketUDP.send(pacoteEnvio);
      System.out.println("[CLIENTE:UDP] [INFO] CONFIRM (Status " + status + ") enviado para mensagem: " + idMensagem);
    } catch (IOException e) {
      System.err.println("[CLIENTE:UDP] [ERROR] Falha ao enviar CONFIRM: " + e.getMessage());
    }
  }

  @Override
  public void run() {
    byte[] bufferRecepcao = new byte[BUFFER];

    while (!socketUDP.isClosed()) {
      DatagramPacket pacoteRecebido = new DatagramPacket(bufferRecepcao, bufferRecepcao.length);
      try {
        socketUDP.receive(pacoteRecebido);

        ByteArrayInputStream bais = new ByteArrayInputStream(pacoteRecebido.getData(), 0, pacoteRecebido.getLength());
        ObjectInputStream ois = new ObjectInputStream(bais);
        APDU apdu = (APDU) ois.readObject();

        String comando = apdu.getOperacao();

        // Notificacao de troca de lider no cluster
        if (Protocolo.SERVER_CHANGED.equals(comando)) {
          String payload = apdu.getTextoMensagem();
          System.out.println("[CLIENTE:UDP] [INFO] Notificacao SERVER_CHANGED recebida: " + payload);
          if (payload != null && !payload.isEmpty()) {
            String[] partes = payload.split(";");
            if (partes.length >= 3) {
              String novoIp = partes[0];
              int novoTcp = Integer.parseInt(partes[1]);
              int novoUdp = Integer.parseInt(partes[2]);
              setDestino(novoIp, novoUdp);
              if (listener != null) {
                listener.onServerChanged(novoIp, novoTcp, novoUdp);
              }
            }
          }
          continue;
        }

        if (Protocolo.SHUTDOWN.equals(comando)) {
          System.out.println("\n[SISTEMA] Sinal de encerramento recebido do servidor. Engatilhando busca por lider...");
          if (listener != null) {
            listener.onConexaoPerdida();
          }
          continue;
        }

        if (Protocolo.UPDATE_USERS.equals(comando)) {
          System.out.println("[CLIENTE:UDP] [INFO] Recebida notificacao de atualizacao de usuarios.");
          if (listener != null) {
            listener.onUpdateUsers();
          }
          continue;
        }

        if (Protocolo.CONFIRM.equals(comando)) {
          System.out.println("[CLIENTE:UDP] [INFO] Confirmacao (tick) recebida para mensagem: " + apdu.getIdMensagem()
              + " Status: " + apdu.getStatusRecebido());
          if (listener != null) {
            String confirmador = apdu.getNomeUsuario();
            if (confirmador == null || confirmador.trim().isEmpty()) {
              confirmador = apdu.getDestinatario();
            }
            listener.onTickReceived(apdu.getIdMensagem(), apdu.getStatusRecebido(), confirmador);
          }
          continue;
        }

        if (Protocolo.SENDPVT.equals(comando)) {
          String nomeRemetente = apdu.getNomeUsuario();
          if (nomeRemetente != null && nomeRemetente.startsWith("@")) {
            nomeRemetente = nomeRemetente.substring(1);
          }
          InfoUser remetente = new InfoUser(nomeRemetente, pacoteRecebido.getAddress().getHostAddress(), apdu.getPortaClienteUDP());
          String mensagemPvt = apdu.getTextoMensagem();
          System.out.println("\n[MENSAGEM PRIVADA] " + remetente.getNome() + " diz: " + mensagemPvt);
          
          if (apdu.getIdMensagem() != null) {
            sendConfirm(apdu.getIdMensagem(), 2, "@" + meuNome, remetente.getNome());
          }

          boolean isVuPvt = apdu.isVisualizacaoUnica();
          if (listener != null) {
            listener.onMessageReceived(apdu.getIdMensagem(), remetente.getNome(), remetente, mensagemPvt, true, isVuPvt);
          }
          continue;
        }

        InfoUser usuario = new InfoUser(apdu.getNomeUsuario(), pacoteRecebido.getAddress().getHostAddress(), apdu.getPortaClienteUDP());
        String mensagem = apdu.getTextoMensagem();
        String grupo = apdu.getNomeGrupo();
        boolean isVuGrupo = apdu.isVisualizacaoUnica() || Protocolo.SENDVU.equals(comando);

        System.out.println("\n[CLIENTE:UDP] [INFO] Nova mensagem recebida no grupo " + grupo + ":\n"
            + usuario.toString() + " enviou: " + mensagem + (isVuGrupo ? " [VU]" : ""));

        if (apdu.getIdMensagem() != null && mensagem != null && !mensagem.startsWith("~")) {
          sendConfirm(apdu.getIdMensagem(), 2, grupo, usuario.getNome());
        }

        if (listener != null) {
          listener.onMessageReceived(apdu.getIdMensagem(), grupo, usuario, mensagem, false, isVuGrupo);
        }

      } catch (SocketException e) {
        // Encerramento normal
      } catch (Exception e) {
        System.out.println("[CLIENTE:UDP] [ERROR] Falha ao processar datagrama UDP: " + e.getMessage());
      }
    }
  }

  public void fecharConexao() {
    this.socketUDP.close();
  }

}
