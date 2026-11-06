/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ServidorUDP
* Funcao.............: Servico de recepcao e roteamento de datagramas UDP com controle de ciclo de vida e estado centralizado.
*************************************************************** */

package model;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.RegistroMensagem;
import controller.Resultado;
import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Servico responsavel pelo recebimento, deserializacao e roteamento de datagramas UDP.
 * O rastreamento de donos e agregacao de confirmacoes reside integralmente
 * no {@link GerenciadorGrupos}, permitindo replicacao total entre os nos do cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class ServidorUDP implements Runnable {

  private final int porta;
  private final GerenciadorGrupos gerenciador;
  private static final int BUFFER = 4096;
  private volatile boolean executando = true;
  private DatagramSocket socketUDP;

  /**
   * Construtor do ServidorUDP.
   * 
   * @param porta       A porta UDP a ser escutada.
   * @param gerenciador O gerenciador compartilhado de grupos e usuarios.
   */
  public ServidorUDP(int porta, GerenciadorGrupos gerenciador) {
    this.porta = porta;
    this.gerenciador = gerenciador;
  }

  /**
   * Encerra de forma limpa o socket e a thread UDP.
   */
  public void parar() {
    this.executando = false;
    if (this.socketUDP != null && !this.socketUDP.isClosed()) {
      try {
        this.socketUDP.close();
      } catch (Exception ignored) {
      }
    }
    System.out.println("[SERVIDOR:UDP] [INFO] Servico na porta " + porta + " encerrado.");
  }

  /**
   * Laco principal de escuta de datagramas UDP.
   */
  @Override
  public void run() {
    try {
      this.socketUDP = new DatagramSocket(null);
      this.socketUDP.setReuseAddress(true);
      this.socketUDP.bind(new InetSocketAddress(this.porta));

      System.out.println("[SERVIDOR:UDP] [INFO] Escutando na porta " + porta);
      byte[] dadosEntrada = new byte[BUFFER];

      while (executando && !socketUDP.isClosed()) {
        DatagramPacket pacoteDadoRecebido = new DatagramPacket(dadosEntrada, dadosEntrada.length);
        try {
          socketUDP.receive(pacoteDadoRecebido);
        } catch (Exception e) {
          if (!executando) break;
          continue;
        }

        try {
          ByteArrayInputStream bais = new ByteArrayInputStream(pacoteDadoRecebido.getData(), 0, pacoteDadoRecebido.getLength());
          ObjectInputStream ois = new ObjectInputStream(bais);
          Protocol.APDU apdu = (Protocol.APDU) ois.readObject();
          processarAPDU(socketUDP, apdu, pacoteDadoRecebido.getAddress(), pacoteDadoRecebido.getPort());
        } catch (Exception ex) {
          if (executando) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao processar datagrama UDP: " + ex.getMessage());
          }
        }
      }

    } catch (Exception e) {
      if (executando) {
        System.err.println("[SERVIDOR:UDP] [ERROR] Erro no socket UDP (porta " + porta + "): " + e.getMessage());
      }
    } finally {
      if (socketUDP != null && !socketUDP.isClosed()) {
        socketUDP.close();
      }
    }
  }

  private byte[] serializarAPDU(Protocol.APDU apdu) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeUnshared(apdu);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao serializar APDU: " + e.getMessage());
      return new byte[0];
    }
  }

  /**
   * Processa a APDU recebida via UDP e encaminha para os destinatarios apropriados.
   */
  private void processarAPDU(DatagramSocket conexaoUDP, Protocol.APDU apdu, java.net.InetAddress ipRemetente, int portaOrigemPacote) {
    String comando = apdu.getOperacao();
    if (comando == null || comando.isEmpty()) {
      return;
    }

    switch (comando) {
      case Protocolo.SEND:
      case Protocolo.SENDVU:
        String nomeGrupo = apdu.getNomeGrupo();
        int portaRemetente = apdu.getPortaClienteUDP() > 0 ? apdu.getPortaClienteUDP() : portaOrigemPacote;
        InfoUser usuarioRemetente = new InfoUser(apdu.getNomeUsuario(), ipRemetente.getHostAddress(), portaRemetente);

        System.out.println("[SERVIDOR:UDP] [INFO] Recebido comando " + comando + " do usuario '" + usuarioRemetente.getNome()
            + "' para o grupo '" + nomeGrupo + "'");

        // Atualiza endpoint caso tenha mudado
        InfoUser cadastrado = gerenciador.buscarUsuarioPorNome(usuarioRemetente.getNome());
        if (cadastrado != null && (!cadastrado.getIp().equals(usuarioRemetente.getIp()) || cadastrado.getPorta() != usuarioRemetente.getPorta())) {
          gerenciador.aplicar(Operacao.atualizarEndpoint(usuarioRemetente.getNome(), usuarioRemetente.getIp(), usuarioRemetente.getPorta()));
        }

        List<InfoUser> destinatarios = gerenciador.getMembrosEnvio(nomeGrupo, usuarioRemetente);

        if (apdu.getIdMensagem() != null && !apdu.getIdMensagem().trim().isEmpty()) {
          Set<String> esperados = new HashSet<>();
          for (InfoUser dest : destinatarios) {
            esperados.add(dest.getNome().trim().toLowerCase());
          }

          Operacao opMsg = Operacao.registrarMsgGrupo(apdu.getIdMensagem().trim(), usuarioRemetente.getNome(), nomeGrupo, esperados, apdu.isVisualizacaoUnica());
          Resultado resMsg = gerenciador.aplicar(opMsg);

          // Auto-ACK do Servidor (Status 1: Mensagem recebida pelo servidor)
          try {
            Protocol.APDU ackServidor = new Protocol.APDU(Protocolo.CONFIRM, apdu.getIdMensagem(), 1, "SERVIDOR", nomeGrupo, usuarioRemetente.getNome());
            byte[] dadosAck = serializarAPDU(ackServidor);
            DatagramPacket pacoteAck = new DatagramPacket(dadosAck, dadosAck.length, ipRemetente, portaRemetente);
            conexaoUDP.send(pacoteAck);
            System.out.println("[SERVIDOR:UDP] [INFO] Auto-ACK (Status 1) enviado para '" + usuarioRemetente.getNome() + "'");
          } catch (Exception e) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao enviar Auto-ACK Status 1: " + e.getMessage());
          }

          // Se for reenvio redundante, ja foi encaminhada antes: nao duplica o envio
          if (resMsg.isDuplicada()) {
            System.out.println("[SERVIDOR:UDP] [INFO] Mensagem " + apdu.getIdMensagem() + " ja encaminhada anteriormente. Ignorando reenvio aos membros.");
            break;
          }
        }

        byte[] dadosEnviados = serializarAPDU(apdu);

        for (InfoUser membroDestinatario : destinatarios) {
          try {
            byte[] dadosParaEnviar;
            if (gerenciador.isBloqueadoMutuo(usuarioRemetente.getNome(), membroDestinatario.getNome())) {
              Protocol.APDU apduBloqueada = new Protocol.APDU(comando, nomeGrupo, usuarioRemetente.getNome(), "~BLOCKED~", usuarioRemetente.getPorta(), apdu.isVisualizacaoUnica());
              dadosParaEnviar = serializarAPDU(apduBloqueada);
              System.out.println("[SERVIDOR:UDP] [INFO] Mascarando mensagem do grupo '" + nomeGrupo + "' de '" + usuarioRemetente.getNome() + "' para '" + membroDestinatario.getNome() + "' (Bloqueado)");
            } else {
              dadosParaEnviar = dadosEnviados;
            }

            InetAddress ipDestinatario = InetAddress.getByName(membroDestinatario.getIp());
            DatagramPacket pacoteEnvio = new DatagramPacket(dadosParaEnviar, dadosParaEnviar.length, ipDestinatario,
                membroDestinatario.getPorta());
            conexaoUDP.send(pacoteEnvio);
          } catch (Exception e) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao enviar para '" + membroDestinatario.getNome() + "' - " + e.getMessage());
          }
        }
        break;

      case Protocolo.SENDPVT:
        String nomeDestino = apdu.getDestinatario();
        if (nomeDestino == null || nomeDestino.trim().isEmpty()) {
          nomeDestino = apdu.getNomeGrupo();
        }
        if (nomeDestino != null) {
          nomeDestino = nomeDestino.trim();
          if (nomeDestino.startsWith("@")) {
            nomeDestino = nomeDestino.substring(1);
          }
        }

        int portaPvt = apdu.getPortaClienteUDP() > 0 ? apdu.getPortaClienteUDP() : portaOrigemPacote;
        InfoUser remetentePvt = new InfoUser(apdu.getNomeUsuario(), ipRemetente.getHostAddress(), portaPvt);

        System.out.println("[SERVIDOR:UDP] [INFO] Recebido comando SENDPVT de '" + remetentePvt.getNome() + "' para '" + nomeDestino + "'");

        InfoUser cadPvt = gerenciador.buscarUsuarioPorNome(remetentePvt.getNome());
        if (cadPvt != null && (!cadPvt.getIp().equals(remetentePvt.getIp()) || cadPvt.getPorta() != remetentePvt.getPorta())) {
          gerenciador.aplicar(Operacao.atualizarEndpoint(remetentePvt.getNome(), remetentePvt.getIp(), remetentePvt.getPorta()));
        }

        if (apdu.getIdMensagem() != null && !apdu.getIdMensagem().trim().isEmpty()) {
          Operacao opPvt = Operacao.registrarMsgPrivada(apdu.getIdMensagem().trim(), remetentePvt.getNome(), nomeDestino);
          Resultado resPvt = gerenciador.aplicar(opPvt);

          // Auto-ACK do Servidor (Status 1: Mensagem recebida pelo servidor)
          try {
            Protocol.APDU ackServidor = new Protocol.APDU(Protocolo.CONFIRM, apdu.getIdMensagem(), 1, "SERVIDOR", "@" + nomeDestino, remetentePvt.getNome());
            byte[] dadosAck = serializarAPDU(ackServidor);
            DatagramPacket pacoteAck = new DatagramPacket(dadosAck, dadosAck.length, ipRemetente, portaPvt);
            conexaoUDP.send(pacoteAck);
            System.out.println("[SERVIDOR:UDP] [INFO] Auto-ACK (Status 1) enviado para '" + remetentePvt.getNome() + "'");
          } catch (Exception e) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao enviar Auto-ACK Status 1: " + e.getMessage());
          }

          if (resPvt.isDuplicada()) {
            System.out.println("[SERVIDOR:UDP] [INFO] SENDPVT " + apdu.getIdMensagem() + " ja entregue anteriormente. Ignorando reenvio.");
            break;
          }
        }

        InfoUser destinoInfo = gerenciador.buscarUsuarioPorNome(nomeDestino);

        if (gerenciador.isBloqueadoMutuo(remetentePvt.getNome(), nomeDestino)) {
          System.out.println("[SERVIDOR:UDP] [WARNING] SENDPVT bloqueado entre '" + remetentePvt.getNome() + "' e '" + nomeDestino + "'");
          if (apdu.getIdMensagem() != null) {
            Protocol.APDU confirmErro = new Protocol.APDU("CONFIRM", apdu.getIdMensagem(), -1, remetentePvt.getNome(), "@" + nomeDestino, remetentePvt.getNome());
            byte[] dadosConfirm = serializarAPDU(confirmErro);
            try {
              DatagramPacket pacoteConfirm = new DatagramPacket(dadosConfirm, dadosConfirm.length, ipRemetente, remetentePvt.getPorta());
              conexaoUDP.send(pacoteConfirm);
            } catch (Exception ignored) {
            }
          }
          break;
        }

        if (destinoInfo != null) {
          try {
            byte[] dadosPvt = serializarAPDU(apdu);
            InetAddress ipDest = InetAddress.getByName(destinoInfo.getIp());
            DatagramPacket pacotePvt = new DatagramPacket(dadosPvt, dadosPvt.length, ipDest, destinoInfo.getPorta());
            conexaoUDP.send(pacotePvt);
            System.out.println("[SERVIDOR:UDP] [INFO] SENDPVT encaminhado com sucesso para '" + nomeDestino + "'");
          } catch (Exception e) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao encaminhar SENDPVT: " + e.getMessage());
          }
        } else {
          System.out.println("[SERVIDOR:UDP] [WARNING] Destinatario '" + nomeDestino + "' nao encontrado online.");
        }
        break;

      case Protocolo.CONFIRM:
        String idMsg = apdu.getIdMensagem();
        String confirmador = apdu.getNomeUsuario();
        int status = apdu.getStatusRecebido();

        RegistroMensagem rastreio = (idMsg != null) ? gerenciador.getRegistroMensagem(idMsg.trim()) : null;

        if (rastreio != null && !rastreio.isPrivado()) {
          // --- MENSAGEM DE GRUPO: RASTREIO UNIFICADO NO GERENCIADOR ---
          String autorMsg = rastreio.getAutor();

          if (status == 2) {
            Resultado rEntregue = gerenciador.aplicar(Operacao.confirmarEntrega(idMsg, confirmador));
            if (rEntregue.isNotificarAutorConfirm()) {
              InfoUser autorInfo = gerenciador.buscarUsuarioPorNome(autorMsg);
              if (autorInfo != null) {
                try {
                  byte[] dadosConfirm = serializarAPDU(apdu);
                  InetAddress ipAutor = InetAddress.getByName(autorInfo.getIp());
                  DatagramPacket pacoteConfirm = new DatagramPacket(dadosConfirm, dadosConfirm.length, ipAutor, autorInfo.getPorta());
                  conexaoUDP.send(pacoteConfirm);
                  System.out.println("[SERVIDOR:UDP] [INFO] CONFIRM (Status 2: Entregue) de '" + confirmador 
                      + "' repassado para o autor '" + autorMsg + "' (Grupo: " + rastreio.getNomeGrupo() + ")");
                } catch (Exception e) {
                  System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao repassar CONFIRM Status 2: " + e.getMessage());
                }
              }
            }
          } else if (status == 3) {
            Resultado rLido = gerenciador.aplicar(Operacao.confirmarLeitura(idMsg, confirmador));
            if (rLido.isNotificarAutorConfirm()) {
              InfoUser autorInfo = gerenciador.buscarUsuarioPorNome(autorMsg);
              if (autorInfo != null) {
                try {
                  Protocol.APDU confirmTodos = new Protocol.APDU(
                      Protocolo.CONFIRM,
                      apdu.getIdMensagem(),
                      3,
                      "TODOS",
                      rastreio.getNomeGrupo(),
                      autorMsg
                  );
                  byte[] dadosConfirm = serializarAPDU(confirmTodos);
                  InetAddress ipAutor = InetAddress.getByName(autorInfo.getIp());
                  DatagramPacket pacoteConfirm = new DatagramPacket(dadosConfirm, dadosConfirm.length, ipAutor, autorInfo.getPorta());
                  conexaoUDP.send(pacoteConfirm);
                  System.out.println("[SERVIDOR:UDP] [INFO] TODOS OS MEMBROS leram a mensagem no grupo '" + rastreio.getNomeGrupo() 
                      + "'! Repassado CONFIRM (Status 3) para o autor '" + autorMsg + "'");
                } catch (Exception e) {
                  System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao repassar CONFIRM Status 3 final: " + e.getMessage());
                }
              }
            }
          }
          break;
        }

        // --- MENSAGEM PRIVADA OU SEM RASTREIO DE GRUPO ---
        String dono = apdu.getDonoDaMensagem();
        if (dono == null || dono.trim().isEmpty()) {
          dono = apdu.getDestinatario();
        }
        if (dono == null || dono.trim().isEmpty()) {
          if (apdu.getNomeGrupo() != null && apdu.getNomeGrupo().startsWith("@")) {
            dono = apdu.getNomeGrupo().substring(1);
          }
        }
        if ((dono == null || dono.trim().isEmpty()) && idMsg != null) {
          dono = gerenciador.getDonoMensagem(idMsg.trim());
        }

        if (dono != null) {
          dono = dono.trim();
          if (dono.startsWith("@")) {
            dono = dono.substring(1).trim();
          }
        }

        if (dono == null || dono.isEmpty()) {
          System.out.println("[SERVIDOR:UDP] [WARNING] CONFIRM descartado: Dono da mensagem desconhecido (ID: " + idMsg + ")");
          break;
        }

        InfoUser donoInfo = gerenciador.buscarUsuarioPorNome(dono);
        if (donoInfo != null) {
          try {
            byte[] dadosConfirm = serializarAPDU(apdu);
            InetAddress ipDono = InetAddress.getByName(donoInfo.getIp());
            DatagramPacket pacoteConfirm = new DatagramPacket(dadosConfirm, dadosConfirm.length, ipDono, donoInfo.getPorta());
            conexaoUDP.send(pacoteConfirm);
            System.out.println("[SERVIDOR:UDP] [INFO] CONFIRM (Status " + apdu.getStatusRecebido() + ") de '" 
                + apdu.getNomeUsuario() + "' repassado com sucesso para o autor '" + dono + "'");
          } catch (Exception e) {
            System.err.println("[SERVIDOR:UDP] [ERROR] Falha ao repassar CONFIRM para '" + dono + "': " + e.getMessage());
          }
        } else {
          System.out.println("[SERVIDOR:UDP] [WARNING] CONFIRM para '" + dono + "' nao repassado: Usuario offline.");
        }
        break;

      default:
        System.out.println("[SERVIDOR:UDP] [WARNING] Comando desconhecido: " + comando);
        break;
    }
  }

}
