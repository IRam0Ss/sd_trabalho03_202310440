/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: ServidorUDP
* Funcao.............: Servico de recepcao e roteamento inter-shard de datagramas UDP (SEND, SENDVU, SENDPVT, CONFIRM).
*************************************************************** */

package model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import Protocol.APDU;
import cluster.Particionador;
import cluster.RoteadorShards;
import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.RegistroMensagem;
import controller.Resultado;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Servico responsavel pelo recebimento, deserializacao e roteamento de datagramas UDP.
 * Na arquitetura particionada (T#03 - Fase 5):
 * <ul>
 *   <li>{@code SEND / SENDVU}: roteados para o lider de {@code Particionador.shardDe(nomeGrupo)},
 *       que registra os ticks localmente, entrega os datagramas UDP aos membros (mascarando bloqueios
 *       via tabela global em memoria local O(1)) e devolve o Auto-ACK (Status 1) direto ao remetente.</li>
 *   <li>{@code SENDPVT}: roteado para o lider de {@code Particionador.shardDe(destinatario)},
 *       que valida bloqueio mutuo em RAM local, salva o tick localmente, devolve o Auto-ACK (Status 1)
 *       e entrega o datagrama UDP ao destinatario.</li>
 *   <li>{@code CONFIRM}: roteado para o shard detentor do rastreio (shard do grupo ou do destinatario),
 *       agregando Status 2 / Status 3 e repassando a confirmacao via UDP ao autor original.</li>
 * </ul>
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 4.0
 */
public class ServidorUDP implements Runnable {

  private final int porta;
  private final GerenciadorGrupos gerenciador;
  private final RoteadorShards roteador;
  private static final int BUFFER = 4096;
  private volatile boolean executando = true;
  private volatile DatagramSocket socketUDP;

  /**
   * Construtor padrao/legado do ServidorUDP (modo isolado sem roteador inter-shard).
   * 
   * @param porta       A porta UDP a ser escutada.
   * @param gerenciador O gerenciador compartilhado de grupos e usuarios.
   */
  public ServidorUDP(int porta, GerenciadorGrupos gerenciador) {
    this(porta, gerenciador, null);
  }

  /**
   * Construtor particionado (T#03) do ServidorUDP com roteamento transparente inter-shard.
   * 
   * @param porta       A porta UDP a ser escutada.
   * @param gerenciador O gerenciador compartilhado desta particao.
   * @param roteador    O roteador inter-shard (ou null em modo isolado).
   */
  public ServidorUDP(int porta, GerenciadorGrupos gerenciador, RoteadorShards roteador) {
    this.porta = porta;
    this.gerenciador = gerenciador;
    this.roteador = roteador;
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
    System.out.println("[UDP] Servico na porta " + porta + " encerrado.");
  }

  /**
   * Ponto de entrada invocado pelo {@link RoteadorShards} quando outro shard encaminha uma APDU UDP
   * para este lider ATIVO processar localmente.
   */
  public void receberPacoteRoteado(APDU apdu, String ipCliente, int portaUdpCliente, int saltos) {
    if (apdu == null || !executando) {
      return;
    }
    try {
      InetAddress ipOrig = InetAddress.getByName(
          (ipCliente != null && !ipCliente.trim().isEmpty()) ? ipCliente.trim() : "127.0.0.1");
      DatagramSocket sock = this.socketUDP;
      if (sock != null && !sock.isClosed()) {
        processarAPDU(sock, apdu, ipOrig, portaUdpCliente, saltos);
      } else {
        try (DatagramSocket efemero = new DatagramSocket()) {
          processarAPDU(efemero, apdu, ipOrig, portaUdpCliente, saltos);
        }
      }
    } catch (Exception e) {
      System.err.println("[UDP] [ERRO] Falha ao processar pacote UDP roteado: " + e.getMessage());
    }
  }

  /**
   * Laco principal de escuta de datagramas UDP recebidos diretamente de clientes.
   */
  @Override
  public void run() {
    try {
      DatagramSocket sock = new DatagramSocket(null);
      sock.setReuseAddress(true);
      sock.bind(new InetSocketAddress(this.porta));
      this.socketUDP = sock;

      System.out.println("[UDP] Escutando na porta " + porta);
      byte[] dadosEntrada = new byte[BUFFER];

      while (executando && !sock.isClosed()) {
        DatagramPacket pacoteDadoRecebido = new DatagramPacket(dadosEntrada, dadosEntrada.length);
        try {
          sock.receive(pacoteDadoRecebido);
        } catch (Exception e) {
          if (!executando) break;
          continue;
        }

        try {
          ByteArrayInputStream bais = new ByteArrayInputStream(
              pacoteDadoRecebido.getData(), 0, pacoteDadoRecebido.getLength());
          ObjectInputStream ois = new ObjectInputStream(bais);
          APDU apdu = (APDU) ois.readObject();
          processarAPDU(sock, apdu, pacoteDadoRecebido.getAddress(), pacoteDadoRecebido.getPort(), 0);
        } catch (Exception ex) {
          if (executando) {
            System.err.println("[UDP] [ERRO] Falha ao processar datagrama UDP: " + ex.getMessage());
          }
        }
      }

    } catch (Exception e) {
      if (executando) {
        System.err.println("[UDP] [ERRO] Erro no socket UDP (porta " + porta + "): " + e.getMessage());
      }
    } finally {
      if (socketUDP != null && !socketUDP.isClosed()) {
        socketUDP.close();
      }
    }
  }

  private byte[] serializarAPDU(APDU apdu) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeUnshared(apdu);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      System.err.println("[UDP] [ERRO] Falha ao serializar APDU: " + e.getMessage());
      return new byte[0];
    }
  }

  /**
   * Resolve o endpoint (IP/Porta UDP) do autor de uma mensagem usando a memoria local O(1)
   * (usuarios deste shard, membros cross-shard ou endpoint gravado no proprio RegistroMensagem).
   */
  private InfoUser obterEndpointAutor(RegistroMensagem rastreio) {
    if (rastreio == null) {
      return null;
    }
    InfoUser local = gerenciador.buscarEndpointConhecido(rastreio.getAutor());
    if (local != null && local.getIp() != null && local.getPorta() > 0) {
      return local;
    }
    if (rastreio.getIpAutor() != null && !rastreio.getIpAutor().isEmpty() && rastreio.getPortaAutor() > 0) {
      return new InfoUser(rastreio.getAutor(), rastreio.getIpAutor(), rastreio.getPortaAutor());
    }
    return null;
  }

  /**
   * Processa a APDU recebida via UDP, roteando para o lider do shard responsavel quando necessario
   * ou executando a entrega final e persistencia de ticks localmente quando este no e o dono da chave.
   */
  private void processarAPDU(DatagramSocket conexaoUDP, APDU apdu,
      InetAddress ipRemetente, int portaOrigemPacote, int saltos) {
    String comando = apdu.getOperacao();
    if (comando == null || comando.isEmpty()) {
      return;
    }

    switch (comando) {
      case Protocolo.SEND:
      case Protocolo.SENDVU:
        processarEnvioGrupo(conexaoUDP, apdu, comando, ipRemetente, portaOrigemPacote, saltos);
        break;

      case Protocolo.SENDPVT:
        processarEnvioPrivado(conexaoUDP, apdu, ipRemetente, portaOrigemPacote, saltos);
        break;

      case Protocolo.CONFIRM:
        processarConfirmacao(conexaoUDP, apdu, ipRemetente, portaOrigemPacote, saltos);
        break;

      default:
        System.out.println("[UDP] [AVISO] Comando desconhecido: " + comando);
        break;
    }
  }

  // =========================================================================
  // 1. SEND e SENDVU (Roteados para Particionador.shardDe(nomeGrupo))
  // =========================================================================

  private void processarEnvioGrupo(DatagramSocket conexaoUDP, APDU apdu, String comando,
      InetAddress ipRemetente, int portaOrigemPacote, int saltos) {
    String nomeGrupo = apdu.getNomeGrupo();
    int portaRemetente = apdu.getPortaClienteUDP() > 0 ? apdu.getPortaClienteUDP() : portaOrigemPacote;
    String ipRemetenteStr = ipRemetente.getHostAddress();

    int shardDestino = Particionador.shardDe(nomeGrupo);
    if (roteador != null && shardDestino != roteador.getMeuShard()) {
      System.out.println("[UDP] Roteando " + comando + " para grupo '" + nomeGrupo
          + "' -> " + Particionador.nome(shardDestino));
      roteador.encaminharUdp(shardDestino, apdu, ipRemetenteStr, portaRemetente, saltos + 1);
      return;
    }

    InfoUser usuarioRemetente = new InfoUser(apdu.getNomeUsuario(), ipRemetenteStr, portaRemetente);
    String origemTag = (saltos > 0) ? " (recebido via roteamento inter-shard)" : "";
    System.out.println("[UDP] Processando " + comando + " para grupo '" + nomeGrupo
        + "' de '" + usuarioRemetente.getNome() + "' em " + Particionador.nome(shardDestino) + origemTag);

    // Atualiza endpoint local (usuario ou membro cross-shard) caso tenha mudado
    InfoUser conhecido = gerenciador.buscarEndpointConhecido(usuarioRemetente.getNome());
    if (conhecido != null
        && (!conhecido.getIp().equals(usuarioRemetente.getIp()) || conhecido.getPorta() != usuarioRemetente.getPorta())) {
      gerenciador.aplicar(Operacao.atualizarEndpoint(
          usuarioRemetente.getNome(), usuarioRemetente.getIp(), usuarioRemetente.getPorta()));
    }

    List<InfoUser> destinatarios = gerenciador.getMembrosEnvio(nomeGrupo, usuarioRemetente);

    if (apdu.getIdMensagem() != null && !apdu.getIdMensagem().trim().isEmpty()) {
      Set<String> esperados = new HashSet<>();
      for (InfoUser dest : destinatarios) {
        esperados.add(dest.getNome().trim().toLowerCase());
      }

      Operacao opMsg = Operacao.registrarMsgGrupo(
          apdu.getIdMensagem().trim(),
          usuarioRemetente.getNome(),
          ipRemetenteStr,
          portaRemetente,
          nomeGrupo,
          esperados,
          apdu.isVisualizacaoUnica());
      Resultado resMsg = gerenciador.aplicar(opMsg);

      // Auto-ACK do Lider do Shard de Destino (Status 1) direto de volta ao IP/Porta do remetente
      try {
        APDU ackServidor = new APDU(
            Protocolo.CONFIRM, apdu.getIdMensagem(), 1, "SERVIDOR", nomeGrupo, usuarioRemetente.getNome());
        byte[] dadosAck = serializarAPDU(ackServidor);
        DatagramPacket pacoteAck = new DatagramPacket(dadosAck, dadosAck.length, ipRemetente, portaRemetente);
        conexaoUDP.send(pacoteAck);
        System.out.println("[UDP] Auto-ACK (Status 1) enviado diretamente para '"
            + usuarioRemetente.getNome() + "' (" + ipRemetenteStr + ":" + portaRemetente + ")");
      } catch (Exception e) {
        System.err.println("[UDP] [ERRO] Falha ao enviar Auto-ACK Status 1: " + e.getMessage());
      }

      if (resMsg.isDuplicada()) {
        System.out.println("[UDP] Mensagem " + apdu.getIdMensagem()
            + " ja encaminhada anteriormente. Ignorando reenvio aos membros.");
        return;
      }
    }

    byte[] dadosEnviados = serializarAPDU(apdu);

    for (InfoUser membroDestinatario : destinatarios) {
      try {
        byte[] dadosParaEnviar;
        // Checagem O(1) na tabela global de bloqueios replicada em memoria RAM local
        if (gerenciador.isBloqueadoMutuo(usuarioRemetente.getNome(), membroDestinatario.getNome())) {
          APDU apduBloqueada = new APDU(
              comando, nomeGrupo, usuarioRemetente.getNome(), "~BLOCKED~",
              usuarioRemetente.getPorta(), apdu.isVisualizacaoUnica());
          dadosParaEnviar = serializarAPDU(apduBloqueada);
          System.out.println("[UDP] [BLOCK] Mascarando mensagem no grupo '" + nomeGrupo + "' de '"
              + usuarioRemetente.getNome() + "' para '" + membroDestinatario.getNome() + "' (~BLOCKED~)");
        } else {
          dadosParaEnviar = dadosEnviados;
        }

        InetAddress ipDestinatario = InetAddress.getByName(membroDestinatario.getIp());
        DatagramPacket pacoteEnvio = new DatagramPacket(
            dadosParaEnviar, dadosParaEnviar.length, ipDestinatario, membroDestinatario.getPorta());
        conexaoUDP.send(pacoteEnvio);
      } catch (Exception e) {
        System.err.println("[UDP] [ERRO] Falha ao enviar para '" + membroDestinatario.getNome()
            + "' - " + e.getMessage());
      }
    }
  }

  // =========================================================================
  // 2. SENDPVT (Roteado para Particionador.shardDe(destinatario))
  // =========================================================================

  private void processarEnvioPrivado(DatagramSocket conexaoUDP, APDU apdu,
      InetAddress ipRemetente, int portaOrigemPacote, int saltos) {
    String nomeDestino = apdu.getDestinatario();
    if (nomeDestino == null || nomeDestino.trim().isEmpty()) {
      nomeDestino = apdu.getNomeGrupo();
    }
    if (nomeDestino != null) {
      nomeDestino = nomeDestino.trim();
      if (nomeDestino.startsWith("@")) {
        nomeDestino = nomeDestino.substring(1).trim();
      }
    }

    int portaPvt = apdu.getPortaClienteUDP() > 0 ? apdu.getPortaClienteUDP() : portaOrigemPacote;
    String ipRemetenteStr = ipRemetente.getHostAddress();

    int shardDestino = Particionador.shardDe(nomeDestino);
    if (roteador != null && shardDestino != roteador.getMeuShard()) {
      System.out.println("[UDP] Roteando SENDPVT para destinatario '" + nomeDestino
          + "' -> " + Particionador.nome(shardDestino));
      roteador.encaminharUdp(shardDestino, apdu, ipRemetenteStr, portaPvt, saltos + 1);
      return;
    }

    InfoUser remetentePvt = new InfoUser(apdu.getNomeUsuario(), ipRemetenteStr, portaPvt);
    String origemTag = (saltos > 0) ? " (recebido via roteamento inter-shard)" : "";
    System.out.println("[UDP] Processando SENDPVT de '" + remetentePvt.getNome()
        + "' para '" + nomeDestino + "' em " + Particionador.nome(shardDestino) + origemTag);

    InfoUser cadPvt = gerenciador.buscarEndpointConhecido(remetentePvt.getNome());
    if (cadPvt != null
        && (!cadPvt.getIp().equals(remetentePvt.getIp()) || cadPvt.getPorta() != remetentePvt.getPorta())) {
      gerenciador.aplicar(Operacao.atualizarEndpoint(
          remetentePvt.getNome(), remetentePvt.getIp(), remetentePvt.getPorta()));
    }

    if (apdu.getIdMensagem() != null && !apdu.getIdMensagem().trim().isEmpty()) {
      Operacao opPvt = Operacao.registrarMsgPrivada(
          apdu.getIdMensagem().trim(),
          remetentePvt.getNome(),
          ipRemetenteStr,
          portaPvt,
          nomeDestino);
      Resultado resPvt = gerenciador.aplicar(opPvt);

      // Auto-ACK do Lider do Shard do Destinatario (Status 1) direto de volta ao remetente
      try {
        APDU ackServidor = new APDU(
            Protocolo.CONFIRM, apdu.getIdMensagem(), 1, "SERVIDOR", "@" + nomeDestino, remetentePvt.getNome());
        byte[] dadosAck = serializarAPDU(ackServidor);
        DatagramPacket pacoteAck = new DatagramPacket(dadosAck, dadosAck.length, ipRemetente, portaPvt);
        conexaoUDP.send(pacoteAck);
        System.out.println("[UDP] Auto-ACK (Status 1) de SENDPVT enviado diretamente para '"
            + remetentePvt.getNome() + "' (" + ipRemetenteStr + ":" + portaPvt + ")");
      } catch (Exception e) {
        System.err.println("[UDP] [ERRO] Falha ao enviar Auto-ACK Status 1: " + e.getMessage());
      }

      if (resPvt.isDuplicada()) {
        System.out.println("[UDP] SENDPVT " + apdu.getIdMensagem()
            + " ja entregue anteriormente. Ignorando reenvio.");
        return;
      }
    }

    // Checagem O(1) de bloqueio mutuo na tabela global em RAM local
    if (gerenciador.isBloqueadoMutuo(remetentePvt.getNome(), nomeDestino)) {
      System.out.println("[UDP] [AVISO] [BLOCK] SENDPVT bloqueado em memoria local entre '"
          + remetentePvt.getNome() + "' e '" + nomeDestino + "'");
      if (apdu.getIdMensagem() != null) {
        APDU confirmErro = new APDU(
            "CONFIRM", apdu.getIdMensagem(), -1, remetentePvt.getNome(), "@" + nomeDestino, remetentePvt.getNome());
        byte[] dadosConfirm = serializarAPDU(confirmErro);
        try {
          DatagramPacket pacoteConfirm = new DatagramPacket(
              dadosConfirm, dadosConfirm.length, ipRemetente, remetentePvt.getPorta());
          conexaoUDP.send(pacoteConfirm);
        } catch (Exception ignored) {
        }
      }
      return;
    }

    InfoUser destinoInfo = gerenciador.buscarUsuarioPorNome(nomeDestino);
    if (destinoInfo != null) {
      try {
        byte[] dadosPvt = serializarAPDU(apdu);
        InetAddress ipDest = InetAddress.getByName(destinoInfo.getIp());
        DatagramPacket pacotePvt = new DatagramPacket(
            dadosPvt, dadosPvt.length, ipDest, destinoInfo.getPorta());
        conexaoUDP.send(pacotePvt);
        System.out.println("[UDP] SENDPVT entregue com sucesso para '" + nomeDestino
            + "' (" + destinoInfo.getIp() + ":" + destinoInfo.getPorta() + ")");
      } catch (Exception e) {
        System.err.println("[UDP] [ERRO] Falha ao encaminhar SENDPVT: " + e.getMessage());
      }
    } else {
      System.out.println("[UDP] [AVISO] Destinatario '" + nomeDestino + "' nao encontrado online em "
          + Particionador.nome(shardDestino) + ".");
    }
  }

  // =========================================================================
  // 3. CONFIRM (Roteado para o shard do grupo ou do destinatario privado)
  // =========================================================================

  private void processarConfirmacao(DatagramSocket conexaoUDP, APDU apdu,
      InetAddress ipRemetente, int portaOrigemPacote, int saltos) {
    String idMsg = apdu.getIdMensagem();
    String confirmador = apdu.getNomeUsuario();
    int status = apdu.getStatusRecebido();
    String nomeGrupo = apdu.getNomeGrupo();
    int portaConfirm = apdu.getPortaClienteUDP() > 0 ? apdu.getPortaClienteUDP() : portaOrigemPacote;
    String ipRemetenteStr = ipRemetente.getHostAddress();

    RegistroMensagem rastreio = (idMsg != null) ? gerenciador.getRegistroMensagem(idMsg.trim()) : null;

    // Se o registro de tick nao esta neste shard, roteia para o shard dono do grupo ou do destinatario
    if (rastreio == null && roteador != null && saltos == 0) {
      int shardAlvo;
      if (nomeGrupo != null && !nomeGrupo.trim().isEmpty()
          && !"GLOBAL".equalsIgnoreCase(nomeGrupo.trim())
          && !nomeGrupo.trim().startsWith("@")) {
        shardAlvo = Particionador.shardDe(nomeGrupo);
      } else if (confirmador != null && !confirmador.trim().isEmpty()) {
        // Mensagem privada: os ticks ficam armazenados no Shard do Destinatario (confirmador)
        shardAlvo = Particionador.shardDe(confirmador);
      } else {
        shardAlvo = Particionador.shardDe(nomeGrupo);
      }

      if (shardAlvo != roteador.getMeuShard()) {
        System.out.println("[UDP] Roteando CONFIRM (Status " + status + ") de '" + confirmador
            + "' -> " + Particionador.nome(shardAlvo));
        roteador.encaminharUdp(shardAlvo, apdu, ipRemetenteStr, portaConfirm, saltos + 1);
        return;
      }
    }

    if (rastreio != null && !rastreio.isPrivado()) {
      // --- MENSAGEM DE GRUPO: AGREGA TICKS E REPASSA DIRETO AO AUTOR ---
      String autorMsg = rastreio.getAutor();

      if (status == 2) {
        Resultado rEntregue = gerenciador.aplicar(Operacao.confirmarEntrega(idMsg, confirmador));
        if (rEntregue.isNotificarAutorConfirm()) {
          InfoUser autorInfo = obterEndpointAutor(rastreio);
          if (autorInfo != null) {
            enviarConfirmParaAutor(conexaoUDP, apdu, autorInfo,
                "[UDP] CONFIRM (Status 2: Entregue) de '" + confirmador + "' repassado para '"
                    + autorMsg + "' (Grupo: " + rastreio.getNomeGrupo() + ")");
          }
        }
      } else if (status == 3) {
        Resultado rLido = gerenciador.aplicar(Operacao.confirmarLeitura(idMsg, confirmador));
        if (rLido.isNotificarAutorConfirm()) {
          InfoUser autorInfo = obterEndpointAutor(rastreio);
          if (autorInfo != null) {
            APDU confirmTodos = new APDU(
                Protocolo.CONFIRM,
                apdu.getIdMensagem(),
                3,
                "TODOS",
                rastreio.getNomeGrupo(),
                autorMsg);
            enviarConfirmParaAutor(conexaoUDP, confirmTodos, autorInfo,
                "[UDP] TODOS OS MEMBROS leram no grupo '" + rastreio.getNomeGrupo()
                    + "'! CONFIRM (Status 3) repassado para '" + autorMsg + "'");
          }
        }
      }
      return;
    }

    if (rastreio != null && rastreio.isPrivado()) {
      // --- MENSAGEM PRIVADA: ATUALIZA TICK NO SHARD DO DESTINATARIO E REPASSA AO AUTOR ---
      if (status == 2) {
        gerenciador.aplicar(Operacao.confirmarEntrega(idMsg, confirmador));
      } else if (status == 3) {
        gerenciador.aplicar(Operacao.confirmarLeitura(idMsg, confirmador));
      }
      InfoUser autorInfo = obterEndpointAutor(rastreio);
      if (autorInfo != null) {
        enviarConfirmParaAutor(conexaoUDP, apdu, autorInfo,
            "[UDP] CONFIRM privado (Status " + status + ") de '" + confirmador
                + "' salvo localmente e repassado para '" + rastreio.getAutor() + "'");
        return;
      }
    }

    // --- FALLBACK LEGADO (mensagens sem RegistroMensagem ou endpoint externo) ---
    String dono = apdu.getDonoDaMensagem();
    if (dono == null || dono.trim().isEmpty()) {
      dono = apdu.getDestinatario();
    }
    if (dono == null || dono.trim().isEmpty()) {
      if (nomeGrupo != null && nomeGrupo.startsWith("@")) {
        dono = nomeGrupo.substring(1);
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
      System.out.println("[UDP] [AVISO] CONFIRM descartado: Dono da mensagem desconhecido (ID: " + idMsg + ")");
      return;
    }

    InfoUser donoInfo = gerenciador.buscarEndpointConhecido(dono);
    if (donoInfo != null) {
      enviarConfirmParaAutor(conexaoUDP, apdu, donoInfo,
          "[UDP] CONFIRM (Status " + status + ") de '" + confirmador + "' repassado para '" + dono + "'");
    } else if (roteador != null && saltos < 2) {
      int shardDono = Particionador.shardDe(dono);
      if (shardDono != roteador.getMeuShard()) {
        System.out.println("[UDP] Roteando entrega final de CONFIRM para autor '" + dono
            + "' -> " + Particionador.nome(shardDono));
        roteador.encaminharUdp(shardDono, apdu, ipRemetenteStr, portaConfirm, saltos + 1);
      } else {
        System.out.println("[UDP] [AVISO] CONFIRM para '" + dono + "' nao repassado: Usuario offline.");
      }
    } else {
      System.out.println("[UDP] [AVISO] CONFIRM para '" + dono + "' nao repassado: Usuario offline.");
    }
  }

  private void enviarConfirmParaAutor(DatagramSocket conexaoUDP, APDU apduConfirm, InfoUser autorInfo, String msgLog) {
    try {
      byte[] dadosConfirm = serializarAPDU(apduConfirm);
      InetAddress ipAutor = InetAddress.getByName(autorInfo.getIp());
      DatagramPacket pacoteConfirm = new DatagramPacket(
          dadosConfirm, dadosConfirm.length, ipAutor, autorInfo.getPorta());
      conexaoUDP.send(pacoteConfirm);
      System.out.println(msgLog);
    } catch (Exception e) {
      System.err.println("[UDP] [ERRO] Falha ao repassar CONFIRM para '" + autorInfo.getNome()
          + "': " + e.getMessage());
    }
  }
}
