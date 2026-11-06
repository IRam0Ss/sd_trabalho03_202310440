/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: AtendimentoCliente
* Funcao.............: Thread dedicada para atendimento e processamento concorrente de requisicoes TCP de cada cliente.
*************************************************************** */

package controller;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import Protocol.APDU;
import cluster.ClusterLog;
import cluster.Particionador;
import cluster.RoteadorShards;
import utils.InfoUser;
import utils.Protocolo;
import model.NotificadorClientes;

/**
 * Classe responsavel por atender clientes individuais via conexao TCP concorrente.
 * Na arquitetura particionada (T#03), atua como Proxy Transparente no Servidor:
 * o cliente e a APDU permanecem intactos, enquanto cada comando e roteado para o lider
 * do Shard responsavel (A..I -> S0, J..R -> S1, S..Z -> S2), com Scatter-Gather resiliente
 * em LIST/USERS, replicacao global assincrona em BLOCK/UNBLOCK e broadcast UDP USUARIO_CAIU.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 * @since 15/06/2026
 */
public class AtendimentoCliente implements Runnable {

  private Socket conexao;
  private GerenciadorGrupos gerenciador;
  private final RoteadorShards roteador;
  private InfoUser usuarioAssociado = null;
  private ObjectOutputStream saidaObjetos;
  private final AtomicBoolean faxinaExecutada = new AtomicBoolean(false);
  /** Grupos em que o cliente ingressou nesta sessao TCP, usados para detectar amnesia caso um shard reinicie. */
  private final Set<String> gruposDaSessao = ConcurrentHashMap.newKeySet();
  private volatile long ultimoRegistroOuJoinMs = 0L;

  private static final java.util.concurrent.atomic.AtomicLong totalPingsAtendidos = new java.util.concurrent.atomic.AtomicLong(0);
  private static volatile long ultimoLogPing = 0;

  public static long getTotalPingsAtendidos() {
    return totalPingsAtendidos.get();
  }

  /**
   * Construtor padrao/legado do AtendimentoCliente (sem roteador inter-shard).
   * 
   * @param conexao     Socket TCP da conexao atual
   * @param gerenciador Referencia ao gerenciador de grupos e usuarios
   */
  public AtendimentoCliente(Socket conexao, GerenciadorGrupos gerenciador) {
    this(conexao, gerenciador, null);
  }

  /**
   * Construtor particionado (T#03) com Proxy Transparente inter-shard.
   * 
   * @param conexao     Socket TCP da conexao atual
   * @param gerenciador Referencia ao gerenciador local da particao
   * @param roteador    Roteador inter-shard (pode ser null em modo standalone)
   */
  public AtendimentoCliente(Socket conexao, GerenciadorGrupos gerenciador, RoteadorShards roteador) {
    this.conexao = conexao;
    this.gerenciador = gerenciador;
    this.roteador = roteador;
    try {
      this.saidaObjetos = new ObjectOutputStream(conexao.getOutputStream());
      this.saidaObjetos.flush();
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [ERROR] Falha ao criar escritor TCP: " + e.getMessage());
    }
  }

  /**
   * Laco principal de leitura de comandos APDU do cliente via stream de objetos TCP.
   * Realiza a leitura continua e despacha cada APDU para processamento interno.
   */
  @Override
  public void run() {
    try {
      ObjectInputStream tradutorObjetos = new ObjectInputStream(this.conexao.getInputStream());
      Protocol.APDU apduRecebida;

      // Le e trata as APDUs recebidas
      while (!conexao.isClosed()) {
        try {
          apduRecebida = (Protocol.APDU) tradutorObjetos.readObject();
          if (apduRecebida != null) {
            processarAPDU(apduRecebida);
          }
        } catch (EOFException | SocketException e) {
          // Queda ou encerramento do socket TCP do cliente -> dispara faxina imediata
          desconectarClienteInativoGlobal("Queda do socket TCP (" + e.getClass().getSimpleName() + ")");
          break;
        }
      }
    } catch (EOFException | SocketException e) {
      desconectarClienteInativoGlobal("Encerramento abrupto do stream TCP (" + e.getClass().getSimpleName() + ")");
    } catch (IOException e) {
      System.err.println("[ATENDIMENTO] [INFO] Conexao IO encerrada com o cliente: " + e.getMessage());
      desconectarClienteInativoGlobal("Falha de I/O TCP (" + e.getMessage() + ")");
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [INFO] Conexao encerrada com o cliente: " + e.getMessage());
      desconectarClienteInativoGlobal("Excecao na sessao TCP");
    } finally {
      try {
        desconectarClienteInativoGlobal("Fechamento da thread de atendimento");
      } finally {
        fecharSocketSilencioso();
      }
    }
  }

  /**
   * Garante a faxina de cliente fantasma exatamente uma vez quando a conexao TCP cai sem LOGOUT previo,
   * roteando REMOVER_USUARIO + SAIR_GRUPO para todos os shards e atualizando a lista global de online.
   */
  private void desconectarClienteInativoGlobal(String motivo) {
    if (!faxinaExecutada.compareAndSet(false, true)) {
      return;
    }
    try {
      if (this.usuarioAssociado != null && this.usuarioAssociado.getNome() != null) {
        String nome = this.usuarioAssociado.getNome();
        if (this.roteador != null) {
          // Limpa localmente (REMOVER_USUARIO + SAIR_GRUPO no proprio shard se ativo) e propaga aos outros shards
          Resultado resLocal = this.gerenciador.desconectarClienteInativo(nome, this);
          if (resLocal != null && !"Sessao ignorada ou ja renovada".equals(resLocal.getMensagem())) {
            ClusterLog.erro("GHOST-CLIENT", "Desconexao TCP inativa detectada para '" + nome
                + "' [" + motivo + "]. Roteando REMOVER_USUARIO e SAIR_GRUPO para todos os shards...");
            this.roteador.tratarQuedaBruscaUsuario(nome);
            dispararUpdateUsuariosGlobal();
          }
        } else {
          System.out.println("[ATENDIMENTO] [CLEANUP] Cliente '" + nome
              + "' encerrou conexao TCP [" + motivo + "]. Limpando sessoes e grupos...");
          Resultado res = this.gerenciador.desconectarClienteInativo(nome, this);
          if (res != null && res.isNotificarAtualizacaoUsuarios()) {
            NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
          }
        }
      }
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [ERROR] Falha na limpeza de cliente inativo: " + e.getMessage());
    }
  }

  private void fecharSocketSilencioso() {
    try {
      if (this.saidaObjetos != null) {
        this.saidaObjetos.close();
      }
    } catch (Exception ignored) {
    }
    try {
      if (this.conexao != null && !this.conexao.isClosed()) {
        this.conexao.close();
      }
    } catch (Exception ignored) {
    }
  }

  /**
   * Fecha imediatamente o socket TCP deste cliente no no de entrada quando um shard de destino
   * reiniciou sem o estado volátil do usuario/grupo (Amnesia de Shard).
   * Nao dispara faxina de cliente fantasma nos demais shards saudaveis, permitindo que o cliente
   * entre em Failover automatico e reenvie REGISTER e JOIN para reconstruir a memoria perdida.
   */
  public void forcarQuedaPorAmnesiaShard(String motivo) {
    if (!faxinaExecutada.compareAndSet(false, true)) {
      return;
    }
    String nome = (this.usuarioAssociado != null && this.usuarioAssociado.getNome() != null)
        ? this.usuarioAssociado.getNome()
        : "desconhecido";
    this.gerenciador.desvincularSessao(nome, this);
    ClusterLog.aviso("AMNESIA-SHARD", String.format(
        "Shard de destino retornou sessao/usuario desconhecido para '%s' (%s). "
            + "Fechando socket TCP de entrada para forcar Failover + Re-REGISTER/Re-JOIN!",
        nome, motivo));
    fecharSocketSilencioso();
  }

  /**
   * Valida se algum shard online perdeu o estado volátil deste cliente; caso afirmativo,
   * encerra o socket TCP imediatamente e retorna true.
   */
  private boolean verificarEDerrubarSeAmnesia() {
    if (this.roteador == null || this.usuarioAssociado == null || this.usuarioAssociado.getNome() == null) {
      return false;
    }
    if (System.currentTimeMillis() - this.ultimoRegistroOuJoinMs < 1200) {
      return false;
    }
    String erroAmnesia = this.roteador.verificarAmnesiaSessao(this.usuarioAssociado.getNome(), this.gruposDaSessao);
    if (erroAmnesia != null) {
      forcarQuedaPorAmnesiaShard(erroAmnesia);
      return true;
    }
    return false;
  }

  /**
   * Envia uma resposta textual serializada padronizada para o cliente via ObjectOutputStream.
   * Compativel com qualquer cliente que faca readObject de String.
   * 
   * @param operacao  Codigo de status (Protocolo.OK ou Protocolo.ERRO).
   * @param mensagem  Texto descritivo ou conteudo da listagem.
   */
  private void enviarResposta(String operacao, String mensagem) {
    try {
      String respostaStr;
      if (mensagem == null || mensagem.isEmpty()) {
        respostaStr = operacao + ": ";
      } else {
        respostaStr = operacao + ": " + mensagem;
      }
      this.saidaObjetos.writeUnshared(respostaStr);
      this.saidaObjetos.flush();
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [ERROR] Erro ao enviar resposta: " + e.getMessage());
      fecharSocketSilencioso();
    }
  }

  /**
   * Processa a APDU recebida pelo servidor e designa o processamento para o
   * gerenciador.
   * 
   * @param apdu APDU recebida
   */
  private void processarAPDU(Protocol.APDU apdu) {
    String comando = apdu.getOperacao();

    if (!Protocolo.PING.equals(comando)) {
      System.out.println("[ATENDIMENTO] [INFO] Processando comando " + comando);
    }

    if (comando == null || comando.isEmpty()) {
      return;
    }

    // Vincula automaticamente a sessao ativa para o usuario remetente se presente
    String nomeRemetente = apdu.getNomeUsuario();
    if (nomeRemetente != null && !nomeRemetente.trim().isEmpty()) {
      String nomeTrim = nomeRemetente.trim();
      if (nomeTrim.startsWith("@")) {
        nomeTrim = nomeTrim.substring(1).trim();
      }
      if (!nomeTrim.isEmpty()) {
        this.gerenciador.vincularSessao(nomeTrim, this);
        if (this.usuarioAssociado == null) {
          String ipConexao = (conexao != null && conexao.getInetAddress() != null)
              ? conexao.getInetAddress().getHostAddress() : "127.0.0.1";
          this.usuarioAssociado = new InfoUser(nomeTrim, ipConexao, apdu.getPortaClienteUDP());
        }
      }
    }

    switch (comando) {
      case Protocolo.PING:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        enviarResposta(Protocolo.OK, "ATIVO");
        long totalPings = totalPingsAtendidos.incrementAndGet();
        long agora = System.currentTimeMillis();
        if (agora - ultimoLogPing >= 15000) {
          ultimoLogPing = agora;
          System.out.println("[LIVENESS:PING] Heartbeats PING de clientes ativos sendo atendidos normalmente (Total: " + totalPings + ")");
        }
        break;

      case Protocolo.JOIN:
        this.ultimoRegistroOuJoinMs = System.currentTimeMillis();
        String grupoJoin = apdu.getNomeGrupo();
        String ipJoin = conexao.getInetAddress().getHostAddress();
        InfoUser usuarioJoin = new InfoUser(apdu.getNomeUsuario(), ipJoin, apdu.getPortaClienteUDP());
        if (this.usuarioAssociado == null) {
          this.usuarioAssociado = usuarioJoin;
        }

        int shardGrupoJoin = Particionador.shardDe(grupoJoin);
        if (this.roteador != null) {
          ClusterLog.info("PROXY:TCP", String.format("Roteando JOIN de '%s' no grupo '%s' -> %s",
              apdu.getNomeUsuario(), grupoJoin, Particionador.nome(shardGrupoJoin)));
        }
        Operacao opJoin = Operacao.entrarGrupo(
            apdu.getIdRequisicao(), grupoJoin, apdu.getNomeUsuario(), ipJoin, apdu.getPortaClienteUDP());
        Resultado resJoin = executarNoShard(shardGrupoJoin, opJoin);
        if (grupoJoin != null && !grupoJoin.trim().isEmpty()
            && (resJoin.isSucesso()
                || (resJoin.getMensagem() != null && resJoin.getMensagem().startsWith("Voce ja esta no grupo")))) {
          this.gruposDaSessao.add(grupoJoin.trim());
        }
        enviarResposta(resJoin.getOperacaoRetorno(), resJoin.getMensagem());
        if (resJoin.isNotificarAtualizacaoUsuarios()) {
          dispararUpdateUsuariosGlobal();
        }
        // Compatibilidade clientes T#01 (que enviam JOIN sem REGISTER previo) quando shardUsuario != shardGrupo
        if (this.roteador != null && resJoin.isSucesso() && apdu.getNomeUsuario() != null) {
          final int shardUsr = Particionador.shardDe(apdu.getNomeUsuario());
          if (shardUsr != shardGrupoJoin) {
            final String idAutoReg = "autoreg-"
                + (apdu.getIdRequisicao() != null ? apdu.getIdRequisicao() : UUID.randomUUID().toString())
                + "-S" + shardUsr;
            final String nomeAuto = apdu.getNomeUsuario();
            final int portaUdpAuto = apdu.getPortaClienteUDP();
            new Thread(() -> {
              Resultado rAuto = this.roteador.executar(
                  shardUsr, Operacao.registrarUsuario(idAutoReg, nomeAuto, ipJoin, portaUdpAuto), 2000);
              if (rAuto != null && rAuto.isNotificarAtualizacaoUsuarios()) {
                dispararUpdateUsuariosGlobal();
              }
            }, "Thread-AutoReg-CrossShard").start();
          }
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de JOIN de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.REGISTER:
        this.ultimoRegistroOuJoinMs = System.currentTimeMillis();
        String ipReg = conexao.getInetAddress().getHostAddress();
        InfoUser usuarioRegister = new InfoUser(apdu.getNomeUsuario(), ipReg, apdu.getPortaClienteUDP());
        if (this.usuarioAssociado == null) {
          this.usuarioAssociado = usuarioRegister;
        }

        int shardReg = Particionador.shardDe(apdu.getNomeUsuario());
        if (this.roteador != null) {
          ClusterLog.info("PROXY:TCP", String.format("Roteando REGISTER de '%s' -> %s",
              apdu.getNomeUsuario(), Particionador.nome(shardReg)));
        }
        Operacao opReg = Operacao.registrarUsuario(
            apdu.getIdRequisicao(), apdu.getNomeUsuario(), ipReg, apdu.getPortaClienteUDP());
        Resultado resReg = executarNoShard(shardReg, opReg);
        enviarResposta(resReg.getOperacaoRetorno(), resReg.getMensagem());
        if (resReg.isNotificarAtualizacaoUsuarios()) {
          dispararUpdateUsuariosGlobal();
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de REGISTER de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.LOGOUT:
        this.gerenciador.desvincularSessao(apdu.getNomeUsuario(), this);
        this.gruposDaSessao.clear();
        this.usuarioAssociado = null;
        if (this.roteador != null) {
          Resultado resLogout = this.roteador.removerUsuarioGlobal(apdu.getIdRequisicao(), apdu.getNomeUsuario());
          if (resLogout.isSucesso()) {
            enviarResposta(resLogout.getOperacaoRetorno(), resLogout.getMensagem());
          } else {
            enviarResposta(Protocolo.OK, "Desconectado com sucesso");
          }
          dispararUpdateUsuariosGlobal();
        } else {
          Operacao opLogout = Operacao.removerUsuario(apdu.getIdRequisicao(), apdu.getNomeUsuario());
          Resultado resLogout = this.gerenciador.aplicar(opLogout);
          enviarResposta(resLogout.getOperacaoRetorno(), resLogout.getMensagem());
          if (resLogout.isNotificarAtualizacaoUsuarios()) {
            NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
          }
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de LOGOUT de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.LEAVE:
        String grupoLeave = apdu.getNomeGrupo();
        if (grupoLeave != null && !grupoLeave.equalsIgnoreCase("GLOBAL")) {
          this.gruposDaSessao.removeIf(g -> g.equalsIgnoreCase(grupoLeave.trim()));
          int shardLeave = Particionador.shardDe(grupoLeave);
          if (this.roteador != null) {
            ClusterLog.info("PROXY:TCP", String.format("Roteando LEAVE de '%s' no grupo '%s' -> %s",
                apdu.getNomeUsuario(), grupoLeave, Particionador.nome(shardLeave)));
          }
          Operacao opLeave = Operacao.sairGrupo(apdu.getIdRequisicao(), grupoLeave, apdu.getNomeUsuario());
          Resultado resLeave = executarNoShard(shardLeave, opLeave);
          enviarResposta(resLeave.getOperacaoRetorno(), resLeave.getMensagem());
          System.out.println("[ATENDIMENTO] [INFO] Processamento de LEAVE de '" + apdu.getNomeUsuario() + "' concluido.");
        } else {
          this.gerenciador.desvincularSessao(apdu.getNomeUsuario(), this);
          this.gruposDaSessao.clear();
          this.usuarioAssociado = null;
          if (this.roteador != null) {
            this.roteador.removerUsuarioGlobal(apdu.getIdRequisicao(), apdu.getNomeUsuario());
            enviarResposta(Protocolo.OK, "Desconectado do servidor");
            dispararUpdateUsuariosGlobal();
          } else {
            Operacao opLeaveGlobal = Operacao.removerUsuario(apdu.getIdRequisicao(), apdu.getNomeUsuario());
            Resultado resLeaveGlobal = this.gerenciador.aplicar(opLeaveGlobal);
            enviarResposta(Protocolo.OK, "Desconectado do servidor");
            if (resLeaveGlobal.isNotificarAtualizacaoUsuarios()) {
              NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
            }
          }
        }
        break;

      case Protocolo.LIST:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        List<String> grupos = (this.roteador != null)
            ? this.roteador.listarGruposGlobal()
            : this.gerenciador.listarGrupos();
        if (grupos.isEmpty()) {
          enviarResposta(Protocolo.OK, "");
        } else {
          enviarResposta(Protocolo.OK, String.join(",", grupos));
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de LIST concluido (" + grupos.size() + " grupos).");
        break;

      case Protocolo.USERS:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        Collection<InfoUser> usuarios = (this.roteador != null)
            ? this.roteador.coletarTodosUsuariosAtivos()
            : this.gerenciador.getTodosUsuariosAtivos();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (InfoUser u : usuarios) {
          if (!first)
            sb.append(",");
          sb.append(u.getNome());
          first = false;
        }
        enviarResposta(Protocolo.OK, sb.toString());
        System.out.println(
            "[ATENDIMENTO] [INFO] Processamento de USERS concluido. Total: " + usuarios.size());
        break;

      case Protocolo.MEMBERS:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        String grupoList = apdu.getNomeGrupo();
        if (this.roteador != null) {
          RoteadorShards.ResultadoMembros rm = this.roteador.listarMembrosGrupo(grupoList);
          if (!rm.sucesso) {
            enviarResposta(Protocolo.ERRO, rm.erro);
            break;
          }
          StringBuilder sbMembers = new StringBuilder();
          boolean firstMember = true;
          for (InfoUser u : rm.membros) {
            if (!firstMember)
              sbMembers.append(",");
            sbMembers.append(u.getNome());
            firstMember = false;
          }
          enviarResposta(Protocolo.OK, sbMembers.toString());
        } else {
          List<InfoUser> membrosGrupo = this.gerenciador.getTodosMembros(grupoList);
          StringBuilder sbMembers = new StringBuilder();
          boolean firstMember = true;
          for (InfoUser u : membrosGrupo) {
            if (!firstMember)
              sbMembers.append(",");
            sbMembers.append(u.getNome());
            firstMember = false;
          }
          enviarResposta(Protocolo.OK, sbMembers.toString());
        }
        System.out.println(
            "[ATENDIMENTO] [INFO] Processamento de MEMBERS concluido para grupo: " + grupoList);
        break;

      case Protocolo.BLOCK:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        String usuarioABloquear = apdu.getDestinatario();
        String bloqueador = apdu.getNomeUsuario();
        Resultado resBlock = executarBloqueioGlobal(true, apdu.getIdRequisicao(), bloqueador, usuarioABloquear);
        enviarResposta(resBlock.getOperacaoRetorno(), resBlock.getMensagem());
        System.out.println("[ATENDIMENTO] [INFO] Usuario '" + bloqueador + "' bloqueou '" + usuarioABloquear + "'");
        break;

      case Protocolo.UNBLOCK:
        if (verificarEDerrubarSeAmnesia()) {
          break;
        }
        String usuarioADesbloquear = apdu.getDestinatario();
        String desbloqueador = apdu.getNomeUsuario();
        Resultado resUnblock = executarBloqueioGlobal(false, apdu.getIdRequisicao(), desbloqueador, usuarioADesbloquear);
        enviarResposta(resUnblock.getOperacaoRetorno(), resUnblock.getMensagem());
        System.out.println("[ATENDIMENTO] [INFO] Usuario '" + desbloqueador + "' desbloqueou '" + usuarioADesbloquear + "'");
        break;

      default:
        System.err.println("[ATENDIMENTO] [ERROR] Comando desconhecido: " + comando);
        enviarResposta(Protocolo.ERRO, "Comando desconhecido: " + comando);
        break;
    }
  }

  /**
   * Roteia a operacao para o lider do shard responsavel (Proxy Transparente) ou aplica localmente em modo isolado.
   */
  private Resultado executarNoShard(int shardAlvo, Operacao op) {
    if (this.roteador != null) {
      return this.roteador.executar(shardAlvo, op);
    }
    return this.gerenciador.aplicar(op);
  }

  /**
   * Executa BLOCK/UNBLOCK sincronamente em uma particao primaria e dispara broadcast assincrono
   * para os demais shards (tabela de bloqueios replicada globalmente).
   */
  private Resultado executarBloqueioGlobal(boolean bloquear, String idReq, String origem, String alvo) {
    if (this.roteador == null) {
      Operacao opLocal = bloquear
          ? Operacao.bloquear(idReq, origem, alvo)
          : Operacao.desbloquear(idReq, origem, alvo);
      return this.gerenciador.aplicar(opLocal);
    }

    String baseReq = (idReq != null && !idReq.trim().isEmpty())
        ? idReq.trim()
        : ("blk-" + UUID.randomUUID());
    int shardPrimario = Particionador.shardDe(origem);
    String reqPrim = baseReq + "-S" + shardPrimario;
    Operacao opPrim = bloquear
        ? Operacao.bloquear(reqPrim, origem, alvo)
        : Operacao.desbloquear(reqPrim, origem, alvo);

    Resultado res = this.roteador.executar(shardPrimario, opPrim);
    int shardAplicado = shardPrimario;

    // Fallback: se o shard do bloqueador estiver offline mas este gateway estiver ATIVO, aplica no shard local
    if (!res.isSucesso() && shardPrimario != this.roteador.getMeuShard()
        && origem != null && !origem.trim().isEmpty() && alvo != null && !alvo.trim().isEmpty()) {
      shardAplicado = this.roteador.getMeuShard();
      String reqFallback = baseReq + "-S" + shardAplicado;
      Operacao opFallback = bloquear
          ? Operacao.bloquear(reqFallback, origem, alvo)
          : Operacao.desbloquear(reqFallback, origem, alvo);
      res = this.roteador.executar(shardAplicado, opFallback);
    }

    if (res.isSucesso()) {
      this.roteador.replicarBloqueioGlobalAssincrono(bloquear, baseReq, origem, alvo, shardAplicado);
    }
    return res;
  }

  /**
   * Dispara em background um Scatter-Gather resiliente de usuarios ativos e envia UPDATE_USERS via UDP.
   */
  private void dispararUpdateUsuariosGlobal() {
    if (this.roteador == null) {
      NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
      return;
    }
    new Thread(() -> {
      try {
        List<InfoUser> todos = this.roteador.coletarTodosUsuariosAtivos();
        if (!todos.isEmpty() || this.gerenciador.isAtivo()) {
          NotificadorClientes.notificarAtualizacaoUsuarios(todos);
        }
      } catch (Exception ignored) {
      }
    }, "Thread-Global-UpdateUsers").start();
  }
}
