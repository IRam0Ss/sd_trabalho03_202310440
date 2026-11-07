/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: GerenciadorGrupos
* Funcao.............: Cerne unico de estado do servidor E.D.E.N. (usuarios, salas, bloqueios, mensagens e ticks).
*************************************************************** */

package controller;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

import cluster.Particionador;
import utils.InfoUser;

/**
 * Componente central do servidor responsavel pelo controle e sincronizacao de grupos,
 * usuarios ativos, bloqueios e rastreamento de mensagens (ticks de entrega e leitura).
 * Implementa mutacoes puramente deterministicas via {@link #aplicar(Operacao)} e
 * suporte completo a exportacao e importacao de snapshot para replicacao no cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class GerenciadorGrupos implements Serializable {

  private static final long serialVersionUID = 3L;

  // Estado central da particao (Shard) deste servidor
  private final int idShard;
  private final Map<String, InfoUser> usuarios;
  /** Endpoints (IP/UDP) de membros de grupos desta particao (inclui membros cujos cadastros pertencem a outros shards). */
  private final Map<String, InfoUser> endpointsMembros;
  private final Map<String, LinkedHashSet<String>> grupos;
  /** Tabela de bloqueios replicada globalmente em todos os shards para verificacao O(1) em SEND. */
  private final Map<String, Set<String>> bloqueios;
  private final Map<String, RegistroMensagem> mensagens;
  private final LinkedHashMap<String, Resultado> requisicoesProcessadas;
  private long versao;

  private static final int MAX_CACHE_REQUISICOES = 2000;

  /**
   * Construtor padrao (modo monolitico/legado sem filtro de shard: idShard = -1).
   */
  public GerenciadorGrupos() {
    this(-1);
  }

  /**
   * Construtor particionado (T#03): armazena apenas usuarios e grupos pertencentes a {@code idShard}.
   */
  public GerenciadorGrupos(int idShard) {
    this.idShard = idShard;
    this.usuarios = new HashMap<>();
    this.endpointsMembros = new HashMap<>();
    this.grupos = new HashMap<>();
    this.bloqueios = new HashMap<>();
    this.mensagens = new HashMap<>();
    this.requisicoesProcessadas = new LinkedHashMap<String, Resultado>(MAX_CACHE_REQUISICOES, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, Resultado> eldest) {
        return size() > MAX_CACHE_REQUISICOES;
      }
    };
    this.versao = 0;
  }

  public int getIdShard() {
    return idShard;
  }

  private volatile boolean ativo = true;
  private final Map<String, Object> sessoesAtivas = new java.util.concurrent.ConcurrentHashMap<>();

  public void setAtivo(boolean ativo) {
    this.ativo = ativo;
    if (!ativo) {
      sessoesAtivas.clear();
    }
  }

  public boolean isAtivo() {
    return this.ativo;
  }

  /** Associa a instancia de sessao TCP ativa ao codinome normalizado do cliente conectado neste no. */
  public void vincularSessao(String nomeUsuario, Object sessao) {
    if (nomeUsuario == null || sessao == null) return;
    sessoesAtivas.put(normalizar(nomeUsuario), sessao);
  }

  /** Remove o vinculo da sessao TCP local apenas se corresponder exatamente a instancia informada. */
  public void desvincularSessao(String nomeUsuario, Object sessao) {
    if (nomeUsuario == null || sessao == null) return;
    sessoesAtivas.remove(normalizar(nomeUsuario), sessao);
  }

  /** Recupera a sessao TCP ativa associada ao usuario neste gateway de entrada, ou {@code null}. */
  public Object obterSessaoAtiva(String nomeUsuario) {
    if (nomeUsuario == null) return null;
    String busca = nomeUsuario.trim();
    if (busca.startsWith("@")) {
      busca = busca.substring(1).trim();
    }
    return sessoesAtivas.get(normalizar(busca));
  }

  /**
   * Valida e encerra a sessao TCP local de um cliente que caiu bruscamente, retornando true
   * apenas se este no ainda estiver ATIVO e a sessao nao tiver sido renovada por reconexao.
   */
  public synchronized boolean encerrarSessaoLocalSeAtiva(String nomeUsuario, Object sessao) {
    if (!ativo || nomeUsuario == null || sessao == null) {
      return false;
    }
    String norm = normalizar(nomeUsuario);
    Object sessaoAtual = sessoesAtivas.get(norm);
    if (sessaoAtual != null && sessaoAtual != sessao) {
      System.out.println("[GERENCIADOR] [INFO] Desconexao ignorada para '" + nomeUsuario
          + "' pois usuario ja possui nova sessao ativa.");
      return false;
    }
    sessoesAtivas.remove(norm);
    return true;
  }

  /**
   * Remove com seguranca um cliente que perdeu a conexao TCP, garantindo que
   * clientes que ja reconectaram com outra sessao nao sejam derrubados por engano.
   */
  public synchronized Resultado desconectarClienteInativo(String nomeUsuario, Object sessao) {
    if (!encerrarSessaoLocalSeAtiva(nomeUsuario, sessao)) {
      return Resultado.ok("Sessao ignorada ou ja renovada");
    }
    String norm = normalizar(nomeUsuario);
    Operacao op = Operacao.removerUsuario("auto-disconnect-" + System.currentTimeMillis() + "-" + norm, nomeUsuario);
    return aplicar(op);
  }

  private String normalizar(String str) {
    return (str != null) ? str.trim().toLowerCase() : "";
  }

  public interface ListenerReplicacao {
    Resultado replicar(Operacao op);
  }

  private volatile ListenerReplicacao listenerReplicacao = null;

  public void setListenerReplicacao(ListenerReplicacao listener) {
    this.listenerReplicacao = listener;
  }

  /**
   * Aplica uma operacao. Se um ListenerReplicacao estiver configurado (papel ATIVO),
   * a operacao e replicada de forma sincrona para os standbys antes de retornar.
   * Caso contrario, e aplicada diretamente no estado local.
   * 
   * @param op Operacao a ser aplicada.
   * @return Resultado da execucao.
   */
  public Resultado aplicar(Operacao op) {
    ListenerReplicacao listener = this.listenerReplicacao;
    if (listener != null) {
      return listener.replicar(op);
    }
    return aplicarLocal(op);
  }

  /**
   * Aplica uma operacao mutavel ao estado do servidor de forma estritamente deterministica.
   * Incrementa o contador de versao da estrutura e armazena o resultado no cache de idempotencia.
   * 
   * @param op Operacao a ser aplicada localmente.
   * @return Resultado da execucao local.
   */
  public synchronized Resultado aplicarLocal(Operacao op) {
    if (op == null) {
      return Resultado.erro("Operacao nula");
    }

    // Deduplicacao idempotente: se a requisicao TCP ja foi processada, devolve resposta guardada
    if (op.getIdRequisicao() != null && !op.getIdRequisicao().trim().isEmpty()) {
      Resultado emCache = requisicoesProcessadas.get(op.getIdRequisicao().trim());
      if (emCache != null) {
        System.out.println("[GERENCIADOR] [INFO] Requisicao idempotente repetida detectada: " + op.getIdRequisicao());
        return emCache;
      }
    }

    // Validacao de propriedade de particao (defesa em profundidade antes de alterar versao)
    if (idShard >= 0) {
      switch (op.getTipo()) {
        case REGISTRAR_USUARIO:
          if (!Particionador.pertence(op.getNomeUsuario(), idShard)) {
            return Resultado.erro("Chave de usuario '" + op.getNomeUsuario() + "' fora da particao S" + idShard);
          }
          break;
        case ENTRAR_GRUPO:
        case SAIR_GRUPO:
        case REGISTRAR_MSG_GRUPO:
          if (!Particionador.pertence(op.getNomeGrupo(), idShard)) {
            return Resultado.erro("Chave de grupo '" + op.getNomeGrupo() + "' fora da particao S" + idShard);
          }
          break;
        case REGISTRAR_MSG_PRIVADA:
          if (!Particionador.pertence(op.getDestinatario(), idShard)) {
            return Resultado.erro("Mensagem privada para '" + op.getDestinatario() + "' fora da particao S" + idShard);
          }
          break;
        default:
          break;
      }
    }

    // Atualiza o relogio de versao da estrutura
    if (op.getVersao() > 0) {
      this.versao = Math.max(this.versao, op.getVersao());
    } else {
      this.versao++;
      op.setVersao(this.versao);
    }

    Resultado resultado;

    switch (op.getTipo()) {
      case REGISTRAR_USUARIO:
        resultado = aplicarRegistrarUsuario(op);
        break;

      case REMOVER_USUARIO:
        resultado = aplicarRemoverUsuario(op);
        break;

      case ENTRAR_GRUPO:
        resultado = aplicarEntrarGrupo(op);
        break;

      case SAIR_GRUPO:
        resultado = aplicarSairGrupo(op);
        break;

      case BLOQUEAR:
        resultado = aplicarBloquear(op);
        break;

      case DESBLOQUEAR:
        resultado = aplicarDesbloquear(op);
        break;

      case ATUALIZAR_ENDPOINT:
        resultado = aplicarAtualizarEndpoint(op);
        break;

      case REGISTRAR_MSG_GRUPO:
        resultado = aplicarRegistrarMsgGrupo(op);
        break;

      case REGISTRAR_MSG_PRIVADA:
        resultado = aplicarRegistrarMsgPrivada(op);
        break;

      case CONFIRMAR_ENTREGA:
        resultado = aplicarConfirmarEntrega(op);
        break;

      case CONFIRMAR_LEITURA:
        resultado = aplicarConfirmarLeitura(op);
        break;

      case LIMPAR_MENSAGENS:
        resultado = aplicarLimparMensagens(op);
        break;

      default:
        resultado = Resultado.erro("Tipo de operacao desconhecido: " + op.getTipo());
        break;
    }

    if (op.getIdRequisicao() != null && !op.getIdRequisicao().trim().isEmpty()) {
      requisicoesProcessadas.put(op.getIdRequisicao().trim(), resultado);
    }

    return resultado;
  }

  private Resultado aplicarRegistrarUsuario(Operacao op) {
    String nome = op.getNomeUsuario();
    if (nome == null || nome.trim().isEmpty()) {
      return Resultado.erro("Nome de usuario invalido");
    }
    String norm = normalizar(nome);
    InfoUser existente = usuarios.get(norm);

    // Atualiza endpoint de grupo caso o usuario ja participe de alguma sala neste shard
    InfoUser epMembro = endpointsMembros.get(norm);
    if (epMembro != null) {
      if (op.getIp() != null && !op.getIp().isEmpty()) epMembro.setIp(op.getIp());
      if (op.getPorta() > 0) epMembro.setPorta(op.getPorta());
    }

    if (existente != null) {
      existente.setIp(op.getIp());
      existente.setPorta(op.getPorta());
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' atualizou sessao (IP: " + op.getIp()
          + " | Porta: " + op.getPorta() + "). Versao: " + versao);
      return Resultado.ok("registrado com sucesso", false);
    } else {
      InfoUser novo = new InfoUser(nome.trim(), op.getIp(), op.getPorta());
      usuarios.put(norm, novo);
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' registrado nesta particao. Total usuarios locais: "
          + usuarios.size() + " | Versao: " + versao);
      return Resultado.ok("registrado com sucesso", true);
    }
  }

  private Resultado aplicarRemoverUsuario(Operacao op) {
    String nome = op.getNomeUsuario();
    if (nome == null || nome.trim().isEmpty()) {
      return Resultado.ok("Desconectado", false);
    }
    String norm = normalizar(nome);
    InfoUser removido = usuarios.remove(norm);
    endpointsMembros.remove(norm);

    boolean membroRemovidoDeAlgumGrupo = false;
    List<String> gruposParaRemover = new ArrayList<>();
    for (Map.Entry<String, LinkedHashSet<String>> entry : grupos.entrySet()) {
      String nomeGrupo = entry.getKey();
      LinkedHashSet<String> membros = entry.getValue();
      if (membros.remove(norm)) {
        membroRemovidoDeAlgumGrupo = true;
        System.out.println("[GERENCIADOR] [INFO] SAIR_GRUPO automatico: Usuario '" + nome
            + "' removido do grupo '" + nomeGrupo + "'.");
        if (membros.isEmpty()) {
          gruposParaRemover.add(nomeGrupo);
        }
      }
    }
    for (String g : gruposParaRemover) {
      grupos.remove(g);
      System.out.println("[GERENCIADOR] [INFO] Grupo '" + g + "' deletado pois ficou vazio.");
    }

    boolean notificar = (removido != null) || membroRemovidoDeAlgumGrupo || !gruposParaRemover.isEmpty();
    if (notificar) {
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' limpo desta particao. Notificar clientes: "
          + notificar + " | Versao: " + versao);
    }
    return Resultado.ok("Desconectado com sucesso", notificar);
  }

  private Resultado aplicarEntrarGrupo(Operacao op) {
    String nomeGrupo = op.getNomeGrupo();
    String nome = op.getNomeUsuario();
    if (nomeGrupo == null || nomeGrupo.trim().isEmpty() || nome == null || nome.trim().isEmpty()) {
      return Resultado.erro("Grupo ou usuario invalido");
    }

    String norm = normalizar(nome);
    boolean usuarioNovo = false;

    // Guarda sempre a copia do endpoint no shard do grupo para entrega UDP sem hop extra
    InfoUser epExistente = endpointsMembros.get(norm);
    if (epExistente == null) {
      endpointsMembros.put(norm, new InfoUser(nome.trim(), op.getIp(), op.getPorta()));
    } else {
      if (op.getIp() != null && !op.getIp().isEmpty()) epExistente.setIp(op.getIp());
      if (op.getPorta() > 0) epExistente.setPorta(op.getPorta());
    }

    // Registra na tabela de usuarios APENAS se este shard tambem for dono da chave do usuario
    if (idShard < 0 || Particionador.pertence(nome, idShard)) {
      if (!usuarios.containsKey(norm)) {
        usuarios.put(norm, new InfoUser(nome.trim(), op.getIp(), op.getPorta()));
        usuarioNovo = true;
        System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' auto-registrado via JOIN nesta particao.");
      } else {
        InfoUser u = usuarios.get(norm);
        if (op.getIp() != null && !op.getIp().isEmpty()) {
          u.setIp(op.getIp());
        }
        if (op.getPorta() > 0) {
          u.setPorta(op.getPorta());
        }
      }
    }

    LinkedHashSet<String> membros = grupos.computeIfAbsent(nomeGrupo, k -> new LinkedHashSet<>());
    if (membros.contains(norm)) {
      System.out.println("[GERENCIADOR] [WARNING] Usuario '" + nome + "' ja esta no grupo '" + nomeGrupo + "'");
      return Resultado.erro("Voce ja esta no grupo " + nomeGrupo);
    }

    membros.add(norm);
    System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' entrou no grupo '" + nomeGrupo
        + "'. Total: " + membros.size() + " | Versao: " + versao);
    return Resultado.ok("Entrou no grupo " + nomeGrupo, usuarioNovo);
  }

  private Resultado aplicarSairGrupo(Operacao op) {
    String nomeGrupo = op.getNomeGrupo();
    String nome = op.getNomeUsuario();
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return Resultado.erro("Voce nao esta no grupo " + nomeGrupo);
    }

    String norm = normalizar(nome);
    LinkedHashSet<String> membros = grupos.get(nomeGrupo);
    if (!membros.remove(norm)) {
      return Resultado.erro("Voce nao esta no grupo " + nomeGrupo);
    }

    System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' saiu do grupo '" + nomeGrupo + "'.");
    if (membros.isEmpty()) {
      grupos.remove(nomeGrupo);
      System.out.println("[GERENCIADOR] [INFO] Grupo '" + nomeGrupo + "' deletado da memoria pois ficou vazio.");
    }

    // Remove endpoint cacheado se o membro nao participar de mais nenhum grupo neste shard
    boolean aindaEmGrupo = false;
    for (LinkedHashSet<String> set : grupos.values()) {
      if (set.contains(norm)) {
        aindaEmGrupo = true;
        break;
      }
    }
    if (!aindaEmGrupo) {
      endpointsMembros.remove(norm);
    }
    return Resultado.ok("Saiu do grupo " + nomeGrupo, false);
  }

  private Resultado aplicarBloquear(Operacao op) {
    String bloqueador = normalizar(op.getNomeUsuario());
    String bloqueado = normalizar(op.getDestinatario());
    if (bloqueador.isEmpty() || bloqueado.isEmpty() || bloqueador.equals(bloqueado)) {
      return Resultado.erro("Parametros de bloqueio invalidos");
    }
    bloqueios.computeIfAbsent(bloqueador, k -> new HashSet<>()).add(bloqueado);
    System.out.println("[GERENCIADOR] [INFO] Bloqueio: '" + op.getNomeUsuario() + "' -> '" + op.getDestinatario() + "'");
    return Resultado.ok("Usuario " + op.getDestinatario() + " bloqueado com sucesso");
  }

  private Resultado aplicarDesbloquear(Operacao op) {
    String bloqueador = normalizar(op.getNomeUsuario());
    String bloqueado = normalizar(op.getDestinatario());
    Set<String> alvos = bloqueios.get(bloqueador);
    if (alvos != null) {
      alvos.remove(bloqueado);
      if (alvos.isEmpty()) {
        bloqueios.remove(bloqueador);
      }
      System.out.println("[GERENCIADOR] [INFO] Desbloqueio: '" + op.getNomeUsuario() + "' -> '" + op.getDestinatario() + "'");
    }
    return Resultado.ok("Usuario " + op.getDestinatario() + " desbloqueado com sucesso");
  }

  private Resultado aplicarAtualizarEndpoint(Operacao op) {
    String norm = normalizar(op.getNomeUsuario());
    InfoUser u = usuarios.get(norm);
    if (u != null) {
      if (op.getIp() != null && !op.getIp().isEmpty()) {
        u.setIp(op.getIp());
      }
      if (op.getPorta() > 0) {
        u.setPorta(op.getPorta());
      }
    }
    InfoUser ep = endpointsMembros.get(norm);
    if (ep != null) {
      if (op.getIp() != null && !op.getIp().isEmpty()) {
        ep.setIp(op.getIp());
      }
      if (op.getPorta() > 0) {
        ep.setPorta(op.getPorta());
      }
    }
    return Resultado.ok("ok");
  }

  private Resultado aplicarRegistrarMsgGrupo(Operacao op) {
    String id = op.getIdMensagem();
    if (id == null || id.trim().isEmpty()) {
      return Resultado.ok("ok");
    }
    String idTrim = id.trim();
    if (mensagens.containsKey(idTrim)) {
      return new Resultado(true, "OK", "duplicada", false, false, 0, true);
    }

    RegistroMensagem reg = new RegistroMensagem(
        idTrim, op.getNomeGrupo(), op.getNomeUsuario(), op.getIp(), op.getPorta(), op.getEsperados(), op.getTimestamp());
    mensagens.put(idTrim, reg);
    return Resultado.ok("registrado");
  }

  private Resultado aplicarRegistrarMsgPrivada(Operacao op) {
    String id = op.getIdMensagem();
    if (id == null || id.trim().isEmpty()) {
      return Resultado.ok("ok");
    }
    String idTrim = id.trim();
    if (mensagens.containsKey(idTrim)) {
      return new Resultado(true, "OK", "duplicada", false, false, 0, true);
    }

    RegistroMensagem reg = new RegistroMensagem(
        idTrim, op.getDestinatario(), op.getNomeUsuario(), op.getIp(), op.getPorta(), op.getTimestamp());
    mensagens.put(idTrim, reg);
    return Resultado.ok("registrado");
  }

  private Resultado aplicarConfirmarEntrega(Operacao op) {
    String id = op.getIdMensagem();
    if (id == null || id.trim().isEmpty()) {
      return Resultado.ok("ok");
    }
    RegistroMensagem reg = mensagens.get(id.trim());
    boolean notificar = false;
    if (reg != null) {
      notificar = reg.registrarEntrega(op.getNomeUsuario());
    }
    return new Resultado(true, "OK", "confirmado", false, notificar, 2, false);
  }

  private Resultado aplicarConfirmarLeitura(Operacao op) {
    String id = op.getIdMensagem();
    if (id == null || id.trim().isEmpty()) {
      return Resultado.ok("ok");
    }
    RegistroMensagem reg = mensagens.get(id.trim());
    boolean todosLeram = false;
    if (reg != null) {
      Set<String> membrosAtuais = Collections.emptySet();
      if (!reg.isPrivado()) {
        membrosAtuais = getMembrosNomes(reg.getNomeGrupo());
      }
      todosLeram = reg.registrarLeitura(op.getNomeUsuario(), membrosAtuais);
    }
    return new Resultado(true, "OK", "confirmado", false, todosLeram, 3, false);
  }

  private Resultado aplicarLimparMensagens(Operacao op) {
    long corte = op.getTimestamp();
    mensagens.entrySet().removeIf(entry -> entry.getValue().getTimestamp() < corte);
    return Resultado.ok("limpo");
  }

  // =========================================================================
  // METODOS DE CONSULTA (READ-ONLY THREAD-SAFE)
  // =========================================================================

  /**
   * Retorna os metadados {@link InfoUser} de todos os membros de um grupo hospedado nesta particao,
   * combinando usuarios locais e endpoints de membros cross-shard.
   */
  public synchronized List<InfoUser> getTodosMembros(String nomeGrupo) {
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return new ArrayList<>();
    }
    List<InfoUser> lista = new ArrayList<>();
    for (String nomeNorm : grupos.get(nomeGrupo)) {
      InfoUser u = usuarios.get(nomeNorm);
      if (u == null) {
        u = endpointsMembros.get(nomeNorm);
      }
      if (u != null) {
        lista.add(u);
      } else {
        lista.add(new InfoUser(nomeNorm, "127.0.0.1", 0));
      }
    }
    return lista;
  }

  /** Retorna o conjunto de nomes normalizados dos integrantes de um grupo desta particao. */
  public synchronized Set<String> getMembrosNomes(String nomeGrupo) {
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return Collections.emptySet();
    }
    return new HashSet<>(grupos.get(nomeGrupo));
  }

  /** Verifica de forma case-insensitive se um usuario integra um grupo hospedado nesta particao. */
  public synchronized boolean isMembroGrupo(String nomeGrupo, String nomeUsuario) {
    if (nomeGrupo == null || nomeUsuario == null) {
      return false;
    }
    String norm = normalizar(nomeUsuario);
    LinkedHashSet<String> membros = grupos.get(nomeGrupo);
    if (membros != null && membros.contains(norm)) {
      return true;
    }
    for (Map.Entry<String, LinkedHashSet<String>> e : grupos.entrySet()) {
      if (e.getKey() != null && e.getKey().equalsIgnoreCase(nomeGrupo.trim()) && e.getValue().contains(norm)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resolve a lista de destinatarios UDP elegiveis para fan-out de mensagem em um grupo desta particao,
   * validando a participacao do remetente e excluindo o proprio autor.
   */
  public synchronized List<InfoUser> getMembrosEnvio(String nomeGrupo, InfoUser usuarioRemetente) {
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return new ArrayList<>();
    }
    String normRemetente = normalizar(usuarioRemetente.getNome());
    LinkedHashSet<String> membros = grupos.get(nomeGrupo);

    if (!membros.contains(normRemetente)) {
      System.out.println("[GERENCIADOR] [WARNING] Usuario '" + usuarioRemetente.getNome()
          + "' tentou enviar mensagem para grupo '" + nomeGrupo + "' sem pertencer a ele.");
      return new ArrayList<>();
    }

    List<InfoUser> destinatarios = new ArrayList<>();
    for (String m : membros) {
      if (!m.equals(normRemetente)) {
        InfoUser u = usuarios.get(m);
        if (u == null) {
          u = endpointsMembros.get(m);
        }
        if (u != null) {
          destinatarios.add(u);
        }
      }
    }
    return destinatarios;
  }

  /** Lista os nomes de todos os grupos publicos hospedados nesta particao. */
  public synchronized List<String> listarGrupos() {
    List<String> lista = new ArrayList<>();
    for (String g : grupos.keySet()) {
      if (g != null && !g.startsWith("@")) {
        lista.add(g);
      }
    }
    return lista;
  }

  public synchronized Set<InfoUser> getTodosUsuariosAtivos() {
    return new HashSet<>(usuarios.values());
  }

  /**
   * Retorna a uniao deduplicada por endpoint fisico (IP:PortaUDP) de:
   *  1) todos os usuarios ativos cadastrados nesta particao (usuarios),
   *  2) todos os endpoints de membros cross-shard em grupos desta particao (endpointsMembros),
   *  3) todos os autores cross-shard rastreados em mensagens pendentes/confirmadas (mensagens).
   * Deduplica por IP:Porta (e nao por InfoUser.equals, que compara apenas nome) para garantir
   * que nenhum cliente em grupo cross-shard ou aguardando CONFIRM perca o SERVER_CHANGED no failover!
   */
  public synchronized List<InfoUser> getTodosDestinatariosNotificacao() {
    Map<String, InfoUser> porEndpoint = new LinkedHashMap<>();

    for (InfoUser u : usuarios.values()) {
      registrarEndpointNotificacao(porEndpoint, u);
    }
    for (InfoUser ep : endpointsMembros.values()) {
      registrarEndpointNotificacao(porEndpoint, ep);
    }
    for (RegistroMensagem reg : mensagens.values()) {
      if (reg != null && reg.getIpAutor() != null && !reg.getIpAutor().trim().isEmpty() && reg.getPortaAutor() > 0) {
        InfoUser autorEp = new InfoUser(
            reg.getAutor() != null ? reg.getAutor() : "autor",
            reg.getIpAutor().trim(),
            reg.getPortaAutor());
        registrarEndpointNotificacao(porEndpoint, autorEp);
      }
    }

    return new ArrayList<>(porEndpoint.values());
  }

  private static void registrarEndpointNotificacao(Map<String, InfoUser> mapa, InfoUser info) {
    if (info == null || info.getIp() == null || info.getIp().trim().isEmpty() || info.getPorta() <= 0) {
      return;
    }
    String chave = info.getIp().trim() + ":" + info.getPorta();
    mapa.putIfAbsent(chave, new InfoUser(info.getNome(), info.getIp().trim(), info.getPorta()));
  }

  /** Busca um usuario registrado diretamente no mapa primario desta particao pelo codinome. */
  public synchronized InfoUser buscarUsuarioPorNome(String nome) {
    if (nome == null) {
      return null;
    }
    String busca = nome.trim();
    if (busca.startsWith("@")) {
      busca = busca.substring(1).trim();
    }
    return usuarios.get(normalizar(busca));
  }

  /**
   * Busca o endpoint (IP/Porta UDP) conhecido nesta particao, consultando primeiro os usuarios
   * cadastrados neste shard e, em seguida, os endpoints de membros cross-shard registrados no JOIN.
   */
  public synchronized InfoUser buscarEndpointConhecido(String nome) {
    if (nome == null) {
      return null;
    }
    String busca = nome.trim();
    if (busca.startsWith("@")) {
      busca = busca.substring(1).trim();
    }
    String norm = normalizar(busca);
    InfoUser u = usuarios.get(norm);
    if (u != null) {
      return u;
    }
    return endpointsMembros.get(norm);
  }

  /** Verifica em O(1) se existe bloqueio unilateral ou mutuo registrado entre dois usuarios. */
  public synchronized boolean isBloqueadoMutuo(String u1, String u2) {
    if (u1 == null || u2 == null) {
      return false;
    }
    String n1 = normalizar(u1);
    String n2 = normalizar(u2);
    boolean b1 = bloqueios.containsKey(n1) && bloqueios.get(n1).contains(n2);
    boolean b2 = bloqueios.containsKey(n2) && bloqueios.get(n2).contains(n1);
    return b1 || b2;
  }

  public synchronized RegistroMensagem getRegistroMensagem(String idMensagem) {
    if (idMensagem == null) {
      return null;
    }
    return mensagens.get(idMensagem.trim());
  }

  public synchronized String getDonoMensagem(String idMensagem) {
    if (idMensagem == null) {
      return null;
    }
    RegistroMensagem reg = mensagens.get(idMensagem.trim());
    return (reg != null) ? reg.getAutor() : null;
  }

  // =========================================================================
  // RETROCOMPATIBILIDADE DE CODIGO (Delegam para aplicar)
  // =========================================================================

  /** Atalho sincronizado de ingresso em grupo delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized boolean join(String nomeGrupo, InfoUser usuario) {
    Resultado r = aplicar(Operacao.entrarGrupo(null, nomeGrupo, usuario.getNome(), usuario.getIp(), usuario.getPorta()));
    return r.isSucesso();
  }

  /** Atalho sincronizado de saida de grupo delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized boolean leave(String nomeGrupo, InfoUser usuario) {
    Resultado r = aplicar(Operacao.sairGrupo(null, nomeGrupo, usuario.getNome()));
    return r.isSucesso();
  }

  /** Atalho sincronizado de registro de usuario delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized boolean registrarUsuario(InfoUser usuario) {
    Resultado r = aplicar(Operacao.registrarUsuario(null, usuario.getNome(), usuario.getIp(), usuario.getPorta()));
    return r.isSucesso();
  }

  /** Atalho sincronizado de remocao de usuario delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized void removerUsuario(InfoUser usuario) {
    if (usuario != null) {
      aplicar(Operacao.removerUsuario(null, usuario.getNome()));
    }
  }

  /** Atalho sincronizado de bloqueio de usuario delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized boolean bloquear(String bloqueador, String bloqueado) {
    Resultado r = aplicar(Operacao.bloquear(null, bloqueador, bloqueado));
    return r.isSucesso();
  }

  /** Atalho sincronizado de desbloqueio de usuario delegando ao pipeline deterministico {@link #aplicar(Operacao)}. */
  public synchronized boolean desbloquear(String bloqueador, String bloqueado) {
    Resultado r = aplicar(Operacao.desbloquear(null, bloqueador, bloqueado));
    return r.isSucesso();
  }

  // =========================================================================
  // REPLICACAO: SNAPSHOT, VERSAO E HASH CANONICO
  // =========================================================================

  public synchronized long getVersao() {
    return versao;
  }

  public synchronized void setVersao(long versao) {
    this.versao = versao;
  }

  /**
   * Exporta todo o estado profundo da estrutura em vetor de bytes serializado,
   * incluindo usuarios, endpointsMembros (cross-shard), grupos, bloqueios,
   * mensagens (estado da APDU de Verificacao / CONFIRM) e cache de idempotencia (requisicoesProcessadas).
   */
  public synchronized byte[] exportarSnapshot() {
    try {
      Map<String, InfoUser> copiaUsuarios = new HashMap<>();
      for (Map.Entry<String, InfoUser> e : this.usuarios.entrySet()) {
        InfoUser u = e.getValue();
        if (u != null) {
          copiaUsuarios.put(e.getKey(), new InfoUser(u.getNome(), u.getIp(), u.getPorta()));
        }
      }

      Map<String, InfoUser> copiaEndpoints = new HashMap<>();
      for (Map.Entry<String, InfoUser> e : this.endpointsMembros.entrySet()) {
        InfoUser ep = e.getValue();
        if (ep != null) {
          copiaEndpoints.put(e.getKey(), new InfoUser(ep.getNome(), ep.getIp(), ep.getPorta()));
        }
      }

      Map<String, LinkedHashSet<String>> copiaGrupos = new HashMap<>();
      for (Map.Entry<String, LinkedHashSet<String>> e : this.grupos.entrySet()) {
        if (e.getValue() != null) {
          copiaGrupos.put(e.getKey(), new LinkedHashSet<>(e.getValue()));
        }
      }

      Map<String, Set<String>> copiaBloqueios = new HashMap<>();
      for (Map.Entry<String, Set<String>> e : this.bloqueios.entrySet()) {
        if (e.getValue() != null) {
          copiaBloqueios.put(e.getKey(), new HashSet<>(e.getValue()));
        }
      }

      Map<String, RegistroMensagem> copiaMensagens = new HashMap<>();
      for (Map.Entry<String, RegistroMensagem> e : this.mensagens.entrySet()) {
        if (e.getValue() != null) {
          copiaMensagens.put(e.getKey(), e.getValue().copiar());
        }
      }

      LinkedHashMap<String, Resultado> copiaRequisicoes = new LinkedHashMap<>(this.requisicoesProcessadas);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeLong(this.versao);
      oos.writeUnshared(copiaUsuarios);
      oos.writeUnshared(copiaEndpoints);
      oos.writeUnshared(copiaGrupos);
      oos.writeUnshared(copiaBloqueios);
      oos.writeUnshared(copiaMensagens);
      oos.writeUnshared(copiaRequisicoes);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      System.err.println("[GERENCIADOR] [ERROR] Falha ao exportar snapshot: " + e.getMessage());
      return new byte[0];
    }
  }

  /**
   * Alias semantico para aplicacao atomica de snapshot completo recebido do lider.
   */
  public synchronized void aplicarSnapshot(byte[] dadosSnapshot) throws Exception {
    importarSnapshot(dadosSnapshot);
  }

  /**
   * Substitui atomicamente todo o estado em memoria a partir de um snapshot serializado,
   * restaurando copias profundas de todos os mapas (usuarios, endpointsMembros, grupos,
   * bloqueios, mensagens/CONFIRM e requisicoesProcessadas).
   */
  @SuppressWarnings("unchecked")
  public synchronized void importarSnapshot(byte[] dadosSnapshot) throws Exception {
    if (dadosSnapshot == null || dadosSnapshot.length == 0) {
      throw new IllegalArgumentException("Dados de snapshot invalidos");
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(dadosSnapshot);
    ObjectInputStream ois = new ObjectInputStream(bais);

    long versaoSnap = ois.readLong();
    Map<String, InfoUser> snapUsuarios = (Map<String, InfoUser>) ois.readObject();
    Map<String, InfoUser> snapEndpoints = (Map<String, InfoUser>) ois.readObject();
    Map<String, LinkedHashSet<String>> snapGrupos = (Map<String, LinkedHashSet<String>>) ois.readObject();
    Map<String, Set<String>> snapBloqueios = (Map<String, Set<String>>) ois.readObject();
    Map<String, RegistroMensagem> snapMensagens = (Map<String, RegistroMensagem>) ois.readObject();
    Map<String, Resultado> snapRequisicoes = null;
    try {
      Object extra = ois.readObject();
      if (extra instanceof Map) {
        snapRequisicoes = (Map<String, Resultado>) extra;
      }
    } catch (java.io.EOFException ignored) {
      // Compatibilidade retroativa com snapshots sem cache de idempotencia
    }

    this.usuarios.clear();
    if (snapUsuarios != null) {
      for (Map.Entry<String, InfoUser> e : snapUsuarios.entrySet()) {
        InfoUser u = e.getValue();
        if (u != null) {
          this.usuarios.put(e.getKey(), new InfoUser(u.getNome(), u.getIp(), u.getPorta()));
        }
      }
    }

    this.endpointsMembros.clear();
    if (snapEndpoints != null) {
      for (Map.Entry<String, InfoUser> e : snapEndpoints.entrySet()) {
        InfoUser ep = e.getValue();
        if (ep != null) {
          this.endpointsMembros.put(e.getKey(), new InfoUser(ep.getNome(), ep.getIp(), ep.getPorta()));
        }
      }
    }

    this.grupos.clear();
    if (snapGrupos != null) {
      for (Map.Entry<String, LinkedHashSet<String>> e : snapGrupos.entrySet()) {
        if (e.getValue() != null) {
          this.grupos.put(e.getKey(), new LinkedHashSet<>(e.getValue()));
        }
      }
    }

    this.bloqueios.clear();
    if (snapBloqueios != null) {
      for (Map.Entry<String, Set<String>> e : snapBloqueios.entrySet()) {
        if (e.getValue() != null) {
          this.bloqueios.put(e.getKey(), new HashSet<>(e.getValue()));
        }
      }
    }

    this.mensagens.clear();
    if (snapMensagens != null) {
      for (Map.Entry<String, RegistroMensagem> e : snapMensagens.entrySet()) {
        if (e.getValue() != null) {
          this.mensagens.put(e.getKey(), e.getValue().copiar());
        }
      }
    }

    this.requisicoesProcessadas.clear();
    if (snapRequisicoes != null) {
      this.requisicoesProcessadas.putAll(snapRequisicoes);
    }

    this.versao = versaoSnap;
    System.out.println("[GERENCIADOR] [INFO] Snapshot importado com sucesso. Versao atual: " + this.versao
        + " | Usuarios: " + this.usuarios.size()
        + " | EndpointsCrossShard: " + this.endpointsMembros.size()
        + " | Grupos: " + this.grupos.size()
        + " | Mensagens(CONFIRM): " + this.mensagens.size());
  }

  /**
   * Calcula um hash CRC32 canonico sobre representacao estritamente ordenada de TODO o estado
   * (usuarios, endpointsMembros cross-shard, grupos, bloqueios e mensagens/CONFIRM).
   * Independe da ordem interna das tabelas de hash e permite validacao deterministica
   * de congruencia de estado entre os nos do cluster.
   */
  public synchronized long calcularHash() {
    CRC32 crc = new CRC32();
    crc.update(String.valueOf(this.versao).getBytes());

    // 1. Usuarios ordenados
    List<String> usersKeys = new ArrayList<>(usuarios.keySet());
    Collections.sort(usersKeys);
    for (String k : usersKeys) {
      InfoUser u = usuarios.get(k);
      if (u != null) {
        crc.update(String.format("U:%s:%s:%d;", u.getNome(), u.getIp(), u.getPorta()).getBytes());
      }
    }

    // 2. Endpoints de membros cross-shard ordenados
    List<String> epKeys = new ArrayList<>(endpointsMembros.keySet());
    Collections.sort(epKeys);
    for (String k : epKeys) {
      InfoUser ep = endpointsMembros.get(k);
      if (ep != null) {
        crc.update(String.format("EP:%s:%s:%d;", ep.getNome(), ep.getIp(), ep.getPorta()).getBytes());
      }
    }

    // 3. Grupos e membros ordenados
    List<String> gruposKeys = new ArrayList<>(grupos.keySet());
    Collections.sort(gruposKeys);
    for (String g : gruposKeys) {
      crc.update(String.format("G:%s[", g).getBytes());
      List<String> membrosList = new ArrayList<>(grupos.get(g));
      Collections.sort(membrosList);
      for (String m : membrosList) {
        crc.update((m + ",").getBytes());
      }
      crc.update("];".getBytes());
    }

    // 4. Bloqueios ordenados
    List<String> blkKeys = new ArrayList<>(bloqueios.keySet());
    Collections.sort(blkKeys);
    for (String b : blkKeys) {
      crc.update(String.format("B:%s[", b).getBytes());
      List<String> blkList = new ArrayList<>(bloqueios.get(b));
      Collections.sort(blkList);
      for (String target : blkList) {
        crc.update((target + ",").getBytes());
      }
      crc.update("];".getBytes());
    }

    // 5. Mensagens e estado de verificacao (CONFIRM ticks) ordenados
    List<String> msgKeys = new ArrayList<>(mensagens.keySet());
    Collections.sort(msgKeys);
    for (String mId : msgKeys) {
      RegistroMensagem reg = mensagens.get(mId);
      if (reg != null) {
        List<String> esp = new ArrayList<>(reg.getDestinatariosEsperados());
        List<String> ent = new ArrayList<>(reg.getDestinatariosEntregues());
        List<String> lid = new ArrayList<>(reg.getDestinatariosLidos());
        Collections.sort(esp);
        Collections.sort(ent);
        Collections.sort(lid);
        crc.update(String.format("M:%s:%s:%s:%d:%b:%b:%b:%s:%s:%s;",
            reg.getIdMensagem(), reg.getAutor(), reg.getIpAutor(), reg.getPortaAutor(),
            reg.isPrivado(), reg.isEntregueNotificado(), reg.isLidoNotificado(),
            esp.toString(), ent.toString(), lid.toString()).getBytes());
      }
    }

    return crc.getValue();
  }

  /**
   * Imprime no console a estrutura de dados completa, versao e hash CRC32.
   */
  public synchronized void imprimirEstado() {
    String rotuloShard = (idShard >= 0) ? Particionador.nome(idShard) : "GLOBAL/MONOLITICO";
    System.out.println("\n=== ESTRUTURA DE DADOS DA PARTICAO (" + rotuloShard + ") ===");
    System.out.printf("Versao: %d | Hash CRC32: 0x%08X\n", versao, calcularHash());
    System.out.println("Usuarios Desta Particao (" + usuarios.size() + "):");
    for (InfoUser u : usuarios.values()) {
      System.out.println("  - " + u.getNome() + " (" + u.getIp() + ":" + u.getPorta() + ")");
    }
    System.out.println("Grupos Desta Particao (" + grupos.size() + "):");
    for (Map.Entry<String, LinkedHashSet<String>> e : grupos.entrySet()) {
      System.out.println("  - " + e.getKey() + " -> membros: " + e.getValue());
    }
    System.out.println("Bloqueios Replicados Globalmente (" + bloqueios.size() + "):");
    for (Map.Entry<String, Set<String>> e : bloqueios.entrySet()) {
      System.out.println("  - " + e.getKey() + " bloqueou: " + e.getValue());
    }
    System.out.println("Mensagens Rastreadas Nesta Particao: " + mensagens.size());
    System.out.println("========================================================\n");
  }

}
