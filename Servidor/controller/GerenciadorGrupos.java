/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
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

  // Estado central do servidor
  private final Map<String, InfoUser> usuarios;
  private final Map<String, LinkedHashSet<String>> grupos;
  private final Map<String, Set<String>> bloqueios;
  private final Map<String, RegistroMensagem> mensagens;
  private final LinkedHashMap<String, Resultado> requisicoesProcessadas;
  private long versao;

  private static final int MAX_CACHE_REQUISICOES = 2000;

  /**
   * Construtor padrao. Inicializa todas as estruturas de dados em memoria.
   */
  public GerenciadorGrupos() {
    this.usuarios = new HashMap<>();
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

  public void vincularSessao(String nomeUsuario, Object sessao) {
    if (nomeUsuario == null || sessao == null) return;
    sessoesAtivas.put(normalizar(nomeUsuario), sessao);
  }

  public void desvincularSessao(String nomeUsuario, Object sessao) {
    if (nomeUsuario == null || sessao == null) return;
    sessoesAtivas.remove(normalizar(nomeUsuario), sessao);
  }

  /**
   * Remove com seguranca um cliente que perdeu a conexao TCP, garantindo que
   * clientes que ja reconectaram com outra sessao nao sejam derrubados por engano.
   */
  public synchronized Resultado desconectarClienteInativo(String nomeUsuario, Object sessao) {
    if (!ativo) {
      return Resultado.ok("Servidor nao ativo");
    }
    if (nomeUsuario == null || sessao == null) {
      return Resultado.ok("Nenhum usuario");
    }
    String norm = normalizar(nomeUsuario);
    Object sessaoAtual = sessoesAtivas.get(norm);
    if (sessaoAtual != null && sessaoAtual != sessao) {
      System.out.println("[GERENCIADOR] [INFO] Desconexao ignorada para '" + nomeUsuario + "' pois usuario ja possui nova sessao ativa.");
      return Resultado.ok("Sessao ja renovada");
    }
    sessoesAtivas.remove(norm);

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

    if (existente != null) {
      existente.setIp(op.getIp());
      existente.setPorta(op.getPorta());
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' atualizou sessao (IP: " + op.getIp()
          + " | Porta: " + op.getPorta() + "). Versao: " + versao);
      return Resultado.ok("registrado com sucesso", false);
    } else {
      InfoUser novo = new InfoUser(nome.trim(), op.getIp(), op.getPorta());
      usuarios.put(norm, novo);
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' registrado com sucesso. Total ativos: "
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

    boolean membroRemovidoDeAlgumGrupo = false;
    List<String> gruposParaRemover = new ArrayList<>();
    for (Map.Entry<String, LinkedHashSet<String>> entry : grupos.entrySet()) {
      String nomeGrupo = entry.getKey();
      LinkedHashSet<String> membros = entry.getValue();
      if (membros.remove(norm)) {
        membroRemovidoDeAlgumGrupo = true;
        System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' removido do grupo '" + nomeGrupo + "'.");
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
    System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' desconectado do servidor. Notificar clientes: " + notificar + " | Versao: " + versao);
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

    // Auto-registro atomico caso o usuario ainda nao conste na tabela global
    if (!usuarios.containsKey(norm)) {
      usuarios.put(norm, new InfoUser(nome.trim(), op.getIp(), op.getPorta()));
      usuarioNovo = true;
      System.out.println("[GERENCIADOR] [INFO] Usuario '" + nome + "' auto-registrado via JOIN.");
    } else {
      InfoUser u = usuarios.get(norm);
      if (op.getIp() != null && !op.getIp().isEmpty()) {
        u.setIp(op.getIp());
      }
      if (op.getPorta() > 0) {
        u.setPorta(op.getPorta());
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
    if (bloqueios.containsKey(bloqueador)) {
      bloqueios.get(bloqueador).remove(bloqueado);
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

    RegistroMensagem reg = new RegistroMensagem(idTrim, op.getNomeGrupo(), op.getNomeUsuario(), op.getEsperados(), op.getTimestamp());
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

    RegistroMensagem reg = new RegistroMensagem(idTrim, op.getDestinatario(), op.getNomeUsuario(), op.getTimestamp());
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

  public synchronized List<InfoUser> getTodosMembros(String nomeGrupo) {
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return new ArrayList<>();
    }
    List<InfoUser> lista = new ArrayList<>();
    for (String nomeNorm : grupos.get(nomeGrupo)) {
      InfoUser u = usuarios.get(nomeNorm);
      if (u != null) {
        lista.add(u);
      }
    }
    return lista;
  }

  public synchronized Set<String> getMembrosNomes(String nomeGrupo) {
    if (nomeGrupo == null || !grupos.containsKey(nomeGrupo)) {
      return Collections.emptySet();
    }
    return new HashSet<>(grupos.get(nomeGrupo));
  }

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
        if (u != null) {
          destinatarios.add(u);
        }
      }
    }
    return destinatarios;
  }

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

  public synchronized boolean join(String nomeGrupo, InfoUser usuario) {
    Resultado r = aplicar(Operacao.entrarGrupo(null, nomeGrupo, usuario.getNome(), usuario.getIp(), usuario.getPorta()));
    return r.isSucesso();
  }

  public synchronized boolean leave(String nomeGrupo, InfoUser usuario) {
    Resultado r = aplicar(Operacao.sairGrupo(null, nomeGrupo, usuario.getNome()));
    return r.isSucesso();
  }

  public synchronized boolean registrarUsuario(InfoUser usuario) {
    Resultado r = aplicar(Operacao.registrarUsuario(null, usuario.getNome(), usuario.getIp(), usuario.getPorta()));
    return r.isSucesso();
  }

  public synchronized void removerUsuario(InfoUser usuario) {
    if (usuario != null) {
      aplicar(Operacao.removerUsuario(null, usuario.getNome()));
    }
  }

  public synchronized boolean bloquear(String bloqueador, String bloqueado) {
    Resultado r = aplicar(Operacao.bloquear(null, bloqueador, bloqueado));
    return r.isSucesso();
  }

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
   * Exporta todo o estado profundo da estrutura em vetor de bytes serializado.
   */
  public synchronized byte[] exportarSnapshot() {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeLong(this.versao);
      oos.writeObject(new HashMap<>(this.usuarios));
      oos.writeObject(new HashMap<>(this.grupos));
      oos.writeObject(new HashMap<>(this.bloqueios));
      oos.writeObject(new HashMap<>(this.mensagens));
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      System.err.println("[GERENCIADOR] [ERROR] Falha ao exportar snapshot: " + e.getMessage());
      return new byte[0];
    }
  }

  /**
   * Substitui atomicamente todo o estado em memoria a partir de um snapshot serializado.
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
    Map<String, LinkedHashSet<String>> snapGrupos = (Map<String, LinkedHashSet<String>>) ois.readObject();
    Map<String, Set<String>> snapBloqueios = (Map<String, Set<String>>) ois.readObject();
    Map<String, RegistroMensagem> snapMensagens = (Map<String, RegistroMensagem>) ois.readObject();

    this.usuarios.clear();
    this.usuarios.putAll(snapUsuarios);

    this.grupos.clear();
    this.grupos.putAll(snapGrupos);

    this.bloqueios.clear();
    this.bloqueios.putAll(snapBloqueios);

    this.mensagens.clear();
    this.mensagens.putAll(snapMensagens);

    this.versao = versaoSnap;
    System.out.println("[GERENCIADOR] [INFO] Snapshot importado com sucesso. Versao atual: " + this.versao
        + " | Total usuarios: " + this.usuarios.size() + " | Grupos: " + this.grupos.size());
  }

  /**
   * Calcula um hash CRC32 canonico sobre representacao estritamente ordenada do estado.
   * Independe da ordem interna das tabelas de hash e permite validacao deterministica
   * de congruencia de estado entre os nos do cluster.
   */
  public synchronized long calcularHash() {
    CRC32 crc = new CRC32();
    crc.update(String.valueOf(this.versao).getBytes());

    // Usuarios ordenados
    List<String> usersKeys = new ArrayList<>(usuarios.keySet());
    Collections.sort(usersKeys);
    for (String k : usersKeys) {
      InfoUser u = usuarios.get(k);
      crc.update(String.format("U:%s:%s:%d;", u.getNome(), u.getIp(), u.getPorta()).getBytes());
    }

    // Grupos e membros ordenados
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

    // Bloqueios ordenados
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

    return crc.getValue();
  }

  /**
   * Imprime no console a estrutura de dados completa, versao e hash CRC32.
   */
  public synchronized void imprimirEstado() {
    System.out.println("\n=== ESTRUTURA DE DADOS REPLICADA (GerenciadorGrupos) ===");
    System.out.printf("Versao: %d | Hash CRC32: 0x%08X\n", versao, calcularHash());
    System.out.println("Usuarios Online (" + usuarios.size() + "):");
    for (InfoUser u : usuarios.values()) {
      System.out.println("  - " + u.getNome() + " (" + u.getIp() + ":" + u.getPorta() + ")");
    }
    System.out.println("Grupos Ativos (" + grupos.size() + "):");
    for (Map.Entry<String, LinkedHashSet<String>> e : grupos.entrySet()) {
      System.out.println("  - " + e.getKey() + " -> membros: " + e.getValue());
    }
    System.out.println("Bloqueios Ativos (" + bloqueios.size() + "):");
    for (Map.Entry<String, Set<String>> e : bloqueios.entrySet()) {
      System.out.println("  - " + e.getKey() + " bloqueou: " + e.getValue());
    }
    System.out.println("Mensagens Rastreadas em Memoria: " + mensagens.size());
    System.out.println("========================================================\n");
  }

}
