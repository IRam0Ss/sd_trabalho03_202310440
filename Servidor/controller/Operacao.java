/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: Operacao
* Funcao.............: Modela uma mutacao determinista de estado a ser aplicada e replicada no cluster.
*************************************************************** */

package controller;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Representa um comando de transicao de estado no GerenciadorGrupos.
 * Carrega todos os parametros deterministicos necessarios para que
 * todos os servidores cheguem exatamente ao mesmo estado apos a aplicacao.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class Operacao implements Serializable {

  private static final long serialVersionUID = 1L;

  private final TipoOperacao tipo;
  private final String idRequisicao;
  private long versao;
  private long epoca;
  private long timestamp;
  private int idOrigem;

  // Parametros da operacao
  private String nomeUsuario;
  private String ip;
  private int porta;
  private String nomeGrupo;
  private String destinatario;
  private String idMensagem;
  private int statusConfirm;
  private Set<String> esperados;
  private boolean visualizacaoUnica;

  /**
   * Constroi uma operacao deterministica de transicao de estado identificada por chave de requisicao.
   *
   * @param tipo         Categoria da mutacao no estado.
   * @param idRequisicao Identificador unico para deduplicacao idempotente (opcional para eventos internos).
   */
  public Operacao(TipoOperacao tipo, String idRequisicao) {
    this.tipo = tipo;
    this.idRequisicao = idRequisicao;
    this.timestamp = System.currentTimeMillis();
  }

  // --- Metodos Fabrica ---

  /** Cria operacao deterministica de cadastro ou renovacao de sessao de usuario. */
  public static Operacao registrarUsuario(String idReq, String nome, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_USUARIO, idReq);
    op.nomeUsuario = nome;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  /** Cria operacao deterministica de remocao de usuario e faxina de participacao em salas. */
  public static Operacao removerUsuario(String idReq, String nome) {
    Operacao op = new Operacao(TipoOperacao.REMOVER_USUARIO, idReq);
    op.nomeUsuario = nome;
    return op;
  }

  /** Cria operacao deterministica de ingresso de usuario em grupo com cache de endpoint UDP. */
  public static Operacao entrarGrupo(String idReq, String nomeGrupo, String nomeUsuario, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.ENTRAR_GRUPO, idReq);
    op.nomeGrupo = nomeGrupo;
    op.nomeUsuario = nomeUsuario;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  /** Cria operacao deterministica de saida de usuario de um grupo especifico (ou {@code GLOBAL}). */
  public static Operacao sairGrupo(String idReq, String nomeGrupo, String nomeUsuario) {
    Operacao op = new Operacao(TipoOperacao.SAIR_GRUPO, idReq);
    op.nomeGrupo = nomeGrupo;
    op.nomeUsuario = nomeUsuario;
    return op;
  }

  /** Cria operacao deterministica de bloqueio de mensagens entre dois usuarios. */
  public static Operacao bloquear(String idReq, String bloqueador, String bloqueado) {
    Operacao op = new Operacao(TipoOperacao.BLOQUEAR, idReq);
    op.nomeUsuario = bloqueador;
    op.destinatario = bloqueado;
    return op;
  }

  /** Cria operacao deterministica de revogacao de bloqueio entre dois usuarios. */
  public static Operacao desbloquear(String idReq, String bloqueador, String bloqueado) {
    Operacao op = new Operacao(TipoOperacao.DESBLOQUEAR, idReq);
    op.nomeUsuario = bloqueador;
    op.destinatario = bloqueado;
    return op;
  }

  /** Cria operacao de atualizacao de endpoint UDP conhecido para um usuario. */
  public static Operacao atualizarEndpoint(String nome, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.ATUALIZAR_ENDPOINT, null);
    op.nomeUsuario = nome;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  /** Cria operacao de rastreamento de confirmacoes de entrega/leitura para mensagem de grupo. */
  public static Operacao registrarMsgGrupo(String idMensagem, String autor, String nomeGrupo, Set<String> esperados, boolean vu) {
    return registrarMsgGrupo(idMensagem, autor, null, 0, nomeGrupo, esperados, vu);
  }

  /** Cria operacao completa de rastreamento de mensagem de grupo preservando o endpoint UDP do autor. */
  public static Operacao registrarMsgGrupo(String idMensagem, String autor, String ipAutor, int portaAutor,
      String nomeGrupo, Set<String> esperados, boolean vu) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_MSG_GRUPO, idMensagem);
    op.idMensagem = idMensagem;
    op.nomeUsuario = autor;
    op.ip = ipAutor;
    op.porta = portaAutor;
    op.nomeGrupo = nomeGrupo;
    op.esperados = (esperados != null) ? new HashSet<>(esperados) : new HashSet<>();
    op.visualizacaoUnica = vu;
    return op;
  }

  /** Cria operacao de rastreamento de confirmacoes para mensagem privada direta. */
  public static Operacao registrarMsgPrivada(String idMensagem, String autor, String destinatario) {
    return registrarMsgPrivada(idMensagem, autor, null, 0, destinatario);
  }

  /** Cria operacao completa de rastreamento de mensagem privada preservando o endpoint UDP do autor. */
  public static Operacao registrarMsgPrivada(String idMensagem, String autor, String ipAutor, int portaAutor, String destinatario) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_MSG_PRIVADA, idMensagem);
    op.idMensagem = idMensagem;
    op.nomeUsuario = autor;
    op.ip = ipAutor;
    op.porta = portaAutor;
    op.destinatario = destinatario;
    return op;
  }

  /** Cria operacao de contabilizacao de recibo de entrega (status 2) para uma mensagem. */
  public static Operacao confirmarEntrega(String idMensagem, String confirmador) {
    Operacao op = new Operacao(TipoOperacao.CONFIRMAR_ENTREGA, null);
    op.idMensagem = idMensagem;
    op.nomeUsuario = confirmador;
    op.statusConfirm = 2;
    return op;
  }

  /** Cria operacao de contabilizacao de recibo de leitura (status 3) para uma mensagem. */
  public static Operacao confirmarLeitura(String idMensagem, String confirmador) {
    Operacao op = new Operacao(TipoOperacao.CONFIRMAR_LEITURA, null);
    op.idMensagem = idMensagem;
    op.nomeUsuario = confirmador;
    op.statusConfirm = 3;
    return op;
  }

  /** Cria operacao deterministica de expurgo de registros antigos de confirmacao por limite temporal. */
  public static Operacao limparMensagens(long timestampLimite) {
    Operacao op = new Operacao(TipoOperacao.LIMPAR_MENSAGENS, null);
    op.timestamp = timestampLimite;
    return op;
  }

  // --- Getters e Setters de Metadados de Cluster ---

  public TipoOperacao getTipo() {
    return tipo;
  }

  public String getIdRequisicao() {
    return idRequisicao;
  }

  public long getVersao() {
    return versao;
  }

  public void setVersao(long versao) {
    this.versao = versao;
  }

  public long getEpoca() {
    return epoca;
  }

  public void setEpoca(long epoca) {
    this.epoca = epoca;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getIdOrigem() {
    return idOrigem;
  }

  public void setIdOrigem(int idOrigem) {
    this.idOrigem = idOrigem;
  }

  // --- Getters de Parametros ---

  public String getNomeUsuario() {
    return nomeUsuario;
  }

  public String getIp() {
    return ip;
  }

  public int getPorta() {
    return porta;
  }

  public String getNomeGrupo() {
    return nomeGrupo;
  }

  public String getDestinatario() {
    return destinatario;
  }

  public String getIdMensagem() {
    return idMensagem;
  }

  public int getStatusConfirm() {
    return statusConfirm;
  }

  public Set<String> getEsperados() {
    return esperados != null ? Collections.unmodifiableSet(esperados) : Collections.emptySet();
  }

  public boolean isVisualizacaoUnica() {
    return visualizacaoUnica;
  }

  @Override
  public String toString() {
    return String.format("Operacao[v=%d, ep=%d, tipo=%s, user=%s, grupo=%s, req=%s]",
        versao, epoca, tipo, nomeUsuario, nomeGrupo, idRequisicao);
  }
}

