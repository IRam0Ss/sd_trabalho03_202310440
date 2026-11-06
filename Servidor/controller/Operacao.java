/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
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

  public Operacao(TipoOperacao tipo, String idRequisicao) {
    this.tipo = tipo;
    this.idRequisicao = idRequisicao;
    this.timestamp = System.currentTimeMillis();
  }

  // --- Metodos Fabrica ---

  public static Operacao registrarUsuario(String idReq, String nome, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_USUARIO, idReq);
    op.nomeUsuario = nome;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  public static Operacao removerUsuario(String idReq, String nome) {
    Operacao op = new Operacao(TipoOperacao.REMOVER_USUARIO, idReq);
    op.nomeUsuario = nome;
    return op;
  }

  public static Operacao entrarGrupo(String idReq, String nomeGrupo, String nomeUsuario, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.ENTRAR_GRUPO, idReq);
    op.nomeGrupo = nomeGrupo;
    op.nomeUsuario = nomeUsuario;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  public static Operacao sairGrupo(String idReq, String nomeGrupo, String nomeUsuario) {
    Operacao op = new Operacao(TipoOperacao.SAIR_GRUPO, idReq);
    op.nomeGrupo = nomeGrupo;
    op.nomeUsuario = nomeUsuario;
    return op;
  }

  public static Operacao bloquear(String idReq, String bloqueador, String bloqueado) {
    Operacao op = new Operacao(TipoOperacao.BLOQUEAR, idReq);
    op.nomeUsuario = bloqueador;
    op.destinatario = bloqueado;
    return op;
  }

  public static Operacao desbloquear(String idReq, String bloqueador, String bloqueado) {
    Operacao op = new Operacao(TipoOperacao.DESBLOQUEAR, idReq);
    op.nomeUsuario = bloqueador;
    op.destinatario = bloqueado;
    return op;
  }

  public static Operacao atualizarEndpoint(String nome, String ip, int porta) {
    Operacao op = new Operacao(TipoOperacao.ATUALIZAR_ENDPOINT, null);
    op.nomeUsuario = nome;
    op.ip = ip;
    op.porta = porta;
    return op;
  }

  public static Operacao registrarMsgGrupo(String idMensagem, String autor, String nomeGrupo, Set<String> esperados, boolean vu) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_MSG_GRUPO, idMensagem);
    op.idMensagem = idMensagem;
    op.nomeUsuario = autor;
    op.nomeGrupo = nomeGrupo;
    op.esperados = (esperados != null) ? new HashSet<>(esperados) : new HashSet<>();
    op.visualizacaoUnica = vu;
    return op;
  }

  public static Operacao registrarMsgPrivada(String idMensagem, String autor, String destinatario) {
    Operacao op = new Operacao(TipoOperacao.REGISTRAR_MSG_PRIVADA, idMensagem);
    op.idMensagem = idMensagem;
    op.nomeUsuario = autor;
    op.destinatario = destinatario;
    return op;
  }

  public static Operacao confirmarEntrega(String idMensagem, String confirmador) {
    Operacao op = new Operacao(TipoOperacao.CONFIRMAR_ENTREGA, null);
    op.idMensagem = idMensagem;
    op.nomeUsuario = confirmador;
    op.statusConfirm = 2;
    return op;
  }

  public static Operacao confirmarLeitura(String idMensagem, String confirmador) {
    Operacao op = new Operacao(TipoOperacao.CONFIRMAR_LEITURA, null);
    op.idMensagem = idMensagem;
    op.nomeUsuario = confirmador;
    op.statusConfirm = 3;
    return op;
  }

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

