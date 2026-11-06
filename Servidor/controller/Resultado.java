/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: Resultado
* Funcao.............: Modela o retorno deterministico da execucao de uma Operacao no GerenciadorGrupos.
*************************************************************** */

package controller;

import java.io.Serializable;

/**
 * Encapsula o resultado da aplicacao de uma operacao no estado.
 * Armazenavel no cache de idempotencia para devolver a mesma resposta
 * a comandos TCP reenviados apos um failover.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class Resultado implements Serializable {

  private static final long serialVersionUID = 1L;

  private final boolean sucesso;
  private final String operacaoRetorno; // "OK" ou "ERRO"
  private final String mensagem;
  private final boolean notificarAtualizacaoUsuarios;
  private final boolean notificarAutorConfirm;
  private final int confirmStatus;
  private final boolean duplicada;

  public Resultado(boolean sucesso, String operacaoRetorno, String mensagem) {
    this(sucesso, operacaoRetorno, mensagem, false, false, 0, false);
  }

  public Resultado(boolean sucesso, String operacaoRetorno, String mensagem, boolean notificarAtualizacaoUsuarios) {
    this(sucesso, operacaoRetorno, mensagem, notificarAtualizacaoUsuarios, false, 0, false);
  }

  public Resultado(boolean sucesso, String operacaoRetorno, String mensagem,
                   boolean notificarAtualizacaoUsuarios, boolean notificarAutorConfirm,
                   int confirmStatus, boolean duplicada) {
    this.sucesso = sucesso;
    this.operacaoRetorno = operacaoRetorno;
    this.mensagem = mensagem;
    this.notificarAtualizacaoUsuarios = notificarAtualizacaoUsuarios;
    this.notificarAutorConfirm = notificarAutorConfirm;
    this.confirmStatus = confirmStatus;
    this.duplicada = duplicada;
  }

  public static Resultado ok(String mensagem) {
    return new Resultado(true, "OK", mensagem);
  }

  public static Resultado ok(String mensagem, boolean notificarUsuarios) {
    return new Resultado(true, "OK", mensagem, notificarUsuarios);
  }

  public static Resultado erro(String mensagem) {
    return new Resultado(false, "ERRO", mensagem);
  }

  public boolean isSucesso() {
    return sucesso;
  }

  public String getOperacaoRetorno() {
    return operacaoRetorno;
  }

  public String getMensagem() {
    return mensagem;
  }

  public boolean isNotificarAtualizacaoUsuarios() {
    return notificarAtualizacaoUsuarios;
  }

  public boolean isNotificarAutorConfirm() {
    return notificarAutorConfirm;
  }

  public int getConfirmStatus() {
    return confirmStatus;
  }

  public boolean isDuplicada() {
    return duplicada;
  }

  @Override
  public String toString() {
    return String.format("Resultado[%s: %s | notifUsers=%b | notifTick=%b]",
        operacaoRetorno, mensagem, notificarAtualizacaoUsuarios, notificarAutorConfirm);
  }
}

