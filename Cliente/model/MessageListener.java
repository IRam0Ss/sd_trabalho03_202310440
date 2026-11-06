/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: MessageListener
* Funcao.............: Interface de callback Observer para eventos de rede, mensagens e failover.
*************************************************************** */

package model;

import utils.InfoUser;

/**
 * Interface observer para despacho e tratamento de eventos de mensageria,
 * ciclo de vida da rede e transicoes de failover transparente no cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public interface MessageListener {

  void onMessageReceived(String idMensagem, String destino, InfoUser remetente, String mensagem, boolean isPrivate, boolean isVisualizacaoUnica);

  void onShutdown();

  void onUpdateUsers();

  void onTickReceived(String idMensagem, int status, String nomeConfirmou);

  /**
   * Disparado quando a conexao com o servidor ativo e interrompida e o cliente busca um novo lider.
   */
  default void onConexaoPerdida() {}

  /**
   * Disparado quando o cliente conclui o failover e restabelece a sessao em um novo lider do cluster.
   */
  default void onReconectado(String ip, int portaTcp, int portaUdp) {}

  /**
   * Disparado quando o servidor envia notificacao SERVER_CHANGED via UDP.
   */
  default void onServerChanged(String novoIp, int portaTcp, int portaUdp) {}

}
