/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: TipoMensagemCluster
* Funcao.............: Tipos de mensagens trocadas no canal de comunicacao entre servidores.
*************************************************************** */

package cluster;

import java.io.Serializable;

/**
 * Enumeracao de todas as mensagens de controle, sincronizacao, heartbeat e eleicao
 * que trafegam pelo canal TCP ponto-a-ponto entre os servidores do cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public enum TipoMensagemCluster implements Serializable {
  HELLO,
  HEARTBEAT,
  REPLICAR,
  ACK,
  NACK,
  PEDIDO_SYNC,
  SNAPSHOT,
  LOTE_OPS,
  SYNC_OK,
  PROPOSTA,
  VOTO,
  COORDENADOR,
  RENUNCIA,
  SAINDO,
  /** Fase 3: anuncio periodico de papel/lider entre shards. */
  ANUNCIO_SHARD,
  /** Fase 3: operacao encaminhada ao lider do shard dono da chave. */
  ROTEAR_OP,
  /** Fase 3: resposta a uma operacao roteada. */
  RESP_OP,
  /** Fase 4: consulta read-only entre shards (scatter-gather LIST/USERS e MEMBERS). */
  CONSULTA_SHARD,
  /** Fase 4: resposta de consulta read-only entre shards. */
  RESP_CONSULTA_SHARD,
  /** Fase 5: encaminhamento de APDU UDP (SEND, SENDVU, SENDPVT, CONFIRM) ao lider do shard responsavel. */
  ENCAMINHAR_UDP,
  /** Fase 6: notifica o no de entrada para fechar o socket TCP de um cliente apos amnesia de shard. */
  INVALIDAR_SESSAO_CLIENTE
}

