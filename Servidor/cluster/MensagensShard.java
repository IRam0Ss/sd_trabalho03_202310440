/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: MensagensShard
* Funcao.............: Payloads serializaveis da comunicacao entre shards (anuncio, pedido roteado, resposta).
*************************************************************** */

package cluster;

import java.io.Serializable;
import java.util.ArrayList;

import Protocol.APDU;
import controller.Operacao;
import controller.Resultado;
import utils.InfoUser;

/**
 * Conjunto de payloads trafegados dentro de {@link MensagemCluster} nas mensagens
 * ANUNCIO_SHARD, ROTEAR_OP e RESP_OP.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public final class MensagensShard {

  private MensagensShard() {
  }

  /** Anuncio periodico: "sou o no X do shard S, meu papel e P, o lider que conheco e L". */
  public static class AnuncioShard implements Serializable {
    private static final long serialVersionUID = 1L;
    public final int idShard;
    public final int idNo;
    public final String papel;
    public final long epoca;
    public final int idLiderConhecido;

    public AnuncioShard(int idShard, int idNo, String papel, long epoca, int idLiderConhecido) {
      this.idShard = idShard;
      this.idNo = idNo;
      this.papel = papel;
      this.epoca = epoca;
      this.idLiderConhecido = idLiderConhecido;
    }
  }

  /** Operacao a ser executada no lider do shard destino. O idCorrelacao torna retries idempotentes. */
  public static class PedidoRoteado implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String idCorrelacao;
    public final int shardDestino;
    public final Operacao operacao;
    /** Endereco do canal de cluster do no que originou o pedido (para onde a resposta volta). */
    public final String hostResposta;
    public final int portaResposta;
    public final int shardOrigem;
    public final int idNoOrigem;

    public PedidoRoteado(String idCorrelacao, int shardDestino, Operacao operacao, String hostResposta,
        int portaResposta, int shardOrigem, int idNoOrigem) {
      this.idCorrelacao = idCorrelacao;
      this.shardDestino = shardDestino;
      this.operacao = operacao;
      this.hostResposta = hostResposta;
      this.portaResposta = portaResposta;
      this.shardOrigem = shardOrigem;
      this.idNoOrigem = idNoOrigem;
    }
  }

  public enum Status {
    /** Operacao executada pelo lider; ver resultado. */
    OK,
    /** O no contatado nao e o lider, mas conhece o lider (idLider). */
    REDIRECIONAR,
    /** O no contatado nao e o lider e nao conhece nenhum. */
    NAO_ATIVO,
    /** Pedido invalido (ex.: shard errado). */
    ERRO
  }

  public static class RespostaRoteada implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String idCorrelacao;
    public final Status status;
    public final Resultado resultado;
    public final int idLider;
    public final String erro;

    public RespostaRoteada(String idCorrelacao, Status status, Resultado resultado, int idLider, String erro) {
      this.idCorrelacao = idCorrelacao;
      this.status = status;
      this.resultado = resultado;
      this.idLider = idLider;
      this.erro = erro;
    }
  }

  /** Tipos de consulta read-only entre shards (Fase 4: LIST, USERS, MEMBERS). */
  public enum TipoConsulta {
    LISTAR_GRUPOS,
    LISTAR_USUARIOS,
    MEMBROS_GRUPO
  }

  /** Pedido de leitura direcionado ao lider de um shard especifico. */
  public static class PedidoConsulta implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String idCorrelacao;
    public final int shardDestino;
    public final TipoConsulta tipo;
    public final String parametro;
    public final String hostResposta;
    public final int portaResposta;
    public final int shardOrigem;
    public final int idNoOrigem;

    public PedidoConsulta(String idCorrelacao, int shardDestino, TipoConsulta tipo, String parametro,
        String hostResposta, int portaResposta, int shardOrigem, int idNoOrigem) {
      this.idCorrelacao = idCorrelacao;
      this.shardDestino = shardDestino;
      this.tipo = tipo;
      this.parametro = parametro;
      this.hostResposta = hostResposta;
      this.portaResposta = portaResposta;
      this.shardOrigem = shardOrigem;
      this.idNoOrigem = idNoOrigem;
    }
  }

  /** Resposta de leitura devolvida pelo shard consultado. */
  public static class RespostaConsulta implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String idCorrelacao;
    public final Status status;
    public final int idLider;
    public final ArrayList<String> grupos;
    public final ArrayList<InfoUser> usuarios;
    public final String erro;

    public RespostaConsulta(String idCorrelacao, Status status, int idLider, ArrayList<String> grupos,
        ArrayList<InfoUser> usuarios, String erro) {
      this.idCorrelacao = idCorrelacao;
      this.status = status;
      this.idLider = idLider;
      this.grupos = grupos;
      this.usuarios = usuarios;
      this.erro = erro;
    }
  }

  /**
   * Envelope de encaminhamento de datagrama APDU UDP (Fase 5: SEND, SENDVU, SENDPVT, CONFIRM)
   * preservando integralmente o IP e a porta UDP originais do cliente remetente.
   */
  public static class PacoteUdpRoteado implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String idPacote;
    public final int shardDestino;
    public final APDU apdu;
    public final String ipCliente;
    public final int portaUdpCliente;
    public final int saltos;

    public PacoteUdpRoteado(String idPacote, int shardDestino, APDU apdu,
        String ipCliente, int portaUdpCliente, int saltos) {
      this.idPacote = idPacote;
      this.shardDestino = shardDestino;
      this.apdu = apdu;
      this.ipCliente = ipCliente;
      this.portaUdpCliente = portaUdpCliente;
      this.saltos = saltos;
    }
  }
}

