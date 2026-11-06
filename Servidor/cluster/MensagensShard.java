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

import controller.Operacao;
import controller.Resultado;

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
}

