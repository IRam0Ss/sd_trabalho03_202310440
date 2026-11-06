/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: MensagemCluster
* Funcao.............: Envelope de mensagem serializavel transmitida no canal TCP interno do cluster.
*************************************************************** */

package cluster;

import java.io.Serializable;

/**
 * Envelope para troca de dados entre os servidores do cluster.
 * Carrega epoca, versao da estrutura e hash CRC32 para garantia de consistencia e ordenacao.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class MensagemCluster implements Serializable {

  private static final long serialVersionUID = 1L;

  private final TipoMensagemCluster tipo;
  private final int idOrigem;
  private long epoca;
  private long versao;
  private Serializable payload;
  private final long timestamp;
  private long hash;
  private String estadoOrigem;
  private int idAtivoConhecido;
  private int portaTcpServico;
  private int portaUdpServico;
  private String hostOrigem;
  private int portaClusterOrigem;
  /** Shard de origem. -1 = nao carimbado (o CanalCluster carimba no envio). */
  private int idShard = -1;

  public int getIdShard() {
    return idShard;
  }

  public void setIdShard(int idShard) {
    this.idShard = idShard;
  }

  public MensagemCluster(TipoMensagemCluster tipo, int idOrigem) {
    this.tipo = tipo;
    this.idOrigem = idOrigem;
    this.timestamp = System.currentTimeMillis();
  }

  public TipoMensagemCluster getTipo() {
    return tipo;
  }

  public int getIdOrigem() {
    return idOrigem;
  }

  public long getEpoca() {
    return epoca;
  }

  public void setEpoca(long epoca) {
    this.epoca = epoca;
  }

  public long getVersao() {
    return versao;
  }

  public void setVersao(long versao) {
    this.versao = versao;
  }

  public Serializable getPayload() {
    return payload;
  }

  public void setPayload(Serializable payload) {
    this.payload = payload;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getHash() {
    return hash;
  }

  public void setHash(long hash) {
    this.hash = hash;
  }

  public String getEstadoOrigem() {
    return estadoOrigem;
  }

  public void setEstadoOrigem(String estadoOrigem) {
    this.estadoOrigem = estadoOrigem;
  }

  public int getIdAtivoConhecido() {
    return idAtivoConhecido;
  }

  public void setIdAtivoConhecido(int idAtivoConhecido) {
    this.idAtivoConhecido = idAtivoConhecido;
  }

  public int getPortaTcpServico() {
    return portaTcpServico;
  }

  public void setPortaTcpServico(int portaTcpServico) {
    this.portaTcpServico = portaTcpServico;
  }

  public int getPortaUdpServico() {
    return portaUdpServico;
  }

  public void setPortaUdpServico(int portaUdpServico) {
    this.portaUdpServico = portaUdpServico;
  }

  public String getHostOrigem() {
    return hostOrigem;
  }

  public void setHostOrigem(String hostOrigem) {
    this.hostOrigem = hostOrigem;
  }

  public int getPortaClusterOrigem() {
    return portaClusterOrigem;
  }

  public void setPortaClusterOrigem(int portaClusterOrigem) {
    this.portaClusterOrigem = portaClusterOrigem;
  }

  @Override
  public String toString() {
    return String.format("MensagemCluster[%s de No %d | ep=%d, v=%d, est=%s, hash=0x%08X]",
        tipo, idOrigem, epoca, versao, estadoOrigem, hash);
  }
}

