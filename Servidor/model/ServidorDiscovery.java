/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: ServidorDiscovery
* Funcao.............: Farol UDP para descoberta automatica do servidor local e nos do cluster/shard.
*************************************************************** */

package model;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import utils.Protocolo;
import cluster.TabelaLideres;

/**
 * Thread de discovery local (farol UDP) com suporte a busca de cluster/shard,
 * compatibilidade legada e interoperabilidade universal com clientes heterogeneos (ex: Lucas).
 * Responde a:
 * - {@code "QUEM_E_MESTRE_PARTICAO_X?" -> "MESTRE_PARTICAO_X"} (ou IP proxy)
 * - {@code "SERVIDOR_IP" -> "IP"}
 * - {@code "DISCOVER_EDEN" -> "EDEN_HERE"}
 * - {@code "DISCOVER_EDEN_CLUSTER" -> "EDEN_CLUSTER:id:papel:tcp:udp:idShard"}
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 4.0
 */
public class ServidorDiscovery implements Runnable {

  private final int porta;
  private final boolean modoFallback;
  private DatagramSocket socket;
  private volatile boolean executando = true;
  private int idNo = 1;
  private int idShard = 0;
  private int portaTcp = Protocolo.PORTA_SERVIDOR_TCP;
  private int portaUdp = Protocolo.PORTA_SERVIDOR_UDP;
  private String papel = "ATIVO";
  private TabelaLideres tabelaLideres;

  /** Constroi o servico de discovery na porta padrao {@code 8888}. */
  public ServidorDiscovery() {
    this(Protocolo.PORTA_DISCOVERY, false);
  }

  /** Constroi o servico de discovery na porta especificada em modo primario. */
  public ServidorDiscovery(int porta) {
    this(porta, false);
  }

  /**
   * Constroi o servico de discovery permitindo operar silenciosamente em modo fallback caso a porta esteja ocupada.
   *
   * @param porta        Porta UDP de escuta do farol.
   * @param modoFallback Se {@code true}, suprime erros de bind caso outro lider local ja detenha a porta.
   */
  public ServidorDiscovery(int porta, boolean modoFallback) {
    this.porta = porta;
    this.modoFallback = modoFallback;
  }

  public void setTabelaLideres(TabelaLideres tabela) {
    this.tabelaLideres = tabela;
  }

  /** Atualiza os metadados anunciados pelo farol assumindo Shard 0. */
  public void configurarInfo(int idNo, String papel, int portaTcp, int portaUdp) {
    configurarInfo(idNo, 0, papel, portaTcp, portaUdp);
  }

  /**
   * Configura a identidade do no, particao e portas de atendimento anunciadas nas respostas UDP.
   *
   * @param idNo     Identificador global do no.
   * @param idShard  Indice da particao atendida (0..2).
   * @param papel    Papel operacional atual (ex: {@code ATIVO}).
   * @param portaTcp Porta TCP principal anunciada.
   * @param portaUdp Porta UDP principal anunciada.
   */
  public void configurarInfo(int idNo, int idShard, String papel, int portaTcp, int portaUdp) {
    this.idNo = idNo;
    this.idShard = idShard;
    this.papel = papel;
    this.portaTcp = portaTcp;
    this.portaUdp = portaUdp;
  }

  /** Encerra o socket UDP do farol de discovery. */
  public void parar() {
    this.executando = false;
    if (this.socket != null && !this.socket.isClosed()) {
      try {
        this.socket.close();
      } catch (Exception ignored) {
      }
    }
    System.out.println("[DISCOVERY:" + porta + "] Farol UDP encerrado.");
  }

  /** Laco continuo de escuta e resposta multi-dialeto as sondas UDP de clientes na rede. */
  @Override
  public void run() {
    try {
      int tentativas = modoFallback ? 4 : 2;
      Exception ultimoErroBind = null;
      for (int t = 1; t <= tentativas && executando; t++) {
        try {
          DatagramSocket candidato = new DatagramSocket(null);
          candidato.setReuseAddress(true);
          candidato.setBroadcast(true);
          candidato.bind(new InetSocketAddress(porta));
          socket = candidato;
          break;
        } catch (Exception exBind) {
          ultimoErroBind = exBind;
          if (t < tentativas && executando) {
            try {
              Thread.sleep(120);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }
      if (socket == null) {
        if (!modoFallback && executando && ultimoErroBind != null) {
          System.err.println("[DISCOVERY:" + porta + "] [ERROR] Falha no farol: " + ultimoErroBind.getMessage());
        }
        return;
      }

      System.out.println("[DISCOVERY:" + porta + "] Farol UDP ligado na porta " + porta + " (Shard S" + idShard + ") aguardando buscas...");
      byte[] buffer = new byte[256];

      while (executando && !socket.isClosed()) {
        DatagramPacket pacoteRecebido = new DatagramPacket(buffer, buffer.length);
        try {
          socket.receive(pacoteRecebido);
        } catch (Exception e) {
          if (!executando) break;
          continue;
        }

        String mensagem = new String(pacoteRecebido.getData(), 0, pacoteRecebido.getLength(),
            StandardCharsets.UTF_8).trim();
        String msgUpper = mensagem.toUpperCase();

        // 1. Dialeto T1/T2 ou colegas (SERVIDOR_IP)
        if ("SERVIDOR_IP".equalsIgnoreCase(mensagem) || "SERVIDOR_IP?".equalsIgnoreCase(mensagem)) {
          byte[] dadosResposta = "IP".getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
          continue;
        }

        // 2. Dialeto Nativo EDEN simples
        if ("DISCOVER_EDEN".equalsIgnoreCase(mensagem)) {
          byte[] dadosResposta = "EDEN_HERE".getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
          continue;
        }

        // 3. Dialeto Nativo EDEN Cluster enriquecido
        if (Protocolo.DISCOVER_EDEN_CLUSTER.equalsIgnoreCase(mensagem)) {
          String respostaCluster = String.format("EDEN_CLUSTER:%d:%s:%d:%d:%d",
              idNo, papel, portaTcp, portaUdp, idShard);
          byte[] dadosResposta = respostaCluster.getBytes(StandardCharsets.UTF_8);
          DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
              pacoteRecebido.getAddress(), pacoteRecebido.getPort());
          socket.send(pacoteResposta);
          continue;
        }

        // 4. Dialeto Universal Sharded (Ex: Colega Lucas "QUEM_E_MESTRE_PARTICAO_X?")
        //    Particoes 1, 2 e 3 correspondem aos nossos Shards 0, 1 e 2.
        for (int p = 1; p <= 3; p++) {
          if (("QUEM_E_MESTRE_PARTICAO_" + p + "?").equals(msgUpper)
              || ("QUEM_E_MESTRE_PARTICAO_" + p).equals(msgUpper)
              || msgUpper.contains("PARTICAO_" + p)
              || msgUpper.contains("PARTICAO " + p)
              || msgUpper.contains("SHARD_" + p)
              || msgUpper.contains("SHARD " + p)) {

            int shardAlvo = p - 1;
            boolean souLiderDesteShard = (idShard == shardAlvo && "ATIVO".equalsIgnoreCase(papel));
            
            // Se eu sou o lider deste Shard, respondo imediatamente!
            // Se eu nao sou, mas estou na porta canonica 8888 e sou ATIVO, atuamos como Gateway / Proxy Transparente
            // para que o cliente nao fique orfao (Anycast logico).
            boolean deveResponder = souLiderDesteShard || ("ATIVO".equalsIgnoreCase(papel) && porta == Protocolo.PORTA_DISCOVERY);

            if (deveResponder) {
              String resposta = "MESTRE_PARTICAO_" + p;
              byte[] dadosResposta = resposta.getBytes(StandardCharsets.UTF_8);
              DatagramPacket pacoteResposta = new DatagramPacket(dadosResposta, dadosResposta.length,
                  pacoteRecebido.getAddress(), pacoteRecebido.getPort());
              socket.send(pacoteResposta);
              System.out.println("[DISCOVERY:" + porta + "] Respondeu ao cliente " + pacoteRecebido.getAddress().getHostAddress()
                  + " para particao " + p + " -> " + resposta + (souLiderDesteShard ? " (LIDER DIRETO)" : " (GATEWAY PROXY)"));
            }
            break;
          }
        }
      }
    } catch (Exception e) {
      if (executando) {
        System.err.println("[DISCOVERY:" + porta + "] [ERROR] Falha no farol: " + e.getMessage());
      }
    } finally {
      if (socket != null && !socket.isClosed()) {
        socket.close();
      }
    }
  }
}
