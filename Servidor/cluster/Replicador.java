/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: Replicador
* Funcao.............: Gerenciador de replicacao sincrona de estado com confirmacao de todos os nos (D2) e snapshots.
*************************************************************** */

package cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;

/**
 * Responsavel pela replicacao sincrona deterministica entre lider e seguidores.
 * Garante que uma operacao so seja respondida ao cliente apos ter sido confirmada
 * por TODOS os nos vivos em standby sincronizado (D2).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class Replicador {

  private final int meuId;
  private final GerenciadorGrupos gerenciador;
  private final CanalCluster canal;
  private final VisaoCluster visao;
  private final ReentrantLock lockReplicacao = new ReentrantLock();

  // Log circular de operacoes em memoria (max 10.000)
  private final List<Operacao> logCircular = Collections.synchronizedList(new ArrayList<>());
  private static final int MAX_LOG = 10000;

  // Mapa de latches para espera de ACKs: versao -> LatchInfo
  private static class LatchInfo {
    final long versao;
    final Set<Integer> pendentes;
    final CountDownLatch latch;

    LatchInfo(long versao, Set<Integer> pendentes) {
      this.versao = versao;
      this.pendentes = pendentes;
      this.latch = new CountDownLatch(pendentes.size());
    }
  }

  private final Map<Long, LatchInfo> acksPendentes = new ConcurrentHashMap<>();

  public Replicador(int meuId, GerenciadorGrupos gerenciador, CanalCluster canal, VisaoCluster visao) {
    this.meuId = meuId;
    this.gerenciador = gerenciador;
    this.canal = canal;
    this.visao = visao;
  }

  /**
   * Executa uma operacao no lider e replica sincronicamente para todos os standbys.
   * Aguarda confirmacao integral (ACK) de todos os pares vivos antes de retornar.
   */
  public Resultado executar(Operacao op, long epocaAtual) {
    lockReplicacao.lock();
    try {
      op.setEpoca(epocaAtual);
      op.setIdOrigem(meuId);

      // 1. Aplica localmente no GerenciadorGrupos
      Resultado resultado = gerenciador.aplicarLocal(op);
      long versaoOp = gerenciador.getVersao();
      op.setVersao(versaoOp);

      // Adiciona ao log circular
      logCircular.add(op);
      if (logCircular.size() > MAX_LOG) {
        logCircular.remove(0);
      }

      // 2. Identifica todos os standbys vivos elegiveis
      List<VisaoCluster.ParEstado> paresVivos = visao.getParesVivos();
      Set<Integer> alvos = new HashSet<>();
      for (VisaoCluster.ParEstado p : paresVivos) {
        if (!"DOWN".equalsIgnoreCase(p.estado) && p.conectado) {
          alvos.add(p.id);
        }
      }

      if (alvos.isEmpty()) {
        // Unico no vivo no cluster: aplica localmente e retorna
        return resultado;
      }

      // 3. Prepara Latch de espera
      LatchInfo info = new LatchInfo(versaoOp, alvos);
      acksPendentes.put(versaoOp, info);

      // 4. Dispara REPLICAR para todos
      MensagemCluster msgReplicar = new MensagemCluster(TipoMensagemCluster.REPLICAR, meuId);
      msgReplicar.setEpoca(epocaAtual);
      msgReplicar.setVersao(versaoOp);
      msgReplicar.setPayload(op);

      for (int idPar : alvos) {
        canal.enviar(idPar, msgReplicar);
      }

      // 5. Aguarda ACK de todos (timeout 2000 ms)
      try {
        boolean todosConfirmaram = info.latch.await(2000, TimeUnit.MILLISECONDS);
        if (!todosConfirmaram) {
          System.err.println("[REPLICADOR] [WARNING] Timeout de ACK na versao " + versaoOp
              + ". Standbys sem resposta: " + info.pendentes);
          // Marca nos sem resposta como DOWN
          for (int semResposta : info.pendentes) {
            visao.marcarDown(semResposta);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        acksPendentes.remove(versaoOp);
      }

      return resultado;

    } finally {
      lockReplicacao.unlock();
    }
  }

  /**
   * Processa confirmacao ACK recebida de um standby.
   */
  public void processarAck(int idOrigem, long versao) {
    LatchInfo info = acksPendentes.get(versao);
    if (info != null) {
      if (info.pendentes.remove(idOrigem)) {
        info.latch.countDown();
      }
    }
  }

  /**
   * Processa mensagem REPLICAR recebida pelo standby vinda do lider ativo.
   */
  public void processarReplicacaoRecebida(MensagemCluster msg) {
    if (msg.getPayload() instanceof Operacao) {
      Operacao op = (Operacao) msg.getPayload();
      long versaoAtual = gerenciador.getVersao();

      if (op.getVersao() == versaoAtual + 1) {
        gerenciador.aplicarLocal(op);
        logCircular.add(op);
        if (logCircular.size() > MAX_LOG) {
          logCircular.remove(0);
        }

        // Responde ACK ao lider
        MensagemCluster ack = new MensagemCluster(TipoMensagemCluster.ACK, meuId);
        ack.setVersao(gerenciador.getVersao());
        canal.enviar(msg.getIdOrigem(), ack);

      } else if (op.getVersao() <= versaoAtual) {
        // Ja aplicada: envia ACK para liberar o lider (idempotente)
        MensagemCluster ack = new MensagemCluster(TipoMensagemCluster.ACK, meuId);
        ack.setVersao(gerenciador.getVersao());
        canal.enviar(msg.getIdOrigem(), ack);

      } else {
        // Lacuna detectada! Pede snapshot completo para recuperar
        System.out.println("[REPLICADOR] [WARNING] Lacuna detectada (minha versao=" + versaoAtual
            + ", recebida=" + op.getVersao() + "). Solicitando sincronizacao...");
        MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, meuId);
        pedido.setVersao(versaoAtual);
        canal.enviar(msg.getIdOrigem(), pedido);
      }
    }
  }

  /**
   * Envia snapshot do estado atual para um no que solicitou sincronizacao.
   */
  public void enviarSnapshot(int idDestino, long epocaAtual) {
    lockReplicacao.lock();
    try {
      byte[] dados = gerenciador.exportarSnapshot();
      MensagemCluster snap = new MensagemCluster(TipoMensagemCluster.SNAPSHOT, meuId);
      snap.setEpoca(epocaAtual);
      snap.setVersao(gerenciador.getVersao());
      snap.setHash(gerenciador.calcularHash());
      snap.setPayload(dados);
      canal.enviar(idDestino, snap);
      System.out.println("[REPLICADOR] [INFO] Snapshot (v=" + gerenciador.getVersao() + ", "
          + dados.length + " bytes) enviado ao No " + idDestino);
    } finally {
      lockReplicacao.unlock();
    }
  }

  /**
   * Instala um snapshot recebido do lider.
   */
  public boolean instalarSnapshot(byte[] dados, long epoca, long versao) {
    lockReplicacao.lock();
    try {
      gerenciador.importarSnapshot(dados);
      System.out.println("[REPLICADOR] [INFO] Snapshot instalado com sucesso. Versao atual: " + gerenciador.getVersao());
      return true;
    } catch (Exception e) {
      System.err.println("[REPLICADOR] [ERROR] Falha ao instalar snapshot: " + e.getMessage());
      return false;
    } finally {
      lockReplicacao.unlock();
    }
  }

}
