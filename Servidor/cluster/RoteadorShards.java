/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: RoteadorShards
* Funcao.............: Roteamento de operacoes entre shards (proxy transparente), tabela de lideres e retry idempotente.
*************************************************************** */

package cluster;

import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cluster.MensagensShard.AnuncioShard;
import cluster.MensagensShard.PedidoRoteado;
import cluster.MensagensShard.RespostaRoteada;
import cluster.MensagensShard.Status;
import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;

/**
 * Camada de comunicacao ENTRE shards (a eleicao/replicacao continua estritamente intra-shard).
 * <ul>
 *   <li>Anuncia o papel deste no aos nos dos outros shards (ANUNCIO_SHARD, 1s) e mantem a {@link TabelaLideres}.</li>
 *   <li>{@link #executar(int, Operacao, long)}: executa a operacao no lider do shard dono (localmente se este no
 *       for o lider; senao envia ROTEAR_OP e aguarda RESP_OP), com redirecionamento, retry e timeout.</li>
 *   <li>Retries reutilizam o mesmo idCorrelacao; o destino guarda o resultado e nunca executa duas vezes.</li>
 *   <li>Shard sem lider alcancavel => Resultado de erro "particao indisponivel" (falha parcial).</li>
 * </ul>
 * Enderecos de outros shards sao derivados da formula {@link PortasShard} e do host local.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class RoteadorShards {

  /** Visao do estado do no, fornecida pelo NoCluster. */
  public interface Contexto {
    String papel();

    int idLiderAtivo();

    long epoca();
  }

  public static final long TIMEOUT_PADRAO_MS = 3000;
  private static final long TIMEOUT_TENTATIVA_MS = 1200;
  private static final String TAG = "ROTEAMENTO";

  private final ConfigCluster config;
  private final GerenciadorGrupos gerenciador;
  private final Contexto ctx;
  private final TabelaLideres tabela = new TabelaLideres();
  private final int meuShard;
  private final int totalReplicas;
  private final String host;

  private final Map<String, CompletableFuture<RespostaRoteada>> pendentes = new ConcurrentHashMap<>();
  private final Map<String, Resultado> dedup = java.util.Collections.synchronizedMap(
      new LinkedHashMap<String, Resultado>(256, 0.75f, false) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Resultado> e) {
          return size() > 2000;
        }
      });
  private final Map<String, Conexao> conexoes = new ConcurrentHashMap<>();
  private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "Thread-Roteador-Worker");
    t.setDaemon(true);
    return t;
  });
  private final boolean[] shardOnline = new boolean[Particionador.NUM_SHARDS];
  private volatile boolean executando = true;

  private static class Conexao {
    Socket socket;
    ObjectOutputStream oos;

    void fechar() {
      try {
        if (oos != null) oos.close();
      } catch (Exception ignored) {
      }
      try {
        if (socket != null) socket.close();
      } catch (Exception ignored) {
      }
      socket = null;
      oos = null;
    }
  }

  public RoteadorShards(ConfigCluster config, GerenciadorGrupos gerenciador, Contexto ctx) {
    this.config = config;
    this.gerenciador = gerenciador;
    this.ctx = ctx;
    this.meuShard = config.getIdShard();
    this.totalReplicas = Math.max(1, config.getNos().size());
    this.host = config.getMeuNoInfo().host;
    for (int i = 0; i < shardOnline.length; i++) {
      shardOnline[i] = true; // evita log espurio na partida; a 1a varredura corrige
    }
  }

  public TabelaLideres getTabela() {
    return tabela;
  }

  // =========================================================================
  // CICLO DE VIDA / ANUNCIOS
  // =========================================================================

  public void iniciar() {
    Thread t = new Thread(() -> {
      while (executando) {
        try {
          atualizarProprioShard();
          anunciar();
          verificarTransicoes();
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          ClusterLog.erro(TAG, "Falha no loop de anuncios: " + e.getMessage());
        }
      }
    }, "Thread-Roteador-Anuncios");
    t.setDaemon(true);
    t.start();
  }

  public void parar() {
    executando = false;
    pool.shutdownNow();
    for (Conexao c : conexoes.values()) {
      synchronized (c) {
        c.fechar();
      }
    }
  }

  private void atualizarProprioShard() {
    int lider = "ATIVO".equalsIgnoreCase(ctx.papel()) ? config.getMeuId() : ctx.idLiderAtivo();
    if (lider > 0) {
      tabela.definirProprio(meuShard, lider, ctx.epoca());
    } else {
      tabela.invalidar(meuShard, tabela.getLider(meuShard));
    }
  }

  private void anunciar() {
    AnuncioShard a = new AnuncioShard(meuShard, config.getMeuId(), ctx.papel(), ctx.epoca(), ctx.idLiderAtivo());
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      if (s == meuShard) continue;
      for (int r = 0; r < totalReplicas; r++) {
        enviar(s, r + 1, criarMsg(TipoMensagemCluster.ANUNCIO_SHARD, a));
      }
    }
  }

  private void verificarTransicoes() {
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      boolean online = tabela.getLider(s) > 0;
      if (online != shardOnline[s]) {
        shardOnline[s] = online;
        if (online) {
          ClusterLog.info(TAG, "Shard " + Particionador.nome(s) + " ONLINE. Lider: replica " + (tabela.getLider(s) - 1)
              + " (ROTEADO com sucesso para este shard a partir de agora).");
        } else {
          ClusterLog.aviso(TAG, "Shard " + Particionador.nome(s) + " INDISPONIVEL: nenhum lider alcancavel. "
              + "Falha parcial: apenas as letras " + Particionador.faixa(s) + " ficam fora do ar.");
        }
      }
    }
  }

  /** Imprime a tabela de lideres no console (comando 'lideres'). */
  public void imprimirTabela() {
    System.out.println("=== TABELA DE LIDERES POR SHARD (visao deste no) ===");
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      int l = tabela.getLider(s);
      String desc = (l > 0)
          ? "replica " + (l - 1) + " | TCP " + PortasShard.tcp(s, l - 1) + " | cluster " + PortasShard.cluster(s, l - 1)
              + " | epoca " + tabela.getEpoca(s)
          : "SEM LIDER ALCANCAVEL (indisponivel)";
      System.out.println("  " + Particionador.nome(s) + (s == meuShard ? " (meu shard)" : "") + " -> " + desc);
    }
    System.out.println("====================================================");
  }

  // =========================================================================
  // TRANSPORTE
  // =========================================================================

  private MensagemCluster criarMsg(TipoMensagemCluster tipo, java.io.Serializable payload) {
    MensagemCluster m = new MensagemCluster(tipo, config.getMeuId());
    m.setIdShard(meuShard);
    m.setPayload(payload);
    m.setHostOrigem(host);
    m.setPortaClusterOrigem(config.getMeuNoInfo().portaCluster);
    m.setEpoca(ctx.epoca());
    return m;
  }

  private boolean enviar(int shard, int idNo, MensagemCluster msg) {
    return enviarPara(host, PortasShard.cluster(shard, idNo - 1), msg);
  }

  private boolean enviarPara(String h, int porta, MensagemCluster msg) {
    String chave = h + ":" + porta;
    Conexao c = conexoes.computeIfAbsent(chave, k -> new Conexao());
    synchronized (c) {
      try {
        if (c.socket == null || c.socket.isClosed()) {
          Socket s = new Socket();
          s.setTcpNoDelay(true);
          s.connect(new InetSocketAddress(h, porta), 300);
          c.socket = s;
          c.oos = new ObjectOutputStream(s.getOutputStream());
          c.oos.flush();
        }
        c.oos.writeUnshared(msg);
        c.oos.reset();
        c.oos.flush();
        return true;
      } catch (Exception e) {
        c.fechar();
        return false;
      }
    }
  }

  /** Ponto de entrada de mensagens inter-shard vindas do CanalCluster (ANUNCIO_SHARD, ROTEAR_OP, RESP_OP). */
  public void receber(MensagemCluster msg) {
    if (msg == null || !executando) return;
    switch (msg.getTipo()) {
      case ANUNCIO_SHARD:
        if (msg.getPayload() instanceof AnuncioShard) {
          processarAnuncio((AnuncioShard) msg.getPayload());
        }
        break;
      case ROTEAR_OP:
        if (msg.getPayload() instanceof PedidoRoteado) {
          final PedidoRoteado p = (PedidoRoteado) msg.getPayload();
          pool.submit(() -> tratarPedido(p));
        }
        break;
      case RESP_OP:
        if (msg.getPayload() instanceof RespostaRoteada) {
          RespostaRoteada r = (RespostaRoteada) msg.getPayload();
          CompletableFuture<RespostaRoteada> f = pendentes.get(r.idCorrelacao);
          if (f != null) {
            f.complete(r);
          }
        }
        break;
      default:
        break;
    }
  }

  private void processarAnuncio(AnuncioShard a) {
    if (a.idShard < 0 || a.idShard >= Particionador.NUM_SHARDS || a.idShard == meuShard) return;
    if ("ATIVO".equalsIgnoreCase(a.papel)) {
      tabela.registrarAtivo(a.idShard, a.idNo, a.epoca);
    } else if (a.idLiderConhecido > 0) {
      tabela.sugerir(a.idShard, a.idLiderConhecido);
    }
  }

  // =========================================================================
  // LADO DESTINO: executar pedido recebido
  // =========================================================================

  private void tratarPedido(PedidoRoteado p) {
    RespostaRoteada resp;
    String tipoOp = (p.operacao != null) ? String.valueOf(p.operacao.getTipo()) : "?";

    if (p.operacao == null || p.shardDestino != meuShard) {
      resp = new RespostaRoteada(p.idCorrelacao, Status.ERRO, null, -1,
          "Pedido para shard " + p.shardDestino + " entregue ao shard " + meuShard);
      ClusterLog.erro(TAG, "Pedido " + tipoOp + " invalido: destino shard " + p.shardDestino + " != meu shard " + meuShard);
    } else if (!"ATIVO".equalsIgnoreCase(ctx.papel())) {
      int lider = ctx.idLiderAtivo();
      boolean conhece = lider > 0 && lider != config.getMeuId();
      resp = new RespostaRoteada(p.idCorrelacao, conhece ? Status.REDIRECIONAR : Status.NAO_ATIVO, null,
          conhece ? lider : -1, null);
      ClusterLog.info(TAG, "Pedido " + tipoOp + " de S" + p.shardOrigem + "/R" + (p.idNoOrigem - 1)
          + " recebido, mas sou " + ctx.papel() + ". Redirecionando para lider " + (conhece ? "replica " + (lider - 1) : "(desconhecido)"));
    } else {
      Resultado res = dedup.get(p.idCorrelacao);
      if (res != null) {
        ClusterLog.info(TAG, "Pedido " + tipoOp + " [" + p.idCorrelacao.substring(0, 8)
            + "] repetido (retry): devolvendo resultado guardado, sem reexecutar.");
      } else {
        ClusterLog.info(TAG, "Operacao roteada " + tipoOp + " de S" + p.shardOrigem + "/R" + (p.idNoOrigem - 1)
            + " [" + p.idCorrelacao.substring(0, 8) + "] sendo aplicada e replicada neste shard...");
        res = gerenciador.aplicar(p.operacao);
        dedup.put(p.idCorrelacao, res);
        ClusterLog.info(TAG, "Operacao roteada " + tipoOp + " aplicada com sucesso (versao " + gerenciador.getVersao() + ").");
      }
      resp = new RespostaRoteada(p.idCorrelacao, Status.OK, res, config.getMeuId(), null);
    }

    boolean ok = enviarPara(p.hostResposta, p.portaResposta, criarMsg(TipoMensagemCluster.RESP_OP, resp));
    if (!ok) {
      ClusterLog.aviso(TAG, "Nao foi possivel entregar RESP_OP ao no de origem (" + p.hostResposta + ":" + p.portaResposta + ").");
    }
  }

  // =========================================================================
  // LADO ORIGEM: executar operacao no shard dono
  // =========================================================================

  public Resultado executar(int shardDestino, Operacao op) {
    return executar(shardDestino, op, TIMEOUT_PADRAO_MS);
  }

  /**
   * Executa uma operacao no lider do shard dono, de forma transparente.
   * 
   * @return Resultado do lider ou Resultado.erro("Particao ... indisponivel") se o shard nao responde.
   */
  public Resultado executar(int shardDestino, Operacao op, long timeoutMs) {
    Particionador.validarShard(shardDestino);
    if (op == null) {
      return Resultado.erro("Operacao nula");
    }

    // Caminho local: este no e o lider do shard dono
    if (shardDestino == meuShard && "ATIVO".equalsIgnoreCase(ctx.papel())) {
      return gerenciador.aplicar(op);
    }

    final String corr = UUID.randomUUID().toString();
    final String curto = corr.substring(0, 8);
    final long limite = System.currentTimeMillis() + timeoutMs;
    final Set<Integer> tentados = new HashSet<>();
    final PedidoRoteado pedido = new PedidoRoteado(corr, shardDestino, op, host, config.getMeuNoInfo().portaCluster,
        meuShard, config.getMeuId());
    final long t0 = System.currentTimeMillis();

    try {
      while (System.currentTimeMillis() < limite && executando) {
        int alvo = tabela.getLider(shardDestino);
        if (alvo <= 0 || tentados.contains(alvo)) {
          alvo = proximoCandidato(shardDestino, tentados);
        }
        if (alvo <= 0) {
          tentados.clear();
          Thread.sleep(150);
          continue;
        }

        CompletableFuture<RespostaRoteada> f = new CompletableFuture<>();
        pendentes.put(corr, f);
        ClusterLog.info(TAG, "Roteando " + op.getTipo() + " [" + curto + "] para " + Particionador.nome(shardDestino)
            + " -> replica " + (alvo - 1) + " (porta cluster " + PortasShard.cluster(shardDestino, alvo - 1) + ")");

        boolean enviado = enviar(shardDestino, alvo, criarMsg(TipoMensagemCluster.ROTEAR_OP, pedido));
        if (!enviado) {
          ClusterLog.aviso(TAG, "Replica " + (alvo - 1) + " do " + Particionador.nome(shardDestino) + " inacessivel. Tentando outra...");
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
          continue;
        }

        RespostaRoteada r;
        try {
          long espera = Math.min(TIMEOUT_TENTATIVA_MS, Math.max(50, limite - System.currentTimeMillis()));
          r = f.get(espera, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException te) {
          ClusterLog.aviso(TAG, "Timeout aguardando replica " + (alvo - 1) + " do " + Particionador.nome(shardDestino) + " [" + curto + "]. Retry idempotente...");
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
          continue;
        }

        switch (r.status) {
          case OK:
            ClusterLog.info(TAG, "Operacao " + op.getTipo() + " [" + curto + "] roteada com sucesso para "
                + Particionador.nome(shardDestino) + " em " + (System.currentTimeMillis() - t0) + "ms.");
            return r.resultado;
          case REDIRECIONAR:
            tentados.clear();
            tentados.add(alvo);
            tabela.invalidar(shardDestino, alvo);
            tabela.sugerir(shardDestino, r.idLider);
            ClusterLog.info(TAG, "Replica " + (alvo - 1) + " nao e lider; redirecionado para replica " + (r.idLider - 1) + ".");
            break;
          case NAO_ATIVO:
            tentados.add(alvo);
            tabela.invalidar(shardDestino, alvo);
            break;
          case ERRO:
          default:
            return Resultado.erro(r.erro != null ? r.erro : "Erro no roteamento");
        }
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      pendentes.remove(corr);
    }

    ClusterLog.erro(TAG, "Particao " + Particionador.nome(shardDestino) + " indisponivel: operacao " + op.getTipo()
        + " [" + curto + "] abortada apos " + (System.currentTimeMillis() - t0) + "ms (timeout).");
    return Resultado.erro("Particao " + Particionador.nome(shardDestino) + " indisponivel no momento");
  }

  private int proximoCandidato(int shard, Set<Integer> tentados) {
    for (int id = 1; id <= totalReplicas; id++) {
      if (!tentados.contains(id) && !(shard == meuShard && id == config.getMeuId())) {
        return id;
      }
    }
    return -1;
  }
}
