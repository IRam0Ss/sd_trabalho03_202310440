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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import Protocol.APDU;
import cluster.MensagensShard.AnuncioShard;
import cluster.MensagensShard.InvalidaSessaoCliente;
import cluster.MensagensShard.PacoteUdpRoteado;
import cluster.MensagensShard.PedidoConsulta;
import cluster.MensagensShard.PedidoRoteado;
import cluster.MensagensShard.RespostaConsulta;
import cluster.MensagensShard.RespostaRoteada;
import cluster.MensagensShard.Status;
import cluster.MensagensShard.TipoConsulta;
import controller.AtendimentoCliente;
import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;
import utils.InfoUser;

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

  /** Callback invocado quando este no (lider ATIVO do shard) recebe uma APDU UDP roteada de outro shard. */
  public interface ReceptorUdpRoteado {
    void entregar(APDU apdu, String ipCliente, int portaUdpCliente, int saltos);
  }

  public static final long TIMEOUT_PADRAO_MS = 3000;
  /** Timeout curto (2s) para scatter-gather resiliente de LIST e USERS (Teorema CAP: disponibilidade parcial). */
  public static final long TIMEOUT_SCATTER_MS = 2000;
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
  private final Map<String, CompletableFuture<RespostaConsulta>> consultasPendentes = new ConcurrentHashMap<>();
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
  private final Boolean[] shardOnline = new Boolean[Particionador.NUM_SHARDS];
  private final long inicioMs = System.currentTimeMillis();
  private volatile boolean executando = true;
  private volatile ReceptorUdpRoteado receptorUdp = null;
  private final Set<String> pacotesUdpVistos = Collections.synchronizedSet(
      Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(512, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
          return size() > 2000;
        }
      }));
  private final Set<String> invalidacoesVistas = Collections.synchronizedSet(
      Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(256, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
          return size() > 1000;
        }
      }));

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
    this.totalReplicas = Math.max(2, config.getNos().size());
    this.host = config.getMeuNoInfo().host;
  }

  public TabelaLideres getTabela() {
    return tabela;
  }

  public void setReceptorUdp(ReceptorUdpRoteado receptorUdp) {
    this.receptorUdp = receptorUdp;
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
    // Aguarda janela inicial de eleicao pioneira antes de notificar estado dos shards
    if (System.currentTimeMillis() - inicioMs < 2500) {
      return;
    }
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      boolean online = tabela.getLider(s) > 0;
      if (shardOnline[s] == null) {
        shardOnline[s] = online;
        if (online) {
          ClusterLog.info(TAG, "Shard " + Particionador.nome(s) + " ONLINE. Lider: replica " + (tabela.getLider(s) - 1)
              + " (TCP=" + PortasShard.tcp(s, tabela.getLider(s) - 1)
              + ", Cluster=" + PortasShard.cluster(s, tabela.getLider(s) - 1) + ").");
        }
      } else if (online != shardOnline[s].booleanValue()) {
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
      case CONSULTA_SHARD:
        if (msg.getPayload() instanceof PedidoConsulta) {
          final PedidoConsulta pc = (PedidoConsulta) msg.getPayload();
          pool.submit(() -> tratarConsulta(pc));
        }
        break;
      case RESP_CONSULTA_SHARD:
        if (msg.getPayload() instanceof RespostaConsulta) {
          RespostaConsulta rc = (RespostaConsulta) msg.getPayload();
          CompletableFuture<RespostaConsulta> fc = consultasPendentes.get(rc.idCorrelacao);
          if (fc != null) {
            fc.complete(rc);
          }
        }
        break;
      case ENCAMINHAR_UDP:
        if (msg.getPayload() instanceof PacoteUdpRoteado) {
          final PacoteUdpRoteado pkt = (PacoteUdpRoteado) msg.getPayload();
          pool.submit(() -> tratarPacoteUdpRoteado(pkt));
        }
        break;
      case INVALIDAR_SESSAO_CLIENTE:
        if (msg.getPayload() instanceof InvalidaSessaoCliente) {
          final InvalidaSessaoCliente inv = (InvalidaSessaoCliente) msg.getPayload();
          pool.submit(() -> tratarInvalidacaoSessao(inv));
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

  public int getMeuShard() {
    return meuShard;
  }

  public int getMinhaReplica() {
    return config.getIdReplica();
  }

  // =========================================================================
  // FASE 4: CONSULTAS READ-ONLY, SCATTER-GATHER RESILIENTE E BROADCASTS
  // =========================================================================

  /** Resultado tipado para consulta roteada de MEMBERS. */
  public static class ResultadoMembros {
    public final boolean sucesso;
    public final List<InfoUser> membros;
    public final String erro;

    public ResultadoMembros(boolean sucesso, List<InfoUser> membros, String erro) {
      this.sucesso = sucesso;
      this.membros = (membros != null) ? membros : Collections.emptyList();
      this.erro = erro;
    }
  }

  private RespostaConsulta avaliarSessaoLocal(String idCorrelacao, String nomeUsuario, List<String> gruposEsperados) {
    boolean valida = true;
    String motivo = null;
    if (nomeUsuario != null && !nomeUsuario.trim().isEmpty()) {
      if (Particionador.pertence(nomeUsuario, meuShard) && gerenciador.buscarUsuarioPorNome(nomeUsuario) == null) {
        valida = false;
        motivo = "Usuario '" + nomeUsuario + "' desconhecido na memoria do " + Particionador.nome(meuShard);
      }
      if (valida && gruposEsperados != null) {
        for (String g : gruposEsperados) {
          if (g != null && Particionador.pertence(g, meuShard) && !gerenciador.isMembroGrupo(g, nomeUsuario)) {
            valida = false;
            motivo = "Usuario '" + nomeUsuario + "' ausente do grupo '" + g + "' na memoria do "
                + Particionador.nome(meuShard);
            break;
          }
        }
      }
    }
    return new RespostaConsulta(idCorrelacao, Status.OK, config.getMeuId(), null, null, null, valida, motivo);
  }

  private void tratarConsulta(PedidoConsulta pc) {
    RespostaConsulta resp;
    if (pc == null || pc.shardDestino != meuShard) {
      resp = new RespostaConsulta(pc != null ? pc.idCorrelacao : "", Status.ERRO, -1, null, null,
          "Consulta destinada a outro shard");
    } else if (!"ATIVO".equalsIgnoreCase(ctx.papel())) {
      int lider = ctx.idLiderAtivo();
      boolean conhece = lider > 0 && lider != config.getMeuId();
      resp = new RespostaConsulta(pc.idCorrelacao, conhece ? Status.REDIRECIONAR : Status.NAO_ATIVO,
          conhece ? lider : -1, null, null, null);
    } else if (pc.tipo == TipoConsulta.VERIFICAR_SESSAO_USUARIO) {
      resp = avaliarSessaoLocal(pc.idCorrelacao, pc.parametro, pc.gruposEsperados);
    } else {
      ArrayList<String> grupos = null;
      ArrayList<InfoUser> usuarios = null;
      switch (pc.tipo) {
        case LISTAR_GRUPOS:
          grupos = new ArrayList<>(gerenciador.listarGrupos());
          break;
        case LISTAR_USUARIOS:
          usuarios = new ArrayList<>(gerenciador.getTodosUsuariosAtivos());
          break;
        case MEMBROS_GRUPO:
          usuarios = new ArrayList<>(gerenciador.getTodosMembros(pc.parametro));
          break;
        default:
          break;
      }
      resp = new RespostaConsulta(pc.idCorrelacao, Status.OK, config.getMeuId(), grupos, usuarios, null);
    }
    enviarPara(pc.hostResposta, pc.portaResposta, criarMsg(TipoMensagemCluster.RESP_CONSULTA_SHARD, resp));
  }

  /**
   * Consulta read-only ao lider de um shard especifico (com redirecionamento e fail-fast quando offline).
   * Retorna null se a particao estiver inacessivel dentro do prazo.
   */
  public RespostaConsulta consultarShard(int shardDestino, TipoConsulta tipo, String parametro, long timeoutMs) {
    return consultarShard(shardDestino, tipo, parametro, null, timeoutMs);
  }

  public RespostaConsulta consultarShard(int shardDestino, TipoConsulta tipo, String parametro,
      ArrayList<String> gruposEsperados, long timeoutMs) {
    Particionador.validarShard(shardDestino);

    // Atalho zero-hop se este proprio no for o lider ATIVO do shard consultado
    if (shardDestino == meuShard && "ATIVO".equalsIgnoreCase(ctx.papel())) {
      if (tipo == TipoConsulta.VERIFICAR_SESSAO_USUARIO) {
        return avaliarSessaoLocal("local", parametro, gruposEsperados);
      }
      ArrayList<String> grupos = null;
      ArrayList<InfoUser> usuarios = null;
      if (tipo == TipoConsulta.LISTAR_GRUPOS) {
        grupos = new ArrayList<>(gerenciador.listarGrupos());
      } else if (tipo == TipoConsulta.LISTAR_USUARIOS) {
        usuarios = new ArrayList<>(gerenciador.getTodosUsuariosAtivos());
      } else if (tipo == TipoConsulta.MEMBROS_GRUPO) {
        usuarios = new ArrayList<>(gerenciador.getTodosMembros(parametro));
      }
      return new RespostaConsulta("local", Status.OK, config.getMeuId(), grupos, usuarios, null);
    }

    final String corr = UUID.randomUUID().toString();
    final long limite = System.currentTimeMillis() + timeoutMs;
    final Set<Integer> tentados = new HashSet<>();
    final int maxRodadasFalha = (timeoutMs <= 700) ? 1 : 2;
    int rodadasConexaoFalha = 0;
    boolean algumNoVivo = false;

    final PedidoConsulta pedido = new PedidoConsulta(corr, shardDestino, tipo, parametro, gruposEsperados,
        host, config.getMeuNoInfo().portaCluster, meuShard, config.getMeuId());

    try {
      while (System.currentTimeMillis() < limite && executando) {
        int alvo = tabela.getLider(shardDestino);
        if (alvo <= 0 || tentados.contains(alvo)) {
          alvo = proximoCandidato(shardDestino, tentados);
        }
        if (alvo <= 0) {
          if (!algumNoVivo) {
            rodadasConexaoFalha++;
            // Se TODAS as replicas do shard recusaram conexao TCP, o shard esta offline (fail-fast)
            if (rodadasConexaoFalha >= maxRodadasFalha) {
              break;
            }
          }
          algumNoVivo = false;
          tentados.clear();
          Thread.sleep(120);
          continue;
        }

        CompletableFuture<RespostaConsulta> fc = new CompletableFuture<>();
        consultasPendentes.put(corr, fc);

        boolean enviado = enviar(shardDestino, alvo, criarMsg(TipoMensagemCluster.CONSULTA_SHARD, pedido));
        if (!enviado) {
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
          continue;
        }
        algumNoVivo = true;

        RespostaConsulta rc;
        try {
          long espera = Math.min(800, Math.max(50, limite - System.currentTimeMillis()));
          rc = fc.get(espera, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException te) {
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
          continue;
        }

        if (rc.status == Status.OK) {
          return rc;
        } else if (rc.status == Status.REDIRECIONAR) {
          tentados.clear();
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
          tabela.sugerir(shardDestino, rc.idLider);
        } else {
          tentados.add(alvo);
          tabela.invalidar(shardDestino, alvo);
        }
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      consultasPendentes.remove(corr);
    }
    return null;
  }

  /**
   * Scatter-Gather resiliente para o comando LIST (timeout curto: 2s).
   * Se um shard estiver totalmente offline, agrega os demais e retorna resposta parcial sem travar.
   */
  public List<String> listarGruposGlobal() {
    if (!executando) {
      return Collections.emptyList();
    }
    long t0 = System.currentTimeMillis();
    List<CompletableFuture<RespostaConsulta>> futuros = new ArrayList<>();
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      final int shard = s;
      futuros.add(CompletableFuture.supplyAsync(
          () -> consultarShard(shard, TipoConsulta.LISTAR_GRUPOS, null, TIMEOUT_SCATTER_MS), pool));
    }

    Set<String> agregados = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    List<String> shardsOk = new ArrayList<>();
    List<String> shardsOff = new ArrayList<>();

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      try {
        long restante = Math.max(50, (t0 + TIMEOUT_SCATTER_MS) - System.currentTimeMillis());
        RespostaConsulta rc = futuros.get(s).get(restante, TimeUnit.MILLISECONDS);
        if (rc != null && rc.status == Status.OK && rc.grupos != null) {
          agregados.addAll(rc.grupos);
          shardsOk.add("S" + s);
        } else {
          shardsOff.add(Particionador.nome(s));
        }
      } catch (Exception e) {
        shardsOff.add(Particionador.nome(s));
      }
    }

    if (shardsOff.isEmpty()) {
      ClusterLog.info("SCATTER-GATHER", String.format(
          "LIST roteado e agregado com sucesso dos 3 shards %s em %dms -> %d grupo(s).",
          shardsOk, (System.currentTimeMillis() - t0), agregados.size()));
    } else {
      ClusterLog.aviso("SCATTER-GATHER", String.format(
          "LIST parcial em %dms: particao(oes) %s offline/timeout -> respondendo com shards disponiveis %s (%d grupo(s)).",
          (System.currentTimeMillis() - t0), shardsOff, shardsOk, agregados.size()));
    }
    return new ArrayList<>(agregados);
  }

  /**
   * Scatter-Gather resiliente para o comando USERS e notificacoes globais UPDATE_USERS (timeout curto: 2s).
   * Retorna os objetos InfoUser de todos os shards alcancaveis sem bloquear o cliente se uma particao cair.
   */
  public List<InfoUser> coletarTodosUsuariosAtivos() {
    if (!executando) {
      return Collections.emptyList();
    }
    long t0 = System.currentTimeMillis();
    List<CompletableFuture<RespostaConsulta>> futuros = new ArrayList<>();
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      final int shard = s;
      futuros.add(CompletableFuture.supplyAsync(
          () -> consultarShard(shard, TipoConsulta.LISTAR_USUARIOS, null, TIMEOUT_SCATTER_MS), pool));
    }

    Map<String, InfoUser> porNome = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    List<String> shardsOk = new ArrayList<>();
    List<String> shardsOff = new ArrayList<>();

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      try {
        long restante = Math.max(50, (t0 + TIMEOUT_SCATTER_MS) - System.currentTimeMillis());
        RespostaConsulta rc = futuros.get(s).get(restante, TimeUnit.MILLISECONDS);
        if (rc != null && rc.status == Status.OK && rc.usuarios != null) {
          for (InfoUser u : rc.usuarios) {
            if (u != null && u.getNome() != null) {
              porNome.put(u.getNome().trim(), u);
            }
          }
          shardsOk.add("S" + s);
        } else {
          shardsOff.add(Particionador.nome(s));
        }
      } catch (Exception e) {
        shardsOff.add(Particionador.nome(s));
      }
    }

    if (shardsOff.isEmpty()) {
      ClusterLog.info("SCATTER-GATHER", String.format(
          "USERS roteado e agregado com sucesso dos 3 shards %s em %dms -> %d usuario(s) online.",
          shardsOk, (System.currentTimeMillis() - t0), porNome.size()));
    } else {
      ClusterLog.aviso("SCATTER-GATHER", String.format(
          "USERS parcial em %dms: particao(oes) %s offline/timeout -> respondendo com shards disponiveis %s (%d usuario(s)).",
          (System.currentTimeMillis() - t0), shardsOff, shardsOk, porNome.size()));
    }
    return new ArrayList<>(porNome.values());
  }

  /**
   * Consulta roteada para MEMBERS: direciona estritamente ao shard dono do grupo.
   */
  public ResultadoMembros listarMembrosGrupo(String nomeGrupo) {
    int shardGrupo = Particionador.shardDe(nomeGrupo);
    ClusterLog.info(TAG, "Consultando MEMBERS do grupo '" + nomeGrupo + "' no " + Particionador.nome(shardGrupo));
    RespostaConsulta rc = consultarShard(shardGrupo, TipoConsulta.MEMBROS_GRUPO, nomeGrupo, 2500);
    if (rc == null || rc.status != Status.OK) {
      return new ResultadoMembros(false, Collections.emptyList(),
          "Particao " + Particionador.nome(shardGrupo) + " indisponivel no momento");
    }
    List<InfoUser> lista = (rc.usuarios != null) ? new ArrayList<>(rc.usuarios) : new ArrayList<>();
    lista.sort(Comparator.comparing(u -> u.getNome() != null ? u.getNome().toLowerCase() : ""));
    return new ResultadoMembros(true, lista, null);
  }

  /**
   * Broadcast assincrono global de BLOCK / UNBLOCK para todos os demais shards,
   * mantendo a tabela de bloqueios replicada em todas as particoes para leitura local O(1) em SEND.
   */
  public void replicarBloqueioGlobalAssincrono(boolean bloquear, String idReqBase,
      String usuarioOrigem, String usuarioAlvo, int shardJaAplicado) {
    final String base = (idReqBase != null && !idReqBase.trim().isEmpty())
        ? idReqBase.trim()
        : ("blk-" + UUID.randomUUID());

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      if (s == shardJaAplicado) continue;
      final int shardAlvo = s;
      pool.submit(() -> {
        String reqShard = base + "-S" + shardAlvo;
        Operacao opCopia = bloquear
            ? Operacao.bloquear(reqShard, usuarioOrigem, usuarioAlvo)
            : Operacao.desbloquear(reqShard, usuarioOrigem, usuarioAlvo);
        Resultado r = executar(shardAlvo, opCopia, 2000);
        if (r.isSucesso()) {
          ClusterLog.info("BLOCK:GLOBAL", String.format(
              "%s ('%s' -> '%s') replicado assincronamente com sucesso no %s.",
              (bloquear ? "BLOCK" : "UNBLOCK"), usuarioOrigem, usuarioAlvo, Particionador.nome(shardAlvo)));
        } else {
          ClusterLog.aviso("BLOCK:GLOBAL", String.format(
              "Nao foi possivel replicar %s para %s (particao possivelmente offline: %s).",
              (bloquear ? "BLOCK" : "UNBLOCK"), Particionador.nome(shardAlvo), r.getMensagem()));
        }
      });
    }
  }

  /**
   * Remove um usuario (LOGOUT / LEAVE GLOBAL) sincronamente no shard dono do cadastro
   * e assincronamente nos demais shards para limpeza imediata de salas cross-shard.
   */
  public Resultado removerUsuarioGlobal(String idReqBase, String nomeUsuario) {
    int shardDono = Particionador.shardDe(nomeUsuario);
    final String base = (idReqBase != null && !idReqBase.trim().isEmpty())
        ? idReqBase.trim()
        : ("logout-" + UUID.randomUUID());

    Resultado resPrincipal = executar(shardDono, Operacao.removerUsuario(base + "-S" + shardDono, nomeUsuario), TIMEOUT_PADRAO_MS);

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      if (s == shardDono) continue;
      final int shardExtra = s;
      pool.submit(() -> executar(shardExtra, Operacao.removerUsuario(base + "-S" + shardExtra, nomeUsuario), 2000));
    }
    return resPrincipal;
  }

  /**
   * Trata a queda brusca de conexao TCP (Ghost Client): dispara broadcast UDP interno USUARIO_CAIU
   * para todos os lideres e roteia sincronamente (em paralelo com timeout curto) REMOVER_USUARIO + SAIR_GRUPO
   * para todos os 3 shards (S0, S1 e S2), limpando o cliente fantasma da memoria e das replicas de todos.
   */
  public void tratarQuedaBruscaUsuario(String nomeUsuario) {
    if (!executando || nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return;
    }
    final String nomeTrim = nomeUsuario.trim();
    final String idEvento = UUID.randomUUID().toString().substring(0, 8);
    BeaconClusterLAN.dispararBroadcastUsuarioCaiu(idEvento, nomeTrim, meuShard, config.getIdReplica());

    List<CompletableFuture<Resultado>> futuros = new ArrayList<>();
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      final int shard = s;
      futuros.add(CompletableFuture.supplyAsync(() -> {
        String idIdempotente = "usuario-caiu-" + idEvento + "-S" + shard;
        return executar(shard, Operacao.removerUsuario(idIdempotente, nomeTrim), 2000);
      }, pool));
    }

    long limite = System.currentTimeMillis() + 2200;
    List<String> shardsLimpos = new ArrayList<>();
    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      try {
        long espera = Math.max(50, limite - System.currentTimeMillis());
        Resultado r = futuros.get(s).get(espera, TimeUnit.MILLISECONDS);
        if (r != null && r.isSucesso()) {
          shardsLimpos.add("S" + s);
        }
      } catch (Exception ignored) {
      }
    }

    ClusterLog.info("GHOST-CLIENT", String.format(
        "Faxina cross-shard concluida para '%s' [ev=%s]: REMOVER_USUARIO e SAIR_GRUPO aplicados nos shards %s.",
        nomeTrim, idEvento, shardsLimpos));
  }

  // =========================================================================
  // FASE 5: ENCAMINHAMENTO DE DATAGRAMAS UDP ENTRE SHARDS
  // =========================================================================

  /**
   * Encaminha assincronamente uma APDU UDP (SEND, SENDVU, SENDPVT, CONFIRM) para o lider ativo do shardDestino,
   * preservando o IP e a porta UDP originais do cliente para entrega direta de mensagens e Auto-ACK.
   */
  public void encaminharUdp(int shardDestino, APDU apdu, String ipCliente, int portaUdpCliente, int saltos) {
    Particionador.validarShard(shardDestino);
    if (apdu == null || saltos > 4) {
      return;
    }
    if (shardDestino == meuShard && "ATIVO".equalsIgnoreCase(ctx.papel())) {
      ReceptorUdpRoteado rec = this.receptorUdp;
      if (rec != null) {
        rec.entregar(apdu, ipCliente, portaUdpCliente, saltos);
      }
      return;
    }

    final String idPacote = UUID.randomUUID().toString();
    final PacoteUdpRoteado pkt = new PacoteUdpRoteado(
        idPacote, shardDestino, apdu, ipCliente, portaUdpCliente, saltos);

    pool.submit(() -> {
      Set<Integer> tentados = new HashSet<>();
      int liderConhecido = tabela.getLider(shardDestino);
      if (liderConhecido > 0) {
        if (enviar(shardDestino, liderConhecido, criarMsg(TipoMensagemCluster.ENCAMINHAR_UDP, pkt))) {
          return;
        }
        tentados.add(liderConhecido);
        tabela.invalidar(shardDestino, liderConhecido);
      }

      for (int id = 1; id <= totalReplicas; id++) {
        if (tentados.contains(id)) continue;
        if (shardDestino == meuShard && id == config.getMeuId()) continue;
        if (enviar(shardDestino, id, criarMsg(TipoMensagemCluster.ENCAMINHAR_UDP, pkt))) {
          return;
        }
      }

      System.err.println("[UDP] [ERRO] Particao " + Particionador.nome(shardDestino)
          + " indisponivel: nao foi possivel encaminhar datagrama " + apdu.getOperacao() + ".");
    });
  }

  private void tratarPacoteUdpRoteado(PacoteUdpRoteado pkt) {
    if (pkt == null || pkt.apdu == null || pkt.shardDestino != meuShard || pkt.saltos > 4) {
      return;
    }

    if ("ATIVO".equalsIgnoreCase(ctx.papel())) {
      if (pkt.idPacote != null && !pacotesUdpVistos.add(pkt.idPacote)) {
        return;
      }
      ReceptorUdpRoteado rec = this.receptorUdp;
      if (rec != null) {
        rec.entregar(pkt.apdu, pkt.ipCliente, pkt.portaUdpCliente, pkt.saltos);
      }
    } else {
      // Se este no for STANDBY (ex.: tabela de lideres do emissor estava defasada), repassa ao lider ATIVO deste shard
      int lider = ctx.idLiderAtivo();
      if (lider > 0 && lider != config.getMeuId()) {
        PacoteUdpRoteado repasse = new PacoteUdpRoteado(
            pkt.idPacote, pkt.shardDestino, pkt.apdu, pkt.ipCliente, pkt.portaUdpCliente, pkt.saltos + 1);
        enviar(meuShard, lider, criarMsg(TipoMensagemCluster.ENCAMINHAR_UDP, repasse));
      }
    }
  }

  // =========================================================================
  // FASE 6: DETECCAO DE AMNESIA DE SHARD E INVALIDACAO DE SOCKET NO GATEWAY
  // =========================================================================

  /**
   * Verifica se os shards responsaveis pelo cadastro do usuario (Home Shard) e pelos grupos ativos
   * desta sessao ainda possuem o estado do cliente em memoria.
   * <ul>
   *   <li>Se um shard estiver OFFLINE (inalcancavel), retorna {@code null} (preserva disponibilidade parcial CAP).</li>
   *   <li>Se um shard estiver ONLINE ({@code Status.OK}) mas tiver reiniciado sem o usuario/grupo na memoria
   *       (amnesia volátil), retorna o motivo do erro para que o gateway derrube o socket TCP do cliente.</li>
   * </ul>
   */
  public String verificarAmnesiaSessao(String nomeUsuario, Set<String> gruposAtivosSessao) {
    if (!executando || nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return null;
    }
    String nomeTrim = nomeUsuario.trim();
    Map<Integer, ArrayList<String>> gruposPorShard = new LinkedHashMap<>();
    int shardHome = Particionador.shardDe(nomeTrim);
    gruposPorShard.put(shardHome, new ArrayList<>());

    if (gruposAtivosSessao != null) {
      for (String g : gruposAtivosSessao) {
        if (g == null || g.trim().isEmpty()) continue;
        int sg = Particionador.shardDe(g);
        gruposPorShard.computeIfAbsent(sg, k -> new ArrayList<>()).add(g);
      }
    }

    for (Map.Entry<Integer, ArrayList<String>> entry : gruposPorShard.entrySet()) {
      int shardAlvo = entry.getKey();
      ArrayList<String> gruposNoShard = entry.getValue();
      RespostaConsulta rc = consultarShard(
          shardAlvo, TipoConsulta.VERIFICAR_SESSAO_USUARIO, nomeTrim, gruposNoShard, 550);
      if (rc != null && rc.status == Status.OK && !rc.sessaoValida) {
        return (rc.motivoInvalidez != null)
            ? rc.motivoInvalidez
            : ("Estado de '" + nomeTrim + "' ausente na memoria do " + Particionador.nome(shardAlvo));
      }
    }
    return null;
  }

  /**
   * Valida assincronamente se o remetente de um comando UDP (ex.: SENDPVT) ainda consta na memoria
   * do seu Home Shard; caso o Home Shard esteja online e tenha sofrido amnesia, derruba o TCP no gateway.
   */
  public void validarCadastroRemetenteAssincrono(String nomeUsuario) {
    if (!executando || nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return;
    }
    final String nomeTrim = nomeUsuario.trim();
    pool.submit(() -> {
      String erroAmnesia = verificarAmnesiaSessao(nomeTrim, Collections.emptySet());
      if (erroAmnesia != null) {
        invalidarSessaoClienteGlobal(nomeTrim, erroAmnesia);
      }
    });
  }

  /**
   * Notifica todos os shards (e executa localmente) o fechamento imediato do socket TCP do cliente
   * cujo estado volátil desapareceu apos reinicio total de um shard, forcando Failover + Re-REGISTER/Re-JOIN.
   */
  public void invalidarSessaoClienteGlobal(String nomeUsuario, String motivo) {
    if (!executando || nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return;
    }
    final String nomeTrim = nomeUsuario.trim();
    final String idEvento = UUID.randomUUID().toString();
    final InvalidaSessaoCliente payload = new InvalidaSessaoCliente(idEvento, nomeTrim, motivo, meuShard);

    tratarInvalidacaoSessao(payload);

    pool.submit(() -> {
      MensagemCluster msg = criarMsg(TipoMensagemCluster.INVALIDAR_SESSAO_CLIENTE, payload);
      for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
        if (s == meuShard) continue;
        int lider = tabela.getLider(s);
        if (lider > 0 && enviar(s, lider, msg)) {
          continue;
        }
        for (int r = 0; r < totalReplicas; r++) {
          enviar(s, r + 1, msg);
        }
      }
    });
  }

  private void tratarInvalidacaoSessao(InvalidaSessaoCliente inv) {
    if (inv == null || inv.nomeUsuario == null || !executando) {
      return;
    }
    if (inv.idEvento != null && !invalidacoesVistas.add(inv.idEvento)) {
      return;
    }
    Object sessao = gerenciador.obterSessaoAtiva(inv.nomeUsuario);
    if (sessao instanceof AtendimentoCliente) {
      ((AtendimentoCliente) sessao).forcarQuedaPorAmnesiaShard(inv.motivo);
    } else if (!"ATIVO".equalsIgnoreCase(ctx.papel())) {
      int lider = ctx.idLiderAtivo();
      if (lider > 0 && lider != config.getMeuId()) {
        enviar(meuShard, lider, criarMsg(TipoMensagemCluster.INVALIDAR_SESSAO_CLIENTE, inv));
      }
    }
  }
}
