/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 02/11/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: NoCluster
* Funcao.............: Orquestrador da maquina de estados do no no cluster, eleicao sem preempcao (D1) e ciclo de vida.
*************************************************************** */

package cluster;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;
import model.NotificadorClientes;
import model.Servidor;
import utils.Protocolo;

/**
 * Controla o papel do no (ATIVO, STANDBY, SINCRONIZANDO, CANDIDATO), a eleicao de lideres,
 * os heartbeats periodicos e a ativacao/desativacao dinamica das portas de atendimento.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class NoCluster implements CanalCluster.ReceptorMensagemCluster {

  private final ConfigCluster config;
  private final GerenciadorGrupos gerenciador;
  private final VisaoCluster visao;
  private final CanalCluster canal;
  private final Replicador replicador;
  private final Servidor servidor;
  private final RoteadorShards roteador;

  private volatile String papel = "INICIANDO"; // INICIANDO, SINCRONIZANDO, STANDBY, CANDIDATO, ATIVO
  private volatile long epoca = 0;
  private volatile int idLiderAtivo = -1;
  private volatile boolean executando = true;

  // Latch de votacao durante eleicao
  private CountDownLatch latchVotos;
  private final AtomicBoolean eleicaoEmAndamento = new AtomicBoolean(false);
  private final long inicioMs = System.currentTimeMillis();
  private volatile long ultimoPedidoSyncMs = 0L;
  private final Set<String> eventosQuedaProcessados = Collections.synchronizedSet(
      Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
          return size() > 512;
        }
      }));

  /**
   * Inicializa os componentes de consenso intra-shard, replicacao, atendimento de rede e roteamento inter-shard.
   *
   * @param config      Configuracao de topologia deste no.
   * @param gerenciador Estrutura de dados particionada e replicada sob responsabilidade deste no.
   */
  public NoCluster(ConfigCluster config, GerenciadorGrupos gerenciador) {
    this.config = config;
    this.gerenciador = gerenciador;
    this.visao = new VisaoCluster(config);
    this.canal = new CanalCluster(config, visao, this);
    this.replicador = new Replicador(config.getMeuId(), gerenciador, canal, visao);
    this.servidor = new Servidor(gerenciador);
    this.roteador = new RoteadorShards(config, gerenciador, new RoteadorShards.Contexto() {
      @Override
      public String papel() {
        return papel;
      }

      @Override
      public int idLiderAtivo() {
        return idLiderAtivo;
      }

      @Override
      public long epoca() {
        return epoca;
      }
    });
    this.servidor.setRoteador(this.roteador);
    this.canal.setReceptorInterShard(roteador::receber);
  }

  public int getMeuId() {
    return config.getMeuId();
  }

  public String getPapel() {
    return papel;
  }

  public long getEpoca() {
    return epoca;
  }

  public int getIdLiderAtivo() {
    return idLiderAtivo;
  }

  public GerenciadorGrupos getGerenciador() {
    return gerenciador;
  }

  public VisaoCluster getVisao() {
    return visao;
  }

  public Replicador getReplicador() {
    return replicador;
  }

  public RoteadorShards getRoteador() {
    return roteador;
  }

  public CanalCluster getCanal() {
    return canal;
  }

  /**
   * Inicia o canal de cluster, descobre lideres e dispara os temporizadores.
   */
  public void iniciar() {
    canal.iniciar();
    roteador.iniciar();

    // Loop de heartbeat e liveness (a cada 1000 ms)
    Thread threadHeartbeat = new Thread(() -> {
      while (executando) {
        try {
          enviarHeartbeat();
          ClusterLog.heartbeatResumo(config.getNos().size(), visao.getParesVivos().size() + 1, idLiderAtivo);

          long decorrido = System.currentTimeMillis() - inicioMs;
          if (decorrido > 3000) {
            boolean ativoCaiu = visao.verificarTimeouts(6000);
            if (!ativoCaiu && idLiderAtivo > 0 && idLiderAtivo != config.getMeuId()) {
              VisaoCluster.ParEstado liderSt = visao.getPar(idLiderAtivo);
              if (liderSt != null
                  && (liderSt.ultimoHeartbeat > 0 && (System.currentTimeMillis() - liderSt.ultimoHeartbeat > 6000))) {
                ClusterLog.aviso("ELEICAO", "Lider ativo conhecido (No " + idLiderAtivo + ") expirou sem heartbeat ha mais de 6s.");
                ativoCaiu = true;
              }
            }

            if (ativoCaiu && !"ATIVO".equalsIgnoreCase(papel)) {
              ClusterLog.eleicao("Lider ausente ha mais de 6s. Disparando eleicao para novo lider...");
              int antigoLider = this.idLiderAtivo;
              this.idLiderAtivo = -1;
              if (antigoLider > 0) {
                visao.marcarDown(antigoLider);
              }
              dispararEleicao();
            }
          }

          // Se estiver iniciando e apos timeout nenhum lider foi detectado
          if ("INICIANDO".equalsIgnoreCase(papel)) {
            avaliarInicializacao();
          }

          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          ClusterLog.erro("CLUSTER", "Falha no loop de heartbeat: " + e.getMessage());
        }
      }
    }, "Thread-NoCluster-Heartbeat");
    threadHeartbeat.setDaemon(true);
    threadHeartbeat.start();
  }

  private void enviarHeartbeat() {
    MensagemCluster hb = new MensagemCluster(TipoMensagemCluster.HEARTBEAT, config.getMeuId());
    hb.setEpoca(epoca);
    hb.setVersao(gerenciador.getVersao());
    hb.setHash(gerenciador.calcularHash());
    hb.setEstadoOrigem(papel);
    hb.setIdAtivoConhecido(idLiderAtivo);

    ConfigCluster.NoInfo info = config.getMeuNoInfo();
    hb.setHostOrigem(info.host);
    hb.setPortaClusterOrigem(info.portaCluster);
    hb.setPortaTcpServico(info.portaTcp);
    hb.setPortaUdpServico(info.portaUdp);

    canal.broadcast(hb);
  }

  private void avaliarInicializacao() {
    long decorrido = System.currentTimeMillis() - inicioMs;
    // Aguarda pelo menos 1500ms para conexoes do cluster se estabelecerem e heartbeats circularem
    if (decorrido < 1500) {
      return;
    }

    // Verifica se ha algum lider ativo nos pares
    int ativoConhecido = visao.getIdAtivoAtual();
    if (ativoConhecido > 0 && ativoConhecido != config.getMeuId()) {
      ClusterLog.info("CLUSTER", "Lider ativo detectado no No " + ativoConhecido + ". Solicitando sincronizacao...");
      this.idLiderAtivo = ativoConhecido;
      this.papel = "SINCRONIZANDO";
      MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
      pedido.setVersao(gerenciador.getVersao());
      canal.enviar(ativoConhecido, pedido);
      return;
    }

    // Se algum no com menor ID existir no meu shard, este no DEVE assumir papel de STANDBY!
    int meuId = config.getMeuId();
    boolean temMenorIdEsperado = false;
    int menorIdEncontrado = -1;
    for (ConfigCluster.NoInfo outro : config.getOutrosNos()) {
      if (outro.id < meuId) {
        temMenorIdEsperado = true;
        menorIdEncontrado = outro.id;
        break;
      }
    }

    if (temMenorIdEsperado) {
      ClusterLog.info("CLUSTER", String.format(
          "Shard S%d | Replica %d (No %d): No %d possui menor ID. Assumindo papel de STANDBY e solicitando snapshot.",
          config.getIdShard(), config.getIdReplica(), meuId, menorIdEncontrado));
      this.papel = "STANDBY";
      this.idLiderAtivo = menorIdEncontrado;
      MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, meuId);
      pedido.setVersao(gerenciador.getVersao());
      canal.enviar(menorIdEncontrado, pedido);
      return;
    }

    ClusterLog.lider("Nenhum lider ativo encontrado na rede. Assumindo lideranca pioneira (No " + meuId + ")...");
    assumirComoAtivo();
  }

  private void aguardarConexaoSaida(int parId, int timeoutMs) {
    long limite = System.currentTimeMillis() + timeoutMs;
    while (!canal.isParConectado(parId) && System.currentTimeMillis() < limite) {
      try {
        Thread.sleep(30);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void solicitarSyncComCooldown(int idLider) {
    if (idLider <= 0 || idLider == config.getMeuId()) {
      return;
    }
    long agora = System.currentTimeMillis();
    if (agora - ultimoPedidoSyncMs < 1000L) {
      return;
    }
    ultimoPedidoSyncMs = agora;
    MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
    pedido.setVersao(gerenciador.getVersao());
    canal.enviar(idLider, pedido);
  }

  /**
   * Chamado quando o Beacon UDP descobre dinamicamente um novo par do mesmo Shard.
   * Executa a regra de eleicao e subordinacao hierarquica deterministica (menor ID prevalece).
   */
  public synchronized void notificarNovoParDescoberto(ConfigCluster.NoInfo novoPar) {
    if (novoPar == null || novoPar.idShard != config.getIdShard()) {
      return;
    }
    config.registrarParDinamico(novoPar);
    visao.adicionarParSeAusente(novoPar);
    int portaClusterReal = PortasShard.clusterDoNo(novoPar.id);
    canal.adicionarParSeAusente(novoPar.id, novoPar.host, portaClusterReal);

    int meuId = config.getMeuId();
    int parId = novoPar.id;

    ClusterLog.info("BEACON:SHARD", String.format(
        "Par descoberto: No %d. Meu ID: %d. Resolvendo lideranca...",
        parId, meuId));

    // Regra de Subordinacao e Hierarquia Intra-Shard:
    // O no com menor ID e a autoridade pioneira / lider natural quando as versoes sao iguais.
    if (parId < meuId) {
      if ("ATIVO".equalsIgnoreCase(this.papel)) {
        ClusterLog.cura(String.format(
            "[BEACON:SHARD] Par descoberto: No %d. Meu ID: %d. No %d possui menor ID: cedendo lideranca e transicionando para STANDBY.",
            parId, meuId, parId));
        rebaixarParaStandby(parId, this.epoca);
        this.papel = "SINCRONIZANDO";
        aguardarConexaoSaida(parId, 300);
        solicitarSyncComCooldown(parId);
      } else if ("INICIANDO".equalsIgnoreCase(this.papel)) {
        ClusterLog.info("BEACON:SHARD", String.format(
            "[BEACON:SHARD] Par descoberto: No %d. Meu ID: %d. Reconhecendo No %d como lider pioneiro (STANDBY).",
            parId, meuId, parId));
        this.idLiderAtivo = parId;
        this.papel = "SINCRONIZANDO";
        aguardarConexaoSaida(parId, 300);
        solicitarSyncComCooldown(parId);
      } else if ("STANDBY".equalsIgnoreCase(this.papel)) {
        // Ja em STANDBY: apenas garante idLiderAtivo apontando para o menor ID
        this.idLiderAtivo = parId;
      }
    } else {
      // Se eu sou o menor ID (ex: Meu=1 vs Par=4) e sou ATIVO:
      if ("ATIVO".equalsIgnoreCase(this.papel)) {
        ClusterLog.lider(String.format(
            "[BEACON:SHARD] Par descoberto: No %d. Meu ID: %d. Sou o lider ATIVO (menor ID). Impondo lideranca ao No %d.",
            parId, meuId, parId));
        aguardarConexaoSaida(parId, 300);
        ConfigCluster.NoInfo meuInfo = config.getMeuNoInfo();
        MensagemCluster coord = new MensagemCluster(TipoMensagemCluster.COORDENADOR, meuId);
        coord.setEpoca(this.epoca);
        coord.setVersao(gerenciador.getVersao());
        coord.setPortaTcpServico(meuInfo.portaTcp);
        coord.setPortaUdpServico(meuInfo.portaUdp);
        canal.enviar(parId, coord);
      }
    }
  }

  /**
   * Chamado quando um anuncio EDEN_SHARD_LAN e recebido de outro host para o MESMO Shard.
   * Detecta split-brain entre maquinas fisicas ou descobre replicas na LAN/localhost.
   */
  public synchronized void processarAnuncioMesmoShardLAN(
      int shardRemoto, int replicaRemota, int idNoRemoto, String papelRemoto, long epocaRemota, int idLiderRemoto, String hostRemoto) {
    int meuId = config.getMeuId();
    String meuHost = config.getMeuNoInfo().host;
    // Rejeita APENAS se for exatamente este mesmo processo (idNoRemoto == meuId E hostRemoto.equals(meuHost)).
    // Processos diferentes no mesmo host precisam se ouvir e se eleger!
    if (hostRemoto == null || (idNoRemoto == meuId && hostRemoto.equals(meuHost))) {
      return;
    }
    // 1. Colisao de ID entre hosts fisicos (duas maquinas diferentes assumiram o mesmo ID)
    if (idNoRemoto == meuId) {
      ClusterLog.conflito(String.format(
          "[COLISAO:LAN] Outra maquina (%s) reivindica o ID %d no Shard S%d (Papel remoto: %s vs Meu papel: %s)!",
          hostRemoto, meuId, config.getIdShard(), papelRemoto, this.papel));

      if ("ATIVO".equalsIgnoreCase(this.papel)) {
        long minhaVersao = gerenciador.getVersao();
        // Desempate deterministico por IP se versoes forem identicas (0): o IP maior cede e vira Standby
        boolean euCedo = (minhaVersao == 0 && meuHost.compareTo(hostRemoto) > 0);
        if (euCedo) {
          ClusterLog.cura(String.format(
              "Meu host (%s) > Host remoto (%s): cedendo ID %d e assumindo modo STANDBY.",
              meuHost, hostRemoto, meuId));
          rebaixarParaStandby(idNoRemoto, epocaRemota);
        }
      }
      return;
    }

    // 2. Par do mesmo shard com ID diferente (ex: No 1 vs No 4)
    ConfigCluster.NoInfo parInfo = new ConfigCluster.NoInfo(
        idNoRemoto, config.getIdShard(), replicaRemota, hostRemoto,
        PortasShard.clusterDoNo(idNoRemoto), PortasShard.tcpDoNo(idNoRemoto),
        PortasShard.udpDoNo(idNoRemoto), Protocolo.PORTA_DISCOVERY
    );
    notificarNovoParDescoberto(parInfo);
  }

  /**
   * Dispara o processo de eleicao sem preempcao.
   */
  public synchronized void dispararEleicao() {
    if (!eleicaoEmAndamento.compareAndSet(false, true)) {
      return;
    }

    new Thread(() -> {
      try {
        int meuId = config.getMeuId();
        // Escalonamento deterministico por ID para evitar tempestades concorrentes de eleicao
        int delayMs = PortasShard.replicaDoNo(meuId) * 80;
        if (delayMs > 0) {
          Thread.sleep(delayMs);
        }

        // Verifica se algum lider ja foi anunciado durante a espera
        if (idLiderAtivo > 0 && idLiderAtivo != meuId) {
          VisaoCluster.ParEstado liderAtual = visao.getPar(idLiderAtivo);
          if (liderAtual != null && liderAtual.isVivo()) {
            return;
          }
        }

        ClusterLog.eleicao("Avaliando candidatos elegiveis vivos...");
        List<VisaoCluster.ParEstado> elegiveis = visao.getParesElegiveis();

        long minhaVersao = gerenciador.getVersao();
        boolean souMelhor = true;

        for (VisaoCluster.ParEstado p : elegiveis) {
          if (p.id == meuId) continue;
          if (p.versao > minhaVersao) {
            souMelhor = false;
            break;
          } else if (p.versao == minhaVersao && p.id < meuId) {
            souMelhor = false;
            break;
          }
        }

        if (souMelhor) {
          this.papel = "CANDIDATO";
          this.epoca = Math.max(this.epoca, visao.getMaiorEpocaConhecida()) + 1;
          ClusterLog.eleicao("Sou o melhor candidato (Versao: " + minhaVersao
              + ", ID: " + meuId + "). Enviando PROPOSTA na epoca " + epoca + "...");

          List<VisaoCluster.ParEstado> alvos = visao.getParesVivos();
          alvos.removeIf(p -> p.id == meuId || !p.conectado || "DOWN".equalsIgnoreCase(p.estado));

          if (alvos.isEmpty()) {
            ClusterLog.lider("Nenhum outro no ativo contactavel (demais nos DOWN). Assumindo lideranca como sobrevivente.");
            assumirComoAtivo();
            return;
          }

          latchVotos = new CountDownLatch(alvos.size());
          MensagemCluster proposta = new MensagemCluster(TipoMensagemCluster.PROPOSTA, meuId);
          proposta.setEpoca(epoca);
          proposta.setVersao(minhaVersao);

          for (VisaoCluster.ParEstado p : alvos) {
            canal.enviar(p.id, proposta);
          }

          boolean venceu = latchVotos.await(2000, TimeUnit.MILLISECONDS);
          if (venceu || latchVotos.getCount() == 0) {
            if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
              ClusterLog.lider("Votos obtidos da maioria dos pares! Assumindo lideranca ativa do cluster.");
              assumirComoAtivo();
            }
          } else {
            ClusterLog.aviso("ELEICAO", "Timeout de votacao. Voltando para STANDBY.");
            if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
              this.papel = "STANDBY";
            }
          }
        } else {
          ClusterLog.standby("Outro no tem maior prioridade para lideranca. Aguardando anuncio de COORDENADOR...");
          this.papel = "STANDBY";
        }
      } catch (Exception e) {
        ClusterLog.erro("ELEICAO", "Falha no processo de eleicao: " + e.getMessage());
      } finally {
        eleicaoEmAndamento.set(false);
      }
    }, "Thread-Eleicao").start();
  }

  /**
   * Promove o no para ATIVO: abre portas de atendimento e notifica os clientes.
   */
  public synchronized void assumirComoAtivo() {
    this.papel = "ATIVO";
    this.idLiderAtivo = config.getMeuId();

    ConfigCluster.NoInfo info = config.getMeuNoInfo();
    // Qualquer lider ativo do Shard 0 (R0 na 5000, ou R1 em failover) mantem as portas legadas 5000/5001 abertas
    boolean incluirLegado = (config.getIdShard() == 0);
    servidor.iniciarServicos(
        info.portaTcp, info.portaUdp, info.portaDiscovery, incluirLegado, config.getMeuId(), config.getIdShard());

    ClusterLog.lider(String.format(
        "Shard %s | Replica %d (No %d) assumiu coordenacao do cluster (Epoca: %d, Versao: %d, Hash: %s)",
        Particionador.nome(config.getIdShard()), config.getIdReplica(), config.getMeuId(),
        epoca, gerenciador.getVersao(), gerenciador.calcularHash()));

    // Envia COORDENADOR para todos os pares
    MensagemCluster coord = new MensagemCluster(TipoMensagemCluster.COORDENADOR, config.getMeuId());
    coord.setEpoca(epoca);
    coord.setVersao(gerenciador.getVersao());
    coord.setPortaTcpServico(info.portaTcp);
    coord.setPortaUdpServico(info.portaUdp);
    canal.broadcast(coord);

    // Dispara notificacao SERVER_CHANGED para todos os clientes registrados (locais e cross-shard)
    NotificadorClientes.notificarServerChanged(
        gerenciador.getTodosDestinatariosNotificacao(),
        info.host,
        info.portaTcp,
        info.portaUdp,
        epoca,
        config.getMeuId()
    );

    // Conecta replicacao sincrona para todas as operacoes recebidas dos clientes (D2)
    gerenciador.setListenerReplicacao(op -> replicador.executar(op, this.epoca));

    // Anuncia imediatamente aos demais Shards e a toda a LAN fisica que este no e o novo lider
    if (roteador != null) {
      roteador.forcarAnuncioImediato();
    }
  }

  /**
   * Rebaixa o no para STANDBY: fecha portas de servico e redireciona clientes se necessario.
   */
  public synchronized void rebaixarParaStandby(int novoLider, long novaEpoca) {
    gerenciador.setListenerReplicacao(null);
    if ("ATIVO".equalsIgnoreCase(this.papel)) {
      ClusterLog.aviso("CLUSTER", "Rebaixando de ATIVO para STANDBY. Novo lider: No " + novoLider);
      ConfigCluster.NoInfo infoNovoLider = config.getNos().get(novoLider);
      if (infoNovoLider != null) {
        NotificadorClientes.notificarServerChanged(
            gerenciador.getTodosDestinatariosNotificacao(),
            infoNovoLider.host,
            infoNovoLider.portaTcp,
            infoNovoLider.portaUdp,
            novaEpoca,
            novoLider
        );
      }
      servidor.pararServicos();
    }
    this.papel = "STANDBY";
    this.idLiderAtivo = novoLider;
    this.epoca = Math.max(this.epoca, novaEpoca);
  }

  /**
   * Resolve deterministica e simetricamente conflito de lideranca (dois nos operando como ATIVO).
   * Hierarquia estrita: Versao dos Dados > Epoca > Menor ID.
   */
  private synchronized void resolverConflitoLideranca(MensagemCluster msg) {
    int parId = msg.getIdOrigem();
    long minhaVersao = gerenciador.getVersao();
    long versaoPar = msg.getVersao();
    long parEpoca = msg.getEpoca();

    ClusterLog.conflito(String.format(
        "Dois nos operando como ATIVO! Local: [ID %d, Versao=%d, Epoca=%d] vs Remoto: [No %d, Versao=%d, Epoca=%d]",
        config.getMeuId(), minhaVersao, this.epoca, parId, versaoPar, parEpoca));

    boolean euVenci;
    // CRITERIO 1: Versao dos dados (Historico de operacoes persistidas)
    if (minhaVersao > versaoPar) {
      euVenci = true;
    } else if (minhaVersao < versaoPar) {
      euVenci = false;
    } else {
      // CRITERIO 2: Desempate deterministico absoluto por menor ID.
      // O no pioneiro (menor ID) do shard sempre vence quando as versoes de dados sao iguais.
      // Isto elimina Split-Brain e impede que uma replica secundaria sequestre a lideranca do pioneiro.
      euVenci = (config.getMeuId() < parId);
    }

    if (euVenci) {
      ClusterLog.cura(String.format("No %d prevalece como Lider ATIVO. Impondo coordenacao ao No %d.",
          config.getMeuId(), parId));
      this.epoca = Math.max(this.epoca, parEpoca) + 1;
      ConfigCluster.NoInfo info = config.getMeuNoInfo();
      MensagemCluster coord = new MensagemCluster(TipoMensagemCluster.COORDENADOR, config.getMeuId());
      coord.setEpoca(this.epoca);
      coord.setVersao(minhaVersao);
      coord.setPortaTcpServico(info.portaTcp);
      coord.setPortaUdpServico(info.portaUdp);
      canal.broadcast(coord);
    } else {
      ClusterLog.cura(String.format("No %d cede lideranca ao No %d (vencedor). Rebaixando para STANDBY e sincronizando snapshot.",
          config.getMeuId(), parId));
      rebaixarParaStandby(parId, Math.max(this.epoca, parEpoca));
      this.papel = "SINCRONIZANDO";
      MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
      pedido.setVersao(gerenciador.getVersao());
      canal.enviar(parId, pedido);
    }
  }

  // =========================================================================
  // PROCESSAMENTO DE MENSAGENS RECEBIDAS DO CLUSTER
  // =========================================================================

  /**
   * Processa uma mensagem interna recebida pelo {@link CanalCluster} dentro do consenso intra-shard.
   *
   * @param msg Envelope serializado recebido de um par do mesmo shard.
   */
  @Override
  public void onMensagemRecebida(MensagemCluster msg) {
    if (msg == null) return;

    // Defesa em profundidade: o consenso (eleicao/replicacao/heartbeat) e estritamente intra-shard
    if (msg.getIdShard() >= 0 && msg.getIdShard() != config.getIdShard()) {
      return;
    }

    // Garante que novos nos descobertos dinamicamente sejam registrados
    if (msg.getIdOrigem() > 0 && msg.getIdOrigem() != config.getMeuId()) {
      String hostOrig = (msg.getHostOrigem() != null && !msg.getHostOrigem().isEmpty()) ? msg.getHostOrigem() : "127.0.0.1";
      int idxRep = PortasShard.replicaDoNo(msg.getIdOrigem());
      int pCluster = (msg.getPortaClusterOrigem() > 0) ? msg.getPortaClusterOrigem() : PortasShard.clusterDoNo(msg.getIdOrigem());
      int pTcp = (msg.getPortaTcpServico() > 0) ? msg.getPortaTcpServico() : PortasShard.tcpDoNo(msg.getIdOrigem());
      int pUdp = (msg.getPortaUdpServico() > 0) ? msg.getPortaUdpServico() : PortasShard.udpDoNo(msg.getIdOrigem());

      ConfigCluster.NoInfo infoPar = new ConfigCluster.NoInfo(
          msg.getIdOrigem(), config.getIdShard(), idxRep, hostOrig, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY
      );
      config.registrarParDinamico(infoPar);
      visao.adicionarParSeAusente(infoPar);
      canal.adicionarParSeAusente(msg.getIdOrigem(), hostOrig, pCluster);
    }

    // Atualiza status do par preservando/inferindo corretamente o estado ATIVO do lider
    String estadoPar = msg.getEstadoOrigem();
    if (estadoPar == null || estadoPar.isEmpty()) {
      if (msg.getTipo() == TipoMensagemCluster.COORDENADOR
          || msg.getTipo() == TipoMensagemCluster.SNAPSHOT
          || msg.getTipo() == TipoMensagemCluster.REPLICAR
          || msg.getIdOrigem() == this.idLiderAtivo) {
        estadoPar = "ATIVO";
      } else {
        VisaoCluster.ParEstado atual = visao.getPar(msg.getIdOrigem());
        estadoPar = (atual != null && !"DOWN".equalsIgnoreCase(atual.estado)) ? atual.estado : "STANDBY";
      }
    }
    visao.atualizarHeartbeat(
        msg.getIdOrigem(),
        estadoPar,
        msg.getEpoca(),
        msg.getVersao(),
        msg.getHash(),
        msg.getPortaTcpServico(),
        msg.getPortaUdpServico()
    );

    if (msg.getEpoca() > this.epoca && !"ATIVO".equalsIgnoreCase(this.papel)) {
      this.epoca = msg.getEpoca();
    }

    switch (msg.getTipo()) {
      case HELLO:
      case HEARTBEAT:
        if ("ATIVO".equalsIgnoreCase(msg.getEstadoOrigem())) {
          if ("ATIVO".equalsIgnoreCase(this.papel) && msg.getIdOrigem() != config.getMeuId()) {
            resolverConflitoLideranca(msg);
          } else {
            this.idLiderAtivo = msg.getIdOrigem();
            this.epoca = Math.max(this.epoca, msg.getEpoca());
            long minhaVersao = gerenciador.getVersao();
            boolean precisaSync = "INICIANDO".equalsIgnoreCase(this.papel)
                || "SINCRONIZANDO".equalsIgnoreCase(this.papel)
                || minhaVersao < msg.getVersao()
                || (msg.getTipo() == TipoMensagemCluster.HEARTBEAT
                    && minhaVersao == msg.getVersao()
                    && gerenciador.calcularHash() != msg.getHash());
            if (precisaSync) {
              if ("INICIANDO".equalsIgnoreCase(this.papel)) {
                this.papel = "SINCRONIZANDO";
              }
              solicitarSyncComCooldown(msg.getIdOrigem());
            }
          }
        }
        break;

      case REPLICAR:
        if (!"ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.processarReplicacaoRecebida(msg);
        }
        break;

      case ACK:
        if ("ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.processarAck(msg.getIdOrigem(), msg.getVersao());
        }
        break;

      case PEDIDO_SYNC:
        if ("ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.enviarSnapshot(msg.getIdOrigem(), epoca);
        }
        break;

      case SNAPSHOT:
        if (!"ATIVO".equalsIgnoreCase(this.papel)) {
          if (msg.getPayload() instanceof byte[]) {
            byte[] dados = (byte[]) msg.getPayload();
            if (replicador.instalarSnapshot(dados, msg.getEpoca(), msg.getVersao())) {
              this.epoca = msg.getEpoca();
              this.papel = "STANDBY"; // No sincronizado entra como STANDBY (D1)
              this.idLiderAtivo = msg.getIdOrigem();

              MensagemCluster ok = new MensagemCluster(TipoMensagemCluster.SYNC_OK, config.getMeuId());
              ok.setVersao(gerenciador.getVersao());
              canal.enviar(msg.getIdOrigem(), ok);
              ClusterLog.standby("Sincronizacao concluida com sucesso. No operando em STANDBY.");
            }
          }
        }
        break;

      case SYNC_OK:
        ClusterLog.info("CLUSTER", "No " + msg.getIdOrigem() + " sincronizou com sucesso e esta em STANDBY.");
        break;

      case PROPOSTA:
        tratarProposta(msg);
        break;

      case VOTO:
        if (latchVotos != null && msg.getEpoca() == this.epoca) {
          if (Boolean.TRUE.equals(msg.getPayload())) {
            latchVotos.countDown();
          }
        }
        break;

      case COORDENADOR:
        if (msg.getIdOrigem() == config.getMeuId()) {
          break;
        }
        if (msg.getEpoca() >= this.epoca) {
          this.idLiderAtivo = msg.getIdOrigem();
          this.epoca = msg.getEpoca();
          if ("ATIVO".equalsIgnoreCase(this.papel)) {
            ClusterLog.cura(String.format(
                "Recebido COORDENADOR com epoca %d do No %d. Cedendo lideranca e rebaixando para STANDBY.",
                msg.getEpoca(), msg.getIdOrigem()));
            rebaixarParaStandby(msg.getIdOrigem(), msg.getEpoca());
          }
          this.papel = "STANDBY";
          ClusterLog.lider(String.format(
              "Novo lider anunciado no Shard %s: Replica %d / No %d (Epoca: %d, Versao: %d)",
              Particionador.nome(config.getIdShard()), PortasShard.replicaDoNo(msg.getIdOrigem()), msg.getIdOrigem(),
              msg.getEpoca(), msg.getVersao()));

          ClusterLog.info("CLUSTER", "Solicitando SNAPSHOT do lider No " + msg.getIdOrigem() + "...");
          this.papel = "SINCRONIZANDO";
          solicitarSyncComCooldown(msg.getIdOrigem());
        } else {
          ClusterLog.aviso("CLUSTER", "Ignorando COORDENADOR obsoleto do No " + msg.getIdOrigem()
              + " (Epoca " + msg.getEpoca() + " < Minha " + this.epoca + ")");
        }
        break;

      case RENUNCIA:
        ClusterLog.info("CLUSTER", "Lider No " + msg.getIdOrigem() + " renunciou graciosamente. Disparando eleicao imediata.");
        visao.marcarDown(msg.getIdOrigem());
        this.idLiderAtivo = -1;
        dispararEleicao();
        break;

      case SAINDO:
        ClusterLog.info("CLUSTER", "No " + msg.getIdOrigem() + " anunciou saida do cluster.");
        visao.marcarDown(msg.getIdOrigem());
        if (msg.getIdOrigem() == this.idLiderAtivo) {
          ClusterLog.eleicao("Lider ativo No " + msg.getIdOrigem() + " saindo do cluster. Disparando eleicao imediata!");
          this.idLiderAtivo = -1;
          dispararEleicao();
        }
        break;

      default:
        break;
    }
  }

  private synchronized void tratarProposta(MensagemCluster msg) {
    MensagemCluster voto = new MensagemCluster(TipoMensagemCluster.VOTO, config.getMeuId());
    voto.setEpoca(msg.getEpoca());

    long minhaVersao = gerenciador.getVersao();
    int meuId = config.getMeuId();

    boolean aceita = true;

    // 1. Proposta com epoca desatualizada: rejeitada
    if (msg.getEpoca() < this.epoca) {
      aceita = false;
    }
    // 2. Se ja sou ATIVO e a proposta nao tem epoca estritamente maior: rejeitada
    else if ("ATIVO".equalsIgnoreCase(this.papel) && msg.getEpoca() <= this.epoca) {
      aceita = false;
    }
    // 3. Se os dados do candidato forem mais antigos que os meus: rejeitada
    else if (msg.getVersao() < minhaVersao) {
      aceita = false;
    }
    // 4. Se a versao for igual: desempate por menor ID
    else if (msg.getVersao() == minhaVersao) {
      if (msg.getIdOrigem() > meuId && ("CANDIDATO".equalsIgnoreCase(this.papel) || "ATIVO".equalsIgnoreCase(this.papel))) {
        aceita = false;
      }
    }

    if (aceita) {
      this.epoca = Math.max(this.epoca, msg.getEpoca());
      if ("ATIVO".equalsIgnoreCase(this.papel)) {
        rebaixarParaStandby(msg.getIdOrigem(), msg.getEpoca());
      } else if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
        this.papel = "STANDBY";
      }
    }

    voto.setPayload(aceita);
    canal.enviar(msg.getIdOrigem(), voto);
  }

  /**
   * Fase 4 (Ghost Client): Processa broadcast UDP interno USUARIO_CAIU.
   * Todos os lideres ATIVOS limpam o usuario de suas memorias locais (incluindo grupos cross-shard),
   * replicando a remocao de forma idempotente para suas replicas STANDBY.
   */
  public void processarUsuarioCaiu(String idEvento, String nomeUsuario) {
    if (!"ATIVO".equalsIgnoreCase(this.papel) || nomeUsuario == null || nomeUsuario.trim().isEmpty()) {
      return;
    }
    String ev = (idEvento != null && !idEvento.trim().isEmpty())
        ? idEvento.trim()
        : String.valueOf(System.currentTimeMillis());
    if (!eventosQuedaProcessados.add(ev + ":" + nomeUsuario.trim().toLowerCase())) {
      return;
    }
    String idIdempotente = "usuario-caiu-" + ev + "-S" + config.getIdShard();
    Operacao op = Operacao.removerUsuario(idIdempotente, nomeUsuario);
    Resultado res = gerenciador.aplicar(op);
    ClusterLog.aviso("GHOST-CLIENT", String.format(
        "Broadcast UDP USUARIO_CAIU processado no %s para '%s' [ev=%s] -> %s",
        Particionador.nome(config.getIdShard()), nomeUsuario, ev,
        res != null ? res.getMensagem() : "sem resposta"));
    if (res != null && res.isNotificarAtualizacaoUsuarios()) {
      new Thread(() -> NotificadorClientes.notificarAtualizacaoUsuarios(roteador.coletarTodosUsuariosAtivos()),
          "Thread-GhostClient-UpdateUsers").start();
    }
  }

  /**
   * Encerramento gracioso do no com aviso imediato aos pares (RENUNCIA / SAINDO).
   */
  public void encerrar() {
    this.executando = false;
    roteador.parar();
    if ("ATIVO".equalsIgnoreCase(this.papel)) {
      // Libera as portas TCP/UDP/Discovery ANTES de disparar RENUNCIA para que o Standby assuma sem colisao de bind!
      servidor.pararServicos();
      ClusterLog.info("CLUSTER", "Enviando RENUNCIA aos pares antes de encerrar...");
      MensagemCluster renuncia = new MensagemCluster(TipoMensagemCluster.RENUNCIA, config.getMeuId());
      canal.broadcast(renuncia);
    } else {
      MensagemCluster saindo = new MensagemCluster(TipoMensagemCluster.SAINDO, config.getMeuId());
      canal.broadcast(saindo);
    }
    canal.fechar();
    ClusterLog.info("CLUSTER", "No " + config.getMeuId() + " encerrado com sucesso.");
  }
}
