import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import Protocol.APDU;
import cluster.BeaconClusterLAN;
import cluster.ClusterLog;
import cluster.ConfigCluster;
import cluster.NoCluster;
import cluster.PortasShard;
import cluster.RoteadorShards;
import controller.GerenciadorGrupos;
import model.ClienteTCP;
import model.ClienteUDP;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.MessageListener;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Suite integrada de validacao final das Fases 7 e 8:
 * 1) Blindagem LAN (serialVersionUID = 2L, timeouts Wi-Fi 2000-3000ms, bind 0.0.0.0 e IP LAN).
 * 2) Topologia completa de 6 nos (3 Shards x 2 Replicas) + Dashboard ANSI colorido.
 * 3) Failover Intra-Shard sem perda de estado (queda de S2/R0 -> S2/R1 assume).
 * 4) Fase 7: Queda Total da Particao S2 (R0 + R1 offline) -> degradacao graciosa sem travar gateway!
 * 5) Fase 7: Recuperacao Pos-Queda Total (S2 volta com RAM limpa) -> deteccao de amnesia + chute TCP + Re-REGISTER/Re-JOIN!
 */
public class TesteFases7e8Final {

  private static class InstanciaNo {
    final ConfigCluster config;
    final GerenciadorGrupos gerenciador;
    final NoCluster no;
    final BeaconClusterLAN beacon;

    private static ConfigCluster criarConfigShard(int idShard, int idReplica, int totalReplicas) {
      int meuId = PortasShard.idDoNo(idShard, idReplica);
      java.util.Map<Integer, ConfigCluster.NoInfo> mapa = new java.util.HashMap<>();
      for (int r = 0; r < totalReplicas; r++) {
        int id = PortasShard.idDoNo(idShard, r);
        mapa.put(id, new ConfigCluster.NoInfo(id, idShard, r, "127.0.0.1",
            PortasShard.clusterDoNo(id), PortasShard.tcpDoNo(id), PortasShard.udpDoNo(id), Protocolo.PORTA_DISCOVERY));
      }
      return new ConfigCluster(meuId, idShard, idReplica, mapa);
    }

    InstanciaNo(int idShard, int idReplica) {
      this.config = criarConfigShard(idShard, idReplica, 2);
      this.gerenciador = new GerenciadorGrupos(idShard);
      this.no = new NoCluster(config, gerenciador);
      this.beacon = new BeaconClusterLAN(config, new BeaconClusterLAN.ListenerNovoParLAN() {
        @Override
        public void onNovoParDescoberto(ConfigCluster.NoInfo novoPar) {
          if (novoPar != null && novoPar.idShard == config.getIdShard()) {
            no.getVisao().adicionarParSeAusente(novoPar);
            no.getCanal().adicionarParSeAusente(novoPar.id, novoPar.host, novoPar.portaCluster);
          }
        }

        @Override
        public void onParSaindo(int idPar) {
          no.getVisao().marcarDown(idPar);
          if (idPar == no.getIdLiderAtivo()) {
            no.dispararEleicao();
          }
        }

        @Override
        public void onUsuarioCaiu(String idEvento, String nomeUsuario) {
          no.processarUsuarioCaiu(idEvento, nomeUsuario);
        }

        @Override
        public void onAnuncioShardLAN(int shardRemoto, int idNo, String papel, long epoca, int idLider, String hostRemoto) {
          no.getRoteador().registrarAnuncioExternoLAN(shardRemoto, idNo, papel, epoca, idLider, hostRemoto);
        }
      });
    }

    void iniciar() {
      beacon.iniciar();
      no.iniciar();
    }

    void parar() {
      beacon.parar();
      no.encerrar();
    }
  }

  private static void assertTrue(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError("FALHA NA ASSERCAO: " + msg);
    }
  }

  private static String enviarComandoTcpDireto(int portaTcp, APDU apdu) throws Exception {
    try (Socket s = new Socket("127.0.0.1", portaTcp)) {
      s.setSoTimeout(5000);
      ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
      oos.flush();
      ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
      oos.writeUnshared(apdu);
      oos.flush();
      Object resp = ois.readObject();
      return String.valueOf(resp);
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.out.println("=== INICIANDO VALIDACAO FINAL DAS FASES 7 E 8 (6 PROCESSOS) ===");

    // 1. Validacao estrita de APDU.serialVersionUID e Timeouts Wi-Fi
    long suid = ObjectStreamClass.lookup(APDU.class).getSerialVersionUID();
    assertTrue(suid == 2L, "APDU.serialVersionUID deve ser exatamente 2L, obtido: " + suid);
    assertTrue(RoteadorShards.TIMEOUT_PADRAO_MS >= 2000 && RoteadorShards.TIMEOUT_PADRAO_MS <= 3000,
        "TIMEOUT_PADRAO_MS fora da faixa segura Wi-Fi [2000..3000]: " + RoteadorShards.TIMEOUT_PADRAO_MS);
    assertTrue(RoteadorShards.TIMEOUT_SCATTER_MS >= 2000 && RoteadorShards.TIMEOUT_SCATTER_MS <= 3000,
        "TIMEOUT_SCATTER_MS fora da faixa segura Wi-Fi [2000..3000]: " + RoteadorShards.TIMEOUT_SCATTER_MS);
    String ipLanDetectado = BeaconClusterLAN.obterIpLocalLAN();
    System.out.println("[OK] APDU.serialVersionUID=2L | TIMEOUT_OP=" + RoteadorShards.TIMEOUT_PADRAO_MS
        + "ms | TIMEOUT_SCATTER=" + RoteadorShards.TIMEOUT_SCATTER_MS + "ms | IP LAN=" + ipLanDetectado);

    InstanciaNo s0r0 = null, s0r1 = null, s1r0 = null, s1r1 = null, s2r0 = null, s2r1 = null;
    GerenciadorConexao gcZeca = null;
    GerenciadorConexao gcAlice = null;
    ClienteTCP tcpZeca = null, tcpAlice = null;
    ClienteUDP udpZeca = null, udpAlice = null;

    try {
      // 2. Sobe a topologia completa de 6 processos (3 Shards x 2 Replicas)
      s0r0 = new InstanciaNo(0, 0);
      s1r0 = new InstanciaNo(1, 0);
      s2r0 = new InstanciaNo(2, 0);
      s0r0.iniciar();
      s1r0.iniciar();
      s2r0.iniciar();
      Thread.sleep(700);

      s0r1 = new InstanciaNo(0, 1);
      s1r1 = new InstanciaNo(1, 1);
      s2r1 = new InstanciaNo(2, 1);
      s0r1.iniciar();
      s1r1.iniciar();
      s2r1.iniciar();

      long limiteBoot = System.currentTimeMillis() + 8000;
      while (System.currentTimeMillis() < limiteBoot) {
        if ("ATIVO".equals(s0r0.no.getPapel()) && "STANDBY".equals(s0r1.no.getPapel())
            && "ATIVO".equals(s1r0.no.getPapel()) && "STANDBY".equals(s1r1.no.getPapel())
            && "ATIVO".equals(s2r0.no.getPapel()) && "STANDBY".equals(s2r1.no.getPapel())
            && s0r0.no.getRoteador().getTabela().getLider(2) == 1) {
          break;
        }
        Thread.sleep(200);
      }

      assertTrue("ATIVO".equals(s0r0.no.getPapel()) && "STANDBY".equals(s0r1.no.getPapel()), "S0 nao formou ATIVO+STANDBY");
      assertTrue("ATIVO".equals(s1r0.no.getPapel()) && "STANDBY".equals(s1r1.no.getPapel()), "S1 nao formou ATIVO+STANDBY");
      assertTrue("ATIVO".equals(s2r0.no.getPapel()) && "STANDBY".equals(s2r1.no.getPapel()), "S2 nao formou ATIVO+STANDBY");
      System.out.println("[OK] Topologia de 6 processos formada: S0(R0/R1), S1(R0/R1) e S2(R0/R1) operacionais!");

      // Testa bind 0.0.0.0 acessando via IP LAN detectado (se diferente de 127.0.0.1)
      if (!"127.0.0.1".equals(ipLanDetectado)) {
        try (Socket sockLan = new Socket(ipLanDetectado, PortasShard.tcp(0, 0))) {
          assertTrue(sockLan.isConnected(), "ServidorTCP deve aceitar conexao no IP LAN via bind 0.0.0.0");
          System.out.println("[OK] Bind 0.0.0.0 validado com sucesso no IP fisico LAN " + ipLanDetectado + ":5000!");
        }
      }

      // Conecta Zeca (Home=S2) fisicamente no gateway S1 (5100) e Alice (Home=S0) no gateway S0 (5000)
      CountDownLatch reconectouZecaPosAmnesia = new CountDownLatch(1);
      CountDownLatch reconectouAlicePosAmnesia = new CountDownLatch(1);
      List<String> msgsRecebidasZeca = new CopyOnWriteArrayList<>();

      tcpZeca = new ClienteTCP("127.0.0.1", PortasShard.tcp(1, 0));
      udpZeca = new ClienteUDP("127.0.0.1", PortasShard.udp(1, 0));
      udpZeca.setMeuNome("Zeca");
      MessageListener listenerZeca = new MessageListener() {
        @Override
        public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {
          msgsRecebidasZeca.add(msg);
        }
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override
        public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          reconectouZecaPosAmnesia.countDown();
        }
      };
      udpZeca.setListener(listenerZeca);
      Thread tUdpZeca = new Thread(udpZeca); tUdpZeca.setDaemon(true); tUdpZeca.start();
      InfoUser uZeca = new InfoUser("Zeca", "127.0.0.1", udpZeca.getPortaLocal());
      assertTrue(Protocolo.OK.equals(tcpZeca.register(uZeca).getOperacao()), "REGISTER Zeca falhou");

      List<EnderecoServidor> iniS1 = Collections.singletonList(
          new EnderecoServidor("127.0.0.1", 5100, 6100, 1, "ATIVO"));
      gcZeca = new GerenciadorConexao(iniS1, tcpZeca, udpZeca, uZeca, listenerZeca, 1);
      gcZeca.iniciarMonitoramento();

      assertTrue(Protocolo.OK.equals(tcpZeca.join("Zelda", uZeca).getOperacao()), "JOIN Zeca Zelda falhou");
      assertTrue(Protocolo.OK.equals(tcpZeca.join("Academia", uZeca).getOperacao()), "JOIN Zeca Academia falhou");

      tcpAlice = new ClienteTCP("127.0.0.1", PortasShard.tcp(0, 0));
      udpAlice = new ClienteUDP("127.0.0.1", PortasShard.udp(0, 0));
      udpAlice.setMeuNome("Alice");
      MessageListener listenerAlice = new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override
        public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          reconectouAlicePosAmnesia.countDown();
        }
      };
      udpAlice.setListener(listenerAlice);
      Thread tUdpAlice = new Thread(udpAlice); tUdpAlice.setDaemon(true); tUdpAlice.start();
      InfoUser uAlice = new InfoUser("Alice", "127.0.0.1", udpAlice.getPortaLocal());
      assertTrue(Protocolo.OK.equals(tcpAlice.register(uAlice).getOperacao()), "REGISTER Alice falhou");

      List<EnderecoServidor> iniS0 = Collections.singletonList(
          new EnderecoServidor("127.0.0.1", 5000, 6000, 0, "ATIVO"));
      gcAlice = new GerenciadorConexao(iniS0, tcpAlice, udpAlice, uAlice, listenerAlice, 0);
      gcAlice.iniciarMonitoramento();

      assertTrue(Protocolo.OK.equals(tcpAlice.join("Zelda", uAlice).getOperacao()), "JOIN Alice Zelda falhou");
      assertTrue(Protocolo.OK.equals(tcpAlice.join("Academia", uAlice).getOperacao()), "JOIN Alice Academia falhou");

      // Exibe Dashboard ANSI com todos os 3 Shards ONLINE
      System.out.println("\n--- [TESTE DASHBOARD ANSI: TODOS OS 3 SHARDS ONLINE] ---");
      ClusterLog.exibirDashboardClusterANSI(
          s0r0.no.getMeuId(), s0r0.no.getPapel(), s0r0.no.getEpoca(), s0r0.no.getIdLiderAtivo(),
          s0r0.config.getNos(), s0r0.no.getVisao().getTodosPares(), s0r0.gerenciador, s0r0.no.getRoteador());

      // 3. Queda apenas do Primario de S2 (S2/R0) -> Standby S2/R1 assume sem perder estado!
      System.out.println("--- [CENARIO 1: QUEDA APENAS DO LIDER S2/R0 -> FAILOVER INTRA-SHARD S2/R1] ---");
      s2r0.parar();
      long limFailover = System.currentTimeMillis() + 6000;
      while (System.currentTimeMillis() < limFailover && !"ATIVO".equals(s2r1.no.getPapel())) {
        Thread.sleep(150);
      }
      assertTrue("ATIVO".equals(s2r1.no.getPapel()), "Standby S2/R1 deveria ter assumido como ATIVO");
      APDU membrosPosStandby = tcpAlice.listMembers("Zelda");
      assertTrue(Protocolo.OK.equals(membrosPosStandby.getOperacao())
          && membrosPosStandby.getTextoMensagem().contains("Zeca")
          && membrosPosStandby.getTextoMensagem().contains("Alice"),
          "Standby S2/R1 perdeu membros replicados de Zelda: " + membrosPosStandby.getTextoMensagem());
      System.out.println("[OK] Failover intra-shard S2/R1 preservou 100% da ED ('Zelda' -> "
          + membrosPosStandby.getTextoMensagem() + ")!");

      // 4. FASE 7: QUEDA TOTAL DA PARTICAO S2 (mata S2/R1 tambem -> S2 100% OFFLINE)
      System.out.println("\n--- [CENARIO 2 (FASE 7): QUEDA TOTAL DA PARTICAO S2 (ATIVO + STANDBY OFFLINE)] ---");
      s2r1.parar();
      Thread.sleep(400);

      // Operacao de escrita roteada de S0 -> S2 (REGISTER 'Zorro') NAO pode travar!
      long tOp = System.currentTimeMillis();
      APDU reqZorro = new APDU(Protocolo.REGISTER, "GLOBAL", "Zorro", null, 9999);
      reqZorro.setIdRequisicao("req-zorro-off");
      String respRegOffline = enviarComandoTcpDireto(PortasShard.tcp(0, 0), reqZorro);
      long dtOp = System.currentTimeMillis() - tOp;
      System.out.println("Resposta REGISTER 'Zorro' com S2 morto em " + dtOp + "ms -> " + respRegOffline);
      assertTrue(respRegOffline.startsWith("ERRO:") && respRegOffline.contains("Particao S2")
          && respRegOffline.contains("indisponivel"),
          "Esperava APDU de erro limpa de Particao S2 indisponivel, obtido: " + respRegOffline);
      assertTrue(dtOp <= 3500, "Gateway travou alem do timeout seguro Wi-Fi: " + dtOp + "ms");

      long tJoinOff = System.currentTimeMillis();
      APDU respJoinOffline = tcpAlice.join("Zumbi", uAlice);
      long dtJoinOff = System.currentTimeMillis() - tJoinOff;
      System.out.println("Resposta JOIN 'Zumbi' via S0 com S2 morto em " + dtJoinOff + "ms -> "
          + respJoinOffline.getOperacao() + ": " + respJoinOffline.getTextoMensagem());
      assertTrue(Protocolo.ERRO.equals(respJoinOffline.getOperacao())
          && respJoinOffline.getTextoMensagem().contains("Particao S2")
          && respJoinOffline.getTextoMensagem().contains("indisponivel"),
          "JOIN em particao morta deveria retornar ERRO limpo: " + respJoinOffline.getTextoMensagem());
      assertTrue(dtJoinOff <= 3500, "JOIN em particao morta demorou demais: " + dtJoinOff + "ms");

      // Scatter-Gather (LIST e USERS) deve retornar resposta parcial de S0 e S1 rapidamente sem travar!
      long tList = System.currentTimeMillis();
      APDU listaParcial = tcpAlice.list();
      long dtList = System.currentTimeMillis() - tList;
      System.out.println("Resposta LIST parcial com S2 morto em " + dtList + "ms -> " + listaParcial.getTextoMensagem());
      assertTrue(Protocolo.OK.equals(listaParcial.getOperacao())
          && listaParcial.getTextoMensagem().contains("Academia"),
          "LIST parcial deveria retornar grupos de S0 ('Academia'): " + listaParcial.getTextoMensagem());
      assertTrue(dtList <= 3200, "Scatter-Gather LIST excedeu timeout seguro Wi-Fi: " + dtList + "ms");

      // Operacoes nos Shards S0 e S1 continuam 100% disponiveis!
      APDU joinS1 = tcpAlice.join("Java", uAlice);
      assertTrue(Protocolo.OK.equals(joinS1.getOperacao()),
          "Shard S1 [J..R] deveria continuar 100% operacional: " + joinS1.getTextoMensagem());
      System.out.println("[OK] Degradacao graciosa validada: S0 e S1 continuam normais enquanto S2 retorna erro limpo!");

      // Exibe Dashboard ANSI mostrando Shard S2 em vermelho OFFLINE
      Thread.sleep(4600); // aguarda expiracao de TabelaLideres (4.5s) para exibir OFFLINE no painel
      System.out.println("\n--- [TESTE DASHBOARD ANSI: PARTICAO S2 OFFLINE] ---");
      ClusterLog.exibirDashboardClusterANSI(
          s0r0.no.getMeuId(), s0r0.no.getPapel(), s0r0.no.getEpoca(), s0r0.no.getIdLiderAtivo(),
          s0r0.config.getNos(), s0r0.no.getVisao().getTodosPares(), s0r0.gerenciador, s0r0.no.getRoteador());

      // 5. FASE 7: RECUPERACAO POS-QUEDA TOTAL COM MEMORIA RAM LIMPA (AMNESIA DE SHARD)
      System.out.println("--- [CENARIO 3 (FASE 7): S2 RESSUSCITA COM RAM LIMPA -> CURA AUTOMATICA DE AMNESIA] ---");
      s2r0 = new InstanciaNo(2, 0);
      s2r0.iniciar();
      long limReboot = System.currentTimeMillis() + 6000;
      while (System.currentTimeMillis() < limReboot && !"ATIVO".equals(s2r0.no.getPapel())) {
        Thread.sleep(150);
      }
      assertTrue("ATIVO".equals(s2r0.no.getPapel()), "S2/R0 reiniciado deveria assumir como ATIVO");
      assertTrue(s2r0.gerenciador.buscarUsuarioPorNome("Zeca") == null, "S2/R0 deveria voltar com RAM limpa");

      // Zeca envia SEND UDP (ou o proprio PING de 2.5s de Zeca e Alice!) -> S2/R0 detecta amnesia ->
      // S1/S0 chutam TCP -> ambos reconectam e reconstroem REGISTER + JOIN('Zelda') automaticamente!
      udpZeca.send("Zelda", uZeca, "Oi apos crash total de S2");

      assertTrue(reconectouZecaPosAmnesia.await(8, TimeUnit.SECONDS),
          "Zeca deveria ter sofrido chute de socket TCP no gateway S1 e reconectado automaticamente!");
      assertTrue(reconectouAlicePosAmnesia.await(8, TimeUnit.SECONDS),
          "Alice tambem deveria ter sofrido chute de socket TCP por amnesia de 'Zelda' e reconectado!");

      long limReconstrucao = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < limReconstrucao) {
        if (s2r0.gerenciador.buscarUsuarioPorNome("Zeca") != null
            && s2r0.gerenciador.isMembroGrupo("Zelda", "Zeca")
            && s2r0.gerenciador.isMembroGrupo("Zelda", "Alice")) {
          break;
        }
        Thread.sleep(150);
      }
      assertTrue(s2r0.gerenciador.buscarUsuarioPorNome("Zeca") != null,
          "Zeca deveria ter sido re-registrado automaticamente em S2/R0 apos failover!");
      assertTrue(s2r0.gerenciador.isMembroGrupo("Zelda", "Zeca"),
          "Zeca deveria ter reconstruido automaticamente sua participacao em 'Zelda' no S2/R0!");
      assertTrue(s2r0.gerenciador.isMembroGrupo("Zelda", "Alice"),
          "Alice deveria ter reconstruido automaticamente sua participacao em 'Zelda' no S2/R0!");

      // Agora Alice envia mensagem UDP para Zelda e Zeca DEVE receber!
      msgsRecebidasZeca.clear();
      udpAlice.send("Zelda", uAlice, "Mensagem entregue apos cura total do S2!");
      long limMsg = System.currentTimeMillis() + 4000;
      while (System.currentTimeMillis() < limMsg && msgsRecebidasZeca.isEmpty()) {
        Thread.sleep(100);
      }
      assertTrue(!msgsRecebidasZeca.isEmpty(),
          "Zeca deveria receber datagrama UDP em 'Zelda' apos recuperacao completa do Shard S2!");
      System.out.println("[OK] Cura automatica pos-queda total validada! Mensagem recebida por Zeca: '"
          + msgsRecebidasZeca.get(0) + "'");

      System.out.println("\n=== TODOS OS TESTES DAS FASES 7 E 8 PASSARAM COM 100% DE SUCESSO! ===");
    } finally {
      if (gcZeca != null) gcZeca.parar();
      if (gcAlice != null) gcAlice.parar();
      if (udpZeca != null) udpZeca.fecharConexao();
      if (udpAlice != null) udpAlice.fecharConexao();
      if (tcpZeca != null) tcpZeca.fecharConexao();
      if (tcpAlice != null) tcpAlice.fecharConexao();
      if (s0r0 != null) s0r0.parar();
      if (s0r1 != null) s0r1.parar();
      if (s1r0 != null) s1r0.parar();
      if (s1r1 != null) s1r1.parar();
      if (s2r0 != null) s2r0.parar();
      if (s2r1 != null) s2r1.parar();
      System.exit(0);
    }
  }
}
