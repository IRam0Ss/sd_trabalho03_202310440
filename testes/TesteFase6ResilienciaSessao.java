import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import Protocol.APDU;
import cluster.BeaconClusterLAN;
import cluster.ConfigCluster;
import cluster.NoCluster;
import controller.GerenciadorGrupos;
import model.ClienteTCP;
import model.ClienteUDP;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.MessageListener;
import utils.InfoUser;
import utils.Protocolo;

public class TesteFase6ResilienciaSessao {

  private static void assertTrue(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError("FALHA NA ASSERCAO: " + msg);
    }
  }

  private static ConfigCluster criarConfigShard(int idShard, int idReplica, int totalReplicas) {
    int meuId = cluster.PortasShard.idDoNo(idShard, idReplica);
    java.util.Map<Integer, ConfigCluster.NoInfo> mapa = new java.util.HashMap<>();
    for (int r = 0; r < totalReplicas; r++) {
      int id = cluster.PortasShard.idDoNo(idShard, r);
      mapa.put(id, new ConfigCluster.NoInfo(id, idShard, r, "127.0.0.1",
          cluster.PortasShard.clusterDoNo(id), cluster.PortasShard.tcpDoNo(id), cluster.PortasShard.udpDoNo(id), Protocolo.PORTA_DISCOVERY));
    }
    return new ConfigCluster(meuId, idShard, idReplica, mapa);
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("java.net.preferIPv4Stack", "true");

    System.out.println("=== [FASE 6] Iniciando 3 Lideres de Shard independentes (S0, S1, S2 - 1 replica cada) ===");
    ConfigCluster cfg0 = criarConfigShard(0, 0, 1);
    ConfigCluster cfg1 = criarConfigShard(1, 0, 1);
    ConfigCluster cfg2 = criarConfigShard(2, 0, 1);

    NoCluster no0 = new NoCluster(cfg0, new GerenciadorGrupos(0));
    NoCluster no1 = new NoCluster(cfg1, new GerenciadorGrupos(1));
    NoCluster no2 = new NoCluster(cfg2, new GerenciadorGrupos(2));

    BeaconClusterLAN b0 = new BeaconClusterLAN(cfg0, new BeaconClusterLAN.ListenerNovoParLAN() {
      @Override public void onNovoParDescoberto(ConfigCluster.NoInfo p) {}
      @Override public void onUsuarioCaiu(String ev, String usr) { no0.processarUsuarioCaiu(ev, usr); }
    });
    BeaconClusterLAN b1 = new BeaconClusterLAN(cfg1, new BeaconClusterLAN.ListenerNovoParLAN() {
      @Override public void onNovoParDescoberto(ConfigCluster.NoInfo p) {}
      @Override public void onUsuarioCaiu(String ev, String usr) { no1.processarUsuarioCaiu(ev, usr); }
    });
    BeaconClusterLAN b2 = new BeaconClusterLAN(cfg2, new BeaconClusterLAN.ListenerNovoParLAN() {
      @Override public void onNovoParDescoberto(ConfigCluster.NoInfo p) {}
      @Override public void onUsuarioCaiu(String ev, String usr) { no2.processarUsuarioCaiu(ev, usr); }
    });

    b0.iniciar(); b1.iniciar(); b2.iniciar();
    no0.iniciar(); no1.iniciar(); no2.iniciar();

    long tWait = System.currentTimeMillis() + 6000;
    while (System.currentTimeMillis() < tWait) {
      if ("ATIVO".equals(no0.getPapel()) && "ATIVO".equals(no1.getPapel()) && "ATIVO".equals(no2.getPapel())) {
        break;
      }
      Thread.sleep(150);
    }
    Thread.sleep(1200); // Aguarda tabela de lideres convergir
    assertTrue("ATIVO".equals(no0.getPapel()) && "ATIVO".equals(no1.getPapel()) && "ATIVO".equals(no2.getPapel()),
        "Todos os 3 shards deveriam estar ATIVOS");

    try {
      // =========================================================================
      // TESTE 1: Parser Discovery UDP de 6 campos (EDEN_CLUSTER:id:estado:tcp:udp:shard)
      // =========================================================================
      System.out.println("=== [TESTE 1] Discovery UDP extraindo o 6o campo (idShard) ===");
      List<EnderecoServidor> descobertos = GerenciadorConexao.descobrirServidoresViaUDP(1200);
      Set<Integer> shardsVistos = new HashSet<>();
      for (EnderecoServidor e : descobertos) {
        shardsVistos.add(e.idShard);
      }
      System.out.println("  Servidores descobertos via UDP: " + descobertos);
      assertTrue(shardsVistos.contains(0) && shardsVistos.contains(1) && shardsVistos.contains(2),
          "Discovery UDP deveria identificar os Shards S0, S1 e S2 no 6o campo! Vistos: " + shardsVistos);
      System.out.println("  [OK] 6o campo (idShard = 0, 1, 2) extraido com sucesso do beacon EDEN_CLUSTER!");

      // =========================================================================
      // TESTE 2: Reconexao Automatica (Failover do Cliente via Discovery UDP)
      // =========================================================================
      System.out.println("=== [TESTE 2] Queda da Porta de Entrada (Shard S1) + Reconexao via Discovery UDP ===");
      CountDownLatch latchAnaReconectou = new CountDownLatch(1);
      CountDownLatch latchZecaReconectou = new CountDownLatch(1);
      CountDownLatch latchMsgPosFailover = new CountDownLatch(1);
      AtomicInteger shardNovoAna = new AtomicInteger(-1);
      AtomicInteger shardNovoZeca = new AtomicInteger(-1);
      CountDownLatch latchUpdateUsers = new CountDownLatch(1);

      ClienteTCP tcpAna = new ClienteTCP("127.0.0.1", 5100); // Conectada fisicamente em S1!
      ClienteUDP udpAna = new ClienteUDP("127.0.0.1", 6100);
      udpAna.setMeuNome("Ana");
      udpAna.setListener(new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() { latchUpdateUsers.countDown(); }
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          shardNovoAna.set(idShard);
          latchAnaReconectou.countDown();
        }
      });
      Thread tUdpAna = new Thread(udpAna); tUdpAna.setDaemon(true); tUdpAna.start();
      InfoUser uAna = new InfoUser("Ana", "127.0.0.1", udpAna.getPortaLocal());
      assertTrue(Protocolo.OK.equals(tcpAna.register(uAna).getOperacao()), "REGISTER Ana via S1 falhou");

      ClienteTCP tcpZeca = new ClienteTCP("127.0.0.1", 5100); // Tambem conectado fisicamente em S1!
      ClienteUDP udpZeca = new ClienteUDP("127.0.0.1", 6100);
      udpZeca.setMeuNome("Zeca");
      udpZeca.setListener(new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {
          if ("Zelda".equals(dest) && msg.contains("apos failover")) {
            latchMsgPosFailover.countDown();
          }
        }
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          shardNovoZeca.set(idShard);
          latchZecaReconectou.countDown();
        }
      });
      Thread tUdpZeca = new Thread(udpZeca); tUdpZeca.setDaemon(true); tUdpZeca.start();
      InfoUser uZeca = new InfoUser("Zeca", "127.0.0.1", udpZeca.getPortaLocal());
      assertTrue(Protocolo.OK.equals(tcpZeca.register(uZeca).getOperacao()), "REGISTER Zeca via S1 falhou");

      // Passamos APENAS S1 (5100) na lista inicial para provar que eles redescobrem S0/S2 via UDP Discovery!
      List<EnderecoServidor> apenasS1 = Collections.singletonList(
          new EnderecoServidor("127.0.0.1", 5100, 6100, 1, "ATIVO"));
      GerenciadorConexao gcAna = new GerenciadorConexao(apenasS1, tcpAna, udpAna, uAna, new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          shardNovoAna.set(idShard);
          latchAnaReconectou.countDown();
        }
      }, 1);
      GerenciadorConexao gcZeca = new GerenciadorConexao(apenasS1, tcpZeca, udpZeca, uZeca, new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          shardNovoZeca.set(idShard);
          latchZecaReconectou.countDown();
        }
      }, 1);
      gcAna.iniciarMonitoramento();
      gcZeca.iniciarMonitoramento();

      assertTrue(Protocolo.OK.equals(tcpAna.join("Zelda", uAna).getOperacao()), "Ana JOIN Zelda falhou");
      assertTrue(Protocolo.OK.equals(tcpZeca.join("Zelda", uZeca).getOperacao()), "Zeca JOIN Zelda falhou");

      // Mata completamente o Shard S1 (Porta de Entrada de AMBOS os clientes!)
      System.out.println("  -> Derrubando lider do Shard S1 (porta 5100)...");
      b1.parar();
      no1.encerrar();

      // Ana executa imediatamente um comando TCP (LIST) em cima do socket recem-morto!
      APDU resListImediato = tcpAna.list();
      assertTrue(Protocolo.OK.equals(resListImediato.getOperacao())
          && resListImediato.getTextoMensagem().contains("Zelda"),
          "Comando LIST imediato de Ana deveria fazer failover transparente e retornar 'Zelda'! Retorno: "
              + resListImediato.getOperacao() + " - " + resListImediato.getTextoMensagem());

      assertTrue(latchAnaReconectou.await(5, TimeUnit.SECONDS), "Ana nao reconectou no prazo");
      assertTrue(latchZecaReconectou.await(5, TimeUnit.SECONDS), "Zeca nao reconectou no prazo");
      assertTrue(shardNovoAna.get() == 0 || shardNovoAna.get() == 2,
          "Ana deveria ter migrado para S0 ou S2, mas ficou em: " + shardNovoAna.get());
      assertTrue(shardNovoZeca.get() == 0 || shardNovoZeca.get() == 2,
          "Zeca deveria ter migrado para S0 ou S2, mas ficou em: " + shardNovoZeca.get());

      // Testa envio UDP pos-failover atraves da nova Porta de Entrada
      udpAna.send("Zelda", uAna, "Viva apos failover da porta de entrada!");
      assertTrue(latchMsgPosFailover.await(3, TimeUnit.SECONDS),
          "Zeca nao recebeu mensagem UDP no grupo Zelda apos o failover de ambos os clientes!");
      System.out.println("  [OK] Ana (S" + shardNovoAna.get() + ") e Zeca (S" + shardNovoZeca.get()
          + ") migraram automaticamente via Discovery UDP e seguiram conversando sem travar!");

      // =========================================================================
      // TESTE 3: Faxina de Conexao TCP no Servidor (REMOVER_USUARIO + SAIR_GRUPO cross-shard)
      // =========================================================================
      System.out.println("=== [TESTE 3] Queda brusca TCP de cliente fantasma (REMOVER_USUARIO + SAIR_GRUPO) ===");
      Socket sockFantasma = new Socket("127.0.0.1", 5000); // Conecta em S0
      ObjectOutputStream oosF = new ObjectOutputStream(sockFantasma.getOutputStream());
      oosF.flush();
      ObjectInputStream oisF = new ObjectInputStream(sockFantasma.getInputStream());

      // Fantasma ('Bruno' -> S0) se registra e entra em 'Amigos' (S0) e 'Zelda' (S2)
      APDU regBruno = new APDU("REGISTER", "GLOBAL", "Bruno", null, 49999);
      regBruno.setIdRequisicao("req-bruno-reg");
      oosF.writeUnshared(regBruno); oosF.flush(); oisF.readObject();

      APDU joinAmigos = new APDU("JOIN", "Amigos", "Bruno", null, 49999);
      joinAmigos.setIdRequisicao("req-bruno-g1");
      oosF.writeUnshared(joinAmigos); oosF.flush(); oisF.readObject();

      APDU joinZelda = new APDU("JOIN", "Zelda", "Bruno", null, 49999);
      joinZelda.setIdRequisicao("req-bruno-g2");
      oosF.writeUnshared(joinZelda); oosF.flush(); oisF.readObject();

      assertTrue(tcpAna.listUsers().getTextoMensagem().contains("Bruno"), "Bruno deveria constar em USERS");
      assertTrue(tcpAna.listMembers("Zelda").getTextoMensagem().contains("Bruno"), "Bruno deveria constar em Zelda (S2)");

      // Queda brusca do socket TCP sem LOGOUT!
      sockFantasma.close();
      Thread.sleep(900);

      String usuariosPosQueda = tcpAna.listUsers().getTextoMensagem();
      String membrosZeldaPosQueda = tcpAna.listMembers("Zelda").getTextoMensagem();
      String gruposPosQueda = tcpAna.list().getTextoMensagem();

      assertTrue(!usuariosPosQueda.contains("Bruno"),
          "Bruno deveria ter sido removido de USERS! Atual: " + usuariosPosQueda);
      assertTrue(!membrosZeldaPosQueda.contains("Bruno"),
          "Bruno deveria ter sofrido SAIR_GRUPO automatico em Zelda (S2)! Atual: " + membrosZeldaPosQueda);
      assertTrue(!gruposPosQueda.contains("Amigos"),
          "Grupo 'Amigos' (S0) deveria ter sido deletado apos SAIR_GRUPO automatico de Bruno! Atual: " + gruposPosQueda);
      System.out.println("  [OK] Faxina cross-shard validada: 'Bruno' removido de USERS, de 'Zelda' (S2) e de 'Amigos' (S0)!");

      // =========================================================================
      // TESTE 4: Amnesia de Shard (Pane total e reinicio do S2 enquanto clientes seguem em S0)
      // =========================================================================
      System.out.println("=== [TESTE 4] Amnesia de Shard: S2 sofre pane total e reinicia do zero ===");
      // Forca Ana e Zeca a estarem conectados no S0 (5000) como Porta de Entrada antes de S2 cair!
      tcpAna.setDestino("127.0.0.1", 5000);
      udpAna.setDestino("127.0.0.1", 6000);
      tcpAna.register(uAna);
      tcpAna.join("Zelda", uAna);

      tcpZeca.setDestino("127.0.0.1", 5000);
      udpZeca.setDestino("127.0.0.1", 6000);
      tcpZeca.register(uZeca);
      tcpZeca.join("Zelda", uZeca);

      CountDownLatch latchZecaConexaoPerdida = new CountDownLatch(1);
      CountDownLatch latchZecaReconstruiu = new CountDownLatch(1);
      CountDownLatch latchAnaReconstruiu = new CountDownLatch(1);
      CountDownLatch latchMsgGrupoPosAmnesia = new CountDownLatch(1);
      CountDownLatch latchMsgPvtPosAmnesia = new CountDownLatch(1);

      gcAna.parar();
      gcZeca.parar();

      List<EnderecoServidor> apenasS0 = Collections.singletonList(
          new EnderecoServidor("127.0.0.1", 5000, 6000, 0, "ATIVO"));

      udpZeca.setListener(new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {
          if (!pvt && "Zelda".equals(dest) && msg.contains("reconstruido apos amnesia")) {
            latchMsgGrupoPosAmnesia.countDown();
          } else if (pvt && msg.contains("PVT pos amnesia")) {
            latchMsgPvtPosAmnesia.countDown();
          }
        }
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
      });

      GerenciadorConexao gcAna2 = new GerenciadorConexao(apenasS0, tcpAna, udpAna, uAna, new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          latchAnaReconstruiu.countDown();
        }
      }, 0);

      GerenciadorConexao gcZeca2 = new GerenciadorConexao(apenasS0, tcpZeca, udpZeca, uZeca, new MessageListener() {
        @Override public void onMessageReceived(String id, String dest, InfoUser rem, String msg, boolean pvt, boolean vu) {}
        @Override public void onShutdown() {}
        @Override public void onUpdateUsers() {}
        @Override public void onTickReceived(String id, int st, String conf) {}
        @Override public void onConexaoPerdida() {
          latchZecaConexaoPerdida.countDown();
        }
        @Override public void onReconectado(String ip, int pTcp, int pUdp, int idShard) {
          latchZecaReconstruiu.countDown();
        }
      }, 0);

      gcAna2.iniciarMonitoramento();
      gcZeca2.iniciarMonitoramento();

      // 1) Derruba S2 completamente (pane total!)
      System.out.println("  -> Derrubando totalmente o Shard S2 (pane total)...");
      b2.parar();
      no2.encerrar();
      Thread.sleep(1800); // Passa pelo menos 1 ciclo de PING enquanto S2 esta OFFLINE

      // Enquanto S2 esta OFFLINE (ainda nao reiniciou), S0 NAO deve derrubar Zeca (disponibilidade parcial CAP!)
      assertTrue(latchZecaConexaoPerdida.getCount() == 1,
          "S0 NAO deveria derrubar o socket TCP enquanto S2 esta totalmente offline (CAP)!");

      // 2) Reinicia S2 do absoluto ZERO (memoria RAM volatil 100% vazia -> Amnesia de Shard!)
      System.out.println("  -> Reiniciando Shard S2 com GerenciadorGrupos(2) VAZIO (Amnesia de Shard)...");
      ConfigCluster cfg2Novo = criarConfigShard(2, 0, 1);
      NoCluster no2Renascido = new NoCluster(cfg2Novo, new GerenciadorGrupos(2));
      BeaconClusterLAN b2Renascido = new BeaconClusterLAN(cfg2Novo, new BeaconClusterLAN.ListenerNovoParLAN() {
        @Override public void onNovoParDescoberto(ConfigCluster.NoInfo p) {}
        @Override public void onUsuarioCaiu(String ev, String usr) { no2Renascido.processarUsuarioCaiu(ev, usr); }
      });
      b2Renascido.iniciar();
      no2Renascido.iniciar();

      try {
        // S0 deve detectar que S2 voltou sem Zeca/Zelda, fechar os sockets TCP de Zeca e Ana,
        // disparar onConexaoPerdida -> Failover -> Re-REGISTER + Re-JOIN("Zelda") automaticamente!
        assertTrue(latchZecaConexaoPerdida.await(6, TimeUnit.SECONDS),
            "Gateway S0 nao fechou o socket TCP de Zeca apos S2 reiniciar com amnesia!");
        assertTrue(latchZecaReconstruiu.await(7, TimeUnit.SECONDS),
            "Zeca nao concluiu Failover + reconstrucao de estado apos amnesia de S2!");
        assertTrue(latchAnaReconstruiu.await(7, TimeUnit.SECONDS),
            "Ana nao concluiu Failover + reconstrucao do grupo Zelda apos amnesia de S2!");

        // Confirma que Zeca voltou para USERS (em S2!) e que Ana + Zeca voltaram para MEMBERS("Zelda") (em S2!)
        String usersReconstruidos = tcpZeca.listUsers().getTextoMensagem();
        String membrosZeldaReconstruidos = tcpZeca.listMembers("Zelda").getTextoMensagem();
        assertTrue(usersReconstruidos.contains("Zeca") && usersReconstruidos.contains("Ana"),
            "USERS deveria conter Zeca e Ana apos reconstrucao automatica! Atual: " + usersReconstruidos);
        assertTrue(membrosZeldaReconstruidos.contains("Zeca") && membrosZeldaReconstruidos.contains("Ana"),
            "MEMBERS('Zelda') em S2 deveria conter Zeca e Ana apos Re-JOIN automatico! Atual: "
                + membrosZeldaReconstruidos);

        // Prova de fogo: envia mensagem de grupo em Zelda (S2) e PVT para Zeca (S2) e valida recepcao UDP!
        InfoUser uAnaAtual = new InfoUser("Ana", "127.0.0.1", udpAna.getPortaLocal());
        udpAna.send("Zelda", uAnaAtual, "Estado reconstruido apos amnesia do S2!");
        udpAna.sendPvt("Zeca", uAnaAtual, "PVT pos amnesia entregue!");

        assertTrue(latchMsgGrupoPosAmnesia.await(3, TimeUnit.SECONDS),
            "Zeca NAO recebeu mensagem UDP no grupo Zelda apos reinicio do S2!");
        assertTrue(latchMsgPvtPosAmnesia.await(3, TimeUnit.SECONDS),
            "Zeca NAO recebeu SENDPVT apos reinicio do S2!");
        System.out.println("  [OK] Amnesia de Shard curada automaticamente: TCP fechado na entrada -> Failover -> REGISTER+JOIN reenviados -> UDP entregando 100%!");
      } finally {
        gcAna2.parar();
        gcZeca2.parar();
        b2Renascido.parar();
        no2Renascido.encerrar();
      }

      tcpAna.fecharConexao();
      tcpZeca.fecharConexao();
      udpAna.fecharConexao();
      udpZeca.fecharConexao();

      System.out.println("\n>>> TODOS OS 4 CENARIOS COMPLETOS DA FASE 6 (INCLUINDO AMNESIA DE SHARD) PASSARAM COM 100% DE SUCESSO! <<<");
    } finally {
      b0.parar(); b2.parar();
      no0.encerrar(); no2.encerrar();
      System.exit(0);
    }
  }
}
