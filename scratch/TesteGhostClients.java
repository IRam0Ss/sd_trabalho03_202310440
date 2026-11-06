package scratch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import model.ClienteTCP;
import model.ClienteUDP;
import model.Servidor;
import utils.InfoUser;
import Protocol.APDU;
import utils.Protocolo;

public class TesteGhostClients {

  public static void main(String[] args) {
    try {
      System.setProperty("java.net.preferIPv4Stack", "true");

      System.out.println("==========================================================");
      System.out.println(" TESTE AUTOMATIZADO: PREVENCAO DE CLIENTES FANTASMAS     ");
      System.out.println("==========================================================");

      // Limpar caches de cluster de testes anteriores
      new File(".cluster_node_lan.id").delete();
      new File(".cluster_node_local.id").delete();

      testarBruteDisconnectServidorUnico();
      testarExclusaoGrupoVazioAposBruteDisconnect();
      testarClusterBruteDisconnectEFailover();

      System.out.println("\n==========================================================");
      System.out.println(" >>> TODOS OS TESTES DE CLIENTES FANTASMAS PASSARAM! <<<  ");
      System.out.println("==========================================================");
      System.exit(0);

    } catch (Throwable t) {
      System.err.println("\n[FALHA NO TESTE] Excecao disparada:");
      t.printStackTrace();
      System.exit(1);
    }
  }

  private static void testarBruteDisconnectServidorUnico() throws Exception {
    System.out.println("\n--- [TESTE 1] Brute Disconnect em Servidor Unico ---");

    Servidor servidor = new Servidor();
    Thread tServidor = new Thread(() -> servidor.iniciarServicos(16789, 17777, 18888, false, 1));
    tServidor.setDaemon(true);
    tServidor.start();
    Thread.sleep(800);

    // 1. Bob conecta e entra no grupo 'SalaGeral'
    ClienteTCP tcpBob = new ClienteTCP("127.0.0.1", 16789);
    ClienteUDP udpBob = new ClienteUDP("127.0.0.1", 17777);
    new Thread(udpBob).start();
    InfoUser userBob = new InfoUser("Bob", "127.0.0.1", udpBob.getPortaLocal());
    tcpBob.register(userBob);
    tcpBob.join("SalaGeral", userBob);

    // 2. Alice conecta e entra no grupo 'SalaGeral'
    ClienteTCP tcpAlice = new ClienteTCP("127.0.0.1", 16789);
    ClienteUDP udpAlice = new ClienteUDP("127.0.0.1", 17777);
    CountDownLatch updateUsersLatch = new CountDownLatch(1);
    AtomicBoolean updateRecebidoAlice = new AtomicBoolean(false);

    udpAlice.setListener(new model.MessageListener() {
      @Override
      public void onMessageReceived(String idMensagem, String destino, InfoUser remetente, String texto, boolean isPrivate, boolean isVisualizacaoUnica) {}
      @Override
      public void onShutdown() {}
      @Override
      public void onUpdateUsers() {
        System.out.println("  [ALICE UDP] Notificacao UPDATE_USERS recebida com sucesso!");
        updateRecebidoAlice.set(true);
        updateUsersLatch.countDown();
      }
      @Override
      public void onTickReceived(String idMensagem, int status, String nomeConfirmou) {}
    });
    new Thread(udpAlice).start();

    InfoUser userAlice = new InfoUser("Alice", "127.0.0.1", udpAlice.getPortaLocal());
    tcpAlice.register(userAlice);
    tcpAlice.join("SalaGeral", userAlice);

    // Valida membros antes do disconnect
    APDU mAntes = tcpAlice.listMembers("SalaGeral");
    System.out.println("  Membros antes do disconnect de Bob: " + mAntes.getTextoMensagem());
    if (!mAntes.getTextoMensagem().contains("Bob") || !mAntes.getTextoMensagem().contains("Alice")) {
      throw new AssertionError("Bob e Alice deveriam estar em SalaGeral!");
    }

    // 3. Simula BRUTE DISCONNECT em Bob (fechando o socket TCP bruscamente sem LEAVE ou LOGOUT)
    System.out.println("  [SIMULACAO] Bob sofre brute disconnect (Ctrl+C / Kill)...");
    tcpBob.fecharConexao();
    udpBob.fecharConexao();

    // Espera notificacao UPDATE_USERS em Alice
    boolean notificou = updateUsersLatch.await(3000, TimeUnit.MILLISECONDS);
    System.out.println("  Alice recebeu UPDATE_USERS apos queda de Bob? " + (notificou && updateRecebidoAlice.get()));
    Thread.sleep(600);

    // 4. Consulta membros de SalaGeral via Alice
    APDU mDepois = tcpAlice.listMembers("SalaGeral");
    System.out.println("  Membros de SalaGeral apos queda de Bob: '" + mDepois.getTextoMensagem() + "'");
    if (mDepois.getTextoMensagem().contains("Bob")) {
      throw new AssertionError("FALHA: Bob ainda consta como membro fantasma em SalaGeral!");
    }
    if (!mDepois.getTextoMensagem().contains("Alice")) {
      throw new AssertionError("FALHA: Alice deveria continuar em SalaGeral!");
    }

    // 5. Consulta usuarios online via Alice
    APDU uDepois = tcpAlice.listUsers();
    System.out.println("  Usuarios online apos queda de Bob: '" + uDepois.getTextoMensagem() + "'");
    if (uDepois.getTextoMensagem().contains("Bob")) {
      throw new AssertionError("FALHA: Bob ainda consta como usuario online fantasma!");
    }

    tcpAlice.fecharConexao();
    udpAlice.fecharConexao();
    servidor.pararServicos();
    System.out.println("  [SUCESSO] Teste 1 concluido com exito!");
  }

  private static void testarExclusaoGrupoVazioAposBruteDisconnect() throws Exception {
    System.out.println("\n--- [TESTE 2] Exclusao de Grupo Vazio apos Brute Disconnect ---");

    Servidor servidor = new Servidor();
    Thread tServidor = new Thread(() -> servidor.iniciarServicos(16799, 17788, 18899, false, 1));
    tServidor.setDaemon(true);
    tServidor.start();
    Thread.sleep(800);

    // Carol conecta e cria/entra no grupo 'SalaExclusiva' (Carol e o unico membro)
    ClienteTCP tcpCarol = new ClienteTCP("127.0.0.1", 16799);
    ClienteUDP udpCarol = new ClienteUDP("127.0.0.1", 17788);
    new Thread(udpCarol).start();
    InfoUser uCarol = new InfoUser("Carol", "127.0.0.1", udpCarol.getPortaLocal());
    tcpCarol.register(uCarol);
    tcpCarol.join("SalaExclusiva", uCarol);

    // Dan conecta apenas para consultar os grupos
    ClienteTCP tcpDan = new ClienteTCP("127.0.0.1", 16799);
    APDU gruposAntes = tcpDan.list();
    System.out.println("  Grupos disponiveis antes da queda de Carol: '" + gruposAntes.getTextoMensagem() + "'");
    if (!gruposAntes.getTextoMensagem().contains("SalaExclusiva")) {
      throw new AssertionError("SalaExclusiva deveria existir na listagem!");
    }

    // Carol sofre brute disconnect
    System.out.println("  [SIMULACAO] Carol sofre brute disconnect...");
    tcpCarol.fecharConexao();
    udpCarol.fecharConexao();

    Thread.sleep(800);

    // Dan consulta os grupos novamente
    APDU gruposDepois = tcpDan.list();
    System.out.println("  Grupos disponiveis apos queda de Carol: '" + gruposDepois.getTextoMensagem() + "'");
    if (gruposDepois.getTextoMensagem().contains("SalaExclusiva")) {
      throw new AssertionError("FALHA: SalaExclusiva deveria ter sido deletada por estar vazia!");
    }

    tcpDan.fecharConexao();
    servidor.pararServicos();
    System.out.println("  [SUCESSO] Teste 2 concluido com exito!");
  }

  private static void testarClusterBruteDisconnectEFailover() throws Exception {
    System.out.println("\n--- [TESTE 3] Cluster Real: Brute Disconnect Replicado e Failover Seguro ---");

    List<Process> procs = new ArrayList<>();
    try {
      // Inicia Nó 1 e Nó 2 em modo local
      ProcessBuilder pb1 = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local");
      pb1.directory(new File("."));
      pb1.redirectErrorStream(true);
      File fLog1 = new File("scratch/node_ghost_1.log");
      pb1.redirectOutput(fLog1);
      Process p1 = pb1.start();
      procs.add(p1);

      Thread.sleep(2000);

      ProcessBuilder pb2 = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local");
      pb2.directory(new File("."));
      pb2.redirectErrorStream(true);
      File fLog2 = new File("scratch/node_ghost_2.log");
      pb2.redirectOutput(fLog2);
      Process p2 = pb2.start();
      procs.add(p2);

      Thread.sleep(2500);

      // Descobre quem e o lider: Nó 1 escuta TCP 6789, Nó 2 escuta TCP 6790
      int portaLider = -1;
      int portaStandby = -1;

      if (isPortaAtiva(6789)) {
        portaLider = 6789;
        portaStandby = 6799;
      } else if (isPortaAtiva(6799)) {
        portaLider = 6799;
        portaStandby = 6789;
      }

      if (portaLider == -1) {
        throw new AssertionError("Nenhum líder ativo encontrado nas portas 6789/6799!");
      }
      System.out.println("  Líder do cluster identificado na porta TCP " + portaLider);

      // Conecta BobCluster e AliceCluster ao líder
      ClienteTCP tcpBob = new ClienteTCP("127.0.0.1", portaLider);
      InfoUser uBob = new InfoUser("BobCluster", "127.0.0.1", 20001);
      tcpBob.register(uBob);
      tcpBob.join("SalaCluster", uBob);

      ClienteTCP tcpAlice = new ClienteTCP("127.0.0.1", portaLider);
      InfoUser uAlice = new InfoUser("AliceCluster", "127.0.0.1", 20002);
      tcpAlice.register(uAlice);
      tcpAlice.join("SalaCluster", uAlice);

      Thread.sleep(800);

      APDU membrosAntes = tcpAlice.listMembers("SalaCluster");
      System.out.println("  Membros no cluster antes da queda: '" + membrosAntes.getTextoMensagem() + "'");
      if (!membrosAntes.getTextoMensagem().contains("BobCluster") || !membrosAntes.getTextoMensagem().contains("AliceCluster")) {
        throw new AssertionError("BobCluster e AliceCluster devem estar na sala!");
      }

      // BobCluster sofre brute disconnect no líder
      System.out.println("  [SIMULACAO] BobCluster sofre brute disconnect no líder...");
      tcpBob.fecharConexao();

      Thread.sleep(1200);

      // Alice verifica que BobCluster saiu
      APDU membrosApos = tcpAlice.listMembers("SalaCluster");
      System.out.println("  Membros no cluster apos queda de Bob: '" + membrosApos.getTextoMensagem() + "'");
      if (membrosApos.getTextoMensagem().contains("BobCluster")) {
        throw new AssertionError("FALHA: BobCluster ainda consta como membro fantasma no cluster!");
      }

      APDU usersApos = tcpAlice.listUsers();
      System.out.println("  Usuarios online no cluster apos queda de Bob: '" + usersApos.getTextoMensagem() + "'");
      if (usersApos.getTextoMensagem().contains("BobCluster")) {
        throw new AssertionError("FALHA: BobCluster ainda consta como online no cluster!");
      }

      // 4. Agora simula queda do líder (Failover) e garante que Alice NAO e deletada por engano
      System.out.println("\n  [SIMULACAO] Derrubando o processo do líder (Failover para Standby)...");
      if (portaLider == 6789) {
        p1.destroyForcibly();
      } else {
        p2.destroyForcibly();
      }

      // Aguarda standby assumir (eleicao + abertura de servicos)
      Thread.sleep(4000);

      if (!isPortaAtiva(portaStandby)) {
        throw new AssertionError("O nó standby deveria ter aberto servicos na porta " + portaStandby + "!");
      }
      System.out.println("  Novo líder assumiu com sucesso na porta " + portaStandby);

      // Conecta ao novo lider e valida que Alice continua salva e Bob NAO existe
      ClienteTCP tcpNovoLider = new ClienteTCP("127.0.0.1", portaStandby);
      APDU membrosNovoLider = tcpNovoLider.listMembers("SalaCluster");
      System.out.println("  Membros em SalaCluster no novo líder: '" + membrosNovoLider.getTextoMensagem() + "'");
      if (membrosNovoLider.getTextoMensagem().contains("BobCluster")) {
        throw new AssertionError("FALHA: BobCluster reapareceu como fantasma no novo líder!");
      }
      if (!membrosNovoLider.getTextoMensagem().contains("AliceCluster")) {
        throw new AssertionError("FALHA: AliceCluster deveria ter sido preservada no novo líder!");
      }

      tcpAlice.fecharConexao();
      tcpNovoLider.fecharConexao();
      System.out.println("  [SUCESSO] Teste 3 concluido com exito!");

    } finally {
      for (Process p : procs) {
        try {
          p.destroyForcibly();
        } catch (Exception ignored) {}
      }
    }
  }

  private static boolean isPortaAtiva(int porta) {
    try {
      ClienteTCP tcp = new ClienteTCP("127.0.0.1", porta);
      APDU res = tcp.ping();
      tcp.fecharConexao();
      return res != null && Protocolo.OK.equals(res.getOperacao());
    } catch (Exception e) {
      return false;
    }
  }
}
