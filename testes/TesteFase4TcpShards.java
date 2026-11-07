import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import Protocol.APDU;

/**
 * Suite de validacao automatizada da Fase 4 (T#03):
 *  1) Proxy Transparente TCP (REGISTER, JOIN, LEAVE, LOGOUT, MEMBERS) com particionamento estrito em 3 Shards.
 *  2) Replicacao sincrona intra-shard (ATIVO -> STANDBY) nos 6 processos (3 Shards x 2 Replicas).
 *  3) Scatter-Gather em LIST e USERS.
 *  4) Replicacao global assincrona de BLOCK e UNBLOCK em todos os Shards.
 *  5) Ghost Client: queda brusca TCP disparando broadcast UDP USUARIO_CAIU e limpando salas cross-shard.
 *  6) Scatter-Gather resiliente sob falha parcial (queda total do Shard 1 -> S0 + S2 respondem sem travar).
 */
public class TesteFase4TcpShards {

  static class NoProcesso {
    final int shard;
    final int replica;
    final Process proc;
    final PrintWriter stdin;
    final List<String> logs = new CopyOnWriteArrayList<>();

    NoProcesso(int shard, int replica, Process proc) {
      this.shard = shard;
      this.replica = replica;
      this.proc = proc;
      this.stdin = new PrintWriter(new OutputStreamWriter(proc.getOutputStream(), StandardCharsets.UTF_8), true);
      Thread leitor = new Thread(() -> {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
          String linha;
          while ((linha = br.readLine()) != null) {
            logs.add(linha);
          }
        } catch (Exception ignored) {
        }
      }, "LogReader-S" + shard + "R" + replica);
      leitor.setDaemon(true);
      leitor.start();
    }

    int cursor() {
      return logs.size();
    }

    String capturarEstadoDesde(int idxInicial) throws Exception {
      stdin.println("estado");
      long fim = System.currentTimeMillis() + 2500;
      while (System.currentTimeMillis() < fim) {
        StringBuilder sb = new StringBuilder();
        for (int i = idxInicial; i < logs.size(); i++) {
          sb.append(logs.get(i)).append('\n');
        }
        String s = sb.toString();
        if (s.contains("========================================================")) {
          return s;
        }
        Thread.sleep(60);
      }
      StringBuilder sb = new StringBuilder();
      for (int i = idxInicial; i < logs.size(); i++) {
        sb.append(logs.get(i)).append('\n');
      }
      return sb.toString();
    }

    boolean contemLog(String trecho) {
      for (String l : logs) {
        if (l.contains(trecho)) return true;
      }
      return false;
    }

    void matarBruscamente() {
      proc.destroyForcibly();
    }
  }

  static class SessaoCliente implements AutoCloseable {
    final String nome;
    final int portaUdp;
    final Socket socket;
    final ObjectOutputStream out;
    final ObjectInputStream in;

    SessaoCliente(String host, int portaTcp, String nome, int portaUdp) throws Exception {
      this.nome = nome;
      this.portaUdp = portaUdp;
      this.socket = new Socket(host, portaTcp);
      this.socket.setSoTimeout(6000);
      this.out = new ObjectOutputStream(socket.getOutputStream());
      this.out.flush();
      this.in = new ObjectInputStream(socket.getInputStream());
    }

    String enviar(APDU apdu) throws Exception {
      if (apdu.getIdRequisicao() == null) {
        apdu.setIdRequisicao("req-" + UUID.randomUUID());
      }
      out.writeUnshared(apdu);
      out.flush();
      return (String) in.readObject();
    }

    @Override
    public void close() {
      try {
        socket.close();
      } catch (Exception ignored) {
      }
    }
  }

  private static NoProcesso iniciarNo(String dirRaiz, int shard, int replica) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-Dcores=false", "-cp", dirRaiz + "/Servidor/bin",
        "Principal", String.valueOf(shard), String.valueOf(replica), "2");
    pb.redirectErrorStream(true);
    return new NoProcesso(shard, replica, pb.start());
  }

  private static void aguardarPortaTcp(int porta, long timeoutMs) throws Exception {
    long fim = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < fim) {
      try (Socket s = new Socket("127.0.0.1", porta)) {
        return;
      } catch (Exception ignored) {
        Thread.sleep(100);
      }
    }
    throw new AssertionError("Timeout aguardando abertura da porta TCP " + porta);
  }

  private static void aguardarStandby(NoProcesso p, long timeoutMs) throws Exception {
    long fim = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < fim) {
      if (p.contemLog("| STANDBY]")) return;
      Thread.sleep(100);
    }
    throw new AssertionError("No S" + p.shard + "/R" + p.replica + " nao entrou em STANDBY a tempo.");
  }

  private static void assertTrue(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError("FALHA DE ASSERCAO: " + msg);
    }
  }

  public static void main(String[] args) throws Exception {
    String dirRaiz = System.getProperty("user.dir");
    List<NoProcesso> todos = new ArrayList<>();

    try {
      System.out.println("=== [FASE 4] Subindo topologia completa: 3 Shards x 2 Replicas (6 processos) ===");
      NoProcesso s0r0 = iniciarNo(dirRaiz, 0, 0); todos.add(s0r0);
      NoProcesso s1r0 = iniciarNo(dirRaiz, 1, 0); todos.add(s1r0);
      NoProcesso s2r0 = iniciarNo(dirRaiz, 2, 0); todos.add(s2r0);

      aguardarPortaTcp(5000, 8000);
      aguardarPortaTcp(5100, 8000);
      aguardarPortaTcp(5200, 8000);

      NoProcesso s0r1 = iniciarNo(dirRaiz, 0, 1); todos.add(s0r1);
      NoProcesso s1r1 = iniciarNo(dirRaiz, 1, 1); todos.add(s1r1);
      NoProcesso s2r1 = iniciarNo(dirRaiz, 2, 1); todos.add(s2r1);

      aguardarStandby(s0r1, 8000);
      aguardarStandby(s1r1, 8000);
      aguardarStandby(s2r1, 8000);
      // Aguarda 1.5s para ANUNCIO_SHARD popular TabelaLideres entre os 3 shards
      Thread.sleep(1500);
      System.out.println("  [OK] 6 processos online: S0(5000/5001), S1(5100/5101), S2(5200/5201).");

      // =========================================================================
      // TESTE 1: REGISTER via Proxy Transparente na porta 5000 (Shard 0)
      // =========================================================================
      System.out.println("=== [TESTE 1] REGISTER roteado por inicial (Ana->S0, 123Bot->S0, Joao->S1, Zeca->S2) ===");
      SessaoCliente cAna = new SessaoCliente("127.0.0.1", 5000, "Ana", 16001);
      SessaoCliente cBot = new SessaoCliente("127.0.0.1", 5000, "123Bot", 16002);
      SessaoCliente cJoao = new SessaoCliente("127.0.0.1", 5000, "Joao", 16003);
      SessaoCliente cZeca = new SessaoCliente("127.0.0.1", 5000, "Zeca", 16004);

      assertTrue(cAna.enviar(new APDU("REGISTER", "GLOBAL", "Ana", null, 16001)).startsWith("OK:"), "REGISTER Ana");
      assertTrue(cBot.enviar(new APDU("REGISTER", "GLOBAL", "123Bot", null, 16002)).startsWith("OK:"), "REGISTER 123Bot");
      assertTrue(cJoao.enviar(new APDU("REGISTER", "GLOBAL", "Joao", null, 16003)).startsWith("OK:"), "REGISTER Joao");
      assertTrue(cZeca.enviar(new APDU("REGISTER", "GLOBAL", "Zeca", null, 16004)).startsWith("OK:"), "REGISTER Zeca");

      String estS0R0 = s0r0.capturarEstadoDesde(s0r0.cursor());
      String estS0R1 = s0r1.capturarEstadoDesde(s0r1.cursor());
      String estS1R0 = s1r0.capturarEstadoDesde(s1r0.cursor());
      String estS1R1 = s1r1.capturarEstadoDesde(s1r1.cursor());
      String estS2R0 = s2r0.capturarEstadoDesde(s2r0.cursor());
      String estS2R1 = s2r1.capturarEstadoDesde(s2r1.cursor());

      assertTrue(estS0R0.contains("- Ana ") && estS0R0.contains("- 123Bot "), "S0/R0 deve ter Ana e 123Bot");
      assertTrue(!estS0R0.contains("- Joao ") && !estS0R0.contains("- Zeca "), "S0/R0 NAO pode ter Joao nem Zeca");
      assertTrue(estS0R1.contains("- Ana ") && estS0R1.contains("- 123Bot "), "Standby S0/R1 deve replicar Ana e 123Bot");

      assertTrue(estS1R0.contains("- Joao ") && !estS1R0.contains("- Ana ") && !estS1R0.contains("- Zeca "), "S1/R0 deve ter apenas Joao");
      assertTrue(estS1R1.contains("- Joao "), "Standby S1/R1 deve replicar Joao");

      assertTrue(estS2R0.contains("- Zeca ") && !estS2R0.contains("- Ana ") && !estS2R0.contains("- Joao "), "S2/R0 deve ter apenas Zeca");
      assertTrue(estS2R1.contains("- Zeca "), "Standby S2/R1 deve replicar Zeca");
      System.out.println("  [OK] Particionamento de usuarios + replicacao intra-shard validados nos 6 nos!");

      // =========================================================================
      // TESTE 2: Cross-Shard JOIN, MEMBERS e LEAVE (sem vazar usuarios entre shards)
      // =========================================================================
      System.out.println("=== [TESTE 2] Cross-Shard JOIN / MEMBERS / LEAVE ===");
      assertTrue(cZeca.enviar(new APDU("JOIN", "Amigos", "Zeca", null, 16004)).startsWith("OK:"), "Zeca JOIN Amigos (S0)");
      assertTrue(cZeca.enviar(new APDU("JOIN", "Jogos", "Zeca", null, 16004)).startsWith("OK:"), "Zeca JOIN Jogos (S1)");
      assertTrue(cZeca.enviar(new APDU("JOIN", "Sala", "Zeca", null, 16004)).startsWith("OK:"), "Zeca JOIN Sala (S2)");
      assertTrue(cAna.enviar(new APDU("JOIN", "Jogos", "Ana", null, 16001)).startsWith("OK:"), "Ana JOIN Jogos (S1)");
      assertTrue(cJoao.enviar(new APDU("JOIN", "Sala", "Joao", null, 16003)).startsWith("OK:"), "Joao JOIN Sala (S2)");

      String mAmigos = cAna.enviar(new APDU("MEMBERS", "Amigos", "Ana", null, 16001));
      String mJogos = cAna.enviar(new APDU("MEMBERS", "Jogos", "Ana", null, 16001));
      String mSala = cAna.enviar(new APDU("MEMBERS", "Sala", "Ana", null, 16001));

      assertTrue("OK: Zeca".equals(mAmigos), "MEMBERS Amigos esperado 'OK: Zeca', veio: " + mAmigos);
      assertTrue("OK: Ana,Zeca".equals(mJogos), "MEMBERS Jogos esperado 'OK: Ana,Zeca', veio: " + mJogos);
      assertTrue("OK: Joao,Zeca".equals(mSala), "MEMBERS Sala esperado 'OK: Joao,Zeca', veio: " + mSala);

      // Garante que Zeca entrar em Amigos (S0) NAO vazou Zeca para a tabela de usuarios de S0!
      String estS0AposJoin = s0r0.capturarEstadoDesde(s0r0.cursor());
      assertTrue(estS0AposJoin.contains("Usuarios Desta Particao (2):"),
          "S0 deve continuar com exatamente 2 usuarios (Ana e 123Bot), veio:\n" + estS0AposJoin);

      // Testa LEAVE cross-shard: Ana sai de Jogos (S1)
      assertTrue(cAna.enviar(new APDU("LEAVE", "Jogos", "Ana", null, 16001)).startsWith("OK:"), "Ana LEAVE Jogos");
      String mJogosAposLeave = cAna.enviar(new APDU("MEMBERS", "Jogos", "Ana", null, 16001));
      assertTrue("OK: Zeca".equals(mJogosAposLeave), "MEMBERS Jogos apos LEAVE de Ana esperado 'OK: Zeca', veio: " + mJogosAposLeave);
      System.out.println("  [OK] JOIN, MEMBERS e LEAVE cross-shard validados com sucesso!");

      // =========================================================================
      // TESTE 3: Scatter-Gather em LIST e USERS (testado a partir de S0:5000 e S2:5200)
      // =========================================================================
      System.out.println("=== [TESTE 3] Scatter-Gather em LIST e USERS ===");
      String respList = cAna.enviar(new APDU("LIST", "GLOBAL", "Ana", null, 16001));
      String respUsers = cAna.enviar(new APDU("USERS", "GLOBAL", "Ana", null, 16001));
      assertTrue("OK: Amigos,Jogos,Sala".equals(respList), "LIST esperado 'OK: Amigos,Jogos,Sala', veio: " + respList);
      assertTrue("OK: 123Bot,Ana,Joao,Zeca".equals(respUsers), "USERS esperado 'OK: 123Bot,Ana,Joao,Zeca', veio: " + respUsers);

      // Tambem testa conectando diretamente no lider do Shard 2 (porta 5200) como Gateway!
      try (SessaoCliente cProbeS2 = new SessaoCliente("127.0.0.1", 5200, "ProbeS2", 16099)) {
        String listViaS2 = cProbeS2.enviar(new APDU("LIST", "GLOBAL", "ProbeS2", null, 16099));
        assertTrue("OK: Amigos,Jogos,Sala".equals(listViaS2), "LIST via Gateway S2:5200 esperado 'OK: Amigos,Jogos,Sala', veio: " + listViaS2);
        cProbeS2.enviar(new APDU("LOGOUT", "GLOBAL", "ProbeS2", null, 16099));
      }
      System.out.println("  [OK] Scatter-Gather de LIST e USERS retornou visao global unificada!");

      // =========================================================================
      // TESTE 4: BLOCK e UNBLOCK com broadcast global assincrono
      // =========================================================================
      System.out.println("=== [TESTE 4] BLOCK / UNBLOCK replicados globalmente de forma assincrona ===");
      String rBlock = cAna.enviar(new APDU("BLOCK", "GLOBAL", "Ana", null, 16001, "Zeca"));
      assertTrue(rBlock.startsWith("OK:"), "BLOCK Ana->Zeca falhou: " + rBlock);
      Thread.sleep(600); // Aguarda propagacao assincrona para S1 e S2 (+ standbys)

      for (NoProcesso np : Arrays.asList(s0r0, s0r1, s1r0, s1r1, s2r0, s2r1)) {
        String est = np.capturarEstadoDesde(np.cursor());
        assertTrue(est.contains("ana bloqueou: [zeca]"),
            "Bloqueio global ausente em S" + np.shard + "/R" + np.replica + ":\n" + est);
      }

      String rUnblock = cAna.enviar(new APDU("UNBLOCK", "GLOBAL", "Ana", null, 16001, "Zeca"));
      assertTrue(rUnblock.startsWith("OK:"), "UNBLOCK Ana->Zeca falhou: " + rUnblock);
      Thread.sleep(600);

      for (NoProcesso np : Arrays.asList(s0r0, s0r1, s1r0, s1r1, s2r0, s2r1)) {
        String est = np.capturarEstadoDesde(np.cursor());
        assertTrue(est.contains("Bloqueios Replicados Globalmente (0):"),
            "Desbloqueio global nao limpou S" + np.shard + "/R" + np.replica + ":\n" + est);
      }
      System.out.println("  [OK] BLOCK e UNBLOCK replicados assincronamente em todos os 3 shards e standbys!");

      // =========================================================================
      // TESTE 5: Ghost Client (Queda Brusca TCP -> Broadcast UDP USUARIO_CAIU)
      // =========================================================================
      System.out.println("=== [TESTE 5] Ghost Client: queda brusca do socket TCP de Zeca ===");
      cZeca.close(); // Fecha TCP abruptamente SEM enviar LOGOUT nem LEAVE!
      Thread.sleep(1200);

      String usersPosQueda = cAna.enviar(new APDU("USERS", "GLOBAL", "Ana", null, 16001));
      String listPosQueda = cAna.enviar(new APDU("LIST", "GLOBAL", "Ana", null, 16001));
      String membrosSalaPosQueda = cAna.enviar(new APDU("MEMBERS", "Sala", "Ana", null, 16001));

      assertTrue("OK: 123Bot,Ana,Joao".equals(usersPosQueda),
          "Zeca deveria ter sido removido de USERS apos queda brusca, veio: " + usersPosQueda);
      assertTrue("OK: Sala".equals(listPosQueda),
          "Salas 'Amigos' (S0) e 'Jogos' (S1) onde so Zeca estava deveriam sumir; veio: " + listPosQueda);
      assertTrue("OK: Joao".equals(membrosSalaPosQueda),
          "Sala (S2) deveria manter apenas Joao apos queda de Zeca, veio: " + membrosSalaPosQueda);
      assertTrue(s1r0.contemLog("USUARIO_CAIU") || s2r0.contemLog("USUARIO_CAIU"),
          "Lideres remotos deveriam registrar recepcao de USUARIO_CAIU no log");
      System.out.println("  [OK] Cliente Fantasma (Zeca) limpo automaticamente nos 3 shards via USUARIO_CAIU!");

      // =========================================================================
      // TESTE 6: Scatter-Gather Resiliente sob Falha Parcial Total do Shard 1 [J..R]
      // =========================================================================
      System.out.println("=== [TESTE 6] Falha Parcial (CAP): derrubando TODO o Shard 1 (S1/R0 + S1/R1) ===");
      // Antes de derrubar S1, Joao entra no grupo "Java" (S1) e Ana entra em "Arquitetura" (S0) e "Sala" (S2)
      assertTrue(cJoao.enviar(new APDU("JOIN", "Java", "Joao", null, 16003)).startsWith("OK:"), "Joao JOIN Java");
      assertTrue(cAna.enviar(new APDU("JOIN", "Arquitetura", "Ana", null, 16001)).startsWith("OK:"), "Ana JOIN Arquitetura");
      assertTrue(cAna.enviar(new APDU("JOIN", "Sala", "Ana", null, 16001)).startsWith("OK:"), "Ana JOIN Sala");

      // Mata AMBAS as replicas do Shard 1 (particao J..R inteira offline, com Joao ainda conectado no Gateway S0!)
      s1r0.matarBruscamente();
      s1r1.matarBruscamente();
      s1r0.proc.waitFor();
      s1r1.proc.waitFor();
      Thread.sleep(400);

      long t0List = System.currentTimeMillis();
      String listParcial = cAna.enviar(new APDU("LIST", "GLOBAL", "Ana", null, 16001));
      long dtList = System.currentTimeMillis() - t0List;

      long t0Users = System.currentTimeMillis();
      String usersParcial = cAna.enviar(new APDU("USERS", "GLOBAL", "Ana", null, 16001));
      long dtUsers = System.currentTimeMillis() - t0Users;

      assertTrue(dtList < 2600, "LIST nao pode travar com S1 offline! Levou " + dtList + "ms");
      assertTrue(dtUsers < 2600, "USERS nao pode travar com S1 offline! Levou " + dtUsers + "ms");
      assertTrue("OK: Arquitetura,Sala".equals(listParcial),
          "LIST parcial deveria retornar S0+S2 ('OK: Arquitetura,Sala'), veio: " + listParcial);
      assertTrue("OK: 123Bot,Ana".equals(usersParcial),
          "USERS parcial deveria retornar S0+S2 ('OK: 123Bot,Ana'), veio: " + usersParcial);

      // Operacao no Shard 1 offline falha graciosamente sem derrubar o cliente
      String joinS1Off = cAna.enviar(new APDU("JOIN", "Jogos", "Ana", null, 16001));
      assertTrue(joinS1Off.startsWith("ERRO:"), "JOIN em S1 offline deve retornar ERRO, veio: " + joinS1Off);

      // Operacoes em S0 e S2 continuam funcionando normalmente!
      assertTrue(cAna.enviar(new APDU("JOIN", "Sistemas", "Ana", null, 16001)).startsWith("OK:"),
          "JOIN Sistemas (S2) deve continuar disponivel mesmo com S1 morto!");

      System.out.println("  [OK] Resiliencia CAP comprovada: LIST (" + dtList + "ms) e USERS (" + dtUsers
          + "ms) responderam parcialmente sem travar, S1 retornou ERRO isolado e S0/S2 seguiram 100% operacionais!");

      cJoao.close();
      cAna.close();
      cBot.close();
      System.out.println("\n>>> TODOS OS 6 CENARIOS DA FASE 4 PASSARAM COM 100% DE SUCESSO! <<<");

    } finally {
      for (NoProcesso np : todos) {
        np.matarBruscamente();
      }
    }
  }
}
