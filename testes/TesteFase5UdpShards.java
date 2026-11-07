import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import Protocol.APDU;
import model.ClienteUDP;
import model.MessageListener;
import utils.InfoUser;

/**
 * Suite automatizada da Fase 5 (Roteamento UDP Inter-Shards em 6 processos: 3 Shards x 2 Replicas):
 *  1) SEND / SENDVU roteados para Particionador.shardDe(nomeGrupo), salvando ticks no shard do grupo,
 *     enviando Auto-ACK (Status 1) direto ao remetente e agregando Status 2 e Status 3 ("TODOS").
 *  2) SENDPVT roteado para Particionador.shardDe(destinatario), salvando ticks no shard do destinatario,
 *     enviando Auto-ACK (Status 1) e repassando Status 2 e Status 3 ao autor cross-shard.
 *  3) BLOCK / UNBLOCK checados em O(1) na RAM local de cada shard (~BLOCKED~ em grupo e Status -1 no privado).
 *  4) Failover intra-shard (queda do lider S2/R0 -> S2/R1 assume) mantendo roteamento UDP transparente.
 */
public class TesteFase5UdpShards {

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
      return "";
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

  static class MsgRecebida {
    final String id;
    final String chatOuGrupo;
    final String remetente;
    final String texto;
    final boolean privado;
    final boolean vu;

    MsgRecebida(String id, String chatOuGrupo, String remetente, String texto, boolean privado, boolean vu) {
      this.id = id;
      this.chatOuGrupo = chatOuGrupo;
      this.remetente = remetente;
      this.texto = texto;
      this.privado = privado;
      this.vu = vu;
    }
  }

  static class ClienteCompleto implements AutoCloseable {
    final String nome;
    final InfoUser info;
    final Socket socketTcp;
    final ObjectOutputStream out;
    final ObjectInputStream in;
    final ClienteUDP udp;
    final List<MsgRecebida> recebidas = new CopyOnWriteArrayList<>();
    final Map<String, List<Integer>> historicoTicks = new ConcurrentHashMap<>();

    ClienteCompleto(String host, int portaTcp, int portaUdpServidor, String nome) throws Exception {
      this.nome = nome;
      this.udp = new ClienteUDP(host, portaUdpServidor);
      this.udp.setMeuNome(nome);
      this.info = new InfoUser(nome, "127.0.0.1", udp.getPortaLocal());

      this.udp.setListener(new MessageListener() {
        @Override
        public void onMessageReceived(String idMensagem, String nomeGrupo, InfoUser remetente,
            String mensagem, boolean isPrivado, boolean isVisualizacaoUnica) {
          recebidas.add(new MsgRecebida(
              idMensagem, nomeGrupo, remetente.getNome(), mensagem, isPrivado, isVisualizacaoUnica));
        }

        @Override
        public void onUpdateUsers() {
        }

        @Override
        public void onShutdown() {
        }

        @Override
        public void onTickReceived(String idMensagem, int status, String nomeConfirmou) {
          if (idMensagem != null) {
            historicoTicks.computeIfAbsent(idMensagem, k -> new CopyOnWriteArrayList<>()).add(status);
          }
        }
      });

      Thread tUdp = new Thread(udp, "ClienteUDP-" + nome);
      tUdp.setDaemon(true);
      tUdp.start();

      this.socketTcp = new Socket(host, portaTcp);
      this.socketTcp.setSoTimeout(6000);
      this.out = new ObjectOutputStream(socketTcp.getOutputStream());
      this.out.flush();
      this.in = new ObjectInputStream(socketTcp.getInputStream());

      String rReg = enviarTcp(new APDU("REGISTER", "GLOBAL", nome, null, info.getPorta()));
      assertTrue(rReg.startsWith("OK:"), "Falha no REGISTER de " + nome + ": " + rReg);
    }

    String enviarTcp(APDU apdu) throws Exception {
      if (apdu.getIdRequisicao() == null) {
        apdu.setIdRequisicao("req-" + UUID.randomUUID());
      }
      out.writeUnshared(apdu);
      out.flush();
      return (String) in.readObject();
    }

    MsgRecebida aguardarMensagem(String textoEsperado, long timeoutMs) throws Exception {
      long fim = System.currentTimeMillis() + timeoutMs;
      while (System.currentTimeMillis() < fim) {
        for (MsgRecebida m : recebidas) {
          if (textoEsperado.equals(m.texto)) {
            return m;
          }
        }
        Thread.sleep(40);
      }
      throw new AssertionError("Cliente '" + nome + "' nao recebeu mensagem '" + textoEsperado
          + "' em " + timeoutMs + "ms.");
    }

    boolean aguardarTick(String idMsg, int statusEsperado, long timeoutMs) throws Exception {
      long fim = System.currentTimeMillis() + timeoutMs;
      while (System.currentTimeMillis() < fim) {
        List<Integer> lista = historicoTicks.get(idMsg);
        if (lista != null && lista.contains(statusEsperado)) {
          return true;
        }
        Thread.sleep(40);
      }
      return false;
    }

    @Override
    public void close() {
      try {
        udp.fecharConexao();
      } catch (Exception ignored) {
      }
      try {
        socketTcp.close();
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
    throw new AssertionError("Timeout aguardando porta TCP " + porta);
  }

  private static void aguardarLog(NoProcesso p, String trecho, long timeoutMs) throws Exception {
    long fim = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < fim) {
      if (p.contemLog(trecho)) return;
      Thread.sleep(100);
    }
    throw new AssertionError("Timeout aguardando log '" + trecho + "' em S" + p.shard + "/R" + p.replica);
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
      System.out.println("=== [FASE 5] Subindo 6 processos (3 Shards x 2 Replicas) ===");
      NoProcesso s0r0 = iniciarNo(dirRaiz, 0, 0); todos.add(s0r0);
      NoProcesso s1r0 = iniciarNo(dirRaiz, 1, 0); todos.add(s1r0);
      NoProcesso s2r0 = iniciarNo(dirRaiz, 2, 0); todos.add(s2r0);

      aguardarPortaTcp(5000, 8000);
      aguardarPortaTcp(5100, 8000);
      aguardarPortaTcp(5200, 8000);

      NoProcesso s0r1 = iniciarNo(dirRaiz, 0, 1); todos.add(s0r1);
      NoProcesso s1r1 = iniciarNo(dirRaiz, 1, 1); todos.add(s1r1);
      NoProcesso s2r1 = iniciarNo(dirRaiz, 2, 1); todos.add(s2r1);

      aguardarLog(s0r1, "| STANDBY]", 8000);
      aguardarLog(s1r1, "| STANDBY]", 8000);
      aguardarLog(s2r1, "| STANDBY]", 8000);
      Thread.sleep(1500);

      // Todos os 3 clientes conectam no Gateway padrao S0 (TCP 5000 / UDP 6000)
      try (ClienteCompleto cAna = new ClienteCompleto("127.0.0.1", 5000, 6000, "Ana");
           ClienteCompleto cJoao = new ClienteCompleto("127.0.0.1", 5000, 6000, "Joao");
           ClienteCompleto cZeca = new ClienteCompleto("127.0.0.1", 5000, 6000, "Zeca")) {

        // =========================================================================
        // TESTE 1: SEND e SENDVU Cross-Shard (Grupo 'Zelda' -> S2) + Ticks 1, 2 e 3
        // =========================================================================
        System.out.println("=== [TESTE 1] SEND e SENDVU para grupo 'Zelda' (S2) + Ticks 1, 2 e 3 ===");
        assertTrue(cAna.enviarTcp(new APDU("JOIN", "Zelda", "Ana", null, cAna.info.getPorta())).startsWith("OK:"), "Ana JOIN Zelda");
        assertTrue(cJoao.enviarTcp(new APDU("JOIN", "Zelda", "Joao", null, cJoao.info.getPorta())).startsWith("OK:"), "Joao JOIN Zelda");
        assertTrue(cZeca.enviarTcp(new APDU("JOIN", "Zelda", "Zeca", null, cZeca.info.getPorta())).startsWith("OK:"), "Zeca JOIN Zelda");

        String idMsgGrupo = cAna.udp.send("Zelda", cAna.info, "Ola Zelda!");
        MsgRecebida mJoao = cJoao.aguardarMensagem("Ola Zelda!", 3000);
        MsgRecebida mZeca = cZeca.aguardarMensagem("Ola Zelda!", 3000);
        assertTrue(!mJoao.vu && !mZeca.vu, "Mensagem SEND padrao nao deve ser VU");

        // Ana deve receber Status 1 (Auto-ACK direto de S2) e Status 2 (CONFIRM automatico de ClienteUDP)
        assertTrue(cAna.aguardarTick(idMsgGrupo, 1, 2500), "Ana nao recebeu Auto-ACK Status 1 de S2");
        assertTrue(cAna.aguardarTick(idMsgGrupo, 2, 2500), "Ana nao recebeu CONFIRM Status 2 roteado por S2");

        // Confere logs estruturados exigidos na especificacao!
        assertTrue(s0r0.contemLog("[UDP] Roteando SEND para grupo 'Zelda' -> S2 [S..Z]"),
            "Gateway S0 deveria logar '[UDP] Roteando SEND para grupo 'Zelda' -> S2 [S..Z]'");

        // Apenas Joao le -> Ana AINDA NAO deve receber Status 3 (pois Zeca ainda nao leu!)
        cJoao.udp.sendConfirm(idMsgGrupo, 3, "Zelda", "Ana");
        Thread.sleep(400);
        assertTrue(!cAna.historicoTicks.get(idMsgGrupo).contains(3),
            "Status 3 nao pode disparar antes de TODOS os membros lerem!");

        // Agora Zeca le -> TODOS leram! S2 dispara Status 3 ("TODOS") para Ana!
        cZeca.udp.sendConfirm(idMsgGrupo, 3, "Zelda", "Ana");
        assertTrue(cAna.aguardarTick(idMsgGrupo, 3, 2500), "Ana nao recebeu CONFIRM Status 3 apos leitura de todos!");

        // Verifica que o tick foi salvo APENAS em S2 (ativo e standby), nunca em S0 ou S1
        String estS0 = s0r0.capturarEstadoDesde(s0r0.cursor());
        String estS2R0 = s2r0.capturarEstadoDesde(s2r0.cursor());
        String estS2R1 = s2r1.capturarEstadoDesde(s2r1.cursor());
        assertTrue(estS0.contains("Mensagens Rastreadas Nesta Particao: 0"), "S0 nao deve armazenar tick do grupo Zelda");
        assertTrue(estS2R0.contains("Mensagens Rastreadas Nesta Particao: 1"), "S2/R0 deve armazenar o tick de Zelda");
        assertTrue(estS2R1.contains("Mensagens Rastreadas Nesta Particao: 1"), "Standby S2/R1 deve replicar o tick de Zelda");

        // Testa SENDVU no grupo 'Jogos' (S1)
        assertTrue(cAna.enviarTcp(new APDU("JOIN", "Jogos", "Ana", null, cAna.info.getPorta())).startsWith("OK:"), "Ana JOIN Jogos");
        assertTrue(cZeca.enviarTcp(new APDU("JOIN", "Jogos", "Zeca", null, cZeca.info.getPorta())).startsWith("OK:"), "Zeca JOIN Jogos");
        String idVu = cAna.udp.sendVu("Jogos", cAna.info, " Missao Secreta VU ");
        MsgRecebida mVu = cZeca.aguardarMensagem(" Missao Secreta VU ", 3000);
        assertTrue(mVu.vu, "Mensagem SENDVU deve chegar com flag isVisualizacaoUnica=true");
        assertTrue(cAna.aguardarTick(idVu, 1, 2500) && cAna.aguardarTick(idVu, 2, 2500), "Ticks 1/2 do SENDVU em S1");
        System.out.println("  [OK] SEND, SENDVU, Auto-ACK direto e agregacao de Ticks 1/2/3 cross-shard validados!");

        // =========================================================================
        // TESTE 2: SENDPVT Cross-Shard (Ana [S0] -> Joao [S1]) + Ticks no Destinatario
        // =========================================================================
        System.out.println("=== [TESTE 2] SENDPVT Cross-Shard (Ana->Joao em S1) + Ticks Privados ===");
        String idPvt = cAna.udp.sendPvt("Joao", cAna.info, "Oi Joao no privado!");
        MsgRecebida pvtJoao = cJoao.aguardarMensagem("Oi Joao no privado!", 3000);
        assertTrue(pvtJoao.privado && "Ana".equals(pvtJoao.remetente), "Joao deveria receber SENDPVT de Ana");
        assertTrue(cAna.aguardarTick(idPvt, 1, 2500), "Ana deveria receber Auto-ACK Status 1 do Shard de Joao (S1)");
        assertTrue(cAna.aguardarTick(idPvt, 2, 2500), "Ana deveria receber CONFIRM Status 2 do privado");

        cJoao.udp.sendConfirm(idPvt, 3, "@Joao", "Ana");
        assertTrue(cAna.aguardarTick(idPvt, 3, 2500), "Ana deveria receber CONFIRM Status 3 do privado");

        assertTrue(s0r0.contemLog("[UDP] Roteando SENDPVT para destinatario 'Joao' -> S1 [J..R]"),
            "Gateway S0 deveria logar roteamento de SENDPVT para S1");
        String estS1R0 = s1r0.capturarEstadoDesde(s1r0.cursor());
        String estS1R1 = s1r1.capturarEstadoDesde(s1r1.cursor());
        // S1 tem 1 msg de 'Jogos' (idVu) + 1 msg privada para 'Joao' (idPvt) = 2 mensagens!
        assertTrue(estS1R0.contains("Mensagens Rastreadas Nesta Particao: 2"),
            "S1/R0 deve guardar Jogos + tick privado de Joao (2 msgs):\n" + estS1R0);
        assertTrue(estS1R1.contains("Mensagens Rastreadas Nesta Particao: 2"),
            "Standby S1/R1 deve replicar os 2 ticks de S1!");
        System.out.println("  [OK] SENDPVT roteado para o Shard do Destinatario com Ticks 1/2/3 completos!");

        // =========================================================================
        // TESTE 3: Bloqueio Mutuo O(1) em RAM Local (Grupo ~BLOCKED~ e Privado Status -1)
        // =========================================================================
        System.out.println("=== [TESTE 3] Bloqueio Mutuo em RAM Local (BLOCK / UNBLOCK) ===");
        assertTrue(cJoao.enviarTcp(new APDU("BLOCK", "GLOBAL", "Joao", null, cJoao.info.getPorta(), "Ana")).startsWith("OK:"),
            "Joao BLOCK Ana");
        Thread.sleep(500); // Aguarda replicacao global assincrona nos 3 shards

        // 3a) No grupo Zelda (S2): Ana envia msg -> Zeca recebe texto real, Joao recebe ~BLOCKED~!
        cJoao.recebidas.clear();
        cZeca.recebidas.clear();
        cAna.udp.send("Zelda", cAna.info, "Texto visivel so para Zeca");
        cZeca.aguardarMensagem("Texto visivel so para Zeca", 3000);
        cJoao.aguardarMensagem("~BLOCKED~", 3000);

        // 3b) No privado: Ana -> Joao (S1) recebe CONFIRM Status -1 e Joao NAO recebe texto!
        int antesJoao = cJoao.recebidas.size();
        String idPvtBloq = cAna.udp.sendPvt("Joao", cAna.info, "Tentativa bloqueada");
        assertTrue(cAna.aguardarTick(idPvtBloq, -1, 2500),
            "Ana deveria receber CONFIRM Status -1 ao enviar SENDPVT bloqueado para Joao");
        Thread.sleep(250);
        assertTrue(cJoao.recebidas.size() == antesJoao, "Joao NAO pode receber SENDPVT bloqueado");

        // 3c) Bloqueio mutuo reverso: Joao -> Ana (S0) tambem bloqueado!
        String idPvtReverso = cJoao.udp.sendPvt("Ana", cJoao.info, "Reverso bloqueado");
        assertTrue(cJoao.aguardarTick(idPvtReverso, -1, 2500),
            "Bloqueio mutuo: Joao tambem deve receber Status -1 ao tentar falar com Ana");

        // 3d) UNBLOCK restaura entrega imediata
        assertTrue(cJoao.enviarTcp(new APDU("UNBLOCK", "GLOBAL", "Joao", null, cJoao.info.getPorta(), "Ana")).startsWith("OK:"),
            "Joao UNBLOCK Ana");
        Thread.sleep(500);
        cAna.udp.sendPvt("Joao", cAna.info, "Paz restaurada!");
        cJoao.aguardarMensagem("Paz restaurada!", 3000);
        System.out.println("  [OK] Bloqueio mutuo em RAM local validado em grupo (~BLOCKED~) e privado (Status -1)!");

        // =========================================================================
        // TESTE 4: Failover Intra-Shard (Queda de S2/R0 -> S2/R1 assume e roteia UDP)
        // =========================================================================
        System.out.println("=== [TESTE 4] Failover Intra-Shard no S2 durante trafego UDP ===");
        s2r0.matarBruscamente();
        s2r0.proc.waitFor();
        aguardarLog(s2r1, "| ATIVO]", 8000);
        Thread.sleep(1200); // Aguarda ANUNCIO_SHARD atualizar TabelaLideres

        String idPosFailover = cAna.udp.send("Zelda", cAna.info, "Zelda sobreviveu ao failover de S2!");
        cZeca.aguardarMensagem("Zelda sobreviveu ao failover de S2!", 3500);
        assertTrue(cAna.aguardarTick(idPosFailover, 1, 3000), "Auto-ACK Status 1 enviado pelo novo lider S2/R1");
        assertTrue(cAna.aguardarTick(idPosFailover, 2, 3000), "CONFIRM Status 2 processado pelo novo lider S2/R1");
        System.out.println("  [OK] Novo lider S2/R1 assumiu o grupo 'Zelda' e manteve entrega UDP + Ticks sem interrupcao!");
      }

      System.out.println("\n>>> TODOS OS 4 CENARIOS COMPLETOS DA FASE 5 PASSARAM COM 100% DE SUCESSO! <<<");

    } finally {
      for (NoProcesso np : todos) {
        np.matarBruscamente();
      }
    }
  }
}
