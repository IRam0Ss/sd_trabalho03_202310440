import java.io.*;
import java.util.*;

/**
 * Teste Fase 3: roteamento inter-shard, tabela de lideres, idempotencia, failover do shard destino e falha parcial.
 * Uso (na raiz do projeto): java -cp /tmp/t3 TesteRoteamentoShards
 */
public class TesteRoteamentoShards {
  static class No {
    final String nome;
    final Process p;
    final PrintWriter in;
    final StringBuffer out = new StringBuffer();

    No(int s, int r) throws Exception {
      nome = "S" + s + "R" + r;
      p = new ProcessBuilder("java", "-Dcores=false", "-cp", "Servidor/bin", "Principal", "" + s, "" + r)
          .redirectErrorStream(true).start();
      in = new PrintWriter(new OutputStreamWriter(p.getOutputStream()), true);
      Thread t = new Thread(() -> {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          String l;
          while ((l = br.readLine()) != null) out.append(l).append('\n');
        } catch (Exception ignored) {}
      });
      t.setDaemon(true);
      t.start();
    }

    void cmd(String c) { in.println(c); }
    boolean contem(String s) { return out.toString().contains(s); }
  }

  static int falhas = 0;

  static void check(String desc, boolean ok) {
    System.out.println((ok ? "[OK]    " : "[FALHA] ") + desc);
    if (!ok) falhas++;
  }

  static void dorme(long ms) throws Exception { Thread.sleep(ms); }

  public static void main(String[] a) throws Exception {
    Map<String, No> nos = new LinkedHashMap<>();
    for (int s = 0; s < 3; s++) for (int r = 0; r < 2; r++) { No n = new No(s, r); nos.put(n.nome, n); }
    try {
      dorme(8000);
      No s0r1 = nos.get("S0R1"), s0r0 = nos.get("S0R0"), s2r0 = nos.get("S2R0"), s2r1 = nos.get("S2R1");

      s0r1.cmd("lideres"); dorme(500);
      check("tabela de lideres de S0R1 enxerga S1 e S2 com lider", s0r1.contem("S1 [J..R] -> replica 0") && s0r1.contem("S2 [S..Z] -> replica 0"));

      s0r1.cmd("rotear 2 zeca REQ1"); dorme(1500);
      check("S0R1 (standby) roteou REGISTER para S2 com sucesso", s0r1.contem("usuario=zeca -> SUCESSO"));
      check("lider S2R0 aplicou a operacao roteada", s2r0.contem("aplicada com sucesso"));
      s2r1.cmd("estado"); dorme(500);
      check("standby S2R1 recebeu 'zeca' por replicacao sincrona", s2r1.contem("- zeca"));

      s0r0.cmd("rotear 2 zeca REQ1"); dorme(1500);
      check("retry com mesmo idRequisicao e idempotente", s2r0.contem("idempotente repetida detectada: REQ1"));

      s0r1.cmd("rotear 0 ana"); dorme(1500);
      check("roteamento no proprio shard a partir de standby (via lider)", s0r1.contem("usuario=ana -> SUCESSO"));

      nos.get("S2R0").p.destroy(); dorme(7000);
      s0r0.cmd("rotear 2 zeca2"); dorme(3500);
      check("apos queda do lider de S2, operacao vai ao novo lider S2R1", s0r0.contem("usuario=zeca2 -> SUCESSO"));

      nos.get("S2R1").p.destroy(); dorme(6000);
      s0r0.cmd("rotear 2 zeca3"); dorme(4500);
      check("shard S2 inteiro fora: erro 'indisponivel'", s0r0.contem("usuario=zeca3 -> FALHA") && s0r0.contem("indisponivel"));
      s0r0.cmd("rotear 1 joao"); dorme(2000);
      check("falha parcial: S1 continua operando", s0r0.contem("usuario=joao -> SUCESSO"));
      s0r0.cmd("lideres"); dorme(500);
      check("tabela marca S2 sem lider", s0r0.contem("S2 [S..Z] -> SEM LIDER"));
    } finally {
      for (No n : nos.values()) n.p.destroy();
      if (falhas > 0) {
        for (No n : nos.values()) {
          System.out.println("\n===== LOG " + n.nome + " =====\n" + n.out);
        }
      }
    }
    System.out.println(falhas == 0 ? "\nTODOS OS TESTES PASSARAM" : "\nFALHAS: " + falhas);
    System.exit(falhas == 0 ? 0 : 1);
  }
}

