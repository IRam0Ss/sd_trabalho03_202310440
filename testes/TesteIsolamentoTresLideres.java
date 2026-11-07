import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import Protocol.APDU;
import model.ClienteTCP;
import utils.Protocolo;

/**
 * Valida o retrabalho da Fase 2:
 * 1) 3 instancias independentes (S0/R0, S1/R0, S2/R0) tornam-se TODAS lideres ATIVAS de seus shards
 *    (nenhuma vira standby de outro shard!) usando a matematica de portas 5000/6000/7000 + shard*100 + replica.
 * 2) Modo interativo (sem argumentos CLI) le idShard e idReplica e aplica a mesma matematica.
 * 3) Porta antiga 5001 do Discovery EDEN esta efetivamente aberta quando S0/R0 assume TCP=5000.
 * 4) Log de inicializacao imprime "[CONFIG] Iniciando Shard: S<x> | Replica: <y> | Portas: TCP=..., UDP=..., Cluster=...".
 */
public class TesteIsolamentoTresLideres {

  static class ProcServidor {
    final String rotulo;
    final Process proc;
    final PrintWriter in;
    final StringBuffer out = new StringBuffer();

    ProcServidor(String rotulo, String... cmd) throws Exception {
      this.rotulo = rotulo;
      this.proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      this.in = new PrintWriter(new OutputStreamWriter(proc.getOutputStream(), StandardCharsets.UTF_8), true);
      Thread leitor = new Thread(() -> {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
          String linha;
          while ((linha = br.readLine()) != null) {
            out.append(linha).append('\n');
          }
        } catch (Exception ignored) {
        }
      });
      leitor.setDaemon(true);
      leitor.start();
    }

    boolean contem(String trecho) {
      return out.toString().contains(trecho);
    }
  }

  static int falhas = 0;

  static void check(String desc, boolean ok) {
    System.out.println((ok ? "[OK]    " : "[FALHA] ") + desc);
    if (!ok) falhas++;
  }

  public static void main(String[] args) throws Exception {
    List<ProcServidor> procs = new ArrayList<>();
    try {
      // S0/R0 via CLI direto
      ProcServidor s0r0 = new ProcServidor("S0R0", "java", "-Dcores=false", "-cp", "Servidor/bin", "Principal", "0", "0");
      procs.add(s0r0);

      // S1/R0 via flag --local
      ProcServidor s1r0 = new ProcServidor("S1R0", "java", "-Dcores=false", "-cp", "Servidor/bin", "Principal", "--local", "1", "0");
      procs.add(s1r0);

      // S2/R0 via modo INTERATIVO (sem argumentos CLI: digita shard=2, replica=0, ENTER p/ LOCAL)
      ProcServidor s2r0 = new ProcServidor("S2R0", "java", "-Dcores=false", "-cp", "Servidor/bin", "Principal");
      procs.add(s2r0);
      s2r0.in.println("2");
      s2r0.in.println("0");
      s2r0.in.println("1");

      Thread.sleep(4500);

      // 1. Validar logs de inicializacao [CONFIG]
      check("Log [CONFIG] exato em S0/R0 (5000/6000/7000)",
          s0r0.contem("[CONFIG] Iniciando Shard: S0 | Replica: 0 | Portas: TCP=5000, UDP=6000, Cluster=7000"));
      check("Log [CONFIG] exato em S1/R0 (5100/6100/7100)",
          s1r0.contem("[CONFIG] Iniciando Shard: S1 | Replica: 0 | Portas: TCP=5100, UDP=6100, Cluster=7100"));
      check("Log [CONFIG] exato em S2/R0 interativo (5200/6200/7200)",
          s2r0.contem("[CONFIG] Iniciando Shard: S2 | Replica: 0 | Portas: TCP=5200, UDP=6200, Cluster=7200"));

      // 2. Validar que OS TRES se tornaram lideres ATIVOS simultaneamente (nenhum virou standby de S0!)
      int[] portasTcp = { 5000, 5100, 5200 };
      for (int s = 0; s < 3; s++) {
        ClienteTCP cli = new ClienteTCP("127.0.0.1", portasTcp[s]);
        APDU resp = cli.ping();
        cli.fecharConexao();
        check("Shard S" + s + " respondeu PING=ATIVO na porta TCP " + portasTcp[s],
            resp != null && Protocolo.OK.equals(resp.getOperacao()) && "ATIVO".equalsIgnoreCase(resp.getTextoMensagem()));
      }

      // 3. Validar que a porta antiga 5001 (Discovery EDEN) esta aberta quando S0/R0 usa TCP 5000
      boolean respondeu5001 = false;
      try (DatagramSocket ds = new DatagramSocket()) {
        ds.setSoTimeout(1200);
        byte[] req = "DISCOVER_EDEN".getBytes(StandardCharsets.UTF_8);
        ds.send(new DatagramPacket(req, req.length, InetAddress.getByName("127.0.0.1"), 5001));
        byte[] buf = new byte[128];
        DatagramPacket pkt = new DatagramPacket(buf, buf.length);
        ds.receive(pkt);
        String txt = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8).trim();
        respondeu5001 = "EDEN_HERE".equals(txt);
      }
      check("Discovery legado na porta UDP 5001 respondeu 'EDEN_HERE' com S0/R0 em TCP=5000", respondeu5001);

      // 4. Iniciar S0/R1 como standby de S0 e garantir que entra em STANDBY apenas em S0
      ProcServidor s0r1 = new ProcServidor("S0R1", "java", "-Dcores=false", "-cp", "Servidor/bin", "Principal", "0", "1");
      procs.add(s0r1);
      Thread.sleep(3000);
      check("Log [CONFIG] exato em S0/R1 (5001/6001/7001)",
          s0r1.contem("[CONFIG] Iniciando Shard: S0 | Replica: 1 | Portas: TCP=5001, UDP=6001, Cluster=7001"));
      check("S0/R1 entrou como STANDBY do Shard S0", s0r1.contem("[SHARD-0 | REPLICA-1 | STANDBY]"));

      // S1/R0 e S2/R0 continuam ATIVOS e sem interferencia
      s1r0.in.println("status");
      s2r0.in.println("status");
      Thread.sleep(500);
      check("S1/R0 permanece ATIVO e isolado", s1r0.contem("No Local: Shard S1 | Replica 0") && !s1r0.contem("7001"));
      check("S2/R0 permanece ATIVO e isolado", s2r0.contem("No Local: Shard S2 | Replica 0") && !s2r0.contem("7001"));

    } finally {
      for (ProcServidor p : procs) {
        p.proc.destroy();
      }
      if (falhas > 0) {
        for (ProcServidor p : procs) {
          System.out.println("\n=== LOG " + p.rotulo + " ===\n" + p.out);
        }
      }
    }

    System.out.println(falhas == 0 ? "\nTODOS OS TESTES DE ISOLAMENTO E PORTAS PASSARAM!" : "\nFALHAS: " + falhas);
    System.exit(falhas == 0 ? 0 : 1);
  }
}

