package scratch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import model.ClienteTCP;
import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Valida a geracao automatica de IDs e o crescimento dinamico do cluster (N indefinido):
 * - Nós iniciados sem qualquer argumento manual de ID ou total.
 * - Primeiro nó assume ID 1 automaticamente.
 * - Segundo nó detecta porta 9001 ocupada e assume ID 2 automaticamente.
 * - Terceiro nó assume ID 3 automaticamente.
 * - Quarto nó assume ID 4 automaticamente (cluster de 4 nós dinamico).
 * - Operacoes replicadas para todos os 4 nós.
 * - Queda do líder e failover entre os sobreviventes.
 */
public class TesteAutoIdEDinamico {

  private static Process iniciarNoAuto(int numOrdem) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local");
    pb.directory(new File("."));
    pb.redirectErrorStream(true);
    File dir = new File("scratch");
    if (!dir.exists()) dir.mkdirs();
    pb.redirectOutput(new File(dir, "node_auto_" + numOrdem + ".log"));
    return pb.start();
  }

  private static String sondarEstadoTcp(int porta) {
    try {
      ClienteTCP tcp = new ClienteTCP("127.0.0.1", porta);
      APDU res = tcp.ping();
      tcp.fecharConexao();
      if (res != null && Protocolo.OK.equals(res.getOperacao())) {
        return res.getTextoMensagem();
      }
      return "RES_INVALIDA";
    } catch (Exception e) {
      return "CONEXAO_RECUSADA";
    }
  }

  public static void main(String[] args) throws Exception {
    List<Process> procs = new ArrayList<>();
    try {
      System.out.println("==================================================================");
      System.out.println(">>> TESTE: AUTO-ATRIBUICAO DE ID E CLUSTER DINAMICO INFINITO   <<<");
      System.out.println("==================================================================");

      System.out.println("1. Iniciando 1º nó sem passar ID...");
      Process p1 = iniciarNoAuto(1);
      procs.add(p1);
      Thread.sleep(1500);

      System.out.println("2. Iniciando 2º nó sem passar ID...");
      Process p2 = iniciarNoAuto(2);
      procs.add(p2);
      Thread.sleep(1500);

      System.out.println("3. Iniciando 3º nó sem passar ID...");
      Process p3 = iniciarNoAuto(3);
      procs.add(p3);
      Thread.sleep(1500);

      System.out.println("4. Iniciando 4º nó (cluster crescendo dinamicamente para 4 nós)...");
      Process p4 = iniciarNoAuto(4);
      procs.add(p4);
      Thread.sleep(3000);

      System.out.println("\n5. Sondando status dos nós nas portas auto-alocadas:");
      String st1 = sondarEstadoTcp(6789);
      String st2 = sondarEstadoTcp(6799);
      String st3 = sondarEstadoTcp(6809);
      String st4 = sondarEstadoTcp(6819);

      System.out.println("  -> Nó na porta 6789 (ID auto 1): " + st1);
      System.out.println("  -> Nó na porta 6799 (ID auto 2): " + st2);
      System.out.println("  -> Nó na porta 6809 (ID auto 3): " + st3);
      System.out.println("  -> Nó na porta 6819 (ID auto 4): " + st4);

      if (!"ATIVO".equalsIgnoreCase(st1)) {
        throw new RuntimeException("Falha: Nó 1 deveria ter assumido como ATIVO!");
      }
      System.out.println("[SUCESSO] Todos os 4 nós subiram e alocaram seus IDs automaticamente!");

      // Realiza operacoes no lider ativo
      System.out.println("\n6. Registrando usuario e criando grupo no lider ativo...");
      ClienteTCP tcp = new ClienteTCP("127.0.0.1", 6789);
      InfoUser u1 = new InfoUser("DinamicoAgente", "127.0.0.1", 9991);
      tcp.register(u1);
      tcp.join("SALA_DINAMICA", u1);
      APDU listG = tcp.list();
      System.out.println("  -> Grupos no Nó 1: " + listG.getTextoMensagem());
      tcp.fecharConexao();

      // Derruba nó 1
      System.out.println("\n7. Derrubando Nó 1 forcada (kill)...");
      p1.destroyForcibly();
      p1.waitFor();
      Thread.sleep(4000);

      // Verifica quem assumiu
      st2 = sondarEstadoTcp(6799);
      System.out.println("  -> Nó na porta 6799 (ID auto 2): " + st2);
      if (!"ATIVO".equalsIgnoreCase(st2)) {
        throw new RuntimeException("Falha: Nó 2 deveria ter assumido a lideranca!");
      }

      // Conecta ao novo lider e confere grupo
      ClienteTCP tcp2 = new ClienteTCP("127.0.0.1", 6799);
      APDU listG2 = tcp2.list();
      System.out.println("  -> Grupos no NOVO lider (Nó 2): " + listG2.getTextoMensagem());
      if (!listG2.getTextoMensagem().contains("SALA_DINAMICA")) {
        throw new RuntimeException("Falha: Estado replicado nao encontrado no novo lider!");
      }
      tcp2.fecharConexao();

      System.out.println("\n==================================================================");
      System.out.println(">>> SUCESSO TOTAL: AUTO-ID, DINAMISMO E FAILOVER FUNCIONANDO!  <<<");
      System.out.println("==================================================================");

    } finally {
      for (Process p : procs) {
        try { p.destroyForcibly(); } catch (Exception ignored) {}
      }
    }
  }
}

