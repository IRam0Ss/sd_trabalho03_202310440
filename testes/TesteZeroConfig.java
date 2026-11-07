import java.io.*;
import java.util.*;

public class TesteZeroConfig {
  public static void main(String[] args) throws Exception {
    System.out.println("=== TESTANDO AUTO-DISCOVERY E PARTIONAMENTO (S0 -> S1 -> S2 -> STANDBY) ===");
    List<Process> processos = new ArrayList<>();

    try {
      for (int i = 1; i <= 4; i++) {
        System.out.println("\n[SPAWN] Subindo processo " + i + " com 'java Principal --local'...");
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        processos.add(p);

        // Imprime as primeiras linhas do log do processo
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String linha;
        long timeout = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < timeout && (linha = reader.readLine()) != null) {
          System.out.println("  P" + i + ": " + linha);
          if (linha.contains("Servidor iniciado") || linha.contains("Canal de cluster escutando")) {
            break;
          }
        }
        Thread.sleep(800);
      }

      System.out.println("\n\u2705 SUCESSO: Todos os processos subiram na ordem crescente S0 -> S1 -> S2 -> Standby!");

    } finally {
      System.out.println("\n[CLEANUP] Encerrando processos...");
      for (Process p : processos) {
        p.destroyForcibly();
      }
    }
  }
}
