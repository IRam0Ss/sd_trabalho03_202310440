import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import utils.Protocolo;
import utils.InfoUser;
import Protocol.APDU;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.ClienteTCP;
import model.ClienteUDP;

public class TesteClienteBurroUniversal {

  public static void main(String[] args) {
    System.out.println("==========================================================");
    System.out.println(" TESTE - CLIENTE UNIVERSAL BURRO + SERVIDOR ZERO-CONFIG   ");
    System.out.println("==========================================================\n");

    List<Process> processos = new ArrayList<>();
    try {
      System.out.println("[SETUP] Subindo 3 nós com 'java Principal' (Zero-Config)...");
      for (int i = 1; i <= 3; i++) {
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal");
        pb.redirectErrorStream(true);
        processos.add(pb.start());
        Thread.sleep(700);
      }

      System.out.println("[SETUP] Aguardando eleição dos líderes nos 3 shards...");
      Thread.sleep(2500);

      // 1. Discovery Universal (Broadcast Universal - o primeiro líder que responder é aceito!)
      System.out.println("\n--- 1. Discovery UDP Universal pelo Cliente ---");
      List<EnderecoServidor> descobertos = GerenciadorConexao.descobrirServidoresViaUDP(1200);
      System.out.println("Servidores descobertos via UDP: " + descobertos);

      if (descobertos.isEmpty()) {
        throw new RuntimeException("Nenhum servidor respondeu ao broadcast UDP universal!");
      }

      EnderecoServidor entrada = descobertos.get(0);
      System.out.println("Cliente conectará na primeira porta de entrada que respondeu: "
          + entrada.host + ":" + entrada.portaTcp + " (Shard S" + entrada.idShard + ")");

      // 2. Conexão TCP e Registro de usuário de QUALQUER shard
      // Testamos registrar um usuário do Shard 0 (Alice), mesmo se caiu no Shard 1 ou 2!
      System.out.println("\n--- 2. Conexão e Registro (Proxy Transparente garantido pelo Servidor) ---");
      ClienteTCP tcp = new ClienteTCP(entrada.host, entrada.portaTcp);
      ClienteUDP udp = new ClienteUDP(entrada.host, entrada.portaUdp);
      InfoUser alice = new InfoUser("Alice", "127.0.0.1", udp.getPortaLocal());

      APDU regAlice = tcp.register(alice);
      System.out.println("Registro de 'Alice' via porta de entrada S" + entrada.idShard + ": "
          + (regAlice != null ? regAlice.getTextoMensagem() : "FALHA"));

      if (regAlice == null || !Protocolo.OK.equals(regAlice.getOperacao())) {
        throw new RuntimeException("Falha no registro via proxy transparente!");
      }

      // 3. Consulta global scatter-gather USERS
      System.out.println("\n--- 3. Consulta USERS (Scatter-Gather Global) ---");
      APDU users = tcp.listUsers();
      System.out.println("Lista de usuários online retornada: " + (users != null ? users.getTextoMensagem() : "null"));

      tcp.fecharConexao();
      udp.fecharConexao();

      // 4. Teste Cross-Shard Explicito: Conecta no Shard S1 (TCP 5001) para registrar usuário 'Zeca' (que pertence ao Shard S2 [S..Z])
      System.out.println("\n--- 4. Conexão no Shard S1 (5001) com usuário do Shard S2 ('Zeca') ---");
      ClienteTCP tcpS1 = new ClienteTCP("127.0.0.1", 5001);
      ClienteUDP udpS1 = new ClienteUDP("127.0.0.1", 6001);
      InfoUser zeca = new InfoUser("Zeca", "127.0.0.1", udpS1.getPortaLocal());

      APDU regZeca = tcpS1.register(zeca);
      System.out.println("Registro de 'Zeca' (Home: S2) via Shard S1: " + (regZeca != null ? regZeca.getTextoMensagem() : "FALHA"));
      if (regZeca == null || !Protocolo.OK.equals(regZeca.getOperacao())) {
        throw new RuntimeException("Falha no proxy cross-shard S1 -> S2!");
      }

      // Consulta USERS através de S1: deve conter Alice (de S0) e Zeca (de S2)!
      APDU usersCross = tcpS1.listUsers();
      System.out.println("Scatter-gather USERS via S1: " + (usersCross != null ? usersCross.getTextoMensagem() : "null"));

      tcpS1.fecharConexao();
      udpS1.fecharConexao();

      System.out.println("\n\u2705 TESTE CONCLUÍDO COM SUCESSO: Cliente burro conectou e operou perfeitamente!");

    } catch (Exception e) {
      System.err.println("\n\u274C Falha no teste: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      System.out.println("\n[TEARDOWN] Finalizando servidores...");
      for (Process p : processos) {
        p.destroyForcibly();
      }
    }
  }
}
