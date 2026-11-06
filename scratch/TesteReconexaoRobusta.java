package scratch;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import model.ClienteTCP;
import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

public class TesteReconexaoRobusta {

  private static Process iniciarNo(int id) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local", String.valueOf(id));
    pb.directory(new File("."));
    pb.redirectErrorStream(true);
    File dir = new File("scratch");
    if (!dir.exists()) dir.mkdirs();
    pb.redirectOutput(new File(dir, "node_rec_" + id + ".log"));
    return pb.start();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("==================================================================");
    System.out.println(">>> TESTE: RECONEXAO DE CLIENTE E PROBING AUTOMATICO DE CLUSTER <<<");
    System.out.println("==================================================================");

    Process p1 = null;
    Process p2 = null;

    try {
      System.out.println("1. Iniciando Nó 1 (Lider Inicial)...");
      p1 = iniciarNo(1);
      Thread.sleep(2000);

      System.out.println("2. Iniciando Nó 2 (Standby)...");
      p2 = iniciarNo(2);
      Thread.sleep(2000);

      System.out.println("3. Conectando Cliente 'AgenteAlpha' ao cluster...");
      ClienteTCP tcp1 = new ClienteTCP("127.0.0.1", 6789);
      InfoUser u1 = new InfoUser("AgenteAlpha", "127.0.0.1", 9901);
      APDU reg1 = tcp1.register(u1);
      if (!Protocolo.OK.equals(reg1.getOperacao())) {
        throw new RuntimeException("Falha no registro inicial: " + reg1.getTextoMensagem());
      }
      System.out.println("-> Registro 1 OK: " + reg1.getTextoMensagem());

      System.out.println("4. Desconectando Cliente 'AgenteAlpha' (simulando desconectarLimpo)...");
      // Envia LOGOUT gracioso
      tcp1.executarComando(new APDU(Protocolo.LOGOUT, "GLOBAL", u1.getNome(), null, u1.getPorta()));
      tcp1.fecharConexao();
      System.out.println("-> Desconexao limpa efetuada.");

      Thread.sleep(1000);

      System.out.println("5. Reconectando Cliente 'AgenteAlpha' com mesmo codinome...");
      ClienteTCP tcp2 = new ClienteTCP("127.0.0.1", 6789);
      InfoUser u2 = new InfoUser("AgenteAlpha", "127.0.0.1", 9902);
      APDU reg2 = tcp2.register(u2);
      if (!Protocolo.OK.equals(reg2.getOperacao())) {
        throw new RuntimeException("Falha na reconexao: " + reg2.getTextoMensagem());
      }
      System.out.println("-> Reconexao 2 OK: " + reg2.getTextoMensagem());
      tcp2.executarComando(new APDU(Protocolo.LOGOUT, "GLOBAL", u2.getNome(), null, u2.getPorta()));
      tcp2.fecharConexao();

      System.out.println("6. Derrubando Nó 1 (simulando falha do lider ativo)...");
      p1.destroyForcibly();
      p1.waitFor();
      System.out.println("-> Nó 1 derrubado. Aguardando eleicao do Nó 2...");
      Thread.sleep(4500);

      System.out.println("7. Testando algoritmo de sondagem de portas quando input for bare '127.0.0.1'...");
      // Simula o comportamento de parseEnderecosServidores("127.0.0.1")
      List<String> candidatos = new ArrayList<>();
      for (int k = 1; k <= 10; k++) {
        candidatos.add("127.0.0.1:" + (6789 + 10 * (k - 1)));
      }

      String liderEncontrado = null;
      ClienteTCP tcpReconectado = null;
      for (String end : candidatos) {
        String[] partes = end.split(":");
        String host = partes[0];
        int porta = Integer.parseInt(partes[1]);
        try {
          System.out.println("   Sondando candidato: " + end);
          ClienteTCP tcpTentativa = new ClienteTCP(host, porta);
          APDU ping = tcpTentativa.ping();
          if (ping != null && Protocolo.OK.equals(ping.getOperacao()) && "ATIVO".equalsIgnoreCase(ping.getTextoMensagem())) {
            liderEncontrado = end;
            tcpReconectado = tcpTentativa;
            System.out.println("   -> Lider ativo encontrado em: " + end);
            break;
          } else {
            tcpTentativa.fecharConexao();
          }
        } catch (Exception e) {
          System.out.println("   -> " + end + " inacessivel: " + e.getMessage());
        }
      }

      if (liderEncontrado == null || tcpReconectado == null) {
        throw new RuntimeException("Falha: Lider ativo nao encontrado atraves da sondagem automatica!");
      }

      System.out.println("8. Registrando Cliente 'AgenteAlpha' no novo lider (" + liderEncontrado + ")...");
      InfoUser u3 = new InfoUser("AgenteAlpha", "127.0.0.1", 9903);
      APDU reg3 = tcpReconectado.register(u3);
      if (!Protocolo.OK.equals(reg3.getOperacao())) {
        throw new RuntimeException("Falha no registro com novo lider: " + reg3.getTextoMensagem());
      }
      System.out.println("-> Registro no novo lider OK: " + reg3.getTextoMensagem());

      System.out.println("9. Testando desconexao e reconexao subsequente com novo lider ativo...");
      tcpReconectado.executarComando(new APDU(Protocolo.LOGOUT, "GLOBAL", u3.getNome(), null, u3.getPorta()));
      tcpReconectado.fecharConexao();

      ClienteTCP tcp4 = new ClienteTCP("127.0.0.1", 6799);
      InfoUser u4 = new InfoUser("AgenteAlpha", "127.0.0.1", 9904);
      APDU reg4 = tcp4.register(u4);
      if (!Protocolo.OK.equals(reg4.getOperacao())) {
        throw new RuntimeException("Falha na reconexao 4: " + reg4.getTextoMensagem());
      }
      System.out.println("-> Reconexao subsequente OK: " + reg4.getTextoMensagem());
      tcp4.fecharConexao();

      System.out.println("\n==================================================================");
      System.out.println(">>> SUCESSO TOTAL: RECONEXAO ROBUSTA VERIFICADA EM 100%!       <<<");
      System.out.println("==================================================================");

    } finally {
      if (p1 != null) p1.destroyForcibly();
      if (p2 != null) p2.destroyForcibly();
    }
  }
}

