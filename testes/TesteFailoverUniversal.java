

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import Protocol.APDU;
import model.ClienteTCP;
import model.ClienteUDP;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.MessageListener;
import utils.InfoUser;

public class TesteFailoverUniversal implements MessageListener {

  private final CountDownLatch latchPerdida = new CountDownLatch(1);
  private final CountDownLatch latchReconectado = new CountDownLatch(1);
  private volatile String novoServidor = null;
  private final AtomicBoolean desconectou = new AtomicBoolean(false);

  @Override
  public void onConexaoPerdida() {
    System.out.println("[TESTE] >>> Callback onConexaoPerdida() disparado com sucesso! <<<");
    desconectou.set(true);
    latchPerdida.countDown();
  }

  @Override
  public void onReconectado(String novoIp, int novaPortaTcp, int novaPortaUdp, int idShard) {
    this.novoServidor = novoIp + ":" + novaPortaTcp + " (Shard S" + idShard + ")";
    System.out.println("[TESTE] >>> Callback onReconectado() disparado para " + novoServidor + "! <<<");
    latchReconectado.countDown();
  }

  @Override
  public void onMessageReceived(String idMensagem, String destino, InfoUser remetente, String mensagem, boolean isPrivate, boolean isVisualizacaoUnica) {}

  @Override
  public void onTickReceived(String idMensagem, int status, String nomeConfirmou) {}

  @Override
  public void onUpdateUsers() {}

  @Override
  public void onShutdown() {}

  public static void main(String[] args) throws Exception {
    System.out.println("=== TESTE DE FAILOVER UNIVERSAL DO CLIENTE (SEM DEPENDENCIA DE PING) ===");

    TesteFailoverUniversal listener = new TesteFailoverUniversal();
    Process p1 = null;
    Process p2 = null;

    try {
      System.out.println("1. Subindo Servidor 1 (ID 1: S0, TCP=5001, Interoperavel=6789)...");
      ProcessBuilder pb1 = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local", "--id=1");
      pb1.redirectErrorStream(true);
      p1 = pb1.start();
      Thread.sleep(2500);

      System.out.println("2. Subindo Servidor 2 (ID 2: S1, TCP=5002, Interoperavel=6790)...");
      ProcessBuilder pb2 = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", "--local", "--id=2");
      pb2.redirectErrorStream(true);
      p2 = pb2.start();
      Thread.sleep(2500);

      System.out.println("3. Conectando Cliente na porta de entrada do Servidor 1 (5001)...");
      ClienteTCP tcp = new ClienteTCP("127.0.0.1", 5001);
      ClienteUDP udp = new ClienteUDP("127.0.0.1", 6001);
      udp.setMeuNome("AgenteUniversal");
      udp.setListener(listener);

      InfoUser eu = new InfoUser("AgenteUniversal", tcp.getIpLocal(), udp.getPortaLocal());
      APDU reg = tcp.register(eu);
      System.out.println("   REGISTER inicial: " + reg.getOperacao() + " -> " + reg.getTextoMensagem());

      APDU join = tcp.join("GERAL", eu);
      System.out.println("   JOIN inicial: " + join.getOperacao());

      GerenciadorConexao gc = new GerenciadorConexao(null, tcp, udp, eu, listener, 0);
      gc.adicionarServidor("127.0.0.1", 5001, 6001, 0);
      gc.adicionarServidor("127.0.0.1", 5002, 6002, 1);
      gc.iniciarMonitoramento();
      System.out.println("   Monitoramento resiliente ativado.");

      System.out.println("\n4. Derrubando abruptamente o Servidor 1 (Porta de Entrada inicial)...");
      p1.destroyForcibly();
      p1.waitFor();

      System.out.println("   Aguardando deteccao de queda e reconexao automatica via REGISTER...");
      boolean perdeu = listener.latchPerdida.await(5, TimeUnit.SECONDS);
      if (!perdeu) {
        throw new RuntimeException("TIMEOUT: Nao detectou queda de conexao!");
      }

      boolean reconectou = listener.latchReconectado.await(10, TimeUnit.SECONDS);
      if (!reconectou) {
        throw new RuntimeException("TIMEOUT: Nao reconectou na nova porta de entrada!");
      }

      System.out.println("\n5. Validando se a sessao continua ativa na nova porta de entrada...");
      APDU listUsers = tcp.listUsers();
      System.out.println("   USERS apos failover: " + listUsers.getTextoMensagem());

      if (listUsers.getTextoMensagem() != null && listUsers.getTextoMensagem().contains("AgenteUniversal")) {
        System.out.println("\n✅ SUCESSO TOTAL: O cliente migrou de porta de entrada e se re-registrou sem depender de PING!");
      } else {
        System.out.println("\n⚠️ Reconectou com sucesso, mas o usuario nao consta na lista: " + listUsers.getTextoMensagem());
      }

    } finally {
      if (p1 != null && p1.isAlive()) p1.destroyForcibly();
      if (p2 != null && p2.isAlive()) p2.destroyForcibly();
    }
  }
}
