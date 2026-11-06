package scratch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import Protocol.APDU;
import model.ClienteTCP;
import model.ClienteUDP;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.MessageListener;
import utils.InfoUser;

/**
 * Teste automatizado do failover transparente do cliente E.D.E.N.
 * Valida se ao derrubar o servidor ativo:
 * 1. O GerenciadorConexao detecta a desconexao e dispara onConexaoPerdida().
 * 2. O cliente comuta para o novo lider eleito sem reiniciar a aplicacao.
 * 3. O callback onReconectado() e disparado.
 * 4. Operacoes posteriores continuam funcionando com o novo lider.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 */
public class TesteFailoverCliente implements MessageListener {

  private final CountDownLatch latchPerdida = new CountDownLatch(1);
  private final CountDownLatch latchReconectado = new CountDownLatch(1);
  private volatile String novoServidor = null;

  @Override
  public void onMessageReceived(String idMensagem, String destino, InfoUser remetente, String mensagem, boolean isPrivate, boolean isVisualizacaoUnica) {}

  @Override
  public void onTickReceived(String idMensagem, int status, String nomeConfirmou) {}

  @Override
  public void onUpdateUsers() {}

  @Override
  public void onShutdown() {}

  @Override
  public void onConexaoPerdida() {
    System.out.println("[TESTE-LISTENER] >>> Callback onConexaoPerdida() disparado com sucesso! <<<");
    latchPerdida.countDown();
  }

  @Override
  public void onReconectado(String novoIp, int novaPortaTcp, int novaPortaUdp) {
    this.novoServidor = novoIp + ":" + novaPortaTcp;
    System.out.println("[TESTE-LISTENER] >>> Callback onReconectado() disparado para " + novoServidor + "! <<<");
    latchReconectado.countDown();
  }

  @Override
  public void onServerChanged(String novoIp, int novaPortaTcp, int novaPortaUdp) {
    System.out.println("[TESTE-LISTENER] >>> Callback onServerChanged() para " + novoIp + ":" + novaPortaTcp + " <<<");
  }

  private static Process iniciarNo(int id, int total) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-cp", "Servidor/bin", "Principal", "--local", String.valueOf(id), String.valueOf(total)
    );
    pb.directory(new File("."));
    pb.redirectErrorStream(true);
    File dirScratch = new File("scratch");
    if (!dirScratch.exists()) dirScratch.mkdirs();
    pb.redirectOutput(new File(dirScratch, "node_cli_" + id + ".log"));
    return pb.start();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("==================================================================");
    System.out.println(">>> TESTE AUTOMATIZADO DE FAILOVER TRANSPARENTE DO CLIENTE     <<<");
    System.out.println("==================================================================");

    TesteFailoverCliente listener = new TesteFailoverCliente();
    List<Process> procs = new ArrayList<>();

    try {
      System.out.println("1. Iniciando 3 nós do cluster...");
      Process p1 = iniciarNo(1, 3);
      procs.add(p1);
      Process p2 = iniciarNo(2, 3);
      procs.add(p2);
      Process p3 = iniciarNo(3, 3);
      procs.add(p3);

      Thread.sleep(3500);

      System.out.println("2. Conectando cliente inicial ao lider (Nó 1 em 6789)...");
      ClienteTCP tcp = new ClienteTCP("127.0.0.1", 6789);
      ClienteUDP udp = new ClienteUDP("127.0.0.1", 7777);
      udp.setMeuNome("AgenteTeste");
      udp.setListener(listener);

      InfoUser eu = new InfoUser("AgenteTeste", tcp.getIpLocal(), udp.getPortaLocal());
      APDU reg = tcp.register(eu);
      System.out.println("   Registro inicial do cliente: " + reg.getOperacao());

      APDU join = tcp.join("SALA_SECRETA", eu);
      System.out.println("   Join inicial em SALA_SECRETA: " + join.getOperacao());

      List<EnderecoServidor> lista = Arrays.asList(
          new EnderecoServidor("127.0.0.1", 6789, 7777),
          new EnderecoServidor("127.0.0.1", 6799, 7787),
          new EnderecoServidor("127.0.0.1", 6809, 7797)
      );

      GerenciadorConexao gc = new GerenciadorConexao(lista, tcp, udp, eu, listener);
      gc.iniciarMonitoramento();
      System.out.println("   Monitoramento PING ativado no cliente.");

      System.out.println("\n3. Derrubando abruptamente o Nó 1 (kill -9)...");
      p1.destroyForcibly();
      p1.waitFor();

      System.out.println("   Aguardando GerenciadorConexao detectar queda e comutar para o novo lider...");
      boolean perdeu = listener.latchPerdida.await(6, TimeUnit.SECONDS);
      if (!perdeu) {
        throw new RuntimeException("TIMEOUT: GerenciadorConexao nao disparou onConexaoPerdida!");
      }

      boolean reconectou = listener.latchReconectado.await(12, TimeUnit.SECONDS);
      if (!reconectou) {
        throw new RuntimeException("TIMEOUT: GerenciadorConexao nao disparou onReconectado!");
      }

      System.out.println("\n4. Validando se operacoes continuam funcionando imediatamente no novo lider...");
      APDU listGrupos = tcp.list();
      System.out.println("   Lista de grupos apos failover: " + listGrupos.getTextoMensagem());

      APDU listMembros = tcp.listMembers("SALA_SECRETA");
      System.out.println("   Membros de SALA_SECRETA apos failover: " + listMembros.getTextoMensagem());

      if (!listGrupos.getTextoMensagem().contains("SALA_SECRETA")) {
        throw new RuntimeException("FALHA: SALA_SECRETA nao encontrada no novo lider!");
      }

      gc.parar();
      tcp.fecharConexao();
      udp.fecharConexao();

      System.out.println("\n==================================================================");
      System.out.println(">>> SUCESSO: CLIENTE REALIZOU FAILOVER 100% TRANSPARENTE!     <<<");
      System.out.println("==================================================================");

    } finally {
      for (Process p : procs) {
        try { p.destroyForcibly(); } catch (Exception ignored) {}
      }
    }
  }
}

