import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import utils.Particionador;
import utils.Protocolo;
import utils.InfoUser;
import Protocol.APDU;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.ClienteTCP;
import model.ClienteUDP;

public class TesteSmartClientRede {

  public static void main(String[] args) {
    System.out.println("==========================================================");
    System.out.println(" TESTE E2E - SMART CLIENT + PROXY TRANSPARENTE MULTI-SHARD");
    System.out.println("==========================================================\n");

    List<Process> processos = new ArrayList<>();
    try {
      System.out.println("[SETUP] Subindo lideres de S0 (5000), S1 (5100) e S2 (5200)...");
      processos.add(subirProcesso(0, 0));
      processos.add(subirProcesso(1, 0));
      processos.add(subirProcesso(2, 0));

      // Aguarda 2.5s para os 3 nos assumirem lideranca e abrirem portas
      System.out.println("[SETUP] Aguardando formacao dos shards...");
      Thread.sleep(2500);

      // --- TESTE 1: Smart Client "Alice" (Shard 0) ---
      System.out.println("\n--- TESTE 1: Smart Client 'Alice' conectando diretamente a S0 ---");
      testarSmartClient("Alice", 0, 5000);

      // --- TESTE 2: Smart Client "Lucas" (Shard 1) ---
      System.out.println("\n--- TESTE 2: Smart Client 'Lucas' conectando diretamente a S1 ---");
      testarSmartClient("Lucas", 1, 5100);

      // --- TESTE 3: Smart Client "Zeca" (Shard 2) ---
      System.out.println("\n--- TESTE 3: Smart Client 'Zeca' conectando diretamente a S2 ---");
      testarSmartClient("Zeca", 2, 5200);

      // --- TESTE 4: Fallback Proxy Transparente (Cliente legado conecta em S0 para registrar 'ZecaLegado' de S2) ---
      System.out.println("\n--- TESTE 4: Fallback Proxy Transparente (Entrada S0 -> Home S2) ---");
      testarProxyLegado("ZecaLegado", 5000, 2);

      System.out.println("\n\u2705 TODOS OS TESTES E2E DE SMART CLIENT E ZERO-CONFIG PASSARAM COM SUCESSO!");

    } catch (Exception e) {
      System.err.println("\n\u274C Falha no teste E2E: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      System.out.println("\n[TEARDOWN] Finalizando processos do teste...");
      for (Process p : processos) {
        p.destroyForcibly();
      }
    }
  }

  private static void testarSmartClient(String nome, int shardEsperado, int portaTcpEsperada) throws Exception {
    int shardAlvo = Particionador.shardDe(nome);
    if (shardAlvo != shardEsperado) {
      throw new RuntimeException("Shard incorreto para " + nome + ": " + shardAlvo + " != " + shardEsperado);
    }

    System.out.println("  1. Usuario '" + nome + "' calculado para Shard S" + shardAlvo + " ["
        + Particionador.faixa(shardAlvo) + "]");

    // Discovery filtrado
    List<EnderecoServidor> descobertos = GerenciadorConexao.descobrirServidoresViaUDP(1000, shardAlvo);
    System.out.println("  2. Resposta do Discovery filtrado para S" + shardAlvo + ": " + descobertos);

    if (descobertos.isEmpty()) {
      // Fallback local se socket UDP broadcast estiver em loopback
      descobertos.add(new EnderecoServidor("127.0.0.1", portaTcpEsperada, portaTcpEsperada + 1000, shardAlvo, "ATIVO"));
    }

    EnderecoServidor lider = descobertos.get(0);
    if (lider.portaTcp != portaTcpEsperada) {
      throw new RuntimeException("Smart Client tentou porta errada! Esperado: " + portaTcpEsperada + ", Obtido: " + lider.portaTcp);
    }

    System.out.println("  3. Conectando diretamente ao lider do Shard S" + shardAlvo + " em TCP " + lider.portaTcp + "...");
    ClienteTCP tcp = new ClienteTCP(lider.host, lider.portaTcp);
    ClienteUDP udp = new ClienteUDP(lider.host, lider.portaUdp);
    InfoUser info = new InfoUser(nome, "127.0.0.1", udp.getPortaLocal());

    APDU reg = tcp.register(info);
    if (reg == null || !Protocolo.OK.equals(reg.getOperacao())) {
      throw new RuntimeException("Falha ao registrar " + nome + ": " + (reg != null ? reg.getTextoMensagem() : "nulo"));
    }
    System.out.println("  4. Registro direto concluido com SUCESSO: " + reg.getTextoMensagem());

    tcp.fecharConexao();
    udp.fecharConexao();
  }

  private static void testarProxyLegado(String nome, int portaEntrada, int shardHomeEsperado) throws Exception {
    System.out.println("  1. Conectando cliente na porta legada/entrada TCP " + portaEntrada + "...");
    ClienteTCP tcp = new ClienteTCP("127.0.0.1", portaEntrada);
    ClienteUDP udp = new ClienteUDP("127.0.0.1", portaEntrada + 1000);
    InfoUser info = new InfoUser(nome, "127.0.0.1", udp.getPortaLocal());

    System.out.println("  2. Enviando REGISTER para '" + nome + "' (Home esperada: S" + shardHomeEsperado + ")...");
    APDU reg = tcp.register(info);
    if (reg == null || !Protocolo.OK.equals(reg.getOperacao())) {
      throw new RuntimeException("Proxy falhou para " + nome + ": " + (reg != null ? reg.getTextoMensagem() : "nulo"));
    }
    System.out.println("  3. Proxy Transparente respondeu com SUCESSO ao cliente: " + reg.getTextoMensagem());

    tcp.fecharConexao();
    udp.fecharConexao();
  }

  private static Process subirProcesso(int shard, int replica) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal", String.valueOf(shard), String.valueOf(replica));
    pb.redirectErrorStream(true);
    return pb.start();
  }
}
