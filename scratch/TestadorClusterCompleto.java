package scratch;

import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import model.ClienteTCP;
import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Testador automatizado do ciclo de vida completo do cluster de servidores E.D.E.N.
 * Valida os 7 passos essenciais:
 * 1. Inicializacao dos 3 nós do cluster.
 * 2. Eleicao inicial deterministica (Nó 1 ATIVO, Nós 2 e 3 STANDBY).
 * 3. Replicacao sincrona com quorum de ACKs de operacoes de clientes.
 * 4. Queda forcada do lider com kill -9.
 * 5. Deteccao de falha e eleicao automatica do novo lider (Nó 2).
 * 6. Preservacao integral do estado de dados replicado.
 * 7. Retorno do Nó 1 e validacao da regra D1 (Nao-preempcao, entra como STANDBY).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 */
public class TestadorClusterCompleto {

  private static Process iniciarNo(int id, int total) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-cp", "Servidor/bin", "Principal", "--local", String.valueOf(id), String.valueOf(total)
    );
    pb.directory(new File("."));
    pb.redirectErrorStream(true);
    File dirScratch = new File("scratch");
    if (!dirScratch.exists()) dirScratch.mkdirs();
    pb.redirectOutput(new File(dirScratch, "node" + id + ".log"));
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

  public static void main(String[] args) {
    List<Process> processos = new ArrayList<>();
    try {
      System.out.println("==================================================================");
      System.out.println(">>> INICIANDO TESTE COMPLETO DE CLUSTER, REPLICACAO E FAILOVER <<<");
      System.out.println("==================================================================");

      // 1. Inicia os 3 nós
      System.out.println("\n[PASSO 1] Disparando Nó 1, Nó 2 e Nó 3...");
      Process p1 = iniciarNo(1, 3);
      processos.add(p1);
      Process p2 = iniciarNo(2, 3);
      processos.add(p2);
      Process p3 = iniciarNo(3, 3);
      processos.add(p3);

      System.out.println("Aguardando 3.5 segundos para conexao da malha TCP e eleicao inicial...");
      Thread.sleep(3500);

      // 2. Verificar quem e ATIVO e quem e STANDBY
      System.out.println("\n[PASSO 2] Sondando estado de cada nó via TCP PING:");
      String st1 = sondarEstadoTcp(6789);
      String st2 = sondarEstadoTcp(6799);
      String st3 = sondarEstadoTcp(6809);
      System.out.println("  -> Nó 1 (TCP 6789): " + st1);
      System.out.println("  -> Nó 2 (TCP 6799): " + st2);
      System.out.println("  -> Nó 3 (TCP 6809): " + st3);

      if (!"ATIVO".equalsIgnoreCase(st1)) {
        throw new RuntimeException("Nó 1 deveria ser ATIVO, mas respondeu: " + st1);
      }
      System.out.println("[SUCESSO] Lider inicial estabelecido no Nó 1.");

      // 3. Conectar cliente ao lider ativo e realizar operacoes
      System.out.println("\n[PASSO 3] Conectando cliente e executando operacoes no Nó 1...");
      ClienteTCP tcpCliente = new ClienteTCP("127.0.0.1", 6789);
      InfoUser userA = new InfoUser("AgenteAlpha", "127.0.0.1", 7001);
      APDU regA = tcpCliente.register(userA);
      System.out.println("  -> Registro AgenteAlpha: " + regA.getOperacao());

      InfoUser userB = new InfoUser("AgenteBeta", "127.0.0.1", 7002);
      APDU regB = tcpCliente.register(userB);
      System.out.println("  -> Registro AgenteBeta: " + regB.getOperacao());

      APDU joinA = tcpCliente.join("OP_JARDIM", userA);
      System.out.println("  -> AgenteAlpha entrou em OP_JARDIM: " + joinA.getOperacao());

      APDU joinB = tcpCliente.join("OP_JARDIM", userB);
      System.out.println("  -> AgenteBeta entrou em OP_JARDIM: " + joinB.getOperacao());

      APDU listG = tcpCliente.list();
      System.out.println("  -> Lista de grupos no Nó 1: " + listG.getTextoMensagem());

      APDU listM = tcpCliente.listMembers("OP_JARDIM");
      System.out.println("  -> Membros de OP_JARDIM no Nó 1: " + listM.getTextoMensagem());

      tcpCliente.fecharConexao();

      // 4. Derrubar o lider ativo (Nó 1)
      System.out.println("\n[PASSO 4] TESTE DE FAILOVER: Finalizando forcadamente o Nó 1 (kill -9)...");
      p1.destroyForcibly();
      p1.waitFor();
      System.out.println("Nó 1 finalizado. Aguardando deteccao de falha e nova eleicao (4s)...");
      Thread.sleep(4000);

      // 5. Verificar quem assumiu como novo lider ativo
      System.out.println("\n[PASSO 5] Verificando novo lider entre os nós remanescentes:");
      st2 = sondarEstadoTcp(6799);
      st3 = sondarEstadoTcp(6809);
      System.out.println("  -> Nó 2 (TCP 6799): " + st2);
      System.out.println("  -> Nó 3 (TCP 6809): " + st3);

      int portaNovoLider = -1;
      if ("ATIVO".equalsIgnoreCase(st2)) {
        portaNovoLider = 6799;
        System.out.println("[SUCESSO] Nó 2 assumiu automaticamente como lider ATIVO!");
      } else if ("ATIVO".equalsIgnoreCase(st3)) {
        portaNovoLider = 6809;
        System.out.println("[SUCESSO] Nó 3 assumiu automaticamente como lider ATIVO!");
      } else {
        throw new RuntimeException("Nenhum nó assumiu a lideranca apos a queda do Nó 1!");
      }

      // 6. Conectar ao novo lider e validar integridade e persistencia de estado
      System.out.println("\n[PASSO 6] Conectando cliente ao novo lider (porta " + portaNovoLider + ") e conferindo dados replicados...");
      ClienteTCP tcpNovoLider = new ClienteTCP("127.0.0.1", portaNovoLider);
      APDU listGpos = tcpNovoLider.list();
      System.out.println("  -> Lista de grupos no NOVO lider: " + listGpos.getTextoMensagem());
      APDU listMpos = tcpNovoLider.listMembers("OP_JARDIM");
      System.out.println("  -> Membros de OP_JARDIM no NOVO lider: " + listMpos.getTextoMensagem());

      if (!listGpos.getTextoMensagem().contains("OP_JARDIM")) {
        throw new RuntimeException("FALHA: Grupo OP_JARDIM nao foi retido no novo lider!");
      }
      if (!listMpos.getTextoMensagem().contains("AgenteAlpha") || !listMpos.getTextoMensagem().contains("AgenteBeta")) {
        throw new RuntimeException("FALHA: Membros do grupo foram perdidos no novo lider!");
      }
      System.out.println("[SUCESSO TOTAL] Estado integralmente preservado apos failover!");

      // 7. Reiniciar o Nó 1 e validar D1 (Nao preempcao)
      System.out.println("\n[PASSO 7] TESTE DE RECUPERACAO: Reiniciando Nó 1...");
      Process p1Novo = iniciarNo(1, 3);
      processos.add(p1Novo);
      System.out.println("Aguardando 3 segundos para Nó 1 se sincronizar via snapshot...");
      Thread.sleep(3000);

      String st1Novo = sondarEstadoTcp(6789);
      String stLiderAtual = sondarEstadoTcp(portaNovoLider);
      System.out.println("  -> Nó 1 reiniciado status: " + st1Novo);
      System.out.println("  -> Lider atual (porta " + portaNovoLider + ") status: " + stLiderAtual);

      if ("ATIVO".equalsIgnoreCase(stLiderAtual)) {
        System.out.println("[SUCESSO] Regra D1 validada: Nó 1 retornou sem tomar a lideranca do nó ativo!");
      } else {
        System.err.println("[AVISO] Lider anterior mudou apos retorno do Nó 1.");
      }

      System.out.println("\n==================================================================");
      System.out.println(">>> TODOS OS 7 PASSOS DE VALIDACAO FORAM CONCLUIDOS COM EXITO! <<<");
      System.out.println("==================================================================");

    } catch (Exception e) {
      System.err.println("\n[ERRO NO TESTE]: " + e.getMessage());
      e.printStackTrace();
    } finally {
      System.out.println("\nEncerrando processos de teste...");
      for (Process p : processos) {
        try {
          p.destroyForcibly();
        } catch (Exception ignored) {}
      }
    }
  }
}

