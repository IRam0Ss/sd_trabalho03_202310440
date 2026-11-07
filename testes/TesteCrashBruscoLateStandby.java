import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Protocol.APDU;
import utils.Protocolo;

public class TesteCrashBruscoLateStandby {
  public static void main(String[] args) throws Exception {
    System.out.println("=== [TESTE EXTRA] Late-Joining Standby (Snapshot Real) + Queda Brusca (kill -9) ===");
    List<TesteValidacaoFailoverIntegral.ProcessoNo> nos = new ArrayList<>();
    try {
      // 1. Liga apenas os 3 lideres (IDs 1, 2, 3 -> S0, S1, S2)
      for (int i = 1; i <= 3; i++) {
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal");
        pb.redirectErrorStream(true);
        nos.add(new TesteValidacaoFailoverIntegral.ProcessoNo(i, pb.start()));
        Thread.sleep(2300);
      }
      Thread.sleep(1500);

      try (TesteValidacaoFailoverIntegral.ReceptorUdpCliente udpAna = new TesteValidacaoFailoverIntegral.ReceptorUdpCliente(17201);
           TesteValidacaoFailoverIntegral.ReceptorUdpCliente udpZoro = new TesteValidacaoFailoverIntegral.ReceptorUdpCliente(17202)) {

        // 2. Popula estado ANTES de existir qualquer Standby!
        TesteValidacaoFailoverIntegral.SessaoTcp tcpAna = new TesteValidacaoFailoverIntegral.SessaoTcp(5001);
        TesteValidacaoFailoverIntegral.SessaoTcp tcpZoro = new TesteValidacaoFailoverIntegral.SessaoTcp(5001);
        TesteValidacaoFailoverIntegral.assertCond(
            tcpAna.enviar(new APDU("REGISTER", "GLOBAL", "Ana", null, 17201)).startsWith("OK:"), "Ana REGISTER");
        TesteValidacaoFailoverIntegral.assertCond(
            tcpZoro.enviar(new APDU("REGISTER", "GLOBAL", "Zoro", null, 17202)).startsWith("OK:"), "Zoro REGISTER");
        TesteValidacaoFailoverIntegral.assertCond(
            tcpAna.enviar(new APDU("JOIN", "Amigos", "Ana", null, 17201)).startsWith("OK:"), "Ana JOIN Amigos");
        TesteValidacaoFailoverIntegral.assertCond(
            tcpZoro.enviar(new APDU("JOIN", "Amigos", "Zoro", null, 17202)).startsWith("OK:"), "Zoro JOIN Amigos");

        APDU sendMsg = new APDU(Protocolo.SEND, "Amigos", "Ana", "Mensagem antes do Standby nascer!", 17201);
        String idMsg = sendMsg.getIdMensagem();
        udpAna.enviarUdp(6001, sendMsg);

        long tLim = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tLim && !udpAna.recebeuConfirmStatus(idMsg, 1)) {
          Thread.sleep(50);
        }
        TesteValidacaoFailoverIntegral.assertCond(udpAna.recebeuConfirmStatus(idMsg, 1), "Auto-ACK Status 1");

        // 3. Agora liga o No 4 (Standby tardio de S0): DEVE clonar todo o estado via SNAPSHOT!
        ProcessBuilder pb4 = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal");
        pb4.redirectErrorStream(true);
        TesteValidacaoFailoverIntegral.ProcessoNo no4 = new TesteValidacaoFailoverIntegral.ProcessoNo(4, pb4.start());
        nos.add(no4);

        long tSync = System.currentTimeMillis() + 7000;
        while (System.currentTimeMillis() < tSync && !no4.contemLog("Snapshot importado com sucesso")) {
          Thread.sleep(100);
        }
        TesteValidacaoFailoverIntegral.assertCond(no4.contemLog("Snapshot importado com sucesso"),
            "No 4 tardio nao importou Snapshot do No 1:\n" + no4.obterUltimosLogs(30));
        System.out.println("  [OK] Standby tardio (No 4) sincronizou Snapshot completo em quente!");

        // 4. Mata o No 1 BRUSCAMENTE (equivalente a kill -9 / queda de energia)!
        nos.get(0).matarBrusco();
        tcpAna.close();
        tcpZoro.close();

        long tFailover = System.currentTimeMillis() + 11000;
        while (System.currentTimeMillis() < tFailover && !no4.contemLog("SERVICOS DE ATENDIMENTO ABERTOS")) {
          Thread.sleep(150);
        }
        TesteValidacaoFailoverIntegral.assertCond(no4.contemLog("SERVICOS DE ATENDIMENTO ABERTOS"),
            "No 4 nao assumiu apos kill -9 do No 1:\n" + no4.obterUltimosLogs(30));

        // 5. Verifica notificacao SERVER_CHANGED para Ana (S0) e Zoro (S2 cross-shard)
        long tNotif = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tNotif
            && (!udpAna.recebeuOperacao(Protocolo.SERVER_CHANGED) || !udpZoro.recebeuOperacao(Protocolo.SERVER_CHANGED))) {
          Thread.sleep(80);
        }
        TesteValidacaoFailoverIntegral.assertCond(udpAna.recebeuOperacao(Protocolo.SERVER_CHANGED), "Ana recebeu SERVER_CHANGED");
        TesteValidacaoFailoverIntegral.assertCond(udpZoro.recebeuOperacao(Protocolo.SERVER_CHANGED), "Zoro recebeu SERVER_CHANGED");

        // 6. Zoro envia CONFIRM Status 2 e 3 na porta fallback 6001 para mensagem enviada antes do No 4 sequer existir!
        Thread.sleep(400);
        udpZoro.enviarUdp(6001, new APDU(Protocolo.CONFIRM, idMsg, 2, "Zoro", "Amigos", "Ana"));
        udpZoro.enviarUdp(6001, new APDU(Protocolo.CONFIRM, idMsg, 3, "Zoro", "Amigos", "Ana"));

        long tConf = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tConf
            && (!udpAna.recebeuConfirmStatus(idMsg, 2) || !udpAna.recebeuConfirmStatus(idMsg, 3))) {
          Thread.sleep(80);
        }
        TesteValidacaoFailoverIntegral.assertCond(udpAna.recebeuConfirmStatus(idMsg, 2), "CONFIRM Status 2 pos kill -9");
        TesteValidacaoFailoverIntegral.assertCond(udpAna.recebeuConfirmStatus(idMsg, 3), "CONFIRM Status 3 pos kill -9");

        // 7. Valida portas TCP de fallback (5000, 5001, 6789)
        for (int p : Arrays.asList(5000, 5001, 6789)) {
          try (TesteValidacaoFailoverIntegral.SessaoTcp cli = new TesteValidacaoFailoverIntegral.SessaoTcp(p)) {
            String r = cli.enviar(new APDU("MEMBERS", "Amigos", "Probe", null, 17299));
            TesteValidacaoFailoverIntegral.assertCond(r.toLowerCase().contains("ana") && r.toLowerCase().contains("zoro"),
                "MEMBERS intacto na porta fallback " + p + ": " + r);
          }
        }
        System.out.println("  [OK] Late-Joining Standby + Kill -9: Zero perda de estado, CONFIRM 2/3 entregues e portas fallback ativas!");
      }
    } finally {
      for (TesteValidacaoFailoverIntegral.ProcessoNo p : nos) {
        p.matarBrusco();
      }
    }
  }
}
