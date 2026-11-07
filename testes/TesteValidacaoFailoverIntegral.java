import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import Protocol.APDU;
import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.RegistroMensagem;
import controller.Resultado;
import utils.InfoUser;
import utils.Protocolo;

public class TesteValidacaoFailoverIntegral {

  static class ProcessoNo {
    final int indiceOrdem;
    final Process proc;
    final PrintWriter stdin;
    final List<String> logs = new CopyOnWriteArrayList<>();

    ProcessoNo(int indiceOrdem, Process proc) {
      this.indiceOrdem = indiceOrdem;
      this.proc = proc;
      this.stdin = new PrintWriter(new OutputStreamWriter(proc.getOutputStream(), StandardCharsets.UTF_8), true);
      Thread t = new Thread(() -> {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
          String linha;
          while ((linha = br.readLine()) != null) {
            logs.add(linha);
          }
        } catch (Exception ignored) {
        }
      }, "Reader-No-" + indiceOrdem);
      t.setDaemon(true);
      t.start();
    }

    boolean contemLog(String trecho) {
      for (String l : logs) {
        if (l.contains(trecho)) return true;
      }
      return false;
    }

    String obterUltimosLogs(int n) {
      StringBuilder sb = new StringBuilder();
      int ini = Math.max(0, logs.size() - n);
      for (int i = ini; i < logs.size(); i++) {
        sb.append(logs.get(i)).append("\n");
      }
      return sb.toString();
    }

    void encerrarGracioso() {
      try {
        stdin.println("sair");
        stdin.flush();
        if (!proc.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) {
          proc.destroyForcibly();
        }
      } catch (Exception ignored) {
        proc.destroyForcibly();
      }
    }

    void matarBrusco() {
      proc.destroyForcibly();
      try {
        proc.waitFor(2, java.util.concurrent.TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }
    }
  }

  static class ReceptorUdpCliente implements AutoCloseable {
    final DatagramSocket sock;
    final int porta;
    final List<APDU> recebidos = new CopyOnWriteArrayList<>();
    volatile boolean ativo = true;

    ReceptorUdpCliente(int porta) throws Exception {
      this.porta = porta;
      this.sock = new DatagramSocket(null);
      this.sock.setReuseAddress(true);
      this.sock.bind(new InetSocketAddress("127.0.0.1", porta));
      Thread t = new Thread(() -> {
        byte[] buf = new byte[4096];
        while (ativo && !sock.isClosed()) {
          try {
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            sock.receive(p);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(p.getData(), 0, p.getLength()));
            Object obj = ois.readObject();
            if (obj instanceof APDU) {
              recebidos.add((APDU) obj);
            }
          } catch (Exception e) {
            if (!ativo) break;
          }
        }
      }, "UDP-Client-" + porta);
      t.setDaemon(true);
      t.start();
    }

    void enviarUdp(int portaServidor, APDU apdu) throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeUnshared(apdu);
      oos.flush();
      byte[] dados = baos.toByteArray();
      sock.send(new DatagramPacket(dados, dados.length, InetAddress.getByName("127.0.0.1"), portaServidor));
    }

    boolean recebeuOperacao(String op) {
      for (APDU a : recebidos) {
        if (op.equals(a.getOperacao())) return true;
      }
      return false;
    }

    boolean recebeuConfirmStatus(String idMsg, int status) {
      for (APDU a : recebidos) {
        if (Protocolo.CONFIRM.equals(a.getOperacao())
            && idMsg.equals(a.getIdMensagem())
            && a.getStatusRecebido() == status) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void close() {
      ativo = false;
      sock.close();
    }
  }

  static class SessaoTcp implements AutoCloseable {
    final Socket s;
    final ObjectOutputStream oos;
    final ObjectInputStream ois;

    SessaoTcp(int porta) throws Exception {
      this.s = new Socket();
      this.s.connect(new InetSocketAddress("127.0.0.1", porta), 2000);
      this.s.setSoTimeout(4000);
      this.oos = new ObjectOutputStream(s.getOutputStream());
      this.oos.flush();
      this.ois = new ObjectInputStream(s.getInputStream());
    }

    String enviar(APDU apdu) throws Exception {
      oos.writeUnshared(apdu);
      oos.reset();
      oos.flush();
      Object r = ois.readObject();
      return String.valueOf(r);
    }

    @Override
    public void close() {
      try {
        s.close();
      } catch (Exception ignored) {
      }
    }
  }

  static void assertCond(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError("FALHA: " + msg);
    }
  }

  static void testarSnapshotProfundoUnitario() throws Exception {
    System.out.println("=== [TESTE 1] Snapshot Profundo Unitario (Todos os 6 mapas + APDU CONFIRM + Hash) ===");
    GerenciadorGrupos lider = new GerenciadorGrupos(0);
    lider.aplicar(Operacao.registrarUsuario("req-1", "Ana", "127.0.0.1", 17001));
    lider.aplicar(Operacao.entrarGrupo("req-2", "Amigos", "Ana", "127.0.0.1", 17001));
    // Membro cross-shard (Zoro pertence ao Shard 2, salvo em endpointsMembros do Shard 0!)
    lider.aplicar(Operacao.entrarGrupo("req-3", "Amigos", "Zoro", "127.0.0.1", 17002));
    lider.aplicar(Operacao.bloquear("req-4", "Ana", "Carlos"));

    Set<String> esperados = new HashSet<>(Collections.singletonList("zoro"));
    lider.aplicar(Operacao.registrarMsgGrupo("msg-99", "Ana", "127.0.0.1", 17001, "Amigos", esperados, false));
    lider.aplicar(Operacao.confirmarEntrega("msg-99", "Zoro"));

    byte[] snap = lider.exportarSnapshot();
    assertCond(snap.length > 0, "Snapshot nao pode ser vazio");

    GerenciadorGrupos standby = new GerenciadorGrupos(0);
    standby.aplicarSnapshot(snap);

    assertCond(standby.getVersao() == lider.getVersao(), "Versao divergente pos-snapshot");
    assertCond(standby.calcularHash() == lider.calcularHash(), "Hash CRC32 divergente pos-snapshot");
    assertCond(standby.buscarEndpointConhecido("Zoro") != null
        && standby.buscarEndpointConhecido("Zoro").getPorta() == 17002,
        "endpointsMembros cross-shard perdido no snapshot!");

    RegistroMensagem regRestaurado = standby.getRegistroMensagem("msg-99");
    assertCond(regRestaurado != null && regRestaurado.isEntregueNotificado() && !regRestaurado.isLidoNotificado(),
        "Estado da APDU de Verificacao (CONFIRM) perdido no snapshot!");

    // Confirma leitura no standby pos-snapshot: deve disparar notificarAutorConfirm = true (status 3)!
    Resultado resLeitura = standby.aplicar(Operacao.confirmarLeitura("msg-99", "Zoro"));
    assertCond(resLeitura.isNotificarAutorConfirm() && resLeitura.getConfirmStatus() == 3,
        "APDU CONFIRM status 3 deveria notificar autor no standby promovido!");

    // Valida cache de idempotencia (requisicoesProcessadas) preservado no snapshot: versao nao incrementa!
    long versaoAntesDup = standby.getVersao();
    Resultado resDup = standby.aplicar(Operacao.entrarGrupo("req-3", "Amigos", "Zoro", "127.0.0.1", 17002));
    assertCond(resDup.isSucesso() && standby.getVersao() == versaoAntesDup,
        "Cache de idempotencia requisicoesProcessadas deveria ter sido preservado no snapshot!");

    // Valida destinatarios de notificacao universal (Ana + Zoro, sem perda por InfoUser.equals)
    List<InfoUser> dests = standby.getTodosDestinatariosNotificacao();
    assertCond(dests.size() == 2, "Deveria conter exatamente 2 endpoints (Ana e Zoro), veio: " + dests.size());
    System.out.println("  [OK] Snapshot profundo validado (usuarios, endpointsMembros, grupos, bloqueios, mensagens/CONFIRM, idempotencia)!");
  }

  public static void main(String[] args) throws Exception {
    testarSnapshotProfundoUnitario();

    System.out.println("=== [TESTE 2] Zero-Config 6 Nos (Fim do Efeito Escadinha: 3 ATIVOS + 3 STANDBYS de primeira) ===");
    List<ProcessoNo> nos = new ArrayList<>();
    try {
      for (int i = 1; i <= 6; i++) {
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "Servidor/bin", "Principal");
        pb.redirectErrorStream(true);
        ProcessoNo p = new ProcessoNo(i, pb.start());
        nos.add(p);
        // Pequeno intervalo realista de abertura de terminais pelo usuario
        Thread.sleep(2400);
      }

      // Aguarda convergencia final
      Thread.sleep(2500);

      for (int i = 0; i < 3; i++) {
        ProcessoNo lider = nos.get(i);
        int idEsperado = i + 1;
        assertCond(lider.contemLog("assumiu o ID " + idEsperado) || lider.contemLog("No " + idEsperado + " -> Shard S" + i),
            "Pioneiro " + idEsperado + " nao assumiu ID esperado:\n" + lider.obterUltimosLogs(25));
        assertCond(lider.contemLog("PAPEL: ATIVO"),
            "Pioneiro " + idEsperado + " deveria estar ATIVO:\n" + lider.obterUltimosLogs(25));
      }

      for (int i = 3; i < 6; i++) {
        ProcessoNo backup = nos.get(i);
        int idEsperado = i + 1;
        int shardEsperado = i - 3;
        assertCond(backup.contemLog("No " + idEsperado + " -> Shard S" + shardEsperado),
            "Backup #" + idEsperado + " sofreu efeito escadinha e nao assumiu ID " + idEsperado + ":\n" + backup.obterUltimosLogs(30));
        assertCond(backup.contemLog("STANDBY"),
            "Backup #" + idEsperado + " nao entrou em STANDBY no S" + shardEsperado + ":\n" + backup.obterUltimosLogs(30));
        assertCond(!backup.contemLog("SERVICOS DE ATENDIMENTO ABERTOS"),
            "Backup #" + idEsperado + " abriu servicos ATIVO indevidamente (Split-Brain)!\n" + backup.obterUltimosLogs(30));
      }
      System.out.println("  [OK] Todos os 6 nos convergiram na 1a tentativa: IDs 1,2,3 ATIVOS e IDs 4,5,6 STANDBYS (Zero Escadinha / Zero Split-Brain)!");

      System.out.println("=== [TESTE 3] Failover S0 sem Perda de Dados + APDU CONFIRM + Broadcast SERVER_CHANGED Cross-Shard + Fallback Ports ===");
      try (ReceptorUdpCliente udpAna = new ReceptorUdpCliente(17101);
           ReceptorUdpCliente udpZoro = new ReceptorUdpCliente(17102)) {

        // Conecta Ana e Zoro no Gateway S0 (No 1 -> porta 5001)
        SessaoTcp tcpAna = new SessaoTcp(5001);
        SessaoTcp tcpZoro = new SessaoTcp(5001);
        assertCond(tcpAna.enviar(new APDU("REGISTER", "GLOBAL", "Ana", null, 17101)).startsWith("OK:"), "Ana REGISTER");
        assertCond(tcpZoro.enviar(new APDU("REGISTER", "GLOBAL", "Zoro", null, 17102)).startsWith("OK:"), "Zoro REGISTER");

        // Ambos entram no grupo "Amigos" (Shard 0 -> Zoro fica em endpointsMembros de S0!)
        assertCond(tcpAna.enviar(new APDU("JOIN", "Amigos", "Ana", null, 17101)).startsWith("OK:"), "Ana JOIN Amigos");
        assertCond(tcpZoro.enviar(new APDU("JOIN", "Amigos", "Zoro", null, 17102)).startsWith("OK:"), "Zoro JOIN Amigos");

        // Ana envia SEND UDP para o grupo "Amigos" antes da queda do No 1
        APDU sendMsg = new APDU(Protocolo.SEND, "Amigos", "Ana", "Ola antes do failover!", 17101);
        String idMsgGrupo = sendMsg.getIdMensagem();
        udpAna.enviarUdp(6001, sendMsg);

        long tLim = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tLim && (!udpZoro.recebeuOperacao(Protocolo.SEND) || !udpAna.recebeuConfirmStatus(idMsgGrupo, 1))) {
          Thread.sleep(50);
        }
        assertCond(udpZoro.recebeuOperacao(Protocolo.SEND), "Zoro deveria ter recebido a mensagem UDP no grupo Amigos");
        assertCond(udpAna.recebeuConfirmStatus(idMsgGrupo, 1), "Ana deveria ter recebido Auto-ACK CONFIRM Status 1");

        // Derruba o Lider S0 (No 1) graciosamente para acionar failover imediato para o Standby S0 (No 4)!
        // (Nota: mantemos tcpAna e tcpZoro abertos ate a queda do No 1 para simular clientes reais durante o crash/renuncia)
        nos.get(0).encerrarGracioso();
        tcpAna.close();
        tcpZoro.close();

        // Aguarda o No 4 (Standby S0) assumir como ATIVO
        ProcessoNo novoLiderS0 = nos.get(3);
        long tFailover = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < tFailover && !novoLiderS0.contemLog("SERVICOS DE ATENDIMENTO ABERTOS")) {
          Thread.sleep(100);
        }
        assertCond(novoLiderS0.contemLog("SERVICOS DE ATENDIMENTO ABERTOS"),
            "Standby S0 (No 4) nao assumiu lideranca apos queda do No 1:\n" + novoLiderS0.obterUltimosLogs(30));

        // Aguarda entrega das rajadas de SERVER_CHANGED para Ana (local S0) E Zoro (cross-shard S2 em endpointsMembros!)
        long tNotif = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tNotif
            && (!udpAna.recebeuOperacao(Protocolo.SERVER_CHANGED) || !udpZoro.recebeuOperacao(Protocolo.SERVER_CHANGED))) {
          Thread.sleep(80);
        }
        assertCond(udpAna.recebeuOperacao(Protocolo.SERVER_CHANGED),
            "Ana (usuario local S0) NAO recebeu SERVER_CHANGED no failover!");
        assertCond(udpZoro.recebeuOperacao(Protocolo.SERVER_CHANGED),
            "Zoro (cliente cross-shard S2 em grupo de S0 / endpointsMembros) NAO recebeu SERVER_CHANGED no failover!");
        System.out.println("  [OK] Broadcast universal SERVER_CHANGED entregue tanto a Ana (S0) quanto a Zoro (cross-shard S2)!");

        // Agora Zoro envia CONFIRM Status 2 (entregue) e Status 3 (lido) da mensagem enviada ANTES da queda!
        // E envia na porta PIONEIRA DE FALLBACK 6001 (simulando cliente de terceiro que nem trocou de porta!)
        Thread.sleep(400);
        APDU confirmEntrega = new APDU(Protocolo.CONFIRM, idMsgGrupo, 2, "Zoro", "Amigos", "Ana");
        udpZoro.enviarUdp(6001, confirmEntrega);

        APDU confirmLeitura = new APDU(Protocolo.CONFIRM, idMsgGrupo, 3, "Zoro", "Amigos", "Ana");
        udpZoro.enviarUdp(6001, confirmLeitura);

        long tConfirm = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < tConfirm
            && (!udpAna.recebeuConfirmStatus(idMsgGrupo, 2) || !udpAna.recebeuConfirmStatus(idMsgGrupo, 3))) {
          Thread.sleep(80);
        }
        assertCond(udpAna.recebeuConfirmStatus(idMsgGrupo, 2),
            "Ana NAO recebeu CONFIRM Status 2 pos-failover (estado de verificacao ou porta fallback 6001 falhou)!");
        assertCond(udpAna.recebeuConfirmStatus(idMsgGrupo, 3),
            "Ana NAO recebeu CONFIRM Status 3 pos-failover (estado de verificacao perdido)!");
        System.out.println("  [OK] Estado da APDU de Verificacao (CONFIRM Status 2 e 3) + Porta UDP Fallback 6001 funcionaram perfeitamente pos-failover!");

        // Testa cliente legado/terceiro conectando nas portas de fallback TCP 5000, 5001 e 6789 e consultando MEMBERS de "Amigos"!
        for (int portaFallback : Arrays.asList(5000, 5001, 6789, 5004)) {
          try (SessaoTcp cliFallback = new SessaoTcp(portaFallback)) {
            String respMembers = cliFallback.enviar(new APDU("MEMBERS", "Amigos", "ProbeFallback", null, 17199));
            assertCond(respMembers.toLowerCase().contains("ana") && respMembers.toLowerCase().contains("zoro"),
                "Porta TCP fallback " + portaFallback + " perdeu membros do grupo Amigos: " + respMembers);
          }
        }
        System.out.println("  [OK] Portas TCP de fallback (5000, 5001, 6789) e nova porta (5004) responderam com estado intacto (Ana, Zoro)!");
      }

      System.out.println("\n>>> TODOS OS TESTES DE BLINDAGEM, SNAPSHOT E FAILOVER PASSARAM COM 100% DE SUCESSO! <<<");
    } finally {
      for (ProcessoNo p : nos) {
        p.matarBrusco();
      }
    }
  }
}
