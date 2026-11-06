/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 06/11/2026
* Nome...............: GerenciadorConexao
* Funcao.............: Orquestrador de Discovery UDP (6 campos com Shard) e failover transparente do cliente.
*************************************************************** */

package model;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Orquestrador da camada de rede do cliente com Discovery UDP automatico e failover cross-shard.
 * <ul>
 *   <li>Extrai o 6o campo ({@code idShard}) do farol {@code EDEN_CLUSTER:<id>:<estado>:<tcp>:<udp>:<shard>}.</li>
 *   <li>Ao capturar queda do socket TCP (IOException, SocketException, EOFException) ou falha no PING,
 *       limpa a conexao quebrada, dispara novo Discovery UDP automatico e reconecta a qualquer lider de
 *       Shard ativo disponivel na rede.</li>
 * </ul>
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 */
public class GerenciadorConexao {

  public static class EnderecoServidor {
    public final String host;
    public final int portaTcp;
    public final int portaUdp;
    /** ID do Shard (0..2) extraido do 6o campo do beacon UDP EDEN_CLUSTER, ou -1 se legado/desconhecido. */
    public final int idShard;
    public final String estado;

    public EnderecoServidor(String host, int portaTcp, int portaUdp) {
      this(host, portaTcp, portaUdp, inferirShardPorPortaTcp(portaTcp), "ATIVO");
    }

    public EnderecoServidor(String host, int portaTcp, int portaUdp, int idShard, String estado) {
      this.host = host;
      this.portaTcp = portaTcp;
      this.portaUdp = portaUdp;
      this.idShard = idShard;
      this.estado = (estado != null) ? estado : "ATIVO";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof EnderecoServidor)) return false;
      EnderecoServidor that = (EnderecoServidor) o;
      return portaTcp == that.portaTcp && host.equalsIgnoreCase(that.host);
    }

    @Override
    public int hashCode() {
      return (host.toLowerCase() + ":" + portaTcp).hashCode();
    }

    @Override
    public String toString() {
      return host + ":" + portaTcp + (idShard >= 0 ? " (Shard S" + idShard + ")" : "");
    }
  }

  private static final int NUM_SHARDS = 3;

  private final List<EnderecoServidor> servidoresConhecidos = Collections.synchronizedList(new ArrayList<>());
  private final ClienteTCP tcp;
  private final ClienteUDP udp;
  private volatile InfoUser usuarioAtual;
  private final MessageListener listener;
  private volatile int shardEntradaAtual = -1;

  private final AtomicBoolean emFailover = new AtomicBoolean(false);
  private volatile CountDownLatch latchFailoverAtual = null;
  private volatile boolean ultimoFailoverSucesso = false;
  private volatile boolean executando = true;

  public GerenciadorConexao(List<EnderecoServidor> servidoresIniciais, ClienteTCP tcp, ClienteUDP udp,
      InfoUser usuario, MessageListener listener) {
    this(servidoresIniciais, tcp, udp, usuario, listener,
        tcp != null ? inferirShardPorPortaTcp(tcp.getPortaServidor()) : -1);
  }

  public GerenciadorConexao(List<EnderecoServidor> servidoresIniciais, ClienteTCP tcp, ClienteUDP udp,
      InfoUser usuario, MessageListener listener, int idShardInicial) {
    if (servidoresIniciais != null) {
      for (EnderecoServidor e : servidoresIniciais) {
        adicionarOuAtualizarServidor(e);
      }
    }
    this.tcp = tcp;
    this.udp = udp;
    this.usuarioAtual = usuario;
    this.listener = listener;
    this.shardEntradaAtual = idShardInicial;

    if (this.tcp != null) {
      this.tcp.setListenerFalha(this::aguardarOuDispararFailover);
    }
  }

  // =========================================================================
  // UTILITARIOS DE PORTAS E DISCOVERY UDP (6 CAMPOS COM SHARD)
  // =========================================================================

  public static int calcularPortaUdpPadrao(int pTcp) {
    if (pTcp >= 5000 && pTcp < 5300) {
      return pTcp + 1000;
    }
    if (pTcp == Protocolo.PORTA_SERVIDOR_LEGACY) {
      return 6000;
    }
    if (pTcp >= 6789 && pTcp <= 6795) {
      return 7777 + (pTcp - 6789);
    }
    return pTcp + (Protocolo.PORTA_SERVIDOR_UDP - Protocolo.PORTA_SERVIDOR_TCP);
  }

  public static int inferirShardPorPortaTcp(int pTcp) {
    if (pTcp > 5000 && pTcp < 5100) {
      int id = pTcp - 5000;
      return Math.floorMod(id - 1, NUM_SHARDS);
    }
    if (pTcp >= 6789 && pTcp <= 6791) {
      return pTcp - 6789;
    }
    if (pTcp == Protocolo.PORTA_SERVIDOR_LEGACY) {
      return 0;
    }
    if (pTcp >= 5100 && pTcp < 5300) {
      return (pTcp - 5000) / 100;
    }
    return -1;
  }

  /**
   * Extrai os metadados do beacon UDP enviado pelo ServidorDiscovery.
   * Formato T#03 (6 campos): {@code EDEN_CLUSTER:<id>:<estado>:<tcp>:<udp>:<shard>}
   */
  public static EnderecoServidor parseRespostaDiscoveryCluster(String msg, String remetenteIp) {
    if (msg == null || remetenteIp == null) {
      return null;
    }
    String limpo = msg.trim();
    if (limpo.startsWith("EDEN_CLUSTER:")) {
      String[] p = limpo.split(":");
      if (p.length >= 5) {
        try {
          String estado = p[2].trim();
          int pTcp = Integer.parseInt(p[3].trim());
          int pUdp = Integer.parseInt(p[4].trim());
          int idShard = -1;
          if (p.length >= 6) {
            try {
              idShard = Integer.parseInt(p[5].trim());
            } catch (NumberFormatException ignored) {
            }
          }
          if (idShard < 0) {
            idShard = inferirShardPorPortaTcp(pTcp);
          }
          return new EnderecoServidor(remetenteIp, pTcp, pUdp, idShard, estado);
        } catch (NumberFormatException ignored) {
          return null;
        }
      }
      return new EnderecoServidor(remetenteIp, 5000, 6000, 0, "ATIVO");
    }
    if ("IP".equalsIgnoreCase(limpo) || "EDEN_HERE".equalsIgnoreCase(limpo)) {
      // Porta padrao canonica 6789 com fallback 5000
      return new EnderecoServidor(remetenteIp, Protocolo.PORTA_SERVIDOR_TCP, Protocolo.PORTA_SERVIDOR_UDP, 0, "ATIVO");
    }
    // Resposta interoperavel de colegas/Lucas (MESTRE_PARTICAO_X)
    String limpoUpper = limpo.toUpperCase();
    for (int p = 1; p <= 3; p++) {
      if (("MESTRE_PARTICAO_" + p).equals(limpoUpper) || limpoUpper.contains("PARTICAO_" + p) || limpoUpper.contains("PARTICAO " + p)) {
        int idShard = p - 1;
        int pTcp = Protocolo.PORTA_SERVIDOR_TCP + (p - 1);
        int pUdp = Protocolo.PORTA_SERVIDOR_UDP + (p - 1);
        return new EnderecoServidor(remetenteIp, pTcp, pUdp, idShard, "ATIVO");
      }
    }
    return null;
  }

  /**
   * Coleta todos os enderecos de broadcast das interfaces de rede ativas + loopback.
   */
  public static List<InetAddress> coletarBroadcasts() {
    List<InetAddress> broadcasts = new ArrayList<>();
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface netInterface = interfaces.nextElement();
        if (netInterface.isLoopback() || !netInterface.isUp()) {
          continue;
        }
        for (InterfaceAddress address : netInterface.getInterfaceAddresses()) {
          InetAddress broadcast = address.getBroadcast();
          if (broadcast != null && !broadcasts.contains(broadcast)) {
            broadcasts.add(broadcast);
          }
        }
      }
    } catch (Exception ignored) {
    }
    try {
      InetAddress globalBcast = InetAddress.getByName("255.255.255.255");
      if (!broadcasts.contains(globalBcast)) {
        broadcasts.add(globalBcast);
      }
    } catch (Exception ignored) {
    }
    try {
      InetAddress loopback = InetAddress.getByName("127.0.0.1");
      if (!broadcasts.contains(loopback)) {
        broadcasts.add(loopback);
      }
    } catch (Exception ignored) {
    }
    return broadcasts;
  }

  /**
   * Executa o Discovery UDP automatico na rede local/loopback.
   * 
   * @param timeoutTotalMs Tempo maximo total de escuta em milissegundos (ex: 900..1500ms).
   * @return Lista de servidores encontrados (lideres ATIVO primeiro).
   */
  public static List<EnderecoServidor> descobrirServidoresViaUDP(int timeoutTotalMs) {
    return descobrirServidoresViaUDP(timeoutTotalMs, -1);
  }

  /**
   * Executa o Discovery UDP com filtragem Smart Client: ignora qualquer shard diferente de {@code shardFiltro}.
   * Se {@code shardFiltro < 0}, aceita lideres de qualquer shard.
   * 
   * @param timeoutTotalMs Tempo maximo de escuta.
   * @param shardFiltro    ID do Shard desejado (0..2), ou -1 para qualquer.
   * @return Lista de servidores filtrados correspondentes ao shard desejado.
   */
  public static List<EnderecoServidor> descobrirServidoresViaUDP(int timeoutTotalMs, int shardFiltro) {
    Map<String, EnderecoServidor> ativos = new LinkedHashMap<>();
    Map<String, EnderecoServidor> standbys = new LinkedHashMap<>();

    int janela = Math.max(400, timeoutTotalMs);
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setBroadcast(true);
      socket.setSoTimeout(Math.min(450, janela / 2));

      byte[] dadosCluster = Protocolo.DISCOVER_EDEN_CLUSTER.getBytes(StandardCharsets.UTF_8);
      byte[] dadosPadrao = "SERVIDOR_IP".getBytes(StandardCharsets.UTF_8);
      byte[] dadosEden = "DISCOVER_EDEN".getBytes(StandardCharsets.UTF_8);
      byte[] dadosP1 = "QUEM_E_MESTRE_PARTICAO_1?".getBytes(StandardCharsets.UTF_8);
      byte[] dadosP2 = "QUEM_E_MESTRE_PARTICAO_2?".getBytes(StandardCharsets.UTF_8);
      byte[] dadosP3 = "QUEM_E_MESTRE_PARTICAO_3?".getBytes(StandardCharsets.UTF_8);

      List<InetAddress> destinos = coletarBroadcasts();
      for (InetAddress dest : destinos) {
        // Porta padrao de broadcast (8888) e porta legada EDEN (5001)
        enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY);
        enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY_EDEN);
        enviarPacoteSilencioso(socket, dadosPadrao, dest, Protocolo.PORTA_DISCOVERY);
        enviarPacoteSilencioso(socket, dadosEden, dest, Protocolo.PORTA_DISCOVERY_EDEN);

        // Perguntas multi-particao (8888, 8889, 8890) compativeis com servidores de colegas/Lucas
        enviarPacoteSilencioso(socket, dadosP1, dest, Protocolo.PORTA_DISCOVERY);
        enviarPacoteSilencioso(socket, dadosP2, dest, Protocolo.PORTA_DISCOVERY + 1);
        enviarPacoteSilencioso(socket, dadosP3, dest, Protocolo.PORTA_DISCOVERY + 2);

        // Portas dedicadas de cada Shard ativo (8898, 8899, 8900) para evitar sombra SO_REUSEADDR em loopback
        if (shardFiltro >= 0 && shardFiltro < NUM_SHARDS) {
          enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY + 10 + shardFiltro);
        } else {
          for (int s = 0; s < NUM_SHARDS; s++) {
            enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY + 10 + s);
          }
        }
      }

      long limite = System.currentTimeMillis() + janela;
      byte[] buffer = new byte[256];
      while (System.currentTimeMillis() < limite) {
        try {
          DatagramPacket resposta = new DatagramPacket(buffer, buffer.length);
          socket.receive(resposta);
          String msg = new String(resposta.getData(), 0, resposta.getLength(), StandardCharsets.UTF_8).trim();
          String remetenteIp = resposta.getAddress().getHostAddress();

          EnderecoServidor parsed = parseRespostaDiscoveryCluster(msg, remetenteIp);
          if (parsed != null) {
            // Smart Client: ignora shards que nao sao o meu
            if (shardFiltro >= 0 && parsed.idShard >= 0 && parsed.idShard != shardFiltro) {
              continue;
            }
            String chave = parsed.host.toLowerCase() + ":" + parsed.portaTcp;
            if ("ATIVO".equalsIgnoreCase(parsed.estado)) {
              if (!ativos.containsKey(chave)) {
                String tagSmart = (shardFiltro >= 0) ? " [SMART-CLIENT:CONEXAO-DIRETA]" : "";
                System.out.println("[DISCOVERY]" + tagSmart + " Farol recebido -> Host: " + parsed.host
                    + " | TCP: " + parsed.portaTcp + " | UDP: " + parsed.portaUdp
                    + " | Papel: " + parsed.estado
                    + " | Shard: S" + (parsed.idShard >= 0 ? parsed.idShard : "N/A"));
              }
              ativos.put(chave, parsed);
            } else {
              standbys.put(chave, parsed);
            }
          }
        } catch (SocketTimeoutException ste) {
          if (!ativos.isEmpty()) {
            break;
          }
        }
      }
    } catch (Exception e) {
      System.out.println("[DISCOVERY] Aviso durante busca UDP: " + e.getMessage());
    }

    List<EnderecoServidor> resultado = new ArrayList<>(ativos.values());
    for (Map.Entry<String, EnderecoServidor> entry : standbys.entrySet()) {
      if (!ativos.containsKey(entry.getKey())) {
        resultado.add(entry.getValue());
      }
    }
    return resultado;
  }

  private static void enviarPacoteSilencioso(DatagramSocket s, byte[] dados, InetAddress dest, int porta) {
    try {
      s.send(new DatagramPacket(dados, dados.length, dest, porta));
    } catch (Exception ignored) {
    }
  }

  // =========================================================================
  // REGISTRO DE SERVIDORES E MONITORAMENTO DE SESSAO
  // =========================================================================

  public void adicionarServidor(String host, int portaTcp, int portaUdp) {
    adicionarServidor(host, portaTcp, portaUdp, inferirShardPorPortaTcp(portaTcp));
  }

  public void adicionarServidor(String host, int portaTcp, int portaUdp, int idShard) {
    if (host == null || host.trim().isEmpty()) return;
    adicionarOuAtualizarServidor(new EnderecoServidor(host.trim(), portaTcp, portaUdp, idShard, "ATIVO"));
  }

  public void adicionarOuAtualizarServidor(EnderecoServidor novo) {
    if (novo == null || novo.host == null || novo.host.trim().isEmpty()) return;
    synchronized (servidoresConhecidos) {
      int idx = servidoresConhecidos.indexOf(novo);
      if (idx >= 0) {
        EnderecoServidor antigo = servidoresConhecidos.get(idx);
        int shardMelhor = (novo.idShard >= 0) ? novo.idShard : antigo.idShard;
        servidoresConhecidos.set(idx, new EnderecoServidor(
            novo.host.trim(), novo.portaTcp, novo.portaUdp, shardMelhor, novo.estado));
      } else {
        servidoresConhecidos.add(novo);
        System.out.println("[FAILOVER] Servidor registrado na lista de nos conhecidos: " + novo);
      }
    }
  }

  public List<EnderecoServidor> getServidoresConhecidos() {
    synchronized (servidoresConhecidos) {
      return new ArrayList<>(servidoresConhecidos);
    }
  }

  public int getShardEntradaAtual() {
    return shardEntradaAtual;
  }

  public void setShardEntradaAtual(int shardEntradaAtual) {
    this.shardEntradaAtual = shardEntradaAtual;
  }

  public void setUsuarioAtual(InfoUser usuarioAtual) {
    this.usuarioAtual = usuarioAtual;
  }

  /**
   * Inicia a thread de monitoramento continuo e resiliente de liveness do socket TCP.
   * Ao capturar queda fisica do socket (IOException, EOFException, SocketException) ou indicacao explicita
   * de STANDBY, limpa a conexao e dispara a busca por uma nova porta de entrada.
   * Nao derruba conexoes com servidores de colegas que respondem OK sem o texto "ATIVO".
   */
  public void iniciarMonitoramento() {
    Thread threadMonitor = new Thread(() -> {
      while (executando) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }

        if (!executando || emFailover.get()) {
          continue;
        }

        // Verificacao ativa de liveness do socket TCP local (detecta FIN / CLOSE_WAIT sem PING de rede)
        if (tcp == null || !tcp.verificarConexaoAtiva()) {
          System.out.println("[FAILOVER] [WARNING] Queda ou fechamento do socket TCP detectado. Disparando busca por nova porta de entrada...");
          if (tcp != null) tcp.fecharConexao();
          executarFailover();
        }
      }
    }, "Thread-Cliente-MonitoramentoSocket");
    threadMonitor.setDaemon(true);
    threadMonitor.start();
  }

  /**
   * Chamado pelo ClienteTCP quando um comando de usuario captura IOException / SocketException / EOFException.
   * Dispara (ou aguarda) a reconexao via Discovery UDP para que o comando seja repetido no novo lider.
   * Nunca bloqueia a Thread de UI (JavaFX / AWT).
   */
  private boolean aguardarOuDispararFailover(IOException causa) {
    if (!executando) {
      return false;
    }
    executarFailover();

    String nomeThread = Thread.currentThread().getName();
    if (nomeThread != null && (nomeThread.contains("JavaFX") || nomeThread.contains("AWT-EventQueue"))) {
      return false;
    }

    CountDownLatch latch = this.latchFailoverAtual;
    if (latch != null) {
      try {
        latch.await(6000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return ultimoFailoverSucesso && tcp.isConectado();
  }

  /**
   * Limpa o socket antigo, atualiza a UI de forma puramente assincrona ("Reconectando..."),
   * executa Discovery UDP automatico em thread dedicada e reconstroi integralmente o estado
   * volátil (REGISTER + JOINs + BLOCKs) no cluster.
   */
  public boolean executarFailover() {
    if (!executando) {
      return false;
    }
    if (!emFailover.compareAndSet(false, true)) {
      return false;
    }

    final String hostAntigo = (tcp != null) ? tcp.getIpServidor() : null;
    final int portaAntiga = (tcp != null) ? tcp.getPortaServidor() : -1;

    // 1. Limpa imediatamente o socket TCP quebrado para nunca reutilizar conexao morta
    if (tcp != null) {
      tcp.fecharConexao();
    }

    final CountDownLatch latch = new CountDownLatch(1);
    this.latchFailoverAtual = latch;
    this.ultimoFailoverSucesso = false;

    System.out.println("\n[FAILOVER] >>> QUEDA OU INVALIDACAO DO SOCKET (" + hostAntigo + ":" + portaAntiga
        + ") DETECTADA! ATUALIZANDO UI E INICIANDO DISCOVERY UDP AUTOMATICO <<<");

    // 2. Dispara atualizacao puramente assincrona da UI ANTES de iniciar qualquer laco bloqueante de rede
    if (listener != null) {
      listener.onConexaoPerdida();
    }

    Thread threadFailover = new Thread(() -> {
      try {
        // Cede um pulso curto para o JavaFX renderizar "Status: Reconectando..." na tela antes do I/O
        try {
          Thread.sleep(250);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }

        boolean encontrou = false;
        int tentativasGerais = 0;

        while (!encontrou && executando && tentativasGerais < 30) {
          tentativasGerais++;

          // 1. Discovery UDP Universal: aceita o lider de qualquer shard disponivel
          List<EnderecoServidor> descobertos = descobrirServidoresViaUDP(900);
          for (EnderecoServidor d : descobertos) {
            adicionarOuAtualizarServidor(d);
          }

          // Monta lista de candidatos combinando Cache + Broadcast + Varredura Rapida de Portas
          List<EnderecoServidor> candidatos = new ArrayList<>();
          EnderecoServidor endpointAntigo = null;

          for (EnderecoServidor d : descobertos) {
            if ("ATIVO".equalsIgnoreCase(d.estado)) {
              if (portaAntiga > 0 && d.portaTcp == portaAntiga && d.host.equalsIgnoreCase(hostAntigo)) {
                endpointAntigo = d;
              } else if (!candidatos.contains(d)) {
                candidatos.add(d);
              }
            }
          }

          for (EnderecoServidor c : getServidoresConhecidos()) {
            if (portaAntiga > 0 && c.portaTcp == portaAntiga && c.host.equalsIgnoreCase(hostAntigo)) {
              if (endpointAntigo == null) endpointAntigo = c;
            } else if (!candidatos.contains(c)) {
              candidatos.add(c);
            }
          }

          // 2. Camada de Varredura de Portas Consagradas (Colegas: 6789..6791 | EDEN: 5001..5004, 5000)
          List<String> hostsParaVarredura = new ArrayList<>();
          if (hostAntigo != null && !hostAntigo.trim().isEmpty() && !hostsParaVarredura.contains(hostAntigo.trim())) {
            hostsParaVarredura.add(hostAntigo.trim());
          }
          if (!hostsParaVarredura.contains("127.0.0.1")) {
            hostsParaVarredura.add("127.0.0.1");
          }
          for (EnderecoServidor c : getServidoresConhecidos()) {
            if (c.host != null && !hostsParaVarredura.contains(c.host.trim())) {
              hostsParaVarredura.add(c.host.trim());
            }
          }

          int[] portasVarredura = new int[] { 6789, 6790, 6791, 5001, 5002, 5003, 5004, 5000 };
          for (String hAlvo : hostsParaVarredura) {
            for (int pTcp : portasVarredura) {
              EnderecoServidor cand = new EnderecoServidor(
                  hAlvo, pTcp, calcularPortaUdpPadrao(pTcp), inferirShardPorPortaTcp(pTcp), "ATIVO");
              if (portaAntiga == pTcp && hAlvo.equalsIgnoreCase(hostAntigo)) {
                if (endpointAntigo == null) endpointAntigo = cand;
              } else if (!candidatos.contains(cand)) {
                candidatos.add(cand);
              }
            }
          }

          // Deixa o endpoint anterior por ultimo (para evitar reconectar imediatamente no mesmo que acabou de cair)
          if (endpointAntigo != null && !candidatos.contains(endpointAntigo)) {
            candidatos.add(endpointAntigo);
          }

          // 3. Validacao Direta da Nova Porta de Entrada (PING brando ou resposta APDU de REGISTER)
          for (EnderecoServidor end : candidatos) {
            if (!executando) break;
            try {
              try (Socket testSocket = new Socket()) {
                testSocket.connect(new InetSocketAddress(end.host, end.portaTcp), 450);
              }

              // Conecta o ClienteTCP ao novo candidato
              tcp.setDestino(end.host, end.portaTcp);

              boolean validado = false;
              InfoUser novoInfo = null;

              // Validacao direta da nova porta de entrada via REGISTER direto (padrao universal, sem dependencia de PING)
              if (usuarioAtual != null) {
                novoInfo = new InfoUser(usuarioAtual.getNome(), tcp.getIpLocal(), udp.getPortaLocal());
                try {
                  APDU resReg = tcp.registerDireto(novoInfo);
                  if (resReg != null) {
                    // Qualquer resposta de protocolo (OK ou erro estruturado do cluster)
                    // confirma que o socket e o processo do servidor estao vivos e atendendo como porta de entrada!
                    validado = true;
                    System.out.println("[FAILOVER] [GATEWAY-OK] Porta de entrada confirmada em "
                        + end.host + ":" + end.portaTcp + " -> " + resReg.getTextoMensagem());
                  }
                } catch (Exception ignored) {
                }
              } else {
                validado = true;
              }

              if (validado) {
                int shardNovo = (end.idShard >= 0) ? end.idShard : inferirShardPorPortaTcp(end.portaTcp);
                this.shardEntradaAtual = shardNovo;

                System.out.println("[FAILOVER] [SUCESSO] Nova porta de entrada conectada: "
                    + end.host + ":" + end.portaTcp + " (Shard S" + (shardNovo >= 0 ? shardNovo : "?") + ")");

                // Atualiza o destino do socket UDP preservando a porta local efemera do cliente
                udp.setDestino(end.host, end.portaUdp);

                // Reconstroi o estado nos shards: REGISTER + JOINs + BLOCKs
                if (usuarioAtual != null) {
                  if (novoInfo == null) {
                    novoInfo = new InfoUser(usuarioAtual.getNome(), tcp.getIpLocal(), udp.getPortaLocal());
                    try {
                      tcp.registerDireto(novoInfo);
                    } catch (Exception ignored) {
                    }
                  }
                  this.usuarioAtual = novoInfo;

                  List<String> gruposParaReconstruir = tcp.getGruposAtivos();
                  for (String grupo : gruposParaReconstruir) {
                    try {
                      tcp.joinDireto(grupo, novoInfo);
                      System.out.println("[FAILOVER] [REBUILD] JOIN restaurado no grupo '" + grupo + "'.");
                    } catch (Exception exJoin) {
                      System.out.println("[FAILOVER] Aviso ao re-ingressar no grupo '" + grupo + "': " + exJoin.getMessage());
                    }
                  }

                  List<String> bloqueadosParaReconstruir = tcp.getUsuariosBloqueados();
                  for (String alvoBloqueado : bloqueadosParaReconstruir) {
                    try {
                      tcp.blockDireto(alvoBloqueado, novoInfo);
                    } catch (Exception ignored) {
                    }
                  }
                }

                this.ultimoFailoverSucesso = true;
                encontrou = true;

                if (listener != null) {
                  listener.onReconectado(end.host, end.portaTcp, end.portaUdp, shardNovo);
                }
                break;
              } else {
                tcp.fecharConexao();
              }

            } catch (Exception e) {
              tcp.fecharConexao();
            }
          }

          if (!encontrou && executando) {
            try {
              Thread.sleep(900);
            } catch (InterruptedException ignored) {
              break;
            }
          }
        }
      } finally {
        emFailover.set(false);
        latch.countDown();
      }
    }, "Thread-ExecucaoFailover");
    threadFailover.setDaemon(true);
    threadFailover.start();

    return true;
  }

  public void parar() {
    this.executando = false;
    if (this.tcp != null) {
      this.tcp.setListenerFalha(null);
    }
    CountDownLatch latch = this.latchFailoverAtual;
    if (latch != null) {
      latch.countDown();
    }
  }

}
