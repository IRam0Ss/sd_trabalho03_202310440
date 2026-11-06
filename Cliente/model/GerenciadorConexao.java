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
    return pTcp + (Protocolo.PORTA_SERVIDOR_UDP - Protocolo.PORTA_SERVIDOR_TCP);
  }

  public static int inferirShardPorPortaTcp(int pTcp) {
    if (pTcp >= 5000 && pTcp < 5300) {
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
      return new EnderecoServidor(remetenteIp, 5000, 6000, 0, "ATIVO");
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
   * Executa o Discovery UDP automatico na rede local/loopback, extraindo o 6o campo (idShard)
   * de cada lider que responder ao farol {@code DISCOVER_EDEN_CLUSTER}.
   * 
   * @param timeoutTotalMs Tempo maximo total de escuta em milissegundos (ex: 900..1500ms).
   * @return Lista de servidores encontrados (lideres ATIVO primeiro).
   */
  public static List<EnderecoServidor> descobrirServidoresViaUDP(int timeoutTotalMs) {
    Map<String, EnderecoServidor> ativos = new LinkedHashMap<>();
    Map<String, EnderecoServidor> standbys = new LinkedHashMap<>();

    int janela = Math.max(400, timeoutTotalMs);
    try (DatagramSocket socket = new DatagramSocket()) {
      socket.setBroadcast(true);
      socket.setSoTimeout(Math.min(450, janela / 2));

      byte[] dadosCluster = Protocolo.DISCOVER_EDEN_CLUSTER.getBytes(StandardCharsets.UTF_8);
      byte[] dadosPadrao = "SERVIDOR_IP".getBytes(StandardCharsets.UTF_8);
      byte[] dadosEden = "DISCOVER_EDEN".getBytes(StandardCharsets.UTF_8);

      List<InetAddress> destinos = coletarBroadcasts();
      for (InetAddress dest : destinos) {
        // Porta padrao de broadcast (8888) e porta legada EDEN (5001)
        enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY);
        enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY_EDEN);
        // Portas dedicadas de cada Shard ativo (8898, 8899, 8900) para evitar sombra SO_REUSEADDR em loopback
        for (int s = 0; s < NUM_SHARDS; s++) {
          enviarPacoteSilencioso(socket, dadosCluster, dest, Protocolo.PORTA_DISCOVERY + 10 + s);
        }
        enviarPacoteSilencioso(socket, dadosPadrao, dest, Protocolo.PORTA_DISCOVERY);
        enviarPacoteSilencioso(socket, dadosEden, dest, Protocolo.PORTA_DISCOVERY_EDEN);
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
            String chave = parsed.host.toLowerCase() + ":" + parsed.portaTcp;
            if ("ATIVO".equalsIgnoreCase(parsed.estado)) {
              if (!ativos.containsKey(chave)) {
                System.out.println("[DISCOVERY] Farol recebido -> Host: " + parsed.host
                    + " | TCP: " + parsed.portaTcp + " | UDP: " + parsed.portaUdp
                    + " | Papel: " + parsed.estado
                    + " | Shard de Entrada: " + (parsed.idShard >= 0 ? "S" + parsed.idShard : "N/A"));
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
   * Inicia a thread de monitoramento continuo com PING periodico (a cada 1500 ms).
   * Ao capturar queda do socket TCP ou resposta nao-ATIVO, limpa a conexao e dispara failover via Discovery UDP.
   */
  public void iniciarMonitoramento() {
    Thread threadPing = new Thread(() -> {
      while (executando) {
        try {
          Thread.sleep(1500);
        } catch (InterruptedException e) {
          break;
        }

        if (!executando || emFailover.get()) {
          continue;
        }

        try {
          APDU resPing = tcp.pingDireto();
          if (resPing == null || !Protocolo.OK.equals(resPing.getOperacao())
              || !"ATIVO".equalsIgnoreCase(resPing.getTextoMensagem())) {
            System.out.println("[FAILOVER] [WARNING] No atual deixou de responder como ATIVO. Disparando failover...");
            tcp.fecharConexao();
            executarFailover();
          }
        } catch (IOException e) {
          if (!executando) break;
          System.out.println("[FAILOVER] [WARNING] Queda do socket TCP detectada no heartbeat ("
              + e.getClass().getSimpleName() + ": " + e.getMessage() + "). Disparando Discovery UDP...");
          tcp.fecharConexao();
          executarFailover();
        }
      }
    }, "Thread-Cliente-MonitoramentoPING");
    threadPing.setDaemon(true);
    threadPing.start();
  }

  /**
   * Chamado pelo ClienteTCP quando um comando de usuario captura IOException / SocketException / EOFException.
   * Dispara (ou aguarda) a reconexao via Discovery UDP para que o comando seja repetido no novo lider.
   */
  private boolean aguardarOuDispararFailover(IOException causa) {
    if (!executando) {
      return false;
    }
    executarFailover();
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
   * Limpa o socket antigo, executa Discovery UDP automatico para localizar qualquer lider de Shard
   * sobrevivente na rede e restabelece a sessao TCP + UDP de forma transparente.
   */
  public synchronized boolean executarFailover() {
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

    System.out.println("\n[FAILOVER] >>> QUEDA DO GATEWAY (" + hostAntigo + ":" + portaAntiga
        + ") DETECTADA! INICIANDO DISCOVERY UDP AUTOMATICO <<<");
    if (listener != null) {
      listener.onConexaoPerdida();
    }

    new Thread(() -> {
      try {
        boolean encontrou = false;
        int tentativasGerais = 0;

        while (!encontrou && executando && tentativasGerais < 30) {
          tentativasGerais++;

          // 2. Dispara Discovery UDP automatico para encontrar qualquer lider de Shard vivo na rede
          List<EnderecoServidor> descobertos = descobrirServidoresViaUDP(900);
          for (EnderecoServidor d : descobertos) {
            adicionarOuAtualizarServidor(d);
          }

          // Monta lista ordenada de candidatos priorizando quem respondeu ATIVO no Discovery UDP agora
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

          // Fallback de portas matematicas locais caso firewall bloqueie pacotes UDP de broadcast
          int[] portasFallbackLocal = new int[] { 5000, 5001, 5100, 5101, 5200, 5201 };
          for (int pLocal : portasFallbackLocal) {
            EnderecoServidor fb = new EnderecoServidor(
                "127.0.0.1", pLocal, calcularPortaUdpPadrao(pLocal), inferirShardPorPortaTcp(pLocal), "ATIVO");
            if (portaAntiga == pLocal && "127.0.0.1".equals(hostAntigo)) {
              if (endpointAntigo == null) endpointAntigo = fb;
            } else if (!candidatos.contains(fb)) {
              candidatos.add(fb);
            }
          }

          // Deixa o endpoint que acabou de cair por ultimo (caso tenha apenas reiniciado rapido)
          if (endpointAntigo != null && !candidatos.contains(endpointAntigo)) {
            candidatos.add(endpointAntigo);
          }

          for (EnderecoServidor end : candidatos) {
            if (!executando) break;
            try {
              try (Socket testSocket = new Socket()) {
                testSocket.connect(new InetSocketAddress(end.host, end.portaTcp), 600);
              }

              // Conecta o ClienteTCP ao novo lider candidato
              tcp.setDestino(end.host, end.portaTcp);
              APDU res = tcp.pingDireto();

              if (res != null && Protocolo.OK.equals(res.getOperacao())
                  && "ATIVO".equalsIgnoreCase(res.getTextoMensagem())) {
                int shardNovo = (end.idShard >= 0) ? end.idShard : inferirShardPorPortaTcp(end.portaTcp);
                this.shardEntradaAtual = shardNovo;

                System.out.println("[FAILOVER] [SUCESSO] Reconectado via Discovery ao lider "
                    + end.host + ":" + end.portaTcp + " (Porta de Entrada: Shard S"
                    + (shardNovo >= 0 ? shardNovo : "?") + ")");

                // Atualiza o destino do socket UDP preservando a porta local efemera do cliente
                udp.setDestino(end.host, end.portaUdp);

                // Re-registra/atualiza endpoint do usuario atraves do novo Gateway
                if (usuarioAtual != null) {
                  try {
                    InfoUser novoInfo = new InfoUser(usuarioAtual.getNome(), tcp.getIpLocal(), udp.getPortaLocal());
                    tcp.registerDireto(novoInfo);
                    this.usuarioAtual = novoInfo;
                  } catch (Exception exReg) {
                    System.out.println("[FAILOVER] Aviso ao re-registrar usuario no novo gateway: " + exReg.getMessage());
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
    }, "Thread-ExecucaoFailover").start();

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
