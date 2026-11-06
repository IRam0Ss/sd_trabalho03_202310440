/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: CanalCluster
* Funcao.............: Gerenciador de conexoes TCP ponto-a-ponto entre servidores com reconexao automatica.
*************************************************************** */

package cluster;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import utils.Protocolo;

/**
 * Canal de comunicacao em malha de conexoes TCP entre todos os servidores do cluster.
 * Mantem canais de transmissao persistentes com reconexao em background e recepcao concorrente.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class CanalCluster {

  public interface ReceptorMensagemCluster {
    void onMensagemRecebida(MensagemCluster msg);
  }

  private final ConfigCluster config;
  private final VisaoCluster visao;
  private final ReceptorMensagemCluster receptor;
  private volatile boolean executando = true;
  private volatile java.util.function.Consumer<MensagemCluster> receptorInterShard;

  /** Fase 3: mensagens ANUNCIO_SHARD/ROTEAR_OP/RESP_OP (de qualquer shard) vao para este consumidor. */
  public void setReceptorInterShard(java.util.function.Consumer<MensagemCluster> receptor) {
    this.receptorInterShard = receptor;
  }

  private ServerSocket serverSocket;
  // Conexoes de saida dedicadas por ID do par: ID -> SaidaPar
  private final Map<Integer, SaidaPar> conexoesSaida = new ConcurrentHashMap<>();

  private static class SaidaPar {
    final int id;
    final String host;
    final int porta;
    Socket socket;
    ObjectOutputStream oos;
    final Object lockEnvio = new Object();

    SaidaPar(int id, String host, int porta) {
      this.id = id;
      this.host = host;
      this.porta = porta;
    }

    boolean isConectado() {
      return socket != null && !socket.isClosed() && socket.isConnected();
    }

    void fechar() {
      synchronized (lockEnvio) {
        try {
          if (oos != null) oos.close();
        } catch (Exception ignored) {}
        try {
          if (socket != null && !socket.isClosed()) socket.close();
        } catch (Exception ignored) {}
        socket = null;
        oos = null;
      }
    }
  }

  public CanalCluster(ConfigCluster config, VisaoCluster visao, ReceptorMensagemCluster receptor) {
    this.config = config;
    this.visao = visao;
    this.receptor = receptor;

    for (ConfigCluster.NoInfo info : config.getOutrosNos()) {
      conexoesSaida.put(info.id, new SaidaPar(info.id, info.host, info.portaCluster));
    }
  }

  /**
   * Inicia o listener de entrada e a thread de reconexao de saida.
   */
  public void iniciar() {
    int minhaPorta = config.getMeuNoInfo().portaCluster;

    // 1. Thread escuta conexoes de entrada dos pares
    Thread threadEntrada = new Thread(() -> {
      try {
        // Zero-Config: reaproveita a porta 7000+ID ja reservada durante a escolha automatica do ID
        serverSocket = ConfigCluster.assumirReservaCluster(minhaPorta);
        if (serverSocket == null) {
          serverSocket = new ServerSocket();
          serverSocket.setReuseAddress(true);
          // Blindagem LAN (Fase 8): bind em 0.0.0.0 para aceitar conexoes locais (127.0.0.1) e fisicas (Wi-Fi/Ethernet)
          serverSocket.bind(new InetSocketAddress(minhaPorta));
        }
        System.out.println("[CLUSTER:TCP] [INFO] Canal de cluster escutando em 0.0.0.0:" + minhaPorta
            + " (host anunciado: " + config.getMeuNoInfo().host + ")");

        while (executando && !serverSocket.isClosed()) {
          try {
            Socket socketEntrada = serverSocket.accept();
            socketEntrada.setTcpNoDelay(true);
            new Thread(new LeitorEntradaPar(socketEntrada), "Thread-LeitorEntradaPar").start();
          } catch (Exception e) {
            if (!executando) break;
          }
        }
      } catch (Exception e) {
        if (executando) {
          System.err.println("[CLUSTER:TCP] [ERROR] Erro no ServerSocket de cluster: " + e.getMessage());
          e.printStackTrace();
        }
      } finally {
        fecharSocketServidor();
      }
    }, "Thread-Cluster-ServerSocket-" + minhaPorta);
    threadEntrada.setDaemon(true);
    threadEntrada.start();

    // 2. Thread reconector continuo de saida
    Thread threadConector = new Thread(() -> {
      while (executando) {
        for (SaidaPar saida : conexoesSaida.values()) {
          if (!saida.isConectado()) {
            tentarConectarSaida(saida);
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }, "Thread-Cluster-ConectorPares");
    threadConector.setDaemon(true);
    threadConector.start();
  }

  public void adicionarParSeAusente(int id, String host, int porta) {
    if (id <= 0 || id == config.getMeuId()) return;
    conexoesSaida.computeIfAbsent(id, k -> {
      SaidaPar saida = new SaidaPar(id, host, porta);
      new Thread(() -> tentarConectarSaida(saida), "Thread-Cluster-ConectarDinamico-" + id).start();
      return saida;
    });
  }

  private void tentarConectarSaida(SaidaPar saida) {
    try {
      Socket s = new Socket();
      s.setTcpNoDelay(true);
      s.connect(new InetSocketAddress(saida.host, saida.porta), 1000);

      ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
      oos.flush();

      synchronized (saida.lockEnvio) {
        saida.socket = s;
        saida.oos = oos;
      }
      visao.marcarConectado(saida.id, true);
      System.out.println("[CLUSTER:TCP] [INFO] Conexao de saida estabelecida com o No " + saida.id + " (" + saida.host + ":" + saida.porta + ")");

      // Envia HELLO de apresentacao imediato com dados do no
      MensagemCluster hello = new MensagemCluster(TipoMensagemCluster.HELLO, config.getMeuId());
      ConfigCluster.NoInfo meuInfo = config.getMeuNoInfo();
      if (meuInfo != null) {
        hello.setHostOrigem(meuInfo.host);
        hello.setPortaClusterOrigem(meuInfo.portaCluster);
        hello.setPortaTcpServico(meuInfo.portaTcp);
        hello.setPortaUdpServico(meuInfo.portaUdp);
      }
      enviar(saida.id, hello);

    } catch (Exception ignored) {
      saida.fechar();
      visao.marcarConectado(saida.id, false);
    }
  }

  /**
   * Envia uma mensagem para um no especifico via conexao de saida.
   */
  public boolean enviar(int idDestino, MensagemCluster msg) {
    SaidaPar saida = conexoesSaida.get(idDestino);
    if (saida == null || !saida.isConectado()) {
      return false;
    }

    msg.setIdShard(config.getIdShard());
    synchronized (saida.lockEnvio) {
      try {
        saida.oos.writeUnshared(msg);
        saida.oos.reset();
        saida.oos.flush();
        return true;
      } catch (Exception e) {
        System.err.println("[CLUSTER:TCP] [WARNING] Falha ao enviar para No " + idDestino + ": " + e.getMessage());
        saida.fechar();
        visao.marcarDown(idDestino);
        return false;
      }
    }
  }

  /**
   * Envia uma mensagem para todos os pares vivos do cluster.
   */
  public void broadcast(MensagemCluster msg) {
    for (Integer id : conexoesSaida.keySet()) {
      enviar(id, msg);
    }
  }

  public boolean isParConectado(int id) {
    SaidaPar p = conexoesSaida.get(id);
    return p != null && p.isConectado();
  }

  /**
   * Leitor de stream de entrada TCP de um par.
   */
  private class LeitorEntradaPar implements Runnable {
    private final Socket socket;
    private int ultimoParId = -1;

    LeitorEntradaPar(Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
        while (executando && !socket.isClosed()) {
          try {
            Object obj = ois.readObject();
            if (obj instanceof MensagemCluster) {
              MensagemCluster msg = (MensagemCluster) obj;

              // Enriquecimento de IP real LAN: se o pacote veio de outra maquina fisica e trazia 127.0.0.1, corrige
              if (socket.getInetAddress() != null && !socket.getInetAddress().isLoopbackAddress()) {
                String ipSocketReal = socket.getInetAddress().getHostAddress();
                if (msg.getHostOrigem() == null || msg.getHostOrigem().isEmpty()
                    || "127.0.0.1".equals(msg.getHostOrigem()) || "0.0.0.0".equals(msg.getHostOrigem())) {
                  msg.setHostOrigem(ipSocketReal);
                }
              }

              // Fase 3/4/5/6/7: trafego inter-shard nao participa do consenso; segue direto ao roteador
              if (receptorInterShard != null && (msg.getTipo() == TipoMensagemCluster.ANUNCIO_SHARD
                  || msg.getTipo() == TipoMensagemCluster.ROTEAR_OP
                  || msg.getTipo() == TipoMensagemCluster.RESP_OP
                  || msg.getTipo() == TipoMensagemCluster.CONSULTA_SHARD
                  || msg.getTipo() == TipoMensagemCluster.RESP_CONSULTA_SHARD
                  || msg.getTipo() == TipoMensagemCluster.ENCAMINHAR_UDP
                  || msg.getTipo() == TipoMensagemCluster.INVALIDAR_SESSAO_CLIENTE)) {
                receptorInterShard.accept(msg);
                continue;
              }

              // Isolamento de shards: mensagens de outro shard nunca participam do consenso deste
              if (msg.getIdShard() >= 0 && msg.getIdShard() != config.getIdShard()) {
                ClusterLog.aviso("ISOLAMENTO", "Descartada mensagem " + msg.getTipo() + " do shard "
                    + msg.getIdShard() + " (este no pertence ao shard " + config.getIdShard() + ").");
                continue;
              }
              this.ultimoParId = msg.getIdOrigem();

              // Auto-descoberta e registro dinamico se o no nao for previamente conhecido
              if (msg.getIdOrigem() > 0 && msg.getIdOrigem() != config.getMeuId()) {
                String hostOrig = (msg.getHostOrigem() != null && !msg.getHostOrigem().isEmpty())
                    ? msg.getHostOrigem()
                    : socket.getInetAddress().getHostAddress();
                int pCluster = (msg.getPortaClusterOrigem() > 0)
                    ? msg.getPortaClusterOrigem()
                    : PortasShard.clusterDoNo(msg.getIdOrigem());
                int pTcp = (msg.getPortaTcpServico() > 0) ? msg.getPortaTcpServico()
                    : PortasShard.tcpDoNo(msg.getIdOrigem());
                int pUdp = (msg.getPortaUdpServico() > 0) ? msg.getPortaUdpServico()
                    : PortasShard.udpDoNo(msg.getIdOrigem());

                ConfigCluster.NoInfo infoPar = new ConfigCluster.NoInfo(
                    msg.getIdOrigem(), config.getIdShard(), PortasShard.replicaDoNo(msg.getIdOrigem()), hostOrig, pCluster, pTcp, pUdp,
                    Protocolo.PORTA_DISCOVERY
                );
                config.registrarParDinamico(infoPar);
                visao.adicionarParSeAusente(infoPar);
                adicionarParSeAusente(msg.getIdOrigem(), hostOrig, pCluster);
              }

              visao.marcarConectado(msg.getIdOrigem(), true);
              if (receptor != null) {
                receptor.onMensagemRecebida(msg);
              }
            }
          } catch (EOFException e) {
            break;
          }
        }
      } catch (Exception ignored) {
      } finally {
        if (ultimoParId > 0) {
          visao.marcarDown(ultimoParId);
          SaidaPar saida = conexoesSaida.get(ultimoParId);
          if (saida != null) {
            saida.fechar();
          }
        }
        try {
          if (!socket.isClosed()) socket.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private void fecharSocketServidor() {
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (Exception ignored) {
      }
    }
  }

  public void fechar() {
    this.executando = false;
    fecharSocketServidor();
    for (SaidaPar s : conexoesSaida.values()) {
      s.fechar();
    }
    System.out.println("[CLUSTER:TCP] [INFO] Canal de cluster encerrado.");
  }
}
