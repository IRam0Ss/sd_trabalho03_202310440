/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: GerenciadorConexao
* Funcao.............: Orquestrador de failover transparente no cliente, monitoramento PING e lista de servidores.
*************************************************************** */

package model;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Orquestrador da camada de rede do cliente com suporte a N servidores em redundancia.
 * Monitora continuamente o lider ativo via heartbeat PING TCP e realiza a comutacao
 * automatica e silenciosa de destino sem que o usuario precise intervir.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class GerenciadorConexao {

  public static class EnderecoServidor {
    public final String host;
    public final int portaTcp;
    public final int portaUdp;

    public EnderecoServidor(String host, int portaTcp, int portaUdp) {
      this.host = host;
      this.portaTcp = portaTcp;
      this.portaUdp = portaUdp;
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
      return host + ":" + portaTcp;
    }
  }

  private final List<EnderecoServidor> servidoresConhecidos = Collections.synchronizedList(new ArrayList<>());
  private ClienteTCP tcp;
  private ClienteUDP udp;
  private InfoUser usuarioAtual;
  private MessageListener listener;

  private final AtomicBoolean emFailover = new AtomicBoolean(false);
  private volatile boolean executando = true;
  private int falhasConsecutivas = 0;

  public GerenciadorConexao(List<EnderecoServidor> servidoresIniciais, ClienteTCP tcp, ClienteUDP udp, InfoUser usuario, MessageListener listener) {
    if (servidoresIniciais != null) {
      for (EnderecoServidor e : servidoresIniciais) {
        adicionarServidor(e.host, e.portaTcp, e.portaUdp);
      }
    }
    this.tcp = tcp;
    this.udp = udp;
    this.usuarioAtual = usuario;
    this.listener = listener;
  }

  public void adicionarServidor(String host, int portaTcp, int portaUdp) {
    if (host == null || host.trim().isEmpty()) return;
    EnderecoServidor end = new EnderecoServidor(host.trim(), portaTcp, portaUdp);
    synchronized (servidoresConhecidos) {
      if (!servidoresConhecidos.contains(end)) {
        servidoresConhecidos.add(end);
        System.out.println("[FAILOVER] Novo servidor registrado na lista de nos conhecidos: " + end);
      }
    }
  }

  public List<EnderecoServidor> getServidoresConhecidos() {
    synchronized (servidoresConhecidos) {
      return new ArrayList<>(servidoresConhecidos);
    }
  }

  public void setUsuarioAtual(InfoUser usuarioAtual) {
    this.usuarioAtual = usuarioAtual;
  }

  /**
   * Inicia a thread de monitoramento continuo com PING periodico (a cada 2000 ms).
   */
  public void iniciarMonitoramento() {
    Thread threadPing = new Thread(() -> {
      while (executando) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }

        if (emFailover.get()) continue;

        try {
          APDU resPing = tcp.ping();
          if (resPing != null && Protocolo.OK.equals(resPing.getOperacao())) {
            falhasConsecutivas = 0;
          } else {
            falhasConsecutivas++;
            System.out.println("[FAILOVER] [WARNING] PING falhou (" + falhasConsecutivas + "/2)");
            if (falhasConsecutivas >= 2) {
              executarFailover();
            }
          }
        } catch (Exception e) {
          falhasConsecutivas++;
          System.out.println("[FAILOVER] [WARNING] PING disparou excecao (" + falhasConsecutivas + "/2): " + e.getMessage());
          if (falhasConsecutivas >= 2) {
            executarFailover();
          }
        }
      }
    }, "Thread-Cliente-MonitoramentoPING");
    threadPing.setDaemon(true);
    threadPing.start();
  }

  /**
   * Executa a busca e comutacao para um novo lider ativo de forma transparente.
   */
  public synchronized boolean executarFailover() {
    if (!emFailover.compareAndSet(false, true)) {
      return false;
    }

    System.out.println("\n[FAILOVER] >>> INICIANDO COMUTACAO TRANSPARENTE DE SERVIDOR <<<");
    if (listener != null) {
      listener.onConexaoPerdida();
    }

    new Thread(() -> {
      try {
        boolean encontrou = false;
        int tentativasGerais = 0;

        while (!encontrou && executando && tentativasGerais < 30) {
          tentativasGerais++;
          List<EnderecoServidor> lista = getServidoresConhecidos();

          for (EnderecoServidor end : lista) {
            try {
              System.out.println("[FAILOVER] Tentando conexao com " + end + "...");
              try (Socket testSocket = new Socket()) {
                testSocket.connect(new java.net.InetSocketAddress(end.host, end.portaTcp), 800);
              }

              // Conecta o ClienteTCP ao novo destino
              tcp.setDestino(end.host, end.portaTcp);
              APDU res = tcp.ping();

              if (res != null && Protocolo.OK.equals(res.getOperacao()) && "ATIVO".equalsIgnoreCase(res.getTextoMensagem())) {
                System.out.println("[FAILOVER] [SUCESSO] Novo lider ativo encontrado em " + end);

                // Atualiza o destino do socket UDP
                udp.setDestino(end.host, end.portaUdp);

                // Re-registra o usuario no novo lider
                if (usuarioAtual != null) {
                  InfoUser novoInfo = new InfoUser(usuarioAtual.getNome(), tcp.getIpLocal(), udp.getPortaLocal());
                  tcp.register(novoInfo);
                  this.usuarioAtual = novoInfo;
                }

                falhasConsecutivas = 0;
                encontrou = true;

                if (listener != null) {
                  listener.onReconectado(end.host, end.portaTcp, end.portaUdp);
                }
                break;
              } else {
                System.out.println("[FAILOVER] " + end + " respondeu, mas nao e o lider ATIVO.");
              }

            } catch (Exception e) {
              System.out.println("[FAILOVER] Servidor " + end + " inacessivel: " + e.getMessage());
            }
          }

          if (!encontrou) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
          }
        }

      } finally {
        emFailover.set(false);
      }
    }, "Thread-ExecucaoFailover").start();

    return true;
  }

  public void parar() {
    this.executando = false;
  }

}

