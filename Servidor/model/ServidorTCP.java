/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ServidorTCP
* Funcao.............: Servico de escuta passiva de conexoes TCP com controle de parada graciosa.
*************************************************************** */

package model;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cluster.RoteadorShards;
import controller.GerenciadorGrupos;
import controller.AtendimentoCliente;

/**
 * Servico responsavel pela abertura do socket servidor TCP (ServerSocket) e despacho
 * concorrente de cada nova conexao de cliente para uma instancia de {@link AtendimentoCliente}.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.1
 */
public class ServidorTCP implements Runnable {

  private final int porta;
  private final GerenciadorGrupos gerenciador;
  private final RoteadorShards roteador;
  private final boolean modoFallback;
  private final Set<Socket> conexoesClientes = ConcurrentHashMap.newKeySet();
  private volatile boolean executando = true;
  private ServerSocket servidorTCP;

  /**
   * Construtor legado/monolitico do ServidorTCP.
   */
  public ServidorTCP(int porta, GerenciadorGrupos gerenciador) {
    this(porta, gerenciador, null, false);
  }

  /**
   * Construtor particionado (T#03) do ServidorTCP com Proxy Transparente inter-shards.
   * 
   * @param porta       A porta TCP a ser escutada.
   * @param gerenciador O gerenciador compartilhado desta particao.
   * @param roteador    O roteador inter-shards (ou null em modo isolado).
   */
  public ServidorTCP(int porta, GerenciadorGrupos gerenciador, RoteadorShards roteador) {
    this(porta, gerenciador, roteador, false);
  }

  /**
   * Construtor com suporte a reabertura proativa de portas de fallback em failover.
   */
  public ServidorTCP(int porta, GerenciadorGrupos gerenciador, RoteadorShards roteador, boolean modoFallback) {
    this.porta = porta;
    this.gerenciador = gerenciador;
    this.roteador = roteador;
    this.modoFallback = modoFallback;
  }

  /**
   * Encerra o servico TCP fechando o ServerSocket e liberando os sockets de clientes conectados
   * para sinalizar EOF imediato em caso de step-down ou encerramento gracioso do lider.
   */
  public void parar() {
    this.executando = false;
    if (this.servidorTCP != null && !this.servidorTCP.isClosed()) {
      try {
        this.servidorTCP.close();
      } catch (Exception ignored) {
      }
    }
    for (Socket s : conexoesClientes) {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
        }
      } catch (Exception ignored) {
      }
    }
    conexoesClientes.clear();
    System.out.println("[SERVIDOR:TCP] [INFO] Servico na porta " + porta + " encerrado.");
  }

  /**
   * Laco principal de aceitacao de conexoes TCP de clientes.
   * Cria uma thread AtendimentoCliente dedicada para cada novo socket conectado.
   */
  @Override
  public void run() {
    try {
      int tentativas = modoFallback ? 4 : 2;
      Exception ultimoErroBind = null;
      boolean bindConcluido = false;
      for (int t = 1; t <= tentativas && executando; t++) {
        try {
          ServerSocket candidato = new ServerSocket();
          candidato.setReuseAddress(true);
          candidato.bind(new java.net.InetSocketAddress(porta));
          this.servidorTCP = candidato;
          bindConcluido = true;
          break;
        } catch (Exception exBind) {
          ultimoErroBind = exBind;
          if (t < tentativas && executando) {
            try {
              Thread.sleep(120);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }
      if (!bindConcluido) {
        if (!modoFallback && executando && ultimoErroBind != null) {
          System.err.println("[SERVIDOR:TCP] [ERROR] Porta " + porta + ": " + ultimoErroBind.getMessage());
        }
        return;
      }
      System.out.println("[SERVIDOR:TCP] [INFO] Escutando na porta " + porta);

      while (executando && !servidorTCP.isClosed()) {
        try {
          final Socket conexaoClienteTCP = servidorTCP.accept();
          conexoesClientes.removeIf(s -> s == null || s.isClosed());
          conexoesClientes.add(conexaoClienteTCP);
          System.out.println(
              "[SERVIDOR:TCP] [INFO] Nova conexao recebida de " + conexaoClienteTCP.getInetAddress().getHostAddress());

          final AtendimentoCliente atendente = new AtendimentoCliente(conexaoClienteTCP, gerenciador, roteador);
          Thread threadAtendimentoCliente = new Thread(() -> {
            try {
              atendente.run();
            } finally {
              conexoesClientes.remove(conexaoClienteTCP);
            }
          });
          threadAtendimentoCliente.setDaemon(true);
          threadAtendimentoCliente.start();
        } catch (Exception ex) {
          if (!executando) break;
          System.err.println("[SERVIDOR:TCP] [WARNING] Falha ao aceitar conexao: " + ex.getMessage());
        }
      }

    } catch (Exception e) {
      if (executando) {
        System.err.println("[SERVIDOR:TCP] [ERROR] " + e.getMessage());
      }
    } finally {
      if (servidorTCP != null && !servidorTCP.isClosed()) {
        try {
          servidorTCP.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

}
