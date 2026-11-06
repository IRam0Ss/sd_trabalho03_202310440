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

import controller.GerenciadorGrupos;
import controller.AtendimentoCliente;

/**
 * Servico responsavel pela abertura do socket servidor TCP (ServerSocket) e despacho
 * concorrente de cada nova conexao de cliente para uma instancia de {@link AtendimentoCliente}.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class ServidorTCP implements Runnable {

  private final int porta;
  private final GerenciadorGrupos gerenciador;
  private volatile boolean executando = true;
  private ServerSocket servidorTCP;

  /**
   * Construtor do ServidorTCP.
   * 
   * @param porta       A porta TCP a ser escutada.
   * @param gerenciador O gerenciador compartilhado de grupos e usuarios.
   */
  public ServidorTCP(int porta, GerenciadorGrupos gerenciador) {
    this.porta = porta;
    this.gerenciador = gerenciador;
  }

  /**
   * Encerra o servico TCP fechando o ServerSocket.
   */
  public void parar() {
    this.executando = false;
    if (this.servidorTCP != null && !this.servidorTCP.isClosed()) {
      try {
        this.servidorTCP.close();
      } catch (Exception ignored) {
      }
    }
    System.out.println("[SERVIDOR:TCP] [INFO] Servico na porta " + porta + " encerrado.");
  }

  /**
   * Laco principal de aceitacao de conexoes TCP de clientes.
   * Cria uma thread AtendimentoCliente dedicada para cada novo socket conectado.
   */
  @Override
  public void run() {
    try {
      this.servidorTCP = new ServerSocket();
      this.servidorTCP.setReuseAddress(true);
      this.servidorTCP.bind(new java.net.InetSocketAddress(porta));
      System.out.println("[SERVIDOR:TCP] [INFO] Escutando na porta " + porta);

      while (executando && !servidorTCP.isClosed()) {
        try {
          Socket conexaoClienteTCP = servidorTCP.accept();
          System.out.println(
              "[SERVIDOR:TCP] [INFO] Nova conexao recebida de " + conexaoClienteTCP.getInetAddress().getHostAddress());

          Thread threadAtendimentoCliente = new Thread(new AtendimentoCliente(conexaoClienteTCP, gerenciador));
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
