/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ClienteTCP
* Funcao.............: Gerencia o canal TCP de controle com suporte a troca dinamica de servidor (failover) e timeouts.
*************************************************************** */

package model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Gerenciador da conexao TCP do cliente. Possui suporte a reconfiguracao dinamica
 * de destino para failover transparente, timeouts defensivos e geracao de ID de requisicao
 * para garantia de idempotencia na rede.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class ClienteTCP {

  private String ipServidor;
  private int portaServidor;
  private Socket conexaoTCP;
  private ObjectOutputStream saidaObjetos;
  private ObjectInputStream entradaObjetos;

  private static final int CONNECT_TIMEOUT_MS = 1000;
  private static final int READ_TIMEOUT_MS = 3000;

  public ClienteTCP(String ipServidor, int portaServidor) throws UnknownHostException, IOException {
    this.ipServidor = ipServidor;
    this.portaServidor = portaServidor;
    conectar();
  }

  public synchronized String getIpServidor() {
    return ipServidor;
  }

  public synchronized int getPortaServidor() {
    return portaServidor;
  }

  public synchronized boolean isConectado() {
    return conexaoTCP != null && !conexaoTCP.isClosed() && conexaoTCP.isConnected();
  }

  /**
   * Altera dinamicamente o servidor de destino (usado no failover).
   */
  public synchronized void setDestino(String novoIp, int novaPorta) throws IOException {
    fecharConexao();
    this.ipServidor = novoIp;
    this.portaServidor = novaPorta;
    conectar();
  }

  private synchronized void conectar() throws IOException {
    this.conexaoTCP = new Socket();
    this.conexaoTCP.setTcpNoDelay(true);
    this.conexaoTCP.connect(new InetSocketAddress(ipServidor, portaServidor), CONNECT_TIMEOUT_MS);
    this.conexaoTCP.setSoTimeout(READ_TIMEOUT_MS);

    this.saidaObjetos = new ObjectOutputStream(conexaoTCP.getOutputStream());
    this.saidaObjetos.flush();
    this.entradaObjetos = new ObjectInputStream(conexaoTCP.getInputStream());
    System.out.println("[CLIENTE:TCP] [INFO] Conectado ao servidor " + ipServidor + ":" + portaServidor);
  }

  private synchronized void assegurarConexao() throws IOException {
    if (conexaoTCP == null || conexaoTCP.isClosed() || conexaoTCP.isInputShutdown() || conexaoTCP.isOutputShutdown()) {
      conectar();
    }
  }

  private APDU lerResposta() throws IOException, ClassNotFoundException {
    Object obj = entradaObjetos.readObject();
    if (obj instanceof APDU) {
      return (APDU) obj;
    } else if (obj instanceof String) {
      String str = ((String) obj).trim();
      String operacao = Protocolo.OK;
      String msg = str;

      if (str.startsWith("OK")) {
        operacao = Protocolo.OK;
        msg = str.substring(2).trim();
      } else if (str.startsWith("ERRO")) {
        operacao = Protocolo.ERRO;
        msg = str.substring(4).trim();
      }

      while (msg.startsWith(":") || msg.startsWith("/") || msg.startsWith("~") || msg.startsWith("-") || msg.startsWith(" ")) {
        if (msg.startsWith("~/")) {
          msg = msg.substring(2).trim();
        } else {
          msg = msg.substring(1).trim();
        }
      }

      return new APDU(operacao, null, null, msg, 0);
    }
    return new APDU(Protocolo.ERRO, null, null, "Resposta de tipo desconhecido: " + obj, 0);
  }

  /**
   * Executa um comando APDU garantindo a atribuicao de idRequisicao unico para idempotencia.
   */
  public synchronized APDU executarComando(APDU apdu) {
    if (apdu.getIdRequisicao() == null || apdu.getIdRequisicao().trim().isEmpty()) {
      apdu.setIdRequisicao(UUID.randomUUID().toString());
    }

    try {
      assegurarConexao();
      saidaObjetos.writeUnshared(apdu);
      saidaObjetos.reset();
      saidaObjetos.flush();
      return lerResposta();
    } catch (IOException | ClassNotFoundException e) {
      try {
        fecharConexao();
        conectar();
        saidaObjetos.writeUnshared(apdu);
        saidaObjetos.reset();
        saidaObjetos.flush();
        return lerResposta();
      } catch (Exception ex) {
        System.err.println("[CLIENTE:TCP] [ERROR] Falha de conexao com " + ipServidor + ":" + portaServidor + ": " + ex.getMessage());
        return new APDU(Protocolo.ERRO, null, null, "Falha de conexao: " + ex.getMessage(), 0);
      }
    }
  }

  public synchronized APDU ping() {
    APDU apdu = new APDU(Protocolo.PING, null, null, null, 0);
    return executarComando(apdu);
  }

  public synchronized APDU join(String nomeGrupo, InfoUser usuario) {
    APDU apdu = new APDU("JOIN", nomeGrupo, usuario.getNome(), null, usuario.getPorta());
    System.out.println("[CLIENTE:TCP] [INFO] JOIN enviado ao servidor: " + usuario.toString());
    return executarComando(apdu);
  }

  public synchronized APDU leave(String nomeGrupo, InfoUser usuario) {
    APDU apdu = new APDU("LEAVE", nomeGrupo, usuario.getNome(), null, usuario.getPorta());
    System.out.println("[CLIENTE:TCP] [INFO] LEAVE enviado ao servidor: " + usuario.toString());
    return executarComando(apdu);
  }

  public synchronized APDU list() {
    APDU apdu = new APDU("LIST", null, null, null, 0);
    System.out.println("[CLIENTE:TCP] [INFO] LIST enviado ao servidor");
    return executarComando(apdu);
  }

  public synchronized APDU register(InfoUser usuario) {
    APDU apdu = new APDU("REGISTER", "GLOBAL", usuario.getNome(), null, usuario.getPorta());
    return executarComando(apdu);
  }

  public synchronized APDU listUsers() {
    APDU apdu = new APDU("USERS", null, null, null, 0);
    return executarComando(apdu);
  }

  public synchronized APDU listMembers(String nomeGrupo) {
    APDU apdu = new APDU("MEMBERS", nomeGrupo, null, null, 0);
    return executarComando(apdu);
  }

  public synchronized APDU block(String usuarioDestino, InfoUser usuario) {
    APDU apdu = new APDU("BLOCK", null, usuario.getNome(), null, usuario.getPorta(), usuarioDestino);
    return executarComando(apdu);
  }

  public synchronized APDU unblock(String usuarioDestino, InfoUser usuario) {
    APDU apdu = new APDU("UNBLOCK", null, usuario.getNome(), null, usuario.getPorta(), usuarioDestino);
    return executarComando(apdu);
  }

  public synchronized void fecharConexao() {
    try {
      if (saidaObjetos != null) saidaObjetos.close();
    } catch (Exception ignored) {}
    try {
      if (entradaObjetos != null) entradaObjetos.close();
    } catch (Exception ignored) {}
    try {
      if (conexaoTCP != null && !conexaoTCP.isClosed()) conexaoTCP.close();
    } catch (Exception ignored) {}
    conexaoTCP = null;
    saidaObjetos = null;
    entradaObjetos = null;
  }

  public String getIpLocal() {
    try {
      if (conexaoTCP != null && !conexaoTCP.isClosed() && conexaoTCP.getLocalAddress() != null) {
        String ip = conexaoTCP.getLocalAddress().getHostAddress();
        if (!"0.0.0.0".equals(ip)) {
          return ip;
        }
      }
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1";
    }
  }

}
