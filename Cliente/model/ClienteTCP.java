/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 06/11/2026
* Nome...............: ClienteTCP
* Funcao.............: Gerencia o canal TCP de controle com deteccao imediata de queda e failover via Discovery UDP.
*************************************************************** */

package model;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;

/**
 * Gerenciador da conexao TCP do cliente. Ao detectar queda do socket TCP (EOFException,
 * SocketException ou IOException), limpa o socket morto imediatamente e aciona o
 * {@link ListenerFalhaConexao} (orquestrado pelo {@link GerenciadorConexao}) para redescobrir
 * via UDP um novo lider de Shard ativo e repetir o comando de forma transparente.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.1
 */
public class ClienteTCP {

  /** Callback invocado fora do lock de I/O quando o socket TCP cai, permitindo failover sem deadlock. */
  public interface ListenerFalhaConexao {
    boolean onQuedaSocketTCP(IOException causa);
  }

  private final Object ioLock = new Object();
  private volatile String ipServidor;
  private volatile int portaServidor;
  private Socket conexaoTCP;
  private ObjectOutputStream saidaObjetos;
  private ObjectInputStream entradaObjetos;
  private volatile ListenerFalhaConexao listenerFalha;
  /** Grupos ativos do cliente, reenviados automaticamente via JOIN em caso de Failover / Amnesia de Shard. */
  private final Set<String> gruposAtivos = ConcurrentHashMap.newKeySet();
  /** Usuarios bloqueados nesta sessao, reaplicados automaticamente apos Failover / Amnesia de Shard. */
  private final Set<String> usuariosBloqueados = ConcurrentHashMap.newKeySet();

  private static final int CONNECT_TIMEOUT_MS = 1000;
  private static final int READ_TIMEOUT_MS = 3500;

  public ClienteTCP(String ipServidor, int portaServidor) throws UnknownHostException, IOException {
    this.ipServidor = ipServidor;
    this.portaServidor = portaServidor;
    synchronized (ioLock) {
      conectarInterno();
    }
  }

  public void setListenerFalha(ListenerFalhaConexao listenerFalha) {
    this.listenerFalha = listenerFalha;
  }

  public String getIpServidor() {
    return ipServidor;
  }

  public int getPortaServidor() {
    return portaServidor;
  }

  public boolean isConectado() {
    synchronized (ioLock) {
      return conexaoTCP != null && !conexaoTCP.isClosed() && conexaoTCP.isConnected();
    }
  }

  /**
   * Altera dinamicamente o servidor de destino (usado no failover para outro Shard/Replica).
   */
  public void setDestino(String novoIp, int novaPorta) throws IOException {
    synchronized (ioLock) {
      fecharConexaoInterno();
      this.ipServidor = novoIp;
      this.portaServidor = novaPorta;
      conectarInterno();
    }
  }

  private void conectarInterno() throws IOException {
    this.conexaoTCP = new Socket();
    this.conexaoTCP.setTcpNoDelay(true);
    this.conexaoTCP.connect(new InetSocketAddress(ipServidor, portaServidor), CONNECT_TIMEOUT_MS);
    this.conexaoTCP.setSoTimeout(READ_TIMEOUT_MS);

    this.saidaObjetos = new ObjectOutputStream(conexaoTCP.getOutputStream());
    this.saidaObjetos.flush();
    this.entradaObjetos = new ObjectInputStream(conexaoTCP.getInputStream());
    System.out.println("[CLIENTE:TCP] [INFO] Conectado ao servidor " + ipServidor + ":" + portaServidor);
  }

  private void assegurarConexaoInterno() throws IOException {
    if (conexaoTCP == null || conexaoTCP.isClosed() || conexaoTCP.isInputShutdown() || conexaoTCP.isOutputShutdown()) {
      conectarInterno();
    }
  }

  private APDU lerRespostaInterno() throws IOException, ClassNotFoundException {
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

  private APDU enviarUmaVez(APDU apdu) throws IOException, ClassNotFoundException {
    synchronized (ioLock) {
      assegurarConexaoInterno();
      saidaObjetos.writeUnshared(apdu);
      saidaObjetos.reset();
      saidaObjetos.flush();
      return lerRespostaInterno();
    }
  }

  /**
   * Executa um PING direto sem acionar recursao de failover, lancando IOException imediatamente
   * se o socket TCP caiu (usado pelo monitor de liveness e pela thread de failover).
   */
  public APDU pingDireto() throws IOException {
    APDU apdu = new APDU(Protocolo.PING, null, null, null, 0);
    apdu.setIdRequisicao(UUID.randomUUID().toString());
    try {
      return enviarUmaVez(apdu);
    } catch (EOFException | SocketException e) {
      fecharConexao();
      throw e;
    } catch (IOException e) {
      fecharConexao();
      throw e;
    } catch (ClassNotFoundException e) {
      fecharConexao();
      throw new IOException("Resposta invalida no PING: " + e.getMessage(), e);
    }
  }

  /**
   * Executa REGISTER direto no socket recem-conectado durante o failover, sem recursao.
   */
  public APDU registerDireto(InfoUser usuario) throws IOException {
    APDU apdu = new APDU("REGISTER", "GLOBAL", usuario.getNome(), null, usuario.getPorta());
    apdu.setIdRequisicao(UUID.randomUUID().toString());
    try {
      return enviarUmaVez(apdu);
    } catch (ClassNotFoundException e) {
      throw new IOException("Resposta invalida no REGISTER: " + e.getMessage(), e);
    }
  }

  /**
   * Executa JOIN direto no socket recem-conectado durante o failover para reconstruir salas em shards reiniciados.
   */
  public APDU joinDireto(String nomeGrupo, InfoUser usuario) throws IOException {
    APDU apdu = new APDU("JOIN", nomeGrupo, usuario.getNome(), null, usuario.getPorta());
    apdu.setIdRequisicao(UUID.randomUUID().toString());
    try {
      return enviarUmaVez(apdu);
    } catch (ClassNotFoundException e) {
      throw new IOException("Resposta invalida no JOIN de reconstrucao: " + e.getMessage(), e);
    }
  }

  /**
   * Executa BLOCK direto no socket recem-conectado durante o failover para restaurar bloqueios em shards reiniciados.
   */
  public APDU blockDireto(String usuarioDestino, InfoUser usuario) throws IOException {
    APDU apdu = new APDU("BLOCK", null, usuario.getNome(), null, usuario.getPorta(), usuarioDestino);
    apdu.setIdRequisicao(UUID.randomUUID().toString());
    try {
      return enviarUmaVez(apdu);
    } catch (ClassNotFoundException e) {
      throw new IOException("Resposta invalida no BLOCK de reconstrucao: " + e.getMessage(), e);
    }
  }

  public List<String> getGruposAtivos() {
    return new ArrayList<>(gruposAtivos);
  }

  public List<String> getUsuariosBloqueados() {
    return new ArrayList<>(usuariosBloqueados);
  }

  public void limparEstadoSessao() {
    gruposAtivos.clear();
    usuariosBloqueados.clear();
  }

  /**
   * Executa um comando APDU garantindo idRequisicao unico e reconexao automatica transparente
   * caso o socket TCP sofra queda (IOException, SocketException ou EOFException).
   */
  public APDU executarComando(APDU apdu) {
    if (apdu.getIdRequisicao() == null || apdu.getIdRequisicao().trim().isEmpty()) {
      apdu.setIdRequisicao(UUID.randomUUID().toString());
    }

    try {
      return enviarUmaVez(apdu);
    } catch (EOFException | SocketException e) {
      return tratarFalhaSocket(apdu, e);
    } catch (IOException e) {
      return tratarFalhaSocket(apdu, e);
    } catch (ClassNotFoundException e) {
      fecharConexao();
      return new APDU(Protocolo.ERRO, null, null, "Falha de serializacao: " + e.getMessage(), 0);
    }
  }

  private APDU tratarFalhaSocket(APDU apdu, IOException erroInicial) {
    // 1. Limpa imediatamente o socket antigo quebrado
    fecharConexao();

    String op = apdu.getOperacao();
    // Em LOGOUT, nao faz sentido reconectar a outro servidor apenas para desconectar
    if (Protocolo.LOGOUT.equals(op)) {
      return new APDU(Protocolo.OK, null, null, "Desconectado localmente", 0);
    }

    ListenerFalhaConexao listener = this.listenerFalha;
    if (listener != null) {
      System.out.println("[CLIENTE:TCP] [FAILOVER] Queda do socket TCP detectada em '" + op
          + "' (" + erroInicial.getClass().getSimpleName() + "). Acionando reconexao via Discovery UDP...");

      String nomeThread = Thread.currentThread().getName();
      boolean ehThreadUi = nomeThread != null
          && (nomeThread.contains("JavaFX") || nomeThread.contains("AWT-EventQueue"));

      if (Protocolo.PING.equals(op) || ehThreadUi) {
        // Dispara failover puramente assincrono sem jamais bloquear PING ou a Thread da UI!
        new Thread(() -> listener.onQuedaSocketTCP(erroInicial), "Thread-Trigger-Failover").start();
        return new APDU(Protocolo.ERRO, null, null, "Conexao perdida: reconectando via Discovery UDP...", 0);
      }

      boolean reconectou = listener.onQuedaSocketTCP(erroInicial);
      if (reconectou) {
        try {
          return enviarUmaVez(apdu);
        } catch (Exception exRetry) {
          fecharConexao();
          return new APDU(Protocolo.ERRO, null, null, "Falha apos reconexao: " + exRetry.getMessage(), 0);
        }
      }
      return new APDU(Protocolo.ERRO, null, null,
          "Nenhum lider de Shard disponivel na rede no momento (" + erroInicial.getMessage() + ")", 0);
    }

    // Fallback sem GerenciadorConexao (testes unitarios isolados): tenta reabrir o mesmo destino 1 vez
    try {
      return enviarUmaVez(apdu);
    } catch (Exception ex) {
      fecharConexao();
      System.err.println("[CLIENTE:TCP] [ERROR] Falha de conexao com " + ipServidor + ":" + portaServidor + ": " + ex.getMessage());
      return new APDU(Protocolo.ERRO, null, null, "Falha de conexao: " + ex.getMessage(), 0);
    }
  }

  public APDU ping() {
    APDU apdu = new APDU(Protocolo.PING, null, null, null, 0);
    return executarComando(apdu);
  }

  public APDU join(String nomeGrupo, InfoUser usuario) {
    APDU apdu = new APDU("JOIN", nomeGrupo, usuario.getNome(), null, usuario.getPorta());
    System.out.println("[CLIENTE:TCP] [INFO] JOIN enviado ao servidor: " + usuario.toString());
    APDU resp = executarComando(apdu);
    if (nomeGrupo != null && !nomeGrupo.trim().isEmpty() && resp != null
        && (Protocolo.OK.equals(resp.getOperacao())
            || (resp.getTextoMensagem() != null && resp.getTextoMensagem().startsWith("Voce ja esta no grupo")))) {
      gruposAtivos.add(nomeGrupo.trim());
    }
    return resp;
  }

  public APDU leave(String nomeGrupo, InfoUser usuario) {
    if (nomeGrupo != null) {
      if ("GLOBAL".equalsIgnoreCase(nomeGrupo.trim())) {
        gruposAtivos.clear();
      } else {
        gruposAtivos.removeIf(g -> g.equalsIgnoreCase(nomeGrupo.trim()));
      }
    }
    APDU apdu = new APDU("LEAVE", nomeGrupo, usuario.getNome(), null, usuario.getPorta());
    System.out.println("[CLIENTE:TCP] [INFO] LEAVE enviado ao servidor: " + usuario.toString());
    return executarComando(apdu);
  }

  public APDU list() {
    APDU apdu = new APDU("LIST", null, null, null, 0);
    System.out.println("[CLIENTE:TCP] [INFO] LIST enviado ao servidor");
    return executarComando(apdu);
  }

  public APDU register(InfoUser usuario) {
    APDU apdu = new APDU("REGISTER", "GLOBAL", usuario.getNome(), null, usuario.getPorta());
    return executarComando(apdu);
  }

  public APDU listUsers() {
    APDU apdu = new APDU("USERS", null, null, null, 0);
    return executarComando(apdu);
  }

  public APDU listMembers(String nomeGrupo) {
    APDU apdu = new APDU("MEMBERS", nomeGrupo, null, null, 0);
    return executarComando(apdu);
  }

  public APDU block(String usuarioDestino, InfoUser usuario) {
    APDU apdu = new APDU("BLOCK", null, usuario.getNome(), null, usuario.getPorta(), usuarioDestino);
    APDU resp = executarComando(apdu);
    if (usuarioDestino != null && !usuarioDestino.trim().isEmpty()
        && resp != null && Protocolo.OK.equals(resp.getOperacao())) {
      usuariosBloqueados.add(usuarioDestino.trim());
    }
    return resp;
  }

  public APDU unblock(String usuarioDestino, InfoUser usuario) {
    if (usuarioDestino != null) {
      usuariosBloqueados.removeIf(u -> u.equalsIgnoreCase(usuarioDestino.trim()));
    }
    APDU apdu = new APDU("UNBLOCK", null, usuario.getNome(), null, usuario.getPorta(), usuarioDestino);
    return executarComando(apdu);
  }

  public void fecharConexao() {
    synchronized (ioLock) {
      fecharConexaoInterno();
    }
  }

  private void fecharConexaoInterno() {
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
    synchronized (ioLock) {
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

}
