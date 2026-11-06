/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: Servidor
* Funcao.............: Orquestrador de servicos de rede (TCP, UDP, Discovery) com suporte a ativacao/desativacao dinamica.
*************************************************************** */

package model;

import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import controller.GerenciadorGrupos;
import utils.Protocolo;

/**
 * Classe responsavel por inicializar, supervisionar e parar dinamicamente os subsistemas de servico:
 * escuta de conexoes confiaveis (TCP), canal de dados/notificacoes (UDP) e beacon de descoberta local (Discovery).
 * Permite que apenas o no eleito como ATIVO no cluster mantenha portas de atendimento abertas.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.0
 */
public class Servidor {

  private final GerenciadorGrupos gerenciador;
  private final List<ServidorTCP> servicosTCP = new ArrayList<>();
  private final List<ServidorUDP> servicosUDP = new ArrayList<>();
  private final List<ServidorDiscovery> servicosDiscovery = new ArrayList<>();
  private boolean servicosAtivos = false;

  public Servidor() {
    this(new GerenciadorGrupos());
  }

  public Servidor(GerenciadorGrupos gerenciador) {
    this.gerenciador = gerenciador;
  }

  public GerenciadorGrupos getGerenciador() {
    return gerenciador;
  }

  public synchronized boolean isServicosAtivos() {
    return servicosAtivos;
  }

  /**
   * Inicializacao padrao monolitica (modo de compatibilidade sem cluster ativo).
   */
  public synchronized void iniciar() {
    iniciarServicos(Protocolo.PORTA_SERVIDOR_TCP, Protocolo.PORTA_SERVIDOR_UDP, Protocolo.PORTA_DISCOVERY, true, 1);
  }

  /**
   * Inicia dinamicamente as portas de servico quando o no assume o papel de ATIVO.
   * 
   * @param portaTcp        Porta de controle TCP.
   * @param portaUdp        Porta de dados UDP.
   * @param portaDiscovery  Porta UDP de descoberta local.
   * @param incluirLegado   Se true, tenta vincular tambem as portas legadas (5000/5001).
   * @param idNo            Identificador do no atual no cluster.
   */
  public synchronized void iniciarServicos(int portaTcp, int portaUdp, int portaDiscovery, boolean incluirLegado, int idNo) {
    if (servicosAtivos) {
      System.out.println("[SERVIDOR] [WARNING] Servicos ja estao ativos.");
      return;
    }

    servicosTCP.clear();
    servicosUDP.clear();
    servicosDiscovery.clear();

    // 1. Servico TCP
    ServidorTCP tcpPrincipal = new ServidorTCP(portaTcp, gerenciador);
    servicosTCP.add(tcpPrincipal);
    new Thread(tcpPrincipal, "Thread-ServidorTCP-" + portaTcp).start();

    // 2. Servico UDP
    ServidorUDP udpPrincipal = new ServidorUDP(portaUdp, gerenciador);
    servicosUDP.add(udpPrincipal);
    new Thread(udpPrincipal, "Thread-ServidorUDP-" + portaUdp).start();

    // 3. Servico Discovery
    ServidorDiscovery discPrincipal = new ServidorDiscovery(portaDiscovery);
    discPrincipal.configurarInfo(idNo, "ATIVO", portaTcp, portaUdp);
    servicosDiscovery.add(discPrincipal);
    new Thread(discPrincipal, "Thread-ServidorDiscovery-" + portaDiscovery).start();

    // Portas legadas se solicitado (apenas se nao houver conflito de porta)
    if (incluirLegado && portaTcp != Protocolo.PORTA_SERVIDOR_LEGACY) {
      try {
        ServidorTCP tcpLegado = new ServidorTCP(Protocolo.PORTA_SERVIDOR_LEGACY, gerenciador);
        servicosTCP.add(tcpLegado);
        new Thread(tcpLegado, "Thread-ServidorTCP-Legado").start();
      } catch (Exception ignored) {
      }
      try {
        ServidorUDP udpLegado = new ServidorUDP(Protocolo.PORTA_SERVIDOR_LEGACY, gerenciador);
        servicosUDP.add(udpLegado);
        new Thread(udpLegado, "Thread-ServidorUDP-Legado").start();
      } catch (Exception ignored) {
      }
      try {
        ServidorDiscovery discEden = new ServidorDiscovery(Protocolo.PORTA_DISCOVERY_EDEN);
        discEden.configurarInfo(idNo, "ATIVO", portaTcp, portaUdp);
        servicosDiscovery.add(discEden);
        new Thread(discEden, "Thread-ServidorDiscovery-Eden").start();
      } catch (Exception ignored) {
      }
    }

    this.servicosAtivos = true;
    this.gerenciador.setAtivo(true);
    System.out.println("=== SERVICOS DE ATENDIMENTO ABERTOS (PAPEL: ATIVO) ===");
    imprimirEnderecosLocais();
    System.out.println("TCP escutando em " + portaTcp);
    System.out.println("UDP escutando em " + portaUdp);
    System.out.println("Discovery escutando em " + portaDiscovery);
    System.out.println("======================================================");
  }

  /**
   * Interrompe todos os servicos de atendimento ao cliente (passando para STANDBY).
   */
  public synchronized void pararServicos() {
    if (!servicosAtivos) {
      return;
    }
    System.out.println("[SERVIDOR] [INFO] Fechando portas de servico de atendimento...");
    this.gerenciador.setAtivo(false);

    for (ServidorTCP tcp : servicosTCP) {
      tcp.parar();
    }
    for (ServidorUDP udp : servicosUDP) {
      udp.parar();
    }
    for (ServidorDiscovery disc : servicosDiscovery) {
      disc.parar();
    }

    servicosTCP.clear();
    servicosUDP.clear();
    servicosDiscovery.clear();
    this.servicosAtivos = false;
    System.out.println("[SERVIDOR] [INFO] Portas de atendimento liberadas com sucesso.");
  }

  /**
   * Exibe no console os enderecos IPv4 disponiveis nas interfaces ativas.
   */
  public void imprimirEnderecosLocais() {
    try {
      System.out.println("Interfaces de rede detectadas:");
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface netIf = interfaces.nextElement();
        if (netIf.isLoopback() || !netIf.isUp()) {
          continue;
        }
        Enumeration<InetAddress> enderecos = netIf.getInetAddresses();
        while (enderecos.hasMoreElements()) {
          InetAddress addr = enderecos.nextElement();
          if (addr instanceof Inet4Address) {
            System.out.println("  -> [" + netIf.getDisplayName() + "] IPv4: " + addr.getHostAddress());
          }
        }
      }
    } catch (Exception e) {
      System.err.println("[SERVIDOR] Erro ao enumerar interfaces de rede: " + e.getMessage());
    }
  }

}
