/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 05/11/2026
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

import cluster.RoteadorShards;
import controller.GerenciadorGrupos;
import utils.Protocolo;

/**
 * Classe responsavel por inicializar, supervisionar e parar dinamicamente os subsistemas de servico:
 * escuta de conexoes confiaveis (TCP), canal de dados/notificacoes (UDP) e beacon de descoberta local (Discovery).
 * Permite que apenas o no eleito como ATIVO no seu Shard mantenha portas de atendimento abertas.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 3.1
 */
public class Servidor {

  private final GerenciadorGrupos gerenciador;
  private RoteadorShards roteador;
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

  public void setRoteador(RoteadorShards roteador) {
    this.roteador = roteador;
  }

  public RoteadorShards getRoteador() {
    return roteador;
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
    iniciarServicos(Protocolo.PORTA_SERVIDOR_LEGACY, 6000, Protocolo.PORTA_DISCOVERY, true, 1, 0);
  }

  public synchronized void iniciarServicos(int portaTcp, int portaUdp, int portaDiscovery, boolean incluirLegado, int idNo) {
    iniciarServicos(portaTcp, portaUdp, portaDiscovery, incluirLegado, idNo, 0);
  }

  /**
   * Inicia dinamicamente as portas de servico quando o no assume o papel de ATIVO em seu Shard.
   * Quando {@code incluirLegado} e true (Shard 0), abre tambem obrigatoriamente a porta 5001
   * de Discovery EDEN (mesmo quando portaTcp == 5000) e o UDP legado 5000.
   */
  public synchronized void iniciarServicos(int portaTcp, int portaUdp, int portaDiscovery,
      boolean incluirLegado, int idNo, int idShard) {
    if (servicosAtivos) {
      System.out.println("[SERVIDOR] [WARNING] Servicos ja estao ativos.");
      return;
    }

    servicosTCP.clear();
    servicosUDP.clear();
    servicosDiscovery.clear();

    // 1. Servico TCP principal (5000 + idShard*100 + idReplica)
    ServidorTCP tcpPrincipal = new ServidorTCP(portaTcp, gerenciador, roteador);
    servicosTCP.add(tcpPrincipal);
    new Thread(tcpPrincipal, "Thread-ServidorTCP-" + portaTcp).start();

    // 2. Servico UDP principal (6000 + idShard*100 + idReplica)
    ServidorUDP udpPrincipal = new ServidorUDP(portaUdp, gerenciador);
    servicosUDP.add(udpPrincipal);
    new Thread(udpPrincipal, "Thread-ServidorUDP-" + portaUdp).start();

    // 3. Servico Discovery padrao (8888)
    ServidorDiscovery discPrincipal = new ServidorDiscovery(portaDiscovery);
    discPrincipal.configurarInfo(idNo, idShard, "ATIVO", portaTcp, portaUdp);
    servicosDiscovery.add(discPrincipal);
    new Thread(discPrincipal, "Thread-ServidorDiscovery-" + portaDiscovery).start();

    boolean discovery5001Aberto = false;
    boolean udp5000Aberto = false;
    boolean tcp5000ExtraAberto = false;

    // 4. Portas legadas para compatibilidade T#01/T#02 (ativas no lider do Shard 0)
    if (incluirLegado) {
      // Se portaTcp ja e 5000 (Shard 0, Replica 0), nao duplica o bind TCP 5000;
      // se houve failover para Replica 1 (portaTcp=5001), assume tambem o TCP 5000!
      if (portaTcp != Protocolo.PORTA_SERVIDOR_LEGACY) {
        try {
          ServidorTCP tcpLegado = new ServidorTCP(Protocolo.PORTA_SERVIDOR_LEGACY, gerenciador, roteador);
          servicosTCP.add(tcpLegado);
          new Thread(tcpLegado, "Thread-ServidorTCP-Legado-5000").start();
          tcp5000ExtraAberto = true;
        } catch (Exception ignored) {
        }
      }

      // UDP legado na 5000 (pois o UDP principal de S0/R0 e 6000)
      if (portaUdp != Protocolo.PORTA_SERVIDOR_LEGACY) {
        try {
          ServidorUDP udpLegado = new ServidorUDP(Protocolo.PORTA_SERVIDOR_LEGACY, gerenciador);
          servicosUDP.add(udpLegado);
          new Thread(udpLegado, "Thread-ServidorUDP-Legado-5000").start();
          udp5000Aberto = true;
        } catch (Exception ignored) {
        }
      }

      // Discovery legado EDEN na porta UDP 5001: SEMPRE aberto no Shard 0 (mesmo com portaTcp == 5000!)
      if (portaDiscovery != Protocolo.PORTA_DISCOVERY_EDEN) {
        try {
          ServidorDiscovery discEden = new ServidorDiscovery(Protocolo.PORTA_DISCOVERY_EDEN);
          discEden.configurarInfo(idNo, idShard, "ATIVO", portaTcp, portaUdp);
          servicosDiscovery.add(discEden);
          new Thread(discEden, "Thread-ServidorDiscovery-Eden-5001").start();
          discovery5001Aberto = true;
        } catch (Exception ignored) {
        }
      }
    }

    this.servicosAtivos = true;
    this.gerenciador.setAtivo(true);
    System.out.println("=== SERVICOS DE ATENDIMENTO ABERTOS (SHARD S" + idShard + " | PAPEL: ATIVO) ===");
    imprimirEnderecosLocais();
    System.out.println("TCP escutando em " + portaTcp + (tcp5000ExtraAberto ? " (+ legado 5000)" : ""));
    System.out.println("UDP escutando em " + portaUdp + (udp5000Aberto ? " (+ legado 5000)" : ""));
    System.out.println("Discovery escutando em " + portaDiscovery
        + (discovery5001Aberto ? " e " + Protocolo.PORTA_DISCOVERY_EDEN : ""));
    System.out.println("=================================================================");
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
