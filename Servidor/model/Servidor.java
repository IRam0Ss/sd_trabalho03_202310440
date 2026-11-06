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

    // 1. Servico TCP principal (5000 + ID)
    ServidorTCP tcpPrincipal = new ServidorTCP(portaTcp, gerenciador, roteador);
    servicosTCP.add(tcpPrincipal);
    new Thread(tcpPrincipal, "Thread-ServidorTCP-" + portaTcp).start();

    // 2. Servico UDP principal (6000 + ID)
    final ServidorUDP udpPrincipal = new ServidorUDP(portaUdp, gerenciador, roteador);
    servicosUDP.add(udpPrincipal);
    new Thread(udpPrincipal, "Thread-ServidorUDP-" + portaUdp).start();
    if (roteador != null) {
      roteador.setReceptorUdp((apdu, ipCliente, portaUdpCliente, saltos) ->
          udpPrincipal.receberPacoteRoteado(apdu, ipCliente, portaUdpCliente, saltos));
    }

    // 3. Servico Discovery padrao (8888) + porta dedicada do Shard (8888 + idShard) para colegas/Lucas
    ServidorDiscovery discPrincipal = new ServidorDiscovery(portaDiscovery);
    discPrincipal.configurarInfo(idNo, idShard, "ATIVO", portaTcp, portaUdp);
    if (roteador != null) {
      discPrincipal.setTabelaLideres(roteador.getTabelaLideres());
    }
    servicosDiscovery.add(discPrincipal);
    new Thread(discPrincipal, "Thread-ServidorDiscovery-" + portaDiscovery).start();

    // Portas especificas por particao (8888, 8889, 8890) compativeis com Lucas (8888 + particao - 1)
    int portaDiscoveryColegas = Protocolo.PORTA_DISCOVERY + Math.max(0, idShard);
    boolean discoveryShardAberto = false;
    if (portaDiscoveryColegas != portaDiscovery && portaDiscoveryColegas != Protocolo.PORTA_DISCOVERY_EDEN) {
      try {
        ServidorDiscovery discShard = new ServidorDiscovery(portaDiscoveryColegas);
        discShard.configurarInfo(idNo, idShard, "ATIVO", portaTcp, portaUdp);
        if (roteador != null) {
          discShard.setTabelaLideres(roteador.getTabelaLideres());
        }
        servicosDiscovery.add(discShard);
        new Thread(discShard, "Thread-ServidorDiscovery-Shard-" + portaDiscoveryColegas).start();
        discoveryShardAberto = true;
      } catch (Exception ignored) {
      }
    }

    boolean discovery5001Aberto = false;
    boolean udp5000Aberto = false;
    boolean tcp5000ExtraAberto = false;
    boolean tcp6789ExtraAberto = false;
    boolean udp7777ExtraAberto = false;

    // 4. Portas universais de entrada para interoperabilidade (6789 TCP / 7777 UDP)
    // Se a portaTcp principal deste no nao for 6789, tenta abrir tambem a 6789 (ou 6789 + idShard)
    // para absorver conexoes diretas de clientes de colegas (Gateway Proxy)
    int portaTcpColegas = Protocolo.PORTA_SERVIDOR_TCP + Math.max(0, idShard);
    if (portaTcp != portaTcpColegas && portaTcp != Protocolo.PORTA_SERVIDOR_TCP) {
      try {
        ServidorTCP tcpColegas = new ServidorTCP(portaTcpColegas, gerenciador, roteador);
        servicosTCP.add(tcpColegas);
        new Thread(tcpColegas, "Thread-ServidorTCP-Interoperavel-" + portaTcpColegas).start();
        tcp6789ExtraAberto = true;
      } catch (Exception ignored) {
      }
    }
    // Tenta tambem abrir a base 6789 se for Shard 0 ou livre
    if (idShard == 0 && portaTcp != Protocolo.PORTA_SERVIDOR_TCP && !tcp6789ExtraAberto) {
      try {
        ServidorTCP tcp6789 = new ServidorTCP(Protocolo.PORTA_SERVIDOR_TCP, gerenciador, roteador);
        servicosTCP.add(tcp6789);
        new Thread(tcp6789, "Thread-ServidorTCP-Base-6789").start();
        tcp6789ExtraAberto = true;
      } catch (Exception ignored) {
      }
    }

    int portaUdpColegas = Protocolo.PORTA_SERVIDOR_UDP + Math.max(0, idShard);
    if (portaUdp != portaUdpColegas && portaUdp != Protocolo.PORTA_SERVIDOR_UDP) {
      try {
        ServidorUDP udpColegas = new ServidorUDP(portaUdpColegas, gerenciador, roteador);
        servicosUDP.add(udpColegas);
        new Thread(udpColegas, "Thread-ServidorUDP-Interoperavel-" + portaUdpColegas).start();
        udp7777ExtraAberto = true;
      } catch (Exception ignored) {
      }
    }

    // 5. Portas legadas para compatibilidade T#01/T#02 (ativas no lider do Shard 0)
    if (incluirLegado) {
      // O lider do Shard 0 assume tambem o TCP legado 5000 (clientes T#01/T#02 de colegas);
      // em failover, o novo lider do S0 reabre a 5000.
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
          ServidorUDP udpLegado = new ServidorUDP(Protocolo.PORTA_SERVIDOR_LEGACY, gerenciador, roteador);
          servicosUDP.add(udpLegado);
          new Thread(udpLegado, "Thread-ServidorUDP-Legado-5000").start();
          udp5000Aberto = true;
        } catch (Exception ignored) {
        }
      }
    }

    // 5b. Portas pioneiras do Shard 0 (TCP 5001 e UDP 6001):
    // Quando uma replica secundaria do Shard 0 (ex: No 4) assume como lider apos a morte do pioneiro (No 1),
    // tenta tambem abrir 5001 (TCP) e 6001 (UDP) caso estejam livres, para absorver conexoes e CONFIRMs
    // de clientes e colegas antes de processarem o SERVER_CHANGED.
    boolean tcp5001ExtraAberto = false;
    boolean udp6001ExtraAberto = false;
    if (idShard == 0) {
      if (portaTcp != 5001) {
        try {
          ServidorTCP tcp5001 = new ServidorTCP(5001, gerenciador, roteador);
          servicosTCP.add(tcp5001);
          new Thread(tcp5001, "Thread-ServidorTCP-Pioneiro-5001").start();
          tcp5001ExtraAberto = true;
        } catch (Exception ignored) {
        }
      }
      if (portaUdp != 6001) {
        try {
          ServidorUDP udp6001 = new ServidorUDP(6001, gerenciador, roteador);
          servicosUDP.add(udp6001);
          new Thread(udp6001, "Thread-ServidorUDP-Pioneiro-6001").start();
          udp6001ExtraAberto = true;
        } catch (Exception ignored) {
        }
      }
    }

    // 6. Discovery universal EDEN na porta UDP 5001: aberto por TODO lider de shard (SO_REUSEADDR),
    //    assim qualquer cliente "burro" encontra algum lider via broadcast mesmo se o Shard 0 cair.
    if (portaDiscovery != Protocolo.PORTA_DISCOVERY_EDEN) {
      try {
        ServidorDiscovery discEden = new ServidorDiscovery(Protocolo.PORTA_DISCOVERY_EDEN);
        discEden.configurarInfo(idNo, idShard, "ATIVO", portaTcp, portaUdp);
        if (roteador != null) {
          discEden.setTabelaLideres(roteador.getTabelaLideres());
        }
        servicosDiscovery.add(discEden);
        new Thread(discEden, "Thread-ServidorDiscovery-Eden-5001").start();
        discovery5001Aberto = true;
      } catch (Exception ignored) {
      }
    }

    this.servicosAtivos = true;
    this.gerenciador.setAtivo(true);
    System.out.println("=== SERVICOS DE ATENDIMENTO ABERTOS (SHARD S" + idShard + " | PAPEL: ATIVO) ===");
    imprimirEnderecosLocais();
    System.out.println("TCP escutando em " + portaTcp
        + (tcp6789ExtraAberto ? " (+ interoperavel " + portaTcpColegas + ")" : "")
        + (tcp5001ExtraAberto ? " (+ pioneiro 5001)" : "")
        + (tcp5000ExtraAberto ? " (+ legado 5000)" : ""));
    System.out.println("UDP escutando em " + portaUdp
        + (udp7777ExtraAberto ? " (+ interoperavel " + portaUdpColegas + ")" : "")
        + (udp6001ExtraAberto ? " (+ pioneiro 6001)" : "")
        + (udp5000Aberto ? " (+ legado 5000)" : ""));
    System.out.println("Discovery escutando em " + portaDiscovery
        + (discoveryShardAberto ? ", " + portaDiscoveryColegas : "")
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
    if (this.roteador != null) {
      this.roteador.setReceptorUdp(null);
    }

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
