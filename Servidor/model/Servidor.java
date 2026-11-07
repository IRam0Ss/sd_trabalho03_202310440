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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cluster.ConfigCluster;
import cluster.PortasShard;
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

  private void abrirTcpExtra(Set<Integer> portasAbertas, int porta, String sufixoThread) {
    if (porta <= 0 || !portasAbertas.add(porta)) {
      return;
    }
    try {
      ServidorTCP tcpExtra = new ServidorTCP(porta, gerenciador, roteador, true);
      servicosTCP.add(tcpExtra);
      new Thread(tcpExtra, "Thread-ServidorTCP-" + sufixoThread + "-" + porta).start();
    } catch (Exception ignored) {
    }
  }

  private void abrirUdpExtra(Set<Integer> portasAbertas, int porta, String sufixoThread) {
    if (porta <= 0 || !portasAbertas.add(porta)) {
      return;
    }
    try {
      ServidorUDP udpExtra = new ServidorUDP(porta, gerenciador, roteador, true);
      servicosUDP.add(udpExtra);
      new Thread(udpExtra, "Thread-ServidorUDP-" + sufixoThread + "-" + porta).start();
    } catch (Exception ignored) {
    }
  }

  private void abrirDiscoveryExtra(Set<Integer> portasAbertas, int porta, int idNo, int idShard,
      int portaTcpAnunciada, int portaUdpAnunciada, String sufixoThread) {
    if (porta <= 0 || !portasAbertas.add(porta)) {
      return;
    }
    try {
      ServidorDiscovery discExtra = new ServidorDiscovery(porta, true);
      discExtra.configurarInfo(idNo, idShard, "ATIVO", portaTcpAnunciada, portaUdpAnunciada);
      if (roteador != null) {
        discExtra.setTabelaLideres(roteador.getTabelaLideres());
      }
      servicosDiscovery.add(discExtra);
      new Thread(discExtra, "Thread-ServidorDiscovery-" + sufixoThread + "-" + porta).start();
    } catch (Exception ignored) {
    }
  }

  /**
   * Inicia dinamicamente as portas de servico quando o no assume o papel de ATIVO em seu Shard.
   * No failover de um Standby, tenta reabrir proativamente (com setReuseAddress(true)) tanto as
   * portas pioneiras do seu Shard quanto as portas legadas/universais (5000, 6789, 7777, 5001/6001),
   * garantindo continuidade transparente mesmo para clientes de terceiros que ignorem SERVER_CHANGED.
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

    Set<Integer> portasTcpAbertas = new HashSet<>();
    Set<Integer> portasUdpAbertas = new HashSet<>();
    Set<Integer> portasDiscoveryAbertas = new HashSet<>();

    int shardSeguro = Math.max(0, idShard);

    // 1. Servico TCP principal (5000 + ID)
    portasTcpAbertas.add(portaTcp);
    ServidorTCP tcpPrincipal = new ServidorTCP(portaTcp, gerenciador, roteador, false);
    servicosTCP.add(tcpPrincipal);
    new Thread(tcpPrincipal, "Thread-ServidorTCP-" + portaTcp).start();

    // 2. Servico UDP principal (6000 + ID)
    portasUdpAbertas.add(portaUdp);
    final ServidorUDP udpPrincipal = new ServidorUDP(portaUdp, gerenciador, roteador, false);
    servicosUDP.add(udpPrincipal);
    new Thread(udpPrincipal, "Thread-ServidorUDP-" + portaUdp).start();
    if (roteador != null) {
      roteador.setReceptorUdp((apdu, ipCliente, portaUdpCliente, saltos) ->
          udpPrincipal.receberPacoteRoteado(apdu, ipCliente, portaUdpCliente, saltos));
    }

    // 3. Servicos Discovery (8888 padrao + 8888+idShard + 5001 universal EDEN)
    portasDiscoveryAbertas.add(portaDiscovery);
    ServidorDiscovery discPrincipal = new ServidorDiscovery(portaDiscovery, true);
    discPrincipal.configurarInfo(idNo, shardSeguro, "ATIVO", portaTcp, portaUdp);
    if (roteador != null) {
      discPrincipal.setTabelaLideres(roteador.getTabelaLideres());
    }
    servicosDiscovery.add(discPrincipal);
    new Thread(discPrincipal, "Thread-ServidorDiscovery-" + portaDiscovery).start();

    int portaDiscoveryColegas = Protocolo.PORTA_DISCOVERY + shardSeguro;
    abrirDiscoveryExtra(portasDiscoveryAbertas, portaDiscoveryColegas, idNo, shardSeguro, portaTcp, portaUdp, "Shard");
    abrirDiscoveryExtra(portasDiscoveryAbertas, Protocolo.PORTA_DISCOVERY_EDEN, idNo, shardSeguro, portaTcp, portaUdp, "Eden");

    // 4. Portas Pioneiras de Fallback do proprio Shard (5001/6001 p/ S0, 5002/6002 p/ S1, 5003/6003 p/ S2):
    // Quando uma replica Standby (ex: No 4, 5 ou 6) assume a lideranca apos a queda do pioneiro,
    // reabre imediatamente as portas pioneiras do seu shard para absorver clientes que nao migraram porta.
    int portaPioneiraTcp = PortasShard.tcp(shardSeguro, 0);
    int portaPioneiraUdp = PortasShard.udp(shardSeguro, 0);
    abrirTcpExtra(portasTcpAbertas, portaPioneiraTcp, "Pioneiro-S" + shardSeguro);
    abrirUdpExtra(portasUdpAbertas, portaPioneiraUdp, "Pioneiro-S" + shardSeguro);

    // 5. Portas Interoperaveis por Shard (6789+shard TCP / 7777+shard UDP)
    int portaTcpColegas = Protocolo.PORTA_SERVIDOR_TCP + shardSeguro;
    int portaUdpColegas = Protocolo.PORTA_SERVIDOR_UDP + shardSeguro;
    abrirTcpExtra(portasTcpAbertas, portaTcpColegas, "Interoperavel");
    abrirUdpExtra(portasUdpAbertas, portaUdpColegas, "Interoperavel");

    // 6. Portas Universais / Legadas de Fallback (5000 TCP/UDP, 6789 TCP, 7777 UDP, 5001 TCP / 6001 UDP):
    // - No Shard 0 (ou modo legado), tenta abrir sempre todas as portas universais.
    // - Nos Shards 1 e 2, tenta abrir proativamente cada par universal caso o Shard 0 nao esteja escutando
    //   localmente na respectiva porta TCP (evitando sequestro de UDP em localhost via SO_REUSEADDR no Linux,
    //   mas assumindo imediatamente o Gateway universal se o S0 inteiro cair ou estiver em outra maquina!).
    if (incluirLegado || shardSeguro == 0) {
      abrirTcpExtra(portasTcpAbertas, Protocolo.PORTA_SERVIDOR_LEGACY, "Legado");
      abrirUdpExtra(portasUdpAbertas, Protocolo.PORTA_SERVIDOR_LEGACY, "Legado");
      abrirTcpExtra(portasTcpAbertas, Protocolo.PORTA_SERVIDOR_TCP, "Universal");
      abrirUdpExtra(portasUdpAbertas, Protocolo.PORTA_SERVIDOR_UDP, "Universal");
      abrirTcpExtra(portasTcpAbertas, 5001, "Universal-S0");
      abrirUdpExtra(portasUdpAbertas, 6001, "Universal-S0");
    } else {
      if (ConfigCluster.isPortaDisponivel("0.0.0.0", Protocolo.PORTA_SERVIDOR_LEGACY)) {
        abrirTcpExtra(portasTcpAbertas, Protocolo.PORTA_SERVIDOR_LEGACY, "Fallback-Legado");
        abrirUdpExtra(portasUdpAbertas, Protocolo.PORTA_SERVIDOR_LEGACY, "Fallback-Legado");
      }
      if (ConfigCluster.isPortaDisponivel("0.0.0.0", Protocolo.PORTA_SERVIDOR_TCP)) {
        abrirTcpExtra(portasTcpAbertas, Protocolo.PORTA_SERVIDOR_TCP, "Fallback-Universal");
        abrirUdpExtra(portasUdpAbertas, Protocolo.PORTA_SERVIDOR_UDP, "Fallback-Universal");
      }
    }

    this.servicosAtivos = true;
    this.gerenciador.setAtivo(true);
    System.out.println("=== SERVICOS DE ATENDIMENTO ABERTOS (SHARD S" + shardSeguro + " | PAPEL: ATIVO) ===");
    imprimirEnderecosLocais();
    System.out.println("TCP escutando em " + portaTcp + " | Todas TCP tentadas: " + portasTcpAbertas);
    System.out.println("UDP escutando em " + portaUdp + " | Todas UDP tentadas: " + portasUdpAbertas);
    System.out.println("Discovery escutando em " + portasDiscoveryAbertas);
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
