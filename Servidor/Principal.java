/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: Principal
* Funcao.............: Ponto de entrada do Servidor com suporte a cluster dinamico, auto-atribuicao de ID e console interativo.
*************************************************************** */

import java.util.Scanner;

import cluster.BeaconClusterLAN;
import cluster.ClusterLog;
import cluster.ConfigCluster;
import cluster.NoCluster;
import cluster.Particionador;
import cluster.VisaoCluster;
import controller.AtendimentoCliente;
import controller.GerenciadorGrupos;
import utils.LogEstruturado;

/**
 * Ponto de entrada do servidor E.D.E.N. com suporte a redundancia (N servidores).
 * Oferece atribuicao automatica de ID (sem necessidade de digitacao manual),
 * suporte a cluster aberto e infinito, e comandos interativos de depuracao:
 * status, estado, ajuda e sair.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 4.0
 */
public class Principal {

  public static void main(String[] args) {
    System.setProperty("java.net.preferIPv4Stack", "true");

    ConfigCluster config = null;
    BeaconClusterLAN beaconLAN = null;
    Scanner scanner = new Scanner(System.in);

    try {
      if (args.length >= 2 && ehInteiro(args[0]) && ehInteiro(args[1])) {
        // Modo T#03: java Principal <ID_SHARD> <ID_REPLICA> [TOTAL_REPLICAS=2]
        int idShard = Integer.parseInt(args[0]);
        int idReplica = Integer.parseInt(args[1]);
        int totalReplicas = (args.length >= 3 && ehInteiro(args[2])) ? Integer.parseInt(args[2]) : 2;
        LogEstruturado.instalar(idShard, idReplica);
        config = ConfigCluster.gerarConfigShard(idShard, idReplica, totalReplicas);

      } else if (args.length >= 1) {
        String flag = args[0].toLowerCase();

        if ("--local".equals(flag)) {
          Integer idForcado = (args.length >= 2) ? Integer.parseInt(args[1]) : null;
          if (args.length >= 3) {
            // Suporte legado a scripts de teste: --local <id> <total>
            int id = Integer.parseInt(args[1]);
            int total = Integer.parseInt(args[2]);
            config = ConfigCluster.gerarConfigLocal(id, total);
          } else {
            config = ConfigCluster.descobrirEConfigurarLocal(idForcado);
          }

        } else if ("--rede".equals(flag) || "--lan".equals(flag)) {
          Integer idForcado = (args.length >= 2) ? Integer.parseInt(args[1]) : null;
          config = ConfigCluster.descobrirEConfigurarRede(idForcado);

        } else {
          // Argumento numerico legado ou arquivo properties: <id> [arquivo]
          try {
            int id = Integer.parseInt(args[0]);
            String arquivo = (args.length >= 2) ? args[1] : "cluster.properties";
            config = ConfigCluster.carregarArquivo(arquivo, id);
          } catch (NumberFormatException nfe) {
            config = ConfigCluster.descobrirEConfigurarLocal(null);
          }
        }

      } else {
        // Modo interativo amigavel sem solicitar IDs ou quantidades manuais
        System.out.println("==================================================");
        System.out.println("        E.D.E.N. - SERVIDOR DISTRIBUIDO           ");
        System.out.println("==================================================");
        System.out.println("Selecione o modo de execucao:");
        System.out.println("  [1] REDE  - Maquinas distintas na rede local LAN (Padrao)");
        System.out.println("  [2] LOCAL - Mesma maquina (atribuicao automatica de portas e ID)");
        System.out.print("Escolha [1/2] (Pressione ENTER para REDE): ");

        String linha = scanner.hasNextLine() ? scanner.nextLine().trim() : "";
        if ("2".equals(linha) || "local".equalsIgnoreCase(linha)) {
          config = ConfigCluster.descobrirEConfigurarLocal(null);
        } else {
          config = ConfigCluster.descobrirEConfigurarRede(null);
        }
      }

    } catch (Exception e) {
      if (args.length >= 2 && ehInteiro(args[0]) && ehInteiro(args[1])) {
        System.err.println("[INICIO] [ERRO] Argumentos de shard invalidos: " + e.getMessage()
            + "\nUso: java Principal <ID_SHARD 0..2> <ID_REPLICA> [TOTAL_REPLICAS=2]");
        System.exit(1);
      }
      System.err.println("[INICIO] [AVISO] Falha ao processar argumentos (" + e.getMessage() + "). Assumindo auto-configuracao de rede.");
      config = ConfigCluster.descobrirEConfigurarRede(null);
    }

    GerenciadorGrupos gerenciador = new GerenciadorGrupos();
    NoCluster noCluster = new NoCluster(config, gerenciador);
    if (args.length >= 2 && ehInteiro(args[0]) && ehInteiro(args[1])) {
      LogEstruturado.definirFornecedorPapel(noCluster::getPapel);
      System.out.println("Shard " + Particionador.nome(config.getIdShard()) + " pronto. Tabela de particao:\n"
          + Particionador.descricao());
    }

    // Se estiver rodando em modo rede, ativa o beacon de broadcast UDP para novos nos
    if (!"127.0.0.1".equals(config.getMeuNoInfo().host)) {
      beaconLAN = new BeaconClusterLAN(config, new BeaconClusterLAN.ListenerNovoParLAN() {
        @Override
        public void onNovoParDescoberto(ConfigCluster.NoInfo novoPar) {
          noCluster.getVisao().adicionarParSeAusente(novoPar);
          noCluster.getCanal().adicionarParSeAusente(novoPar.id, novoPar.host, novoPar.portaCluster);
        }

        @Override
        public void onParSaindo(int idPar) {
          ClusterLog.info("BEACON:LAN", "No " + idPar + " anunciou saida via broadcast LAN.");
          noCluster.getVisao().marcarDown(idPar);
          if (idPar == noCluster.getIdLiderAtivo()) {
            ClusterLog.eleicao("Lider ativo anunciou saida da rede. Disparando eleicao imediata!");
            noCluster.dispararEleicao();
          }
        }
      });
      beaconLAN.iniciar();
    }

    final BeaconClusterLAN finalBeacon = beaconLAN;

    // Registra shutdown hook para saida graciosa e failover imediato
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("\n[SISTEMA] Sinal de encerramento detectado. Finalizando no...");
      if (finalBeacon != null) {
        finalBeacon.parar();
      }
      noCluster.encerrar();
    }));

    // Inicia o cluster
    noCluster.iniciar();

    System.out.println("\n[SISTEMA] Servidor iniciado com sucesso. Digite 'status' ou 'ajuda' para comandos de depuracao.\n");

    // Loop interativo de comandos de console para demonstracao e debug (D5)
    while (true) {
      try {
        if (!scanner.hasNextLine()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            break;
          }
          continue;
        }
        String linhaCmd = scanner.nextLine().trim();
        String comando = linhaCmd.toLowerCase();
        if (comando.isEmpty()) continue;

        if (comando.startsWith("rotear ")) {
          // Depuracao Fase 3: rotear <shard> <usuario> [idRequisicao]  (REGISTER roteado ao shard dono)
          String[] t = linhaCmd.split("\\s+");
          if (t.length >= 3 && ehInteiro(t[1])) {
            final int shardAlvo = Integer.parseInt(t[1]);
            final String usuario = t[2];
            final String idReq = (t.length >= 4) ? t[3] : "dbg-" + java.util.UUID.randomUUID();
            new Thread(() -> {
              controller.Resultado r = noCluster.getRoteador().executar(shardAlvo,
                  controller.Operacao.registrarUsuario(idReq, usuario, "127.0.0.1", 0));
              System.out.println("[ROTEAMENTO] RESULTADO shard=" + shardAlvo + " usuario=" + usuario + " -> "
                  + (r.isSucesso() ? "SUCESSO" : "FALHA") + " | " + r.getMensagem());
            }, "Thread-Console-Rotear").start();
          } else {
            System.out.println("Uso: rotear <shard 0..2> <usuario> [idRequisicao]");
          }
          continue;
        }

        switch (comando) {
          case "status":
          case "cluster":
            exibirStatus(noCluster, config, gerenciador);
            break;

          case "estado":
            gerenciador.imprimirEstado();
            break;

          case "lideres":
            noCluster.getRoteador().imprimirTabela();
            break;

          case "shards":
            exibirShards(config);
            break;

          case "ajuda":
          case "help":
            exibirAjuda();
            break;

          case "sair":
          case "exit":
          case "quit":
            System.out.println("[SISTEMA] Encerrando o servidor...");
            if (finalBeacon != null) finalBeacon.parar();
            noCluster.encerrar();
            System.exit(0);
            break;

          default:
            System.out.println("Comando desconhecido: '" + comando + "'. Digite 'ajuda' para a lista de comandos.");
            break;
        }

      } catch (Exception e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
  }

  private static boolean ehInteiro(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void exibirShards(ConfigCluster config) {
    System.out.println("Particao da ED (3 shards fixos):\n" + Particionador.descricao()
        + "Este no: shard " + config.getIdShard() + ", replica " + config.getIdReplica());
    for (ConfigCluster.NoInfo n : config.getNos().values()) {
      System.out.println("  replica " + n.idReplica + " -> TCP " + n.portaTcp + " | UDP " + n.portaUdp
          + " | Cluster " + n.portaCluster + (n.id == config.getMeuId() ? "  (este no)" : ""));
    }
  }

  private static void exibirStatus(NoCluster noCluster, ConfigCluster config, GerenciadorGrupos gerenciador) {
    ClusterLog.exibirTabelaStatus(
        noCluster.getMeuId(),
        noCluster.getPapel(),
        noCluster.getEpoca(),
        noCluster.getIdLiderAtivo(),
        config.getNos(),
        noCluster.getVisao().getTodosPares(),
        gerenciador
    );
  }

  private static void exibirAjuda() {
    System.out.println("\nComandos disponiveis no console:");
    System.out.println("  status / cluster - Exibe tabela visual do cluster, papeis, epocas, versoes e saude.");
    System.out.println("  estado           - Exibe dump da estrutura de dados replicada (usuarios, salas, ticks).");
    System.out.println("  lideres          - Exibe a tabela de lideres de cada shard (visao deste no).");
    System.out.println("  rotear <s> <usr> - Depuracao: registra <usr> no shard <s> via roteamento inter-shard.");
    System.out.println("  shards           - Exibe a tabela de particao (A..I, J..R, S..Z) e as portas do shard local.");
    System.out.println("  ajuda            - Mostra esta mensagem de ajuda.");
    System.out.println("  sair             - Encerra este no graciosamente com failover imediato para os outros.\n");
  }

}
