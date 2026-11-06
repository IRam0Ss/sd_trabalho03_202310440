/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: Principal
* Funcao.............: Ponto de entrada do Servidor T#03 com particionamento em 3 Shards e calculo dinamico de portas.
*************************************************************** */

import java.util.Scanner;
import java.util.UUID;

import cluster.BeaconClusterLAN;
import cluster.ClusterLog;
import cluster.ConfigCluster;
import cluster.NoCluster;
import cluster.Particionador;
import cluster.PortasShard;
import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;
import utils.LogEstruturado;

/**
 * Ponto de entrada do servidor E.D.E.N. (Trabalho 03 - Particionamento + Replicacao).
 * Filosofia Zero-Configuracao: descobre seu proprio ID automaticamente ao entrar na rede.
 * <ul>
 *   <li>Shard      = (ID - 1) % 3 (Ordem crescente: ID 1 -> S0 [A..I], ID 2 -> S1 [J..R], ID 3 -> S2 [S..Z], ID 4 -> S0 ...)</li>
 *   <li>Replica    = (ID - 1) / 3</li>
 *   <li>TCP        = 5000 + ID</li>
 *   <li>UDP        = 6000 + ID</li>
 *   <li>ClusterLAN = 7000 + ID</li>
 * </ul>
 * Uso:
 * <pre>
 *   java -cp Servidor/bin Principal               # 100% autonomo (Zero-Config)
 *   java -cp Servidor/bin Principal --local       # Forca host anunciado 127.0.0.1
 *   java -cp Servidor/bin Principal &lt;ID&gt;          # Opcional: forca ID especifico
 * </pre>
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 6.0
 */
public class Principal {

  public static void main(String[] args) {
    System.setProperty("java.net.preferIPv4Stack", "true");

    Scanner scanner = new Scanner(System.in);
    ConfigCluster config = null;

    Integer idForcado = null;
    boolean forcarLoopback = false;

    try {
      if (args != null && args.length > 0) {
        for (String arg : args) {
          if ("--local".equalsIgnoreCase(arg) || "--loopback".equalsIgnoreCase(arg)) {
            forcarLoopback = true;
          } else if ("--rede".equalsIgnoreCase(arg) || "--lan".equalsIgnoreCase(arg)) {
            forcarLoopback = false;
          } else if (ehInteiro(arg)) {
            idForcado = Integer.parseInt(arg);
          }
        }
      }

      // Inicializacao autonoma Zero-Config: descobre o menor ID livre ou usa o pioneiro 1
      config = ConfigCluster.descobrirEConfigurarAutomatico(idForcado, forcarLoopback);

      // Instala log estruturado com prefixo [SHARD-x | REPLICA-y | PAPEL] e cores ANSI
      LogEstruturado.instalar(config.getIdShard(), config.getIdReplica());

    } catch (Exception e) {
      System.err.println("[CONFIG] [ERRO] Falha ao inicializar servidor: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    GerenciadorGrupos gerenciador = new GerenciadorGrupos(config.getIdShard());
    NoCluster noCluster = new NoCluster(config, gerenciador);
    LogEstruturado.definirFornecedorPapel(noCluster::getPapel);

    System.out.println("Particao ativa neste processo: " + Particionador.nome(config.getIdShard()));

    // Ativa o Beacon UDP filtrado estritamente por idShard para auto-descoberta de replicas do MESMO shard
    final ConfigCluster finalConfig = config;
    BeaconClusterLAN beaconShard = new BeaconClusterLAN(config, new BeaconClusterLAN.ListenerNovoParLAN() {
      @Override
      public void onNovoParDescoberto(ConfigCluster.NoInfo novoPar) {
        if (novoPar == null || novoPar.idShard != finalConfig.getIdShard()) {
          return;
        }
        noCluster.getVisao().adicionarParSeAusente(novoPar);
        noCluster.getCanal().adicionarParSeAusente(novoPar.id, novoPar.host, novoPar.portaCluster);
      }

      @Override
      public void onParSaindo(int idPar) {
        ClusterLog.info("BEACON:SHARD", "Replica " + (idPar - 1) + " (No " + idPar + ") anunciou saida do Shard S"
            + finalConfig.getIdShard() + ".");
        noCluster.getVisao().marcarDown(idPar);
        if (idPar == noCluster.getIdLiderAtivo()) {
          ClusterLog.eleicao("Lider ativo do Shard S" + finalConfig.getIdShard() + " anunciou saida. Disparando eleicao imediata!");
          noCluster.dispararEleicao();
        }
      }

      @Override
      public void onUsuarioCaiu(String idEvento, String nomeUsuario) {
        noCluster.processarUsuarioCaiu(idEvento, nomeUsuario);
      }

      @Override
      public void onAnuncioShardLAN(int shardRemoto, int idNo, String papel, long epoca, int idLider, String hostRemoto) {
        noCluster.getRoteador().registrarAnuncioExternoLAN(shardRemoto, idNo, papel, epoca, idLider, hostRemoto);
      }
    });
    beaconShard.iniciar();

    // Registra shutdown hook para saida graciosa e failover imediato dentro do shard
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("\n[SISTEMA] Sinal de encerramento detectado. Finalizando no...");
      beaconShard.parar();
      noCluster.encerrar();
    }));

    // Inicia consenso intra-shard e roteador inter-shard
    noCluster.iniciar();

    System.out.println("[SISTEMA] Servidor iniciado. Digite 'status', 'lideres', 'shards', 'estado', 'ajuda' ou 'sair'.");

    // Loop interativo de comandos de console para demonstracao e debug
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
          // Depuracao Fase 3: rotear <shard> <usuario> [idRequisicao]
          String[] t = linhaCmd.split("\\s+");
          if (t.length >= 3 && ehInteiro(t[1])) {
            final int shardAlvo = Integer.parseInt(t[1]);
            final String usuario = t[2];
            final String idReq = (t.length >= 4) ? t[3] : "dbg-" + UUID.randomUUID();
            new Thread(() -> {
              Resultado r = noCluster.getRoteador().executar(shardAlvo,
                  Operacao.registrarUsuario(idReq, usuario, "127.0.0.1", 0));
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
            exibirStatus(noCluster, config, gerenciador);
            break;

          case "ajuda":
          case "help":
            exibirAjuda();
            break;

          case "sair":
          case "exit":
          case "quit":
            System.out.println("[SISTEMA] Encerrando o servidor...");
            beaconShard.parar();
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

  private static void imprimirBannerInicial() {
    System.out.println("==========================================================");
    System.out.println("   E.D.E.N. - SERVIDOR DISTRIBUIDO PARTICIONADO (T#03)    ");
    System.out.println("==========================================================");
    System.out.print(Particionador.descricao());
    System.out.println("----------------------------------------------------------");
  }

  private static void imprimirUsoESair(String motivo) {
    if (motivo != null && !motivo.isEmpty()) {
      System.err.println("[CONFIG] [ERRO] " + motivo);
    }
    System.err.println("\nUso (T#03 - Filosofia Zero-Configuracao):");
    System.err.println("  java -cp Servidor/bin Principal               # Inicializacao 100% autonoma");
    System.err.println("  java -cp Servidor/bin Principal --local       # Forca host anunciado 127.0.0.1");
    System.err.println("  java -cp Servidor/bin Principal <ID>          # Opcional: forca um ID especifico\n");
    System.exit(1);
  }

  private static boolean ehInteiro(String s) {
    if (s == null || s.isEmpty()) return false;
    try {
      Integer.parseInt(s);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void exibirShards(ConfigCluster config) {
    System.out.println("Particao da ED (3 shards fixos):\n" + Particionador.descricao()
        + "Este no: Shard S" + config.getIdShard() + " | Replica " + config.getIdReplica());
    for (ConfigCluster.NoInfo n : config.getNos().values()) {
      System.out.println("  Replica " + n.idReplica + " -> TCP=" + n.portaTcp + ", UDP=" + n.portaUdp
          + ", Cluster=" + n.portaCluster + (n.id == config.getMeuId() ? "  (este no)" : ""));
    }
  }

  private static void exibirStatus(NoCluster noCluster, ConfigCluster config, GerenciadorGrupos gerenciador) {
    ClusterLog.exibirDashboardClusterANSI(
        noCluster.getMeuId(),
        noCluster.getPapel(),
        noCluster.getEpoca(),
        noCluster.getIdLiderAtivo(),
        config.getNos(),
        noCluster.getVisao().getTodosPares(),
        gerenciador,
        noCluster.getRoteador()
    );
  }

  private static void exibirAjuda() {
    System.out.println("\nComandos disponiveis no console:");
    System.out.println("  status / shards  - Exibe tabela ASCII colorida (ANSI) dos 3 shards + saude da particao local.");
    System.out.println("  lideres          - Exibe tabela ASCII colorida (ANSI) dos lideres de S0, S1 e S2.");
    System.out.println("  estado           - Exibe dump da estrutura de dados replicada neste shard.");
    System.out.println("  rotear <s> <usr> - Depuracao: registra <usr> no shard <s> via roteamento inter-shard.");
    System.out.println("  ajuda            - Mostra esta mensagem de ajuda.");
    System.out.println("  sair             - Encerra este no graciosamente com failover imediato no shard.\n");
  }

}
