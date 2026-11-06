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
 * Exige ou le interativamente {@code idShard} (0..2) e {@code idReplica} (0..N), aplicando:
 * <ul>
 *   <li>TCP        = 5000 + (idShard * 100) + idReplica</li>
 *   <li>UDP        = 6000 + (idShard * 100) + idReplica</li>
 *   <li>ClusterLAN = 7000 + (idShard * 100) + idReplica</li>
 * </ul>
 * Uso via linha de comando:
 * <pre>
 *   java -cp Servidor/bin Principal &lt;ID_SHARD 0..2&gt; &lt;ID_REPLICA 0..N&gt; [TOTAL_REPLICAS=2]
 *   java -cp Servidor/bin Principal --local &lt;ID_SHARD 0..2&gt; &lt;ID_REPLICA 0..N&gt; [TOTAL_REPLICAS=2]
 *   java -cp Servidor/bin Principal --rede  &lt;ID_SHARD 0..2&gt; &lt;ID_REPLICA 0..N&gt;
 * </pre>
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 5.0
 */
public class Principal {

  public static void main(String[] args) {
    System.setProperty("java.net.preferIPv4Stack", "true");

    Scanner scanner = new Scanner(System.in);
    ConfigCluster config = null;

    int idShard = -1;
    int idReplica = -1;
    int totalReplicas = 2;
    boolean modoRede = false;

    try {
      if (args.length == 0) {
        // Modo interativo T#03: solicita explicitamente idShard (0..2) e idReplica (0..N)
        imprimirBannerInicial();
        idShard = lerInteiroIntervalo(scanner,
            "Informe o ID do Shard [0=A..I | 1=J..R | 2=S..Z]: ", 0, Particionador.NUM_SHARDS - 1);
        mostrarDicaReplicasLocais(idShard);
        idReplica = lerInteiroIntervalo(scanner,
            "Informe o ID da Replica [0=Primaria, 1=Standby, ...]: ", 0, PortasShard.MAX_REPLICAS - 1);

        System.out.print("Modo de execucao [1=LOCAL 127.0.0.1 (Padrao) | 2=REDE LAN] (ENTER para LOCAL): ");
        String linhaModo = scanner.hasNextLine() ? scanner.nextLine().trim() : "";
        modoRede = ("2".equals(linhaModo) || "rede".equalsIgnoreCase(linhaModo) || "lan".equalsIgnoreCase(linhaModo));

      } else if ("--local".equalsIgnoreCase(args[0])) {
        modoRede = false;
        if (args.length >= 3 && ehInteiro(args[1]) && ehInteiro(args[2])) {
          idShard = Integer.parseInt(args[1]);
          idReplica = Integer.parseInt(args[2]);
          if (args.length >= 4 && ehInteiro(args[3])) {
            totalReplicas = Integer.parseInt(args[3]);
          }
        } else if (args.length == 1) {
          imprimirBannerInicial();
          idShard = lerInteiroIntervalo(scanner,
              "Informe o ID do Shard [0=A..I | 1=J..R | 2=S..Z]: ", 0, Particionador.NUM_SHARDS - 1);
          mostrarDicaReplicasLocais(idShard);
          idReplica = lerInteiroIntervalo(scanner,
              "Informe o ID da Replica [0=Primaria, 1=Standby, ...]: ", 0, PortasShard.MAX_REPLICAS - 1);
        } else {
          imprimirUsoESair("Parametros insuficientes para --local.");
        }

      } else if ("--rede".equalsIgnoreCase(args[0]) || "--lan".equalsIgnoreCase(args[0])) {
        modoRede = true;
        if (args.length >= 3 && ehInteiro(args[1]) && ehInteiro(args[2])) {
          idShard = Integer.parseInt(args[1]);
          idReplica = Integer.parseInt(args[2]);
        } else if (args.length == 1) {
          imprimirBannerInicial();
          idShard = lerInteiroIntervalo(scanner,
              "Informe o ID do Shard [0=A..I | 1=J..R | 2=S..Z]: ", 0, Particionador.NUM_SHARDS - 1);
          idReplica = lerInteiroIntervalo(scanner,
              "Informe o ID da Replica [0=Primaria, 1=Standby, ...]: ", 0, PortasShard.MAX_REPLICAS - 1);
        } else {
          imprimirUsoESair("Parametros insuficientes para --rede.");
        }

      } else if (args.length >= 2 && ehInteiro(args[0]) && ehInteiro(args[1])) {
        // Uso direto padrao: java Principal <ID_SHARD> <ID_REPLICA> [TOTAL_REPLICAS]
        idShard = Integer.parseInt(args[0]);
        idReplica = Integer.parseInt(args[1]);
        if (args.length >= 3 && ehInteiro(args[2])) {
          totalReplicas = Integer.parseInt(args[2]);
        }

      } else {
        imprimirUsoESair("Argumentos invalidos: informe obrigatoriamente <ID_SHARD 0..2> e <ID_REPLICA 0..N>.");
      }

      // Valida limites antes de instalar o log
      Particionador.validarShard(idShard);
      if (idReplica < 0 || idReplica >= PortasShard.MAX_REPLICAS) {
        throw new IllegalArgumentException(
            "ID_REPLICA fora do intervalo permitido: " + idReplica + " (esperado 0.." + (PortasShard.MAX_REPLICAS - 1) + ")");
      }

      // Instala log estruturado com prefixo [SHARD-x | REPLICA-y | PAPEL] e cores ANSI
      LogEstruturado.instalar(idShard, idReplica);

      if (modoRede) {
        config = ConfigCluster.configurarRedeShard(idShard, idReplica);
      } else {
        config = ConfigCluster.gerarConfigShard(idShard, idReplica, totalReplicas);
      }

    } catch (Exception e) {
      System.err.println("[CONFIG] [ERRO] Falha ao inicializar servidor: " + e.getMessage());
      imprimirUsoESair(null);
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

  private static void mostrarDicaReplicasLocais(int idShard) {
    StringBuilder emUso = new StringBuilder();
    int primeiraLivre = -1;
    for (int r = 0; r < 6; r++) {
      boolean ativa = ConfigCluster.isPortaEmUso("127.0.0.1", PortasShard.cluster(idShard, r));
      if (ativa) {
        if (emUso.length() > 0) emUso.append(", ");
        emUso.append("R").append(r);
      } else if (primeiraLivre < 0) {
        primeiraLivre = r;
      }
    }
    if (emUso.length() > 0) {
      System.out.println("  [Dica Shard S" + idShard + "] Replicas ja ativas localmente: " + emUso
          + " | Proxima replica livre sugerida: " + primeiraLivre);
    } else {
      System.out.println("  [Dica Shard S" + idShard + "] Nenhuma replica ativa localmente. Sugestao para lider primario: 0");
    }
  }

  private static int lerInteiroIntervalo(Scanner scanner, String prompt, int min, int max) {
    while (true) {
      System.out.print(prompt);
      if (!scanner.hasNextLine()) {
        imprimirUsoESair("Entrada padrao encerrada sem informar parametros obrigatorios.");
      }
      String linha = scanner.nextLine().trim();
      if (ehInteiro(linha)) {
        int val = Integer.parseInt(linha);
        if (val >= min && val <= max) {
          return val;
        }
      }
      System.out.println("  Valor invalido! Digite um numero inteiro entre " + min + " e " + max + ".");
    }
  }

  private static void imprimirUsoESair(String motivo) {
    if (motivo != null && !motivo.isEmpty()) {
      System.err.println("[CONFIG] [ERRO] " + motivo);
    }
    System.err.println("\nUso correto (T#03 - 3 Shards x N Replicas):");
    System.err.println("  java -cp Servidor/bin Principal <ID_SHARD 0..2> <ID_REPLICA 0..N> [TOTAL_REPLICAS=2]");
    System.err.println("  Exemplos para subir os 3 lideres independentes (S0, S1 e S2):");
    System.err.println("    java -cp Servidor/bin Principal 0 0   # Shard S0 [A..I] -> TCP=5000, UDP=6000, Cluster=7000");
    System.err.println("    java -cp Servidor/bin Principal 1 0   # Shard S1 [J..R] -> TCP=5100, UDP=6100, Cluster=7100");
    System.err.println("    java -cp Servidor/bin Principal 2 0   # Shard S2 [S..Z] -> TCP=5200, UDP=6200, Cluster=7200");
    System.err.println("  Exemplos para subir os 3 standbys:");
    System.err.println("    java -cp Servidor/bin Principal 0 1   # Shard S0 [A..I] -> TCP=5001, UDP=6001, Cluster=7001");
    System.err.println("    java -cp Servidor/bin Principal 1 1   # Shard S1 [J..R] -> TCP=5101, UDP=6101, Cluster=7101");
    System.err.println("    java -cp Servidor/bin Principal 2 1   # Shard S2 [S..Z] -> TCP=5201, UDP=6201, Cluster=7201\n");
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
    System.out.println("  status / cluster - Exibe tabela visual do shard local, papeis, epocas, versoes e saude.");
    System.out.println("  estado           - Exibe dump da estrutura de dados replicada neste shard.");
    System.out.println("  lideres          - Exibe a tabela de lideres dos 3 shards (S0, S1, S2).");
    System.out.println("  rotear <s> <usr> - Depuracao: registra <usr> no shard <s> via roteamento inter-shard.");
    System.out.println("  shards           - Exibe a tabela de particao (A..I, J..R, S..Z) e as portas do shard local.");
    System.out.println("  ajuda            - Mostra esta mensagem de ajuda.");
    System.out.println("  sair             - Encerra este no graciosamente com failover imediato no shard.\n");
  }

}
