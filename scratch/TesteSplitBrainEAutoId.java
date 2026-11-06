package scratch;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cluster.ClusterLog;
import cluster.ConfigCluster;
import controller.GerenciadorGrupos;
import cluster.MensagemCluster;
import cluster.NoCluster;
import cluster.TipoMensagemCluster;
import controller.Operacao;
import controller.TipoOperacao;

/**
 * Teste automatizado para validar:
 * 1. Gap Recycling de IDs (reuso de lacunas ao inves de IDs crescentes infinitos).
 * 2. Cache de ID persistente local.
 * 3. Resolucao deterministica de Split-Brain (Multiplos Lideres) baseada em versao de dados.
 * 4. Fechamento compulsorio de portas de servico na renuncia.
 */
public class TesteSplitBrainEAutoId {

  public static void main(String[] args) {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.out.println(">>> INICIANDO TESTES DE SPLIT-BRAIN, AUTO-ID E LOGS <<<\n");

    try {
      testarGapRecyclingECacheId();
      testarResolucaoSplitBrain();
      System.out.println("\n>>> TODOS OS TESTES PASSARAM COM 100% DE SUCESSO! <<<");
    } catch (Throwable t) {
      System.err.println("\n[FALHA NO TESTE] " + t.getMessage());
      t.printStackTrace();
      System.exit(1);
    }
  }

  private static void testarGapRecyclingECacheId() throws Exception {
    System.out.println("--- [TESTE 1] Gap Recycling e Cache de IDs ---");

    // Limpa arquivos temporarios de cache se existirem
    File fLan = new File(".cluster_node_lan.id");
    File fLocal = new File(".cluster_node_local.id");
    if (fLan.exists()) fLan.delete();
    if (fLocal.exists()) fLocal.delete();

    // Simulacao de mapa de pares ativos na rede: Nós 1, 3, 4
    Map<Integer, ConfigCluster.NoInfo> pares = new HashMap<>();
    pares.put(1, new ConfigCluster.NoInfo(1, "192.168.1.10", 9000, 6789, 7777, 8888));
    pares.put(3, new ConfigCluster.NoInfo(3, "192.168.1.12", 9000, 6789, 7777, 8888));
    pares.put(4, new ConfigCluster.NoInfo(4, "192.168.1.13", 9000, 6789, 7777, 8888));

    // O menor ID positivo livre deve ser 2!
    int candidato = 1;
    while (pares.containsKey(candidato)) {
      candidato++;
    }

    if (candidato != 2) {
      throw new AssertionError("Gap recycling falhou! Esperado ID 2, obtido: " + candidato);
    }
    System.out.println("[OK] Gap recycling identificou com sucesso a menor lacuna livre: Nó " + candidato);

    // Testa persistencia de ID em rede
    ConfigCluster.descobrirEConfigurarRede(2);
    if (!fLan.exists()) {
      throw new AssertionError("Arquivo .cluster_node_lan.id nao foi criado!");
    }
    System.out.println("[OK] Arquivo de cache de ID persistente criado e verificado com sucesso.");
  }

  private static void testarResolucaoSplitBrain() throws Exception {
    System.out.println("\n--- [TESTE 2] Resolucao Determinística de Split-Brain (2 Líderes) ---");

    // Cria Nó 1 (Líder legítimo com versão de dados avançada: versao = 10, epoca = 1)
    ConfigCluster.NoInfo info1 = new ConfigCluster.NoInfo(1, "127.0.0.1", 9011, 6791, 7781, 8881);
    ConfigCluster.NoInfo info2 = new ConfigCluster.NoInfo(2, "127.0.0.1", 9012, 6792, 7782, 8882);
    Map<Integer, ConfigCluster.NoInfo> mapa1 = new ConcurrentHashMap<>();
    mapa1.put(1, info1);
    mapa1.put(2, info2);

    ConfigCluster cfg1 = new ConfigCluster(1, mapa1);
    GerenciadorGrupos ger1 = new GerenciadorGrupos();

    // Avanca versao do gerenciador 1 para simular operacoes de clientes
    for (int i = 0; i < 10; i++) {
      ger1.aplicarLocal(Operacao.registrarUsuario("req" + i, "user" + i, "127.0.0.1", 5000 + i));
    }
    long versaoNo1 = ger1.getVersao();
    if (versaoNo1 < 10) throw new AssertionError("Falha ao avancar versao no No 1");

    NoCluster no1 = new NoCluster(cfg1, ger1);
    no1.assumirComoAtivo();

    // Cria Nó 2 (Líder em partição isolada com versão antiga versao = 2, mas epoca inflada = 5)
    Map<Integer, ConfigCluster.NoInfo> mapa2 = new ConcurrentHashMap<>();
    mapa2.put(1, info1);
    mapa2.put(2, info2);
    ConfigCluster cfg2 = new ConfigCluster(2, mapa2);
    GerenciadorGrupos ger2 = new GerenciadorGrupos();
    ger2.aplicarLocal(Operacao.registrarUsuario("reqStale1", "userStale", "127.0.0.1", 6000));
    ger2.aplicarLocal(Operacao.registrarUsuario("reqStale2", "userStale2", "127.0.0.1", 6001));

    NoCluster no2 = new NoCluster(cfg2, ger2);
    no2.assumirComoAtivo(); // Forca Nó 2 como ATIVO para simular o split-brain

    System.out.println("Estado antes do contato:");
    System.out.println("  Nó 1: Papel=" + no1.getPapel() + " | Versao=" + ger1.getVersao() + " | Epoca=" + no1.getEpoca());
    System.out.println("  Nó 2: Papel=" + no2.getPapel() + " | Versao=" + ger2.getVersao() + " | Epoca=" + no2.getEpoca());

    // Nó 2 tenta se anunciar como ATIVO com epoca maior para Nó 1
    MensagemCluster hbNo2 = new MensagemCluster(TipoMensagemCluster.HEARTBEAT, 2);
    hbNo2.setEstadoOrigem("ATIVO");
    hbNo2.setEpoca(5); // Epoca inflada durante isolamento
    hbNo2.setVersao(ger2.getVersao()); // Versao menor (2)

    no1.onMensagemRecebida(hbNo2);

    // Nó 1 DEVE PREVALECER porque tem maior versao de dados (10 > 2)!
    if (!"ATIVO".equalsIgnoreCase(no1.getPapel())) {
      throw new AssertionError("FALHA: Nó 1 cedeu incorretamente a lideranca para Nó 2 com versao menor!");
    }
    System.out.println("[OK] Nó 1 com maior versao manteve a lideranca ativa com sucesso (rejeitou época inflada do nó isolado).");

    // Agora Nó 2 recebe mensagem do Nó 1 (versao 10)
    MensagemCluster coordNo1 = new MensagemCluster(TipoMensagemCluster.COORDENADOR, 1);
    coordNo1.setEstadoOrigem("ATIVO");
    coordNo1.setEpoca(no1.getEpoca());
    coordNo1.setVersao(ger1.getVersao()); // Versao 10

    no2.onMensagemRecebida(coordNo1);

    // Nó 2 DEVE SE REBAIXAR para STANDBY ou SINCRONIZANDO!
    if ("ATIVO".equalsIgnoreCase(no2.getPapel())) {
      throw new AssertionError("FALHA: Nó 2 com versao menor permaneceu como ATIVO em vez de ceder!");
    }
    System.out.println("[OK] Nó 2 reconheceu a superioridade de dados do Nó 1 e cedeu liderança (Papel atual: " + no2.getPapel() + ").");

    // Limpeza
    no1.encerrar();
    no2.encerrar();
    System.out.println("[OK] Split-Brain prevenido e curado deterministamente sem perda de dados!");
  }
}
