import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import utils.Particionador;
import utils.Protocolo;
import utils.InfoUser;
import Protocol.APDU;
import model.GerenciadorConexao;
import model.GerenciadorConexao.EnderecoServidor;
import model.ClienteTCP;
import model.ClienteUDP;

/**
 * Teste automatizado de verificacao do Smart Client e Zero-Config do T#03.
 */
public class TesteSmartClient {

  public static void main(String[] args) {
    System.out.println("==========================================================");
    System.out.println("   TESTE AUTOMATIZADO - SMART CLIENT & ZERO-CONFIG (T#03) ");
    System.out.println("==========================================================\n");

    int falhas = 0;

    // 1. Teste de Particionamento do Smart Client
    falhas += testarParticionamento();

    // 2. Teste de Filtragem do Discovery UDP do Smart Client
    falhas += testarFiltroDiscoveryUDP();

    if (falhas == 0) {
      System.out.println("\n\u2705 TODOS OS TESTES UNITARIOS DO SMART CLIENT PASSARAM COM SUCESSO!");
    } else {
      System.err.println("\n\u274C OCORRERAM " + falhas + " FALHAS NOS TESTES DO SMART CLIENT!");
      System.exit(1);
    }
  }

  private static int testarParticionamento() {
    System.out.println("--- 1. Validando Particionador.shardDe(nome) no Cliente ---");
    int erros = 0;

    Map<String, Integer> casos = new LinkedHashMap<>();
    // Shard 0: [A..I] + caracteres especiais/numeros
    casos.put("Alice", 0);
    casos.put("alice", 0);
    casos.put("Bruno", 0);
    casos.put("Igor", 0);
    casos.put("igor", 0);
    casos.put("123Agente", 0);
    casos.put("#hashtag", 0);
    casos.put("!admin", 0);
    casos.put("@servidor", 2); // '@' e removido como mencao, 'servidor' comeca com 's' (Shard 2)
    casos.put("@alice", 0);    // '@' e removido, 'alice' comeca com 'a' (Shard 0)
    casos.put("", 0);
    casos.put(null, 0);

    // Shard 1: [J..R]
    casos.put("Julia", 1);
    casos.put("julia", 1);
    casos.put("Lucas", 1);
    casos.put("Rafael", 1);
    casos.put("rafael", 1);

    // Shard 2: [S..Z]
    casos.put("Sabrina", 2);
    casos.put("sabrina", 2);
    casos.put("Tiago", 2);
    casos.put("Zeca", 2);
    casos.put("zeca", 2);

    for (Map.Entry<String, Integer> entry : casos.entrySet()) {
      String nome = entry.getKey();
      int esperado = entry.getValue();
      int obtido = Particionador.shardDe(nome);
      if (obtido != esperado) {
        System.err.println("  [FALHA] shardDe(\"" + nome + "\") esperava " + esperado + ", mas obteve " + obtido);
        erros++;
      } else {
        System.out.println("  [OK] '" + nome + "' -> Shard S" + obtido + " [" + Particionador.faixa(obtido) + "]");
      }
    }

    return erros;
  }

  private static int testarFiltroDiscoveryUDP() {
    System.out.println("\n--- 2. Validando Filtragem do Farol Discovery (EDEN_CLUSTER:...:<shard>) ---");
    int erros = 0;

    // Simula respostas de farois UDP dos 3 Shards
    String beaconS0 = "EDEN_CLUSTER:1:ATIVO:5000:6000:0";
    String beaconS1 = "EDEN_CLUSTER:2:ATIVO:5100:6100:1";
    String beaconS2 = "EDEN_CLUSTER:3:ATIVO:5200:6200:2";

    EnderecoServidor endS0 = GerenciadorConexao.parseRespostaDiscoveryCluster(beaconS0, "127.0.0.1");
    EnderecoServidor endS1 = GerenciadorConexao.parseRespostaDiscoveryCluster(beaconS1, "127.0.0.1");
    EnderecoServidor endS2 = GerenciadorConexao.parseRespostaDiscoveryCluster(beaconS2, "127.0.0.1");

    if (endS0 == null || endS0.idShard != 0 || endS0.portaTcp != 5000) {
      System.err.println("  [FALHA] Parser falhou ao extrair Shard S0: " + endS0);
      erros++;
    } else {
      System.out.println("  [OK] Parser Shard S0: " + endS0);
    }

    if (endS1 == null || endS1.idShard != 1 || endS1.portaTcp != 5100) {
      System.err.println("  [FALHA] Parser falhou ao extrair Shard S1: " + endS1);
      erros++;
    } else {
      System.out.println("  [OK] Parser Shard S1: " + endS1);
    }

    if (endS2 == null || endS2.idShard != 2 || endS2.portaTcp != 5200) {
      System.err.println("  [FALHA] Parser falhou ao extrair Shard S2: " + endS2);
      erros++;
    } else {
      System.out.println("  [OK] Parser Shard S2: " + endS2);
    }

    // Testa filtro Smart Client: para usuario "Lucas", alvo = 1. Farois de S0 e S2 devem ser descartados.
    int shardLucas = Particionador.shardDe("Lucas");
    boolean aceitaS0 = (endS0.idShard == shardLucas);
    boolean aceitaS1 = (endS1.idShard == shardLucas);
    boolean aceitaS2 = (endS2.idShard == shardLucas);

    if (aceitaS0 || !aceitaS1 || aceitaS2) {
      System.err.println("  [FALHA] Logica de descarte de shards para Lucas (S1) falhou!");
      erros++;
    } else {
      System.out.println("  [OK] Smart Client para 'Lucas' (S1): S0 descartado=" + !aceitaS0
          + ", S1 aceito=" + aceitaS1 + ", S2 descartado=" + !aceitaS2);
    }

    // Testa filtro Smart Client: para usuario "Zeca", alvo = 2. Farois de S0 e S1 devem ser descartados.
    int shardZeca = Particionador.shardDe("Zeca");
    boolean zAceitaS0 = (endS0.idShard == shardZeca);
    boolean zAceitaS1 = (endS1.idShard == shardZeca);
    boolean zAceitaS2 = (endS2.idShard == shardZeca);

    if (zAceitaS0 || zAceitaS1 || !zAceitaS2) {
      System.err.println("  [FALHA] Logica de descarte de shards para Zeca (S2) falhou!");
      erros++;
    } else {
      System.out.println("  [OK] Smart Client para 'Zeca' (S2): S0 descartado=" + !zAceitaS0
          + ", S1 descartado=" + !zAceitaS1 + ", S2 aceito=" + zAceitaS2);
    }

    return erros;
  }
}
