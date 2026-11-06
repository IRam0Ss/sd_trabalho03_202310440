import cluster.ConfigCluster;
import cluster.Particionador;
import cluster.PortasShard;

/** Teste das Fases 1 e 2: particao por letra e formula de portas. Uso: java -cp Servidor/bin:scratch TesteParticionador */
public class TesteParticionador {
  static int falhas = 0;

  static void check(String desc, boolean ok) {
    System.out.println((ok ? "[OK]   " : "[FALHA] ") + desc);
    if (!ok) falhas++;
  }

  public static void main(String[] a) {
    String[][] casos = { {"Ana", "0"}, {"iury", "0"}, {"I", "0"}, {"Joao", "1"}, {"j", "1"}, {"Rita", "1"}, {"r", "1"},
        {"Sala", "2"}, {"s", "2"}, {"Zeca", "2"}, {"@Zeca", "2"}, {"  maria ", "1"}, {"Álvaro", "0"}, {"Çelso", "0"},
        {"Ítalo", "0"}, {"Ônix", "1"}, {"Úrsula", "2"}, {"123", "0"}, {"_x", "0"}, {"", "0"}, {null, "0"}, {"@", "0"} };
    for (String[] c : casos) {
      check("shardDe(" + c[0] + ") == " + c[1], Particionador.shardDe(c[0]) == Integer.parseInt(c[1]));
    }
    // Cobertura completa do alfabeto
    for (char ch = 'a'; ch <= 'z'; ch++) {
      int esperado = ch <= 'i' ? 0 : ch <= 'r' ? 1 : 2;
      check("letra " + ch, Particionador.shardDe("" + ch) == esperado && Particionador.shardDe(("" + ch).toUpperCase()) == esperado);
    }
    check("TCP 0/0 == 5000 (legado)", PortasShard.tcp(0, 0) == 5000);
    check("TCP 1/1 == 5101", PortasShard.tcp(1, 1) == 5101);
    check("UDP 2/1 == 6201", PortasShard.udp(2, 1) == 6201);
    check("Cluster 2/0 == 7200", PortasShard.cluster(2, 0) == 7200);
    java.util.Set<Integer> usadas = new java.util.HashSet<>();
    for (int s = 0; s < 3; s++) for (int r = 0; r < 2; r++) {
      usadas.add(PortasShard.tcp(s, r)); usadas.add(PortasShard.udp(s, r)); usadas.add(PortasShard.cluster(s, r));
    }
    check("18 portas distintas para 6 nos", usadas.size() == 18);
    ConfigCluster c = ConfigCluster.gerarConfigShard(1, 1, 2);
    check("config shard=1, so conhece 2 nos do proprio shard", c.getIdShard() == 1 && c.getNos().size() == 2);
    check("config replica=1 / id interno=2", c.getIdReplica() == 1 && c.getMeuId() == 2);
    check("config porta tcp = 5101", c.getMeuNoInfo().portaTcp == 5101);
    boolean erro = false;
    try { ConfigCluster.gerarConfigShard(3, 0, 2); } catch (IllegalArgumentException e) { erro = true; }
    check("shard 3 rejeitado", erro);
    System.out.println(falhas == 0 ? "\nTODOS OS TESTES PASSARAM" : "\nFALHAS: " + falhas);
    System.exit(falhas == 0 ? 0 : 1);
  }
}

