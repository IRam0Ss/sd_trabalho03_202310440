import cluster.ConfigCluster;
import cluster.Particionador;
import cluster.PortasShard;

/** Teste das Fases 1 e 2: particao por letra e formula de portas. Uso: java -cp Servidor/bin:testes TesteParticionador */
public class TesteParticionador {
  static int falhas = 0;

  static ConfigCluster criarConfigShard(int idShard, int idReplica, int totalReplicas) {
    cluster.Particionador.validarShard(idShard);
    int meuId = PortasShard.idDoNo(idShard, idReplica);
    java.util.Map<Integer, ConfigCluster.NoInfo> mapa = new java.util.HashMap<>();
    for (int r = 0; r < totalReplicas; r++) {
      int id = PortasShard.idDoNo(idShard, r);
      mapa.put(id, new ConfigCluster.NoInfo(id, idShard, r, "127.0.0.1",
          PortasShard.clusterDoNo(id), PortasShard.tcpDoNo(id), PortasShard.udpDoNo(id), 8888));
    }
    return new ConfigCluster(meuId, idShard, idReplica, mapa);
  }

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
    check("TCP 0/0 == 5001 (No 1)", PortasShard.tcp(0, 0) == 5001);
    check("TCP 1/1 == 5005 (No 5)", PortasShard.tcp(1, 1) == 5005);
    check("UDP 2/1 == 6006 (No 6)", PortasShard.udp(2, 1) == 6006);
    check("Cluster 2/0 == 7003 (No 3)", PortasShard.cluster(2, 0) == 7003);
    java.util.Set<Integer> usadas = new java.util.HashSet<>();
    for (int s = 0; s < 3; s++) for (int r = 0; r < 2; r++) {
      usadas.add(PortasShard.tcp(s, r)); usadas.add(PortasShard.udp(s, r)); usadas.add(PortasShard.cluster(s, r));
    }
    check("18 portas distintas para 6 nos", usadas.size() == 18);
    ConfigCluster c = criarConfigShard(1, 1, 2);
    check("config shard=1, so conhece 2 nos do proprio shard", c.getIdShard() == 1 && c.getNos().size() == 2);
    check("config replica=1 / id global=5", c.getIdReplica() == 1 && c.getMeuId() == 5);
    check("config porta tcp = 5005", c.getMeuNoInfo().portaTcp == 5005);
    boolean erro = false;
    try { criarConfigShard(3, 0, 2); } catch (IllegalArgumentException e) { erro = true; }
    check("shard 3 rejeitado", erro);
    System.out.println(falhas == 0 ? "\nTODOS OS TESTES PASSARAM" : "\nFALHAS: " + falhas);
    System.exit(falhas == 0 ? 0 : 1);
  }
}

