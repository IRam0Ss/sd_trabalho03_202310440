/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 07/11/2026
* Nome...............: Particionador
* Funcao.............: Funcao de particao da ED em 3 shards pela letra inicial: [A..I], [J..R], [S..Z].
*************************************************************** */

package cluster;

import java.text.Normalizer;

/**
 * Particionador estatico e deterministico da estrutura de dados (T#03).
 * Regras:
 * <ul>
 *   <li>Shard 0: nomes iniciados por A..I</li>
 *   <li>Shard 1: nomes iniciados por J..R</li>
 *   <li>Shard 2: nomes iniciados por S..Z</li>
 *   <li>Qualquer outro caractere inicial (numeros, simbolos, vazio, nulo) vai FIXAMENTE para o Shard 0.</li>
 * </ul>
 * A chave e normalizada: trim, remocao de '@' inicial, minusculas e decomposicao NFD sem acentos
 * (caracteres acentuados ou cedilha sao mapeados para a letra base ASCII antes do calculo).
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public final class Particionador {

  /** Quantidade fixa de shards (sem rebalanceamento dinamico). */
  public static final int NUM_SHARDS = 3;

  /** Shard destinado a chaves cuja inicial nao e uma letra A-Z. */
  public static final int SHARD_PADRAO = 0;

  private static final String[] FAIXAS = { "A..I", "J..R", "S..Z" };

  private Particionador() {
  }

  /**
   * Normaliza uma chave (nome de usuario ou grupo) para uso na particao.
   * 
   * @return chave normalizada (possivelmente vazia, nunca nula).
   */
  public static String normalizar(String chave) {
    if (chave == null) {
      return "";
    }
    String s = chave.trim();
    while (s.startsWith("@")) {
      s = s.substring(1).trim();
    }
    s = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
    return s.toLowerCase();
  }

  /**
   * Calcula o shard responsavel por uma chave.
   * 
   * @param chave nome do usuario ou do grupo.
   * @return 0, 1 ou 2.
   */
  public static int shardDe(String chave) {
    String n = normalizar(chave);
    if (n.isEmpty()) {
      return SHARD_PADRAO;
    }
    char c = n.charAt(0);
    if (c >= 'a' && c <= 'i') {
      return 0;
    }
    if (c >= 'j' && c <= 'r') {
      return 1;
    }
    if (c >= 's' && c <= 'z') {
      return 2;
    }
    return SHARD_PADRAO;
  }

  /** Retorna a faixa textual do shard, ex.: "A..I". */
  public static String faixa(int shard) {
    validarShard(shard);
    return FAIXAS[shard];
  }

  /** Retorna o nome amigavel do shard, ex.: "S1 [J..R]". */
  public static String nome(int shard) {
    return "S" + shard + " [" + faixa(shard) + "]";
  }

  /** Verifica se a chave pertence ao shard informado. */
  public static boolean pertence(String chave, int shard) {
    return shardDe(chave) == shard;
  }

  /**
   * Valida se o identificador de shard pertence ao intervalo valido {@code [0, NUM_SHARDS - 1]}.
   *
   * @param shard Indice do shard a validar.
   * @throws IllegalArgumentException Caso o indice informado seja invalido.
   */
  public static void validarShard(int shard) {
    if (shard < 0 || shard >= NUM_SHARDS) {
      throw new IllegalArgumentException("ID_SHARD invalido: " + shard + " (esperado 0.." + (NUM_SHARDS - 1) + ")");
    }
  }

  /** Descricao multi-linha da tabela de particao, para o console. */
  public static String descricao() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < NUM_SHARDS; i++) {
      sb.append("  Shard ").append(i).append(" -> letras ").append(FAIXAS[i]);
      if (i == SHARD_PADRAO) {
        sb.append(" (+ numeros/simbolos)");
      }
      sb.append('\n');
    }
    return sb.toString();
  }
}

