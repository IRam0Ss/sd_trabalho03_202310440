/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 06/11/2026
* Ultima alteracao...: 06/11/2026
* Nome...............: Particionador
* Funcao.............: Funcao de particao da ED no lado Cliente para conexao direta do Smart Client (S0, S1, S2).
*************************************************************** */

package utils;

import java.text.Normalizer;

/**
 * Utilitario de particionamento estatico e deterministico no lado do Cliente (T#03 - Smart Client).
 * Permite que o cliente calcule antecipadamente o Shard responsavel pelo seu cadastro
 * e direcione a conexao inicial diretamente ao lider correspondente:
 * <ul>
 *   <li>Shard 0: [A..I] (e qualquer caractere nao-letra)</li>
 *   <li>Shard 1: [J..R]</li>
 *   <li>Shard 2: [S..Z]</li>
 * </ul>
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public final class Particionador {

  public static final int NUM_SHARDS = 3;
  public static final int SHARD_PADRAO = 0;
  private static final String[] FAIXAS = { "A..I", "J..R", "S..Z" };

  private Particionador() {
  }

  /**
   * Normaliza uma chave (nome de usuario ou grupo) para calculo da particao.
   * 
   * @param chave Nome do usuario ou grupo.
   * @return Chave normalizada em caixa baixa sem acentos.
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
   * Calcula o Shard destino (0, 1 ou 2) a partir do nome de usuario ou grupo.
   * 
   * @param chave Nome do usuario ou grupo.
   * @return 0 para [A..I] / nao-letras, 1 para [J..R], 2 para [S..Z].
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

  public static String faixa(int shard) {
    validarShard(shard);
    return FAIXAS[shard];
  }

  public static String nome(int shard) {
    return "S" + shard + " [" + faixa(shard) + "]";
  }

  public static boolean pertence(String chave, int shard) {
    return shardDe(chave) == shard;
  }

  public static void validarShard(int shard) {
    if (shard < 0 || shard >= NUM_SHARDS) {
      throw new IllegalArgumentException("Shard invalido: " + shard + " (esperado 0, 1 ou 2)");
    }
  }
}
