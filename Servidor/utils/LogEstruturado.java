/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 05/11/2026
* Ultima alteracao...: 05/11/2026
* Nome...............: LogEstruturado
* Funcao.............: Redireciona System.out/err prefixando TODA linha com [SHARD-x | REPLICA-y | PAPEL] e cores ANSI.
*************************************************************** */

package utils;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;

/**
 * Instala um filtro em System.out e System.err que, para cada linha emitida por QUALQUER
 * classe do servidor, adiciona o prefixo de identidade e o papel atual do no:
 * <pre>[SHARD-0 | REPLICA-1 | STANDBY] [21:34:00] [ELEICAO] ...</pre>
 * e colore a linha por semantica: vermelho (queda/erro/timeout), verde (lider eleito/sucesso/roteado),
 * amarelo (sync/replicacao), ciano (eleicao), magenta (split-brain).
 * Cores podem ser desligadas com -Dcores=false ou variavel NO_COLOR.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public final class LogEstruturado {

  private static final String RESET = "\u001B[0m";
  private static final String VERMELHO = "\u001B[31m";
  private static final String VERDE = "\u001B[32m";
  private static final String AMARELO = "\u001B[33m";
  private static final String MAGENTA = "\u001B[35m";
  private static final String CIANO = "\u001B[36m";
  private static final String NEGRITO = "\u001B[1m";

  private static volatile Supplier<String> fornecedorPapel = () -> "INICIANDO";
  private static volatile int idShard = -1;
  private static volatile int idReplica = -1;
  private static volatile boolean cores = !"false".equalsIgnoreCase(System.getProperty("cores"))
      && System.getenv("NO_COLOR") == null;
  private static boolean instalado = false;

  private LogEstruturado() {
  }

  /** Instala o filtro (idempotente). Deve ser chamado o mais cedo possivel no main. */
  public static synchronized void instalar(int shard, int replica) {
    idShard = shard;
    idReplica = replica;
    if (instalado) {
      return;
    }
    PrintStream outOriginal = System.out;
    PrintStream errOriginal = System.err;
    try {
      System.setOut(new PrintStream(new FiltroLinhas(outOriginal, false), true, "UTF-8"));
      System.setErr(new PrintStream(new FiltroLinhas(errOriginal, true), true, "UTF-8"));
    } catch (Exception e) {
      return;
    }
    instalado = true;
  }

  /** Define de onde ler o papel atual (ATIVO/STANDBY/...) a cada linha. */
  public static void definirFornecedorPapel(Supplier<String> fornecedor) {
    if (fornecedor != null) {
      fornecedorPapel = fornecedor;
    }
  }

  /** Prefixo puro (sem cor) usado em linhas e na GUI/relatorios. */
  public static String prefixo() {
    String papel;
    try {
      papel = fornecedorPapel.get();
    } catch (Exception e) {
      papel = "?";
    }
    return "[SHARD-" + idShard + " | REPLICA-" + idReplica + " | " + papel + "]";
  }

  private static String corDaLinha(String l, boolean erroStream) {
    String u = l.toUpperCase();
    if (u.contains("[ERRO]") || u.contains("[ERROR]") || u.contains("DOWN") || u.contains("TIMEOUT")
        || u.contains("CAIU") || u.contains("INACESSIVEL") || u.contains("FALHA") || u.contains("DESCONECTADO")
        || u.contains("INDISPONIVEL") || u.contains("EXCEPTION")) {
      return VERMELHO;
    }
    if (u.contains("[LIDER ATIVO]") || u.contains("ASSUMIU") || u.contains("ROTEAD") || u.contains("ROTEANDO")
        || u.contains("VOTOS OBTIDOS")) {
      return VERDE;
    }
    if (u.contains("[SPLIT-BRAIN]") || u.contains("[ISOLAMENTO]")) {
      return MAGENTA;
    }
    if (u.contains("[REPLICACAO]") || u.contains("SNAPSHOT") || u.contains("SINCRONIZ") || u.contains("REPLIC")) {
      return AMARELO;
    }
    if (u.contains("[ELEICAO]")) {
      return CIANO;
    }
    if (u.contains("[AVISO]") || u.contains("[WARNING]")) {
      return AMARELO;
    }
    if (u.contains("SUCESSO")) {
      return VERDE;
    }
    return erroStream ? VERMELHO : "";
  }

  private static String corDoPapel(String papel) {
    if ("ATIVO".equalsIgnoreCase(papel)) return VERDE;
    if ("STANDBY".equalsIgnoreCase(papel)) return CIANO;
    if ("CANDIDATO".equalsIgnoreCase(papel) || "SINCRONIZANDO".equalsIgnoreCase(papel)) return AMARELO;
    return MAGENTA;
  }

  /** Acumula bytes ate '\n' e emite a linha completa, prefixada, no stream original. */
  private static final class FiltroLinhas extends OutputStream {
    private final PrintStream destino;
    private final boolean erroStream;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    FiltroLinhas(PrintStream destino, boolean erroStream) {
      this.destino = destino;
      this.erroStream = erroStream;
    }

    @Override
    public synchronized void write(int b) {
      if (b == '\n') {
        emitir();
      } else {
        buffer.write(b);
      }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
      for (int i = off; i < off + len; i++) {
        write(b[i]);
      }
    }

    @Override
    public synchronized void flush() {
      // linhas parciais so sao emitidas ao receber '\n'
    }

    private void emitir() {
      String linha;
      try {
        linha = new String(buffer.toByteArray(), "UTF-8");
      } catch (Exception e) {
        linha = buffer.toString();
      }
      buffer.reset();
      if (linha.endsWith("\r")) {
        linha = linha.substring(0, linha.length() - 1);
      }
      String papel;
      try {
        papel = fornecedorPapel.get();
      } catch (Exception e) {
        papel = "?";
      }
      String pref = "[SHARD-" + idShard + " | REPLICA-" + idReplica + " | ";
      String saida;
      if (cores) {
        String cor = corDaLinha(linha, erroStream);
        saida = NEGRITO + pref + corDoPapel(papel) + papel + RESET + NEGRITO + "]" + RESET + " " + cor + linha + RESET;
      } else {
        saida = pref + papel + "] " + linha;
      }
      destino.println(saida);
      destino.flush();
    }
  }
}

