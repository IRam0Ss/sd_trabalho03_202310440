/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 16/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: ClusterLog
* Funcao.............: Utilitario de logging elegante com timestamps [HH:mm:ss], badges semanticos e controle anti-spam.
*************************************************************** */

package cluster;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utilitario de log padronizado para o cluster E.D.E.N.
 * Garante que todas as mensagens possuam marcacao temporal [HH:mm:ss],
 * identificadores semanticos visuais (LIDER, STANDBY, ELEICAO, REPLICACAO, SPLIT-BRAIN)
 * e supressao de repeticoes continuas de heartbeats.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class ClusterLog {

  private static final SimpleDateFormat FORMATO_HORA = new SimpleDateFormat("HH:mm:ss");
  private static final AtomicLong ultimoLogHbMs = new AtomicLong(0);
  private static int ultimoQtdVivos = -1;
  private static int ultimoLiderNotificado = -1;

  private static synchronized String timestamp() {
    return "[" + FORMATO_HORA.format(new Date()) + "]";
  }

  public static void info(String tag, String mensagem) {
    System.out.println(timestamp() + " [" + tag + "] " + mensagem);
  }

  public static void aviso(String tag, String mensagem) {
    System.out.println(timestamp() + " [" + tag + "] [AVISO] " + mensagem);
  }

  public static void erro(String tag, String mensagem) {
    System.err.println(timestamp() + " [" + tag + "] [ERRO] " + mensagem);
  }

  public static void erro(String tag, String mensagem, Throwable t) {
    System.err.println(timestamp() + " [" + tag + "] [ERRO] " + mensagem + (t != null ? " (" + t.getMessage() + ")" : ""));
  }

  public static void lider(String mensagem) {
    System.out.println(timestamp() + " [LIDER ATIVO] " + mensagem);
  }

  public static void standby(String mensagem) {
    System.out.println(timestamp() + " [STANDBY] " + mensagem);
  }

  public static void eleicao(String mensagem) {
    System.out.println(timestamp() + " [ELEICAO] " + mensagem);
  }

  public static void replicacao(String mensagem) {
    System.out.println(timestamp() + " [REPLICACAO] " + mensagem);
  }

  public static void conflito(String mensagem) {
    System.out.println(timestamp() + " [SPLIT-BRAIN] [CONFLITO] " + mensagem);
  }

  public static void cura(String mensagem) {
    System.out.println(timestamp() + " [SPLIT-BRAIN] [CURA] " + mensagem);
  }

  public static void isolamento(String mensagem) {
    System.out.println(timestamp() + " [ISOLAMENTO] " + mensagem);
  }

  /**
   * Registra o estado da saude dos heartbeats de forma throttled (a cada 10s ou quando mudar o cluster),
   * impedindo a poluicao da tela a cada segundo.
   */
  public static void heartbeatResumo(int totalConhecidos, int totalVivos, int idLider) {
    long agora = System.currentTimeMillis();
    boolean mudou = (totalVivos != ultimoQtdVivos || idLider != ultimoLiderNotificado);
    boolean timeoutDecorrido = (agora - ultimoLogHbMs.get() >= 10000);

    if (mudou || timeoutDecorrido) {
      ultimoLogHbMs.set(agora);
      ultimoQtdVivos = totalVivos;
      ultimoLiderNotificado = idLider;
      String liderStr = (idLider > 0) ? ("No " + idLider) : "NENHUM";
      System.out.println(timestamp() + " [HEARTBEAT] Cluster operacional: " + totalVivos + "/" + totalConhecidos
          + " nos ativos | Lider atual: " + liderStr);
    }
  }

  /**
   * Imprime uma tabela formatada com o status completo do cluster e de todos os pares.
   */
  public static void exibirTabelaStatus(int meuId, String meuPapel, long epoca, int idLider,
      Map<Integer, ConfigCluster.NoInfo> nos, Collection<VisaoCluster.ParEstado> pares,
      controller.GerenciadorGrupos gerenciador) {

    System.out.println();
    System.out.println("==========================================================================================");
    System.out.println("                         STATUS DO CLUSTER DISTRIBUIDO E.D.E.N.                          ");
    System.out.println("==========================================================================================");
    System.out.printf(" No Local: No %d %s | Papel: %s | Epoca: %d | Versao de Dados: %d\n",
        meuId, (meuId == idLider ? "[LIDER ATIVO]" : "[STANDBY]"), meuPapel, epoca, gerenciador.getVersao());
    System.out.printf(" Lider Conhecido: No %s | Total de Membros: %d\n",
        (idLider > 0 ? String.valueOf(idLider) : "NENHUM"), nos.size());
    System.out.println("------------------------------------------------------------------------------------------");
    System.out.printf("| %-4s | %-20s | %-9s | %-6s | %-7s | %-8s | %-12s |\n",
        "ID", "ENDERECO", "PAPEL", "EPOCA", "VERSAO", "SAUDE", "ULTIMO HB");
    System.out.println("------------------------------------------------------------------------------------------");

    // Imprime no local
    ConfigCluster.NoInfo meuInfo = nos.get(meuId);
    String hostLocal = (meuInfo != null) ? (meuInfo.host + ":" + meuInfo.portaCluster) : "local";
    System.out.printf("| %-4s | %-20s | %-9s | %-6d | %-7d | %-8s | %-12s |\n",
        (meuId + "*"), hostLocal, meuPapel, epoca, gerenciador.getVersao(), "LOCAL", "agora");

    // Imprime pares conhecidos
    for (VisaoCluster.ParEstado p : pares) {
      if (p.id == meuId) continue;
      long deltaHb = (p.ultimoHeartbeat > 0) ? (System.currentTimeMillis() - p.ultimoHeartbeat) : -1;
      String hbStr = (deltaHb >= 0) ? (deltaHb + "ms atras") : "nunca";
      String saude = p.isVivo() ? "ONLINE" : "DOWN";
      String idDisplay = (p.id == idLider) ? (p.id + " (L)") : String.valueOf(p.id);

      System.out.printf("| %-4s | %-20s | %-9s | %-6d | %-7d | %-8s | %-12s |\n",
          idDisplay, (p.host + ":" + p.portaCluster), p.estado, p.epoca, p.versao, saude, hbStr);
    }

    System.out.println("------------------------------------------------------------------------------------------");
    System.out.println(" Legenda: (*) No local | (L) Lider Ativo atual");
    System.out.println("==========================================================================================\n");
  }
}

