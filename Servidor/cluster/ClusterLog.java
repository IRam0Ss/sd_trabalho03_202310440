/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 02/11/2026
* Ultima alteracao...: 07/11/2026
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

  /** Registra mensagem informativa padronizada com tag semantica e timestamp. */
  public static void info(String tag, String mensagem) {
    System.out.println(timestamp() + " [" + tag + "] " + mensagem);
  }

  /** Registra mensagem de alerta padronizada com tag semantica e timestamp. */
  public static void aviso(String tag, String mensagem) {
    System.out.println(timestamp() + " [" + tag + "] [AVISO] " + mensagem);
  }

  /** Registra mensagem de erro na saida de erro padrao com tag semantica. */
  public static void erro(String tag, String mensagem) {
    System.err.println(timestamp() + " [" + tag + "] [ERRO] " + mensagem);
  }

  /** Registra mensagem de erro acompanhada do resumo da excecao capturada. */
  public static void erro(String tag, String mensagem, Throwable t) {
    System.err.println(timestamp() + " [" + tag + "] [ERRO] " + mensagem + (t != null ? " (" + t.getMessage() + ")" : ""));
  }

  /** Registra evento semantico associado ao papel de lider ativo da particao. */
  public static void lider(String mensagem) {
    System.out.println(timestamp() + " [LIDER ATIVO] " + mensagem);
  }

  /** Registra evento semantico associado ao papel de replica em espera (Standby). */
  public static void standby(String mensagem) {
    System.out.println(timestamp() + " [STANDBY] " + mensagem);
  }

  /** Registra evento semantico de coordenacao ou eleicao intra-shard. */
  public static void eleicao(String mensagem) {
    System.out.println(timestamp() + " [ELEICAO] " + mensagem);
  }

  /** Registra evento semantico de sincronizacao ou replicacao primaria-backup. */
  public static void replicacao(String mensagem) {
    System.out.println(timestamp() + " [REPLICACAO] " + mensagem);
  }

  /** Registra deteccao de divergencia ou dualidade temporaria de lideranca (Split-Brain). */
  public static void conflito(String mensagem) {
    System.out.println(timestamp() + " [SPLIT-BRAIN] [CONFLITO] " + mensagem);
  }

  /** Registra convergencia e resolucao automatica de conflito de lideranca. */
  public static void cura(String mensagem) {
    System.out.println(timestamp() + " [SPLIT-BRAIN] [CURA] " + mensagem);
  }

  /** Registra filtragem estrita de isolamento entre particoes distintas. */
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

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_BOLD = "\u001B[1m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_WHITE = "\u001B[37m";

  private static String c(String codigoAnsi, String texto) {
    if (!utils.LogEstruturado.isCoresAtivas()) {
      return texto;
    }
    return codigoAnsi + texto + ANSI_RESET;
  }

  private static String padDireita(String textoVisivel, String textoColorido, int largura) {
    int faltam = largura - textoVisivel.length();
    if (faltam <= 0) {
      return textoColorido;
    }
    StringBuilder sb = new StringBuilder(textoColorido);
    for (int i = 0; i < faltam; i++) {
      sb.append(' ');
    }
    return sb.toString();
  }

  /**
   * Exibe tabela ASCII colorida (ANSI) exclusivamente com a visao global dos lideres dos 3 shards (S0, S1, S2).
   */
  public static void exibirTabelaLideresANSI(int meuShard, String meuHost, TabelaLideres tabela) {
    StringBuilder sb = new StringBuilder();
    String linhaBorda = "+------------+-----------+--------------------+----------------+--------------------------+--------+";
    sb.append("\n").append(c(ANSI_BOLD + ANSI_CYAN, linhaBorda)).append("\n");
    sb.append(c(ANSI_BOLD + ANSI_CYAN,
        "|                    VISAO GLOBAL DE LIDERES POR PARTICAO (SHARDS S0, S1, S2)                     |")).append("\n");
    sb.append(c(ANSI_BOLD + ANSI_CYAN, linhaBorda)).append("\n");
    sb.append(String.format("| %-10s | %-9s | %-18s | %-14s | %-24s | %-6s |\n",
        "PARTICAO", "FAIXA ED", "STATUS", "LIDER ATUAL", "ENDPOINT (HOST:TCP/CLS)", "EPOCA"));
    sb.append(c(ANSI_CYAN, linhaBorda)).append("\n");

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      int lider = (tabela != null) ? tabela.getLider(s) : -1;
      String particaoVis = "Shard S" + s + (s == meuShard ? "*" : "");
      String particaoCol = (s == meuShard) ? c(ANSI_BOLD + ANSI_CYAN, particaoVis) : particaoVis;
      String faixa = Particionador.faixa(s);

      String statusVis;
      String statusCol;
      String liderStr;
      String endpointStr;
      String epocaStr;

      if (lider > 0) {
        int rep = PortasShard.replicaDoNo(lider);
        String hostLider = tabela.getHostLider(s, meuHost != null ? meuHost : "127.0.0.1");
        statusVis = "ONLINE";
        statusCol = c(ANSI_BOLD + ANSI_GREEN, statusVis);
        liderStr = "No " + lider + " (R" + rep + ")";
        endpointStr = hostLider + ":" + PortasShard.tcpDoNo(lider) + "/" + PortasShard.clusterDoNo(lider);
        epocaStr = String.valueOf(tabela.getEpoca(s));
      } else {
        statusVis = "OFFLINE (INDISP.)";
        statusCol = c(ANSI_BOLD + ANSI_RED, statusVis);
        liderStr = "NENHUM";
        endpointStr = "---";
        epocaStr = "-";
      }

      sb.append("| ")
          .append(padDireita(particaoVis, particaoCol, 10)).append(" | ")
          .append(String.format("%-9s", faixa)).append(" | ")
          .append(padDireita(statusVis, statusCol, 18)).append(" | ")
          .append(String.format("%-14s", liderStr)).append(" | ")
          .append(String.format("%-24s", endpointStr)).append(" | ")
          .append(String.format("%-6s", epocaStr)).append(" |\n");
    }

    sb.append(c(ANSI_BOLD + ANSI_CYAN, linhaBorda)).append("\n");
    sb.append("  (*) Particao local deste processo\n\n");
    utils.LogEstruturado.imprimirBlocoBruto(sb.toString());
  }

  /**
   * Exibe o Dashboard completo ANSI colorido no console do servidor (Fase 8):
   * 1) Visao global dos lideres conhecidos dos Shards S0 [A..I], S1 [J..R] e S2 [S..Z].
   * 2) Quantidade de nos ativos na particao local, papeis (ATIVO/STANDBY), epoca, versao, hash e ED local.
   */
  public static void exibirDashboardClusterANSI(
      int meuId,
      String meuPapel,
      long epoca,
      int idLiderLocal,
      Map<Integer, ConfigCluster.NoInfo> nos,
      Collection<VisaoCluster.ParEstado> pares,
      controller.GerenciadorGrupos gerenciador,
      RoteadorShards roteador) {

    ConfigCluster.NoInfo meuInfo = (nos != null) ? nos.get(meuId) : null;
    int shardId = (meuInfo != null) ? meuInfo.idShard : 0;
    int replicaId = (meuInfo != null) ? meuInfo.idReplica : (meuId - 1);
    String hostConfig = (meuInfo != null) ? meuInfo.host : "127.0.0.1";
    String ipLanReal = BeaconClusterLAN.obterIpLocalLAN();
    TabelaLideres tabela = (roteador != null) ? roteador.getTabela() : null;

    int nosAtivosParticaoLocal = 1; // Este proprio processo esta ativo
    int totalReplicasConhecidas = (nos != null) ? nos.size() : 1;
    if (pares != null) {
      for (VisaoCluster.ParEstado p : pares) {
        if (p.id != meuId && p.isVivo()) {
          nosAtivosParticaoLocal++;
        }
      }
    }

    int qtdUsuariosLocais = (gerenciador != null) ? gerenciador.getTodosUsuariosAtivos().size() : 0;
    int qtdGruposLocais = (gerenciador != null) ? gerenciador.listarGrupos().size() : 0;
    long versaoLocal = (gerenciador != null) ? gerenciador.getVersao() : 0L;
    long hashLocal = (gerenciador != null) ? gerenciador.calcularHash() : 0L;

    String papelBadge = "ATIVO".equalsIgnoreCase(meuPapel)
        ? c(ANSI_BOLD + ANSI_GREEN, "ATIVO (LIDER)")
        : c(ANSI_BOLD + ANSI_YELLOW, meuPapel);
    String ativosBadge = (nosAtivosParticaoLocal >= totalReplicasConhecidas)
        ? c(ANSI_BOLD + ANSI_GREEN, nosAtivosParticaoLocal + "/" + totalReplicasConhecidas + " ONLINE")
        : c(ANSI_BOLD + ANSI_YELLOW, nosAtivosParticaoLocal + "/" + totalReplicasConhecidas + " ONLINE (DEGRADADO)");

    String sepTopo = "====================================================================================================";
    String sepMeio = "+------------+-----------+--------------------+----------------+--------------------------+--------+";
    String sepRep  = "+-----------+----------------------+------------+--------+---------+------------+------------------+";

    StringBuilder sb = new StringBuilder();
    sb.append("\n").append(c(ANSI_BOLD + ANSI_CYAN, sepTopo)).append("\n");
    sb.append(c(ANSI_BOLD + ANSI_WHITE,
        "                 E.D.E.N. T#03 - PAINEL DE OBSERVABILIDADE DO SISTEMA DISTRIBUIDO                   ")).append("\n");
    sb.append(c(ANSI_BOLD + ANSI_CYAN, sepTopo)).append("\n");
    sb.append(String.format(" Processo Local : Shard %s | Replica %d (No %d) | Papel: %s\n",
        c(ANSI_BOLD + ANSI_CYAN, Particionador.nome(shardId)), replicaId, meuId, papelBadge));
    sb.append(String.format(" Rede / Bind    : Escuta 0.0.0.0 (TCP=%d, UDP=%d, Cluster=%d) | Host=%s | IP LAN=%s\n",
        PortasShard.tcpDoNo(meuId), PortasShard.udpDoNo(meuId),
        PortasShard.clusterDoNo(meuId), hostConfig, c(ANSI_BOLD + ANSI_GREEN, ipLanReal)));
    sb.append(String.format(" Particao Local : Nos ativos na particao S%d: %s | Epoca=%d | Versao=%d | CRC32=%d\n",
        shardId, ativosBadge, epoca, versaoLocal, hashLocal));
    sb.append(String.format(" Dados em S%d    : %d usuario(s) registrado(s) | %d grupo(s) hospedado(s) na faixa %s\n",
        shardId, qtdUsuariosLocais, qtdGruposLocais, Particionador.faixa(shardId)));

    // 1. TABELA GLOBAL DOS 3 SHARDS
    sb.append("\n ").append(c(ANSI_BOLD + ANSI_CYAN, "[1] LIDERES CONHECIDOS NAS 3 PARTICOES (S0, S1, S2):")).append("\n");
    sb.append(c(ANSI_CYAN, sepMeio)).append("\n");
    sb.append(String.format("| %-10s | %-9s | %-18s | %-14s | %-24s | %-6s |\n",
        "PARTICAO", "FAIXA ED", "STATUS", "LIDER ATUAL", "ENDPOINT (HOST:TCP/CLS)", "EPOCA"));
    sb.append(c(ANSI_CYAN, sepMeio)).append("\n");

    for (int s = 0; s < Particionador.NUM_SHARDS; s++) {
      int lider = (tabela != null) ? tabela.getLider(s) : (s == shardId ? idLiderLocal : -1);
      long epShard = (tabela != null && lider > 0) ? tabela.getEpoca(s) : (s == shardId ? epoca : -1);
      String partVis = "Shard S" + s + (s == shardId ? "*" : "");
      String partCol = (s == shardId) ? c(ANSI_BOLD + ANSI_CYAN, partVis) : partVis;
      String faixa = Particionador.faixa(s);

      String stVis;
      String stCol;
      String lidStr;
      String endStr;
      String epStr;

      if (lider > 0) {
        int rep = PortasShard.replicaDoNo(lider);
        String hLid = (tabela != null) ? tabela.getHostLider(s, hostConfig) : hostConfig;
        stVis = "ONLINE";
        stCol = c(ANSI_BOLD + ANSI_GREEN, stVis);
        lidStr = "No " + lider + " (R" + rep + ")";
        endStr = hLid + ":" + PortasShard.tcpDoNo(lider) + "/" + PortasShard.clusterDoNo(lider);
        epStr = String.valueOf(epShard);
      } else {
        stVis = "OFFLINE (INDISP.)";
        stCol = c(ANSI_BOLD + ANSI_RED, stVis);
        lidStr = "NENHUM";
        endStr = "---";
        epStr = "-";
      }

      sb.append("| ")
          .append(padDireita(partVis, partCol, 10)).append(" | ")
          .append(String.format("%-9s", faixa)).append(" | ")
          .append(padDireita(stVis, stCol, 18)).append(" | ")
          .append(String.format("%-14s", lidStr)).append(" | ")
          .append(String.format("%-24s", endStr)).append(" | ")
          .append(String.format("%-6s", epStr)).append(" |\n");
    }
    sb.append(c(ANSI_CYAN, sepMeio)).append("\n");

    // 2. TABELA DE NOS DA PARTICAO LOCAL
    sb.append("\n ").append(c(ANSI_BOLD + ANSI_CYAN,
        "[2] REPLICAS DA PARTICAO LOCAL (" + Particionador.nome(shardId) + ") — NOS ATIVOS: "
            + nosAtivosParticaoLocal + "/" + totalReplicasConhecidas)).append("\n");
    sb.append(c(ANSI_CYAN, sepRep)).append("\n");
    sb.append(String.format("| %-9s | %-20s | %-10s | %-6s | %-7s | %-10s | %-16s |\n",
        "REPLICA", "ENDERECO CLUSTER", "PAPEL", "EPOCA", "VERSAO", "SAUDE", "ULTIMO HEARTBEAT"));
    sb.append(c(ANSI_CYAN, sepRep)).append("\n");

    String idLocalVis = "R" + replicaId + "/#" + meuId + (meuId == idLiderLocal ? " [L]" : " [*]");
    String idLocalCol = c(ANSI_BOLD + ANSI_CYAN, idLocalVis);
    String papelLocalVis = meuPapel != null ? meuPapel : "?";
    String papelLocalCol = "ATIVO".equalsIgnoreCase(papelLocalVis)
        ? c(ANSI_BOLD + ANSI_GREEN, papelLocalVis)
        : c(ANSI_YELLOW, papelLocalVis);
    String saudeLocalVis = "LOCAL/UP";
    String saudeLocalCol = c(ANSI_BOLD + ANSI_GREEN, saudeLocalVis);
    String endLocal = hostConfig + ":" + PortasShard.clusterDoNo(meuId);

    sb.append("| ")
        .append(padDireita(idLocalVis, idLocalCol, 9)).append(" | ")
        .append(String.format("%-20s", endLocal)).append(" | ")
        .append(padDireita(papelLocalVis, papelLocalCol, 10)).append(" | ")
        .append(String.format("%-6d", epoca)).append(" | ")
        .append(String.format("%-7d", versaoLocal)).append(" | ")
        .append(padDireita(saudeLocalVis, saudeLocalCol, 10)).append(" | ")
        .append(String.format("%-16s", "processo atual")).append(" |\n");

    if (pares != null) {
      for (VisaoCluster.ParEstado p : pares) {
        if (p.id == meuId) continue;
        long deltaHb = (p.ultimoHeartbeat > 0) ? (System.currentTimeMillis() - p.ultimoHeartbeat) : -1;
        String hbStr = (deltaHb >= 0) ? (deltaHb + " ms atras") : "sem resposta";
        boolean vivo = p.isVivo();
        String saudeVis = vivo ? "ONLINE" : "DOWN";
        String saudeCol = vivo ? c(ANSI_BOLD + ANSI_GREEN, saudeVis) : c(ANSI_BOLD + ANSI_RED, saudeVis);
        String pPapelVis = (p.estado != null) ? p.estado : "DESCONHEC.";
        String pPapelCol = "ATIVO".equalsIgnoreCase(pPapelVis)
            ? c(ANSI_BOLD + ANSI_GREEN, pPapelVis)
            : (vivo ? c(ANSI_CYAN, pPapelVis) : c(ANSI_RED, pPapelVis));
        String idVis = "R" + (p.id - 1) + "/#" + p.id + (p.id == idLiderLocal ? " [L]" : "");

        sb.append("| ")
            .append(String.format("%-9s", idVis)).append(" | ")
            .append(String.format("%-20s", p.host + ":" + p.portaCluster)).append(" | ")
            .append(padDireita(pPapelVis, pPapelCol, 10)).append(" | ")
            .append(String.format("%-6d", p.epoca)).append(" | ")
            .append(String.format("%-7d", p.versao)).append(" | ")
            .append(padDireita(saudeVis, saudeCol, 10)).append(" | ")
            .append(String.format("%-16s", hbStr)).append(" |\n");
      }
    }

    sb.append(c(ANSI_CYAN, sepRep)).append("\n");
    sb.append(" Legenda: [*] Este processo | [L] Lider Ativo da particao\n");
    sb.append(c(ANSI_BOLD + ANSI_CYAN, sepTopo)).append("\n\n");

    utils.LogEstruturado.imprimirBlocoBruto(sb.toString());
  }

  /**
   * Imprime uma tabela formatada com o status completo do cluster e de todos os pares.
   */
  public static void exibirTabelaStatus(int meuId, String meuPapel, long epoca, int idLider,
      Map<Integer, ConfigCluster.NoInfo> nos, Collection<VisaoCluster.ParEstado> pares,
      controller.GerenciadorGrupos gerenciador) {
    exibirDashboardClusterANSI(meuId, meuPapel, epoca, idLider, nos, pares, gerenciador, null);
  }
}

