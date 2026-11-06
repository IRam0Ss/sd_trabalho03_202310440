/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: VisaoCluster
* Funcao.............: Tabela em memoria do estado de conectividade, versao e papel de todos os pares.
*************************************************************** */

package cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mantem a visao local do estado de saude, papel, versao e latencia
 * de todos os nos conhecidos do cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class VisaoCluster implements Serializable {

  private static final long serialVersionUID = 1L;

  public static class ParEstado implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int id;
    public final String host;
    public final int portaCluster;
    public int portaTcp;
    public int portaUdp;
    public volatile String estado; // ATIVO, STANDBY, SINCRONIZANDO, CANDIDATO, DOWN
    public volatile long epoca;
    public volatile long versao;
    public volatile long hash;
    public volatile long ultimoHeartbeat;
    public volatile boolean conectado;

    public ParEstado(ConfigCluster.NoInfo info) {
      this.id = info.id;
      this.host = info.host;
      this.portaCluster = info.portaCluster;
      this.portaTcp = info.portaTcp;
      this.portaUdp = info.portaUdp;
      this.estado = "DOWN";
      this.epoca = 0;
      this.versao = 0;
      this.hash = 0;
      this.ultimoHeartbeat = 0;
      this.conectado = false;
    }

    public boolean isVivo() {
      return conectado && !"DOWN".equalsIgnoreCase(estado);
    }

    public boolean isElegivel() {
      return isVivo() && ("STANDBY".equalsIgnoreCase(estado) || "ATIVO".equalsIgnoreCase(estado) || "CANDIDATO".equalsIgnoreCase(estado));
    }
  }

  private final int meuId;
  private final Map<Integer, ParEstado> pares = new HashMap<>();

  public VisaoCluster(ConfigCluster config) {
    this.meuId = config.getMeuId();
    for (ConfigCluster.NoInfo info : config.getNos().values()) {
      if (info.id != meuId) {
        pares.put(info.id, new ParEstado(info));
      }
    }
  }

  public synchronized void adicionarParSeAusente(ConfigCluster.NoInfo info) {
    if (info != null && info.id != meuId && !pares.containsKey(info.id)) {
      pares.put(info.id, new ParEstado(info));
      System.out.println("[VISAO] No " + info.id + " adicionado a tabela de saude do cluster.");
    }
  }

  public synchronized void marcarConectado(int id, boolean conectado) {
    ParEstado p = pares.get(id);
    if (p != null) {
      p.conectado = conectado;
      if (!conectado) {
        p.estado = "DOWN";
      }
    }
  }

  public synchronized void marcarDown(int id) {
    ParEstado p = pares.get(id);
    if (p != null) {
      p.estado = "DOWN";
      p.conectado = false;
    }
  }

  public synchronized void atualizarHeartbeat(int id, String estado, long epoca, long versao, long hash, int portaTcp, int portaUdp) {
    ParEstado p = pares.get(id);
    if (p != null) {
      p.estado = estado;
      p.epoca = epoca;
      p.versao = versao;
      p.hash = hash;
      p.ultimoHeartbeat = System.currentTimeMillis();
      p.conectado = true;
      if (portaTcp > 0) p.portaTcp = portaTcp;
      if (portaUdp > 0) p.portaUdp = portaUdp;
    }
  }

  /**
   * Verifica expiracao de heartbeats e marca como DOWN nos que nao responderam no prazo.
   * 
   * @param timeoutMs Tempo maximo sem heartbeat (ex: 3000 ms).
   * @return true se o no que caiu era o ATIVO (engatilhando nova eleicao).
   */
  public synchronized boolean verificarTimeouts(long timeoutMs) {
    long agora = System.currentTimeMillis();
    boolean ativoCaiu = false;

    for (ParEstado p : pares.values()) {
      if ("ATIVO".equalsIgnoreCase(p.estado) && (!p.conectado || agora - p.ultimoHeartbeat > timeoutMs)) {
        System.out.println("[CLUSTER] [WARNING] Lider ATIVO (No " + p.id + ") desconectado ou sem heartbeat -> Marcado como DOWN");
        ativoCaiu = true;
        p.estado = "DOWN";
        p.conectado = false;
      } else if (p.isVivo() && (agora - p.ultimoHeartbeat > timeoutMs)) {
        System.out.println("[CLUSTER] [WARNING] No " + p.id + " sem heartbeat ha " + (agora - p.ultimoHeartbeat)
            + "ms -> Marcado como DOWN (era: " + p.estado + ")");
        p.estado = "DOWN";
        p.conectado = false;
      }
    }
    return ativoCaiu;
  }

  public synchronized ParEstado getPar(int id) {
    return pares.get(id);
  }

  public synchronized List<ParEstado> getTodosPares() {
    return new ArrayList<>(pares.values());
  }

  public synchronized List<ParEstado> getParesVivos() {
    List<ParEstado> vivos = new ArrayList<>();
    for (ParEstado p : pares.values()) {
      if (p.isVivo()) {
        vivos.add(p);
      }
    }
    return vivos;
  }

  public synchronized List<ParEstado> getParesElegiveis() {
    List<ParEstado> elegiveis = new ArrayList<>();
    for (ParEstado p : pares.values()) {
      if (p.isElegivel()) {
        elegiveis.add(p);
      }
    }
    return elegiveis;
  }

  public synchronized int getIdAtivoAtual() {
    for (ParEstado p : pares.values()) {
      if (p.isVivo() && "ATIVO".equalsIgnoreCase(p.estado)) {
        return p.id;
      }
    }
    return -1;
  }

  public synchronized long getMaiorEpocaConhecida() {
    long maior = 0;
    for (ParEstado p : pares.values()) {
      if (p.epoca > maior) {
        maior = p.epoca;
      }
    }
    return maior;
  }
}
