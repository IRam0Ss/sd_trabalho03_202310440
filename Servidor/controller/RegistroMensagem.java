/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: RegistroMensagem
* Funcao.............: Modela o rastreamento integral de entrega e leitura de mensagens (grupo ou direta).
*************************************************************** */

package controller;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Entidade que unifica a autoria, destino e acompanhamento de confirmacoes (ticks 1, 2 e 3)
 * de uma mensagem no chat E.D.E.N. E totalmente serializavel para replicacao entre servidores.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class RegistroMensagem implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String idMensagem;
  private final String nomeGrupo;
  private final String autor;
  private final String ipAutor;
  private final int portaAutor;
  private final boolean privado;
  private final Set<String> destinatariosEsperados;
  private final Set<String> destinatariosEntregues;
  private final Set<String> destinatariosLidos;
  private boolean entregueNotificado;
  private boolean lidoNotificado;
  private final long timestamp;

  /**
   * Construtor para mensagem de grupo.
   */
  public RegistroMensagem(String idMensagem, String nomeGrupo, String autor, Set<String> destinatariosEsperados, long timestamp) {
    this(idMensagem, nomeGrupo, autor, null, 0, destinatariosEsperados, timestamp);
  }

  /**
   * Construtor para mensagem de grupo com endpoint do autor (para devolucao direta de ticks cross-shard).
   */
  public RegistroMensagem(String idMensagem, String nomeGrupo, String autor, String ipAutor, int portaAutor,
      Set<String> destinatariosEsperados, long timestamp) {
    this.idMensagem = idMensagem;
    this.nomeGrupo = nomeGrupo;
    this.autor = autor;
    this.ipAutor = ipAutor;
    this.portaAutor = portaAutor;
    this.privado = false;
    this.destinatariosEsperados = (destinatariosEsperados != null) ? new HashSet<>(destinatariosEsperados) : new HashSet<>();
    this.destinatariosEntregues = new HashSet<>();
    this.destinatariosLidos = new HashSet<>();
    this.entregueNotificado = false;
    this.lidoNotificado = false;
    this.timestamp = timestamp;
  }

  /**
   * Construtor para mensagem privada.
   */
  public RegistroMensagem(String idMensagem, String destinatario, String autor, long timestamp) {
    this(idMensagem, destinatario, autor, null, 0, timestamp);
  }

  /**
   * Construtor para mensagem privada com endpoint do autor (ticks salvos no Shard do Destinatario).
   */
  public RegistroMensagem(String idMensagem, String destinatario, String autor, String ipAutor, int portaAutor, long timestamp) {
    this.idMensagem = idMensagem;
    this.nomeGrupo = "@" + destinatario;
    this.autor = autor;
    this.ipAutor = ipAutor;
    this.portaAutor = portaAutor;
    this.privado = true;
    this.destinatariosEsperados = new HashSet<>();
    if (destinatario != null && !destinatario.trim().isEmpty()) {
      this.destinatariosEsperados.add(destinatario.trim().toLowerCase());
    }
    this.destinatariosEntregues = new HashSet<>();
    this.destinatariosLidos = new HashSet<>();
    this.entregueNotificado = false;
    this.lidoNotificado = false;
    this.timestamp = timestamp;
  }

  private RegistroMensagem(String idMensagem, String nomeGrupo, String autor, String ipAutor, int portaAutor,
      boolean privado, Set<String> esperados, Set<String> entregues, Set<String> lidos,
      boolean entregueNotificado, boolean lidoNotificado, long timestamp) {
    this.idMensagem = idMensagem;
    this.nomeGrupo = nomeGrupo;
    this.autor = autor;
    this.ipAutor = ipAutor;
    this.portaAutor = portaAutor;
    this.privado = privado;
    this.destinatariosEsperados = new HashSet<>(esperados);
    this.destinatariosEntregues = new HashSet<>(entregues);
    this.destinatariosLidos = new HashSet<>(lidos);
    this.entregueNotificado = entregueNotificado;
    this.lidoNotificado = lidoNotificado;
    this.timestamp = timestamp;
  }

  /**
   * Cria um clone profundo e thread-safe deste registro de mensagem (incluindo estado de ticks CONFIRM).
   */
  public synchronized RegistroMensagem copiar() {
    return new RegistroMensagem(
        this.idMensagem,
        this.nomeGrupo,
        this.autor,
        this.ipAutor,
        this.portaAutor,
        this.privado,
        this.destinatariosEsperados,
        this.destinatariosEntregues,
        this.destinatariosLidos,
        this.entregueNotificado,
        this.lidoNotificado,
        this.timestamp);
  }

  public String getIdMensagem() {
    return idMensagem;
  }

  public String getNomeGrupo() {
    return nomeGrupo;
  }

  public String getAutor() {
    return autor;
  }

  public String getIpAutor() {
    return ipAutor;
  }

  public int getPortaAutor() {
    return portaAutor;
  }

  public boolean isPrivado() {
    return privado;
  }

  public synchronized Set<String> getDestinatariosEsperados() {
    return new HashSet<>(destinatariosEsperados);
  }

  public synchronized Set<String> getDestinatariosEntregues() {
    return new HashSet<>(destinatariosEntregues);
  }

  public synchronized Set<String> getDestinatariosLidos() {
    return new HashSet<>(destinatariosLidos);
  }

  public synchronized boolean isEntregueNotificado() {
    return entregueNotificado;
  }

  public synchronized void setEntregueNotificado(boolean entregueNotificado) {
    this.entregueNotificado = entregueNotificado;
  }

  public synchronized boolean isLidoNotificado() {
    return lidoNotificado;
  }

  public synchronized void setLidoNotificado(boolean lidoNotificado) {
    this.lidoNotificado = lidoNotificado;
  }

  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Registra a entrega no dispositivo de um destinatario.
   * 
   * @param usuario Nome do usuario destinatario.
   * @return true se foi a primeira entrega geral registrada que ainda nao foi notificada ao autor.
   */
  public synchronized boolean registrarEntrega(String usuario) {
    if (usuario != null && !usuario.trim().isEmpty()) {
      destinatariosEntregues.add(usuario.trim().toLowerCase());
    }
    if (!entregueNotificado) {
      entregueNotificado = true;
      return true;
    }
    return false;
  }

  /**
   * Registra a leitura por um destinatario.
   * 
   * @param usuario           Nome do leitor.
   * @param membrosAtuaisGrupo Nomes normalizados dos membros atuais da sala.
   * @return true se todos os membros esperados leram e essa conclusao ainda nao havia sido notificada.
   */
  public synchronized boolean registrarLeitura(String usuario, Set<String> membrosAtuaisGrupo) {
    if (usuario != null && !usuario.trim().isEmpty()) {
      String norm = usuario.trim().toLowerCase();
      destinatariosEntregues.add(norm);
      destinatariosLidos.add(norm);
    }

    Set<String> alvo = new HashSet<>();
    if (membrosAtuaisGrupo != null && !membrosAtuaisGrupo.isEmpty()) {
      for (String m : membrosAtuaisGrupo) {
        if (!m.equalsIgnoreCase(autor)) {
          alvo.add(m.toLowerCase());
        }
      }
    }
    if (alvo.isEmpty()) {
      alvo.addAll(destinatariosEsperados);
    }

    boolean todosLeram = !alvo.isEmpty() && destinatariosLidos.containsAll(alvo);
    if (todosLeram && !lidoNotificado) {
      lidoNotificado = true;
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("RegistroMensagem[id=%s, autor=%s, grupo=%s, entregues=%d/%d, lidos=%d/%d]",
        idMensagem, autor, nomeGrupo, destinatariosEntregues.size(), destinatariosEsperados.size(),
        destinatariosLidos.size(), destinatariosEsperados.size());
  }
}

