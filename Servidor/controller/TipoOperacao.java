/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 17/10/2026
* Nome...............: TipoOperacao
* Funcao.............: Enumeracao dos tipos de operacoes deterministas de mutacao do estado do servidor.
*************************************************************** */

package controller;

import java.io.Serializable;

/**
 * Catalogo dos tipos de operacoes mutaveis aplicaveis ao GerenciadorGrupos
 * e replicaveis de forma sincrona entre todos os servidores do cluster.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public enum TipoOperacao implements Serializable {
  REGISTRAR_USUARIO,
  REMOVER_USUARIO,
  ENTRAR_GRUPO,
  SAIR_GRUPO,
  BLOQUEAR,
  DESBLOQUEAR,
  ATUALIZAR_ENDPOINT,
  REGISTRAR_MSG_GRUPO,
  REGISTRAR_MSG_PRIVADA,
  CONFIRMAR_ENTREGA,
  CONFIRMAR_LEITURA,
  LIMPAR_MENSAGENS
}

