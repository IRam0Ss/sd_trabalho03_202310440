#!/bin/bash
# ==============================================================================
# E.D.E.N. (T#03) - Encerramento Limpo dos Processos do Cluster
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.cluster_pids"

echo "=== Encerrando processos do cluster E.D.E.N. ==="

ENCERRADOS=0

if [ -f "$PID_FILE" ]; then
  while IFS= read -r pid; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      ENCERRADOS=$((ENCERRADOS + 1))
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
fi

# Garante limpeza de eventuais nos abertos em janelas de terminal separadas
PIDS_RESTANTES=$(pgrep -f "Principal [0-2] [0-9]" 2>/dev/null || true)
if [ -z "$PIDS_RESTANTES" ]; then
  PIDS_RESTANTES=$(pgrep -f "java -cp .*Servidor/bin Principal" 2>/dev/null || true)
fi
if [ -n "$PIDS_RESTANTES" ]; then
  for p in $PIDS_RESTANTES; do
    kill "$p" 2>/dev/null || true
    ENCERRADOS=$((ENCERRADOS + 1))
  done
fi

echo "Cluster encerrado ($ENCERRADOS sinal(is) de parada enviados)."

