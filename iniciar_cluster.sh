#!/bin/bash
# ==============================================================================
# E.D.E.N. (T#03) - Inicializador Automatico da Topologia Completa (6 Processos)
# 3 Shards ([A..I], [J..R], [S..Z]) x 2 Replicas (1 Ativo + 1 Standby por Shard)
#
# Uso:
#   ./iniciar_cluster.sh              # Abre 6 terminais (ou background se sem GUI)
#   ./iniciar_cluster.sh --rede       # Modo Rede Fisica (LAN / Wi-Fi)
#   ./iniciar_cluster.sh --bg         # Forca execucao em background (logs em logs_cluster/)
#   ./iniciar_cluster.sh --bg --rede  # Background + Rede Fisica LAN
#   ./iniciar_cluster.sh --parar      # Encerra todos os 6 processos do cluster
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="$SCRIPT_DIR/.cluster_pids"
LOG_DIR="$SCRIPT_DIR/logs_cluster"

FORCAR_BG=false
MODO_FLAG=""

for arg in "$@"; do
  case "$arg" in
    --bg|--background)
      FORCAR_BG=true
      ;;
    --rede|--lan)
      MODO_FLAG="--rede"
      ;;
    --local)
      MODO_FLAG="--local"
      ;;
    --parar|--stop)
      exec "$SCRIPT_DIR/parar_cluster.sh"
      ;;
    -h|--help)
      echo "Uso: ./iniciar_cluster.sh [--rede] [--bg] [--parar]"
      exit 0
      ;;
  esac
done

# Compila automaticamente caso os binarios ainda nao existam
if [ ! -f "$SCRIPT_DIR/Servidor/bin/Principal.class" ]; then
  echo "[SETUP] Binarios nao encontrados. Compilando projeto antes de subir o cluster..."
  "$SCRIPT_DIR/compilar.sh"
fi

detectar_terminal_gui() {
  if [ "$FORCAR_BG" = true ]; then
    echo ""
    return
  fi
  local gui_ok=false
  if [ -n "${WAYLAND_DISPLAY:-}" ] && [ -e "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/$WAYLAND_DISPLAY" ]; then
    gui_ok=true
  elif [ -n "${DISPLAY:-}" ]; then
    gui_ok=true
  fi
  if [ "$gui_ok" != true ]; then
    echo ""
    return
  fi
  for term in gnome-terminal konsole xfce4-terminal mate-terminal tilix kitty alacritty xterm x-terminal-emulator; do
    if command -v "$term" >/dev/null 2>&1; then
      echo "$term"
      return
    fi
  done
  echo ""
}

TERMINAL_GUI="$(detectar_terminal_gui)"
mkdir -p "$LOG_DIR"
: > "$PID_FILE"

subir_no() {
  local num="$1"
  local titulo="EDEN No $num (Zero-Config)"
  local log_arq="$LOG_DIR/servidor_no_${num}.log"
  local cmd_java="cd \"$SCRIPT_DIR\" && java -cp Servidor/bin Principal $MODO_FLAG"

  if [ -n "$TERMINAL_GUI" ]; then
    case "$TERMINAL_GUI" in
      gnome-terminal)
        gnome-terminal --title="$titulo" -- bash -c "$cmd_java; echo ''; echo '[Processo encerrado. Pressione ENTER]'; read -r _" &
        ;;
      konsole)
        konsole -p tabtitle="$titulo" -e bash -c "$cmd_java; read -r _" &
        ;;
      xfce4-terminal)
        xfce4-terminal --title="$titulo" -x bash -c "$cmd_java; read -r _" &
        ;;
      mate-terminal)
        mate-terminal --title="$titulo" -x bash -c "$cmd_java; read -r _" &
        ;;
      tilix)
        tilix -t "$titulo" -e "bash -c '$cmd_java; read -r _'" &
        ;;
      kitty)
        kitty --title "$titulo" bash -c "$cmd_java; read -r _" &
        ;;
      alacritty)
        alacritty --title "$titulo" -e bash -c "$cmd_java; read -r _" &
        ;;
      xterm|x-terminal-emulator)
        "$TERMINAL_GUI" -T "$titulo" -e bash -c "$cmd_java; echo ''; echo '[Processo encerrado. Pressione ENTER]'; read -r _" &
        ;;
    esac
    echo "  -> [$titulo] janela disparada (auto-descoberta de ID e Shard)..."
  else
    nohup java -cp "$SCRIPT_DIR/Servidor/bin" Principal $MODO_FLAG </dev/null >"$log_arq" 2>&1 &
    local pid=$!
    disown "$pid" 2>/dev/null || true
    echo "$pid" >> "$PID_FILE"
    echo "  -> [$titulo] PID=$pid | Log: logs_cluster/servidor_no_${num}.log"
  fi
}

echo "=========================================================================="
echo "   E.D.E.N. (T#03) - INICIANDO CLUSTER PARTICIONADO (6 PROCESSOS)         "
echo "   Modo: ${MODO_FLAG:---local (bind 0.0.0.0)} | Execucao: ${TERMINAL_GUI:-background}"
echo "=========================================================================="

echo "[1/2] Iniciando 3 primeiros nos (Lideres Pioneiros dos Shards)..."
subir_no 1
sleep 0.7
subir_no 2
sleep 0.7
subir_no 3

sleep 1.0

echo "[2/2] Iniciando 3 nos seguintes (Replicas Standbys dos Shards)..."
subir_no 4
sleep 0.7
subir_no 5
sleep 0.7
subir_no 6

echo "--------------------------------------------------------------------------"
echo " Topologia de 6 processos disparada com sucesso!"
echo " Para encerrar todos os nos de uma vez, execute: ./parar_cluster.sh"
echo " Para iniciar um cliente JavaFX, execute: java -cp Cliente/bin Principal"
echo "=========================================================================="

