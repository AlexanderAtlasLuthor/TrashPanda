#!/usr/bin/env bash
#
# Auto-restarting SSH tunnel for TrashPanda. Linux/macOS counterpart
# to deploy/tunnel.ps1 — keeps the operator's laptop tunnel
# resilient to drops without depending on autossh.
#
# Usage:
#   bash deploy/tunnel.sh                      # uses defaults
#   VPS_HOST=root@1.2.3.4 bash deploy/tunnel.sh
#
# Maps:  localhost:8001 -> VPS 127.0.0.1:8000

set -uo pipefail

VPS_HOST="${VPS_HOST:-root@192.3.105.145}"
LOCAL_PORT="${LOCAL_PORT:-8001}"
VPS_PORT="${VPS_PORT:-8000}"
LOG_PATH="${LOG_PATH:-${TMPDIR:-/tmp}/trashpanda-tunnel.log}"

log() {
    local ts level msg
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    level="$1"; shift
    msg="$*"
    printf '[%s] [%s] %s\n' "$ts" "$level" "$msg" | tee -a "$LOG_PATH"
}

trap 'log INFO "stopping (signal)"; exit 0' INT TERM

log INFO "starting tunnel supervisor"
log INFO "  local:${LOCAL_PORT} -> ${VPS_HOST}:127.0.0.1:${VPS_PORT}"
log INFO "  log file: ${LOG_PATH}"
log INFO "press Ctrl+C to stop"

attempt=0
backoff=2
max_backoff=60

while true; do
    attempt=$((attempt + 1))
    log INFO "attempt #${attempt}: opening tunnel"
    started_at=$(date +%s)

    ssh -N \
        -o ServerAliveInterval=10 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -o StrictHostKeyChecking=accept-new \
        -L "${LOCAL_PORT}:127.0.0.1:${VPS_PORT}" \
        "${VPS_HOST}"
    code=$?

    elapsed=$(( $(date +%s) - started_at ))
    log WARN "tunnel exited (code=${code}) after ${elapsed}s"

    if [[ "${elapsed}" -ge 30 ]]; then
        backoff=2
    else
        backoff=$(( backoff * 2 ))
        [[ "${backoff}" -gt "${max_backoff}" ]] && backoff="${max_backoff}"
    fi
    log INFO "reconnecting in ${backoff}s"
    sleep "${backoff}"
done
