#!/usr/bin/env bash
#
# Pull the latest code on the VPS, refresh dependencies, and restart
# the backend service. Idempotent and safe to run from a cron / CI hook.
#
# Usage (run on the VPS as root):
#   bash /root/trashpanda/deploy/update_vps.sh
#   # or pin a specific branch:
#   REPO_BRANCH=claude/reduce-bounce-rate-ZJSLz bash deploy/update_vps.sh

set -euo pipefail

REPO_DIR="${REPO_DIR:-/root/trashpanda}"
REPO_BRANCH="${REPO_BRANCH:-main}"
SERVICE_NAME="trashpanda-backend"

log() { printf '\033[1;32m[update]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[update]\033[0m %s\n' "$*" >&2; }

if [[ "${EUID}" -ne 0 ]]; then
    warn "must be run as root"
    exit 1
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
    warn "no git repo at ${REPO_DIR} — run install_vps.sh first"
    exit 1
fi

log "pulling ${REPO_BRANCH}"
git -C "${REPO_DIR}" fetch --depth=1 origin "${REPO_BRANCH}"
git -C "${REPO_DIR}" checkout "${REPO_BRANCH}"
git -C "${REPO_DIR}" reset --hard "origin/${REPO_BRANCH}"

log "refreshing python dependencies"
"${REPO_DIR}/.venv/bin/pip" install --upgrade pip wheel >/dev/null
"${REPO_DIR}/.venv/bin/pip" install -r "${REPO_DIR}/requirements.txt"

# Re-install systemd unit in case it changed.
if ! diff -q \
    "${REPO_DIR}/deploy/trashpanda-backend.service" \
    "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null 2>&1; then
    log "systemd unit changed — reinstalling"
    install -m 0644 \
        "${REPO_DIR}/deploy/trashpanda-backend.service" \
        "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
fi

log "restarting ${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

log "waiting for /healthz"
for i in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS "http://127.0.0.1:8000/healthz" >/dev/null 2>&1; then
        log "backend healthy"
        exit 0
    fi
    sleep 1
done
warn "/healthz did not respond — see: journalctl -u ${SERVICE_NAME} -n 100"
exit 1
