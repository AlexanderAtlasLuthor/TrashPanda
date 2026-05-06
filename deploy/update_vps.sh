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
RETRY_UNIT="trashpanda-retry-worker"
POLLER_UNIT="trashpanda-pilot-bounce-poller"

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

# Stamp the running revision so /version surfaces what's actually
# deployed (rather than depending on a git call inside the running
# process, which can race with this very pull).
git -C "${REPO_DIR}" rev-parse HEAD > "${REPO_DIR}/VERSION"
log "stamped VERSION = $(cat "${REPO_DIR}/VERSION" | cut -c1-7)"

log "refreshing python dependencies"
"${REPO_DIR}/.venv/bin/pip" install --upgrade pip wheel >/dev/null
"${REPO_DIR}/.venv/bin/pip" install -r "${REPO_DIR}/requirements.txt"

# Re-install systemd units (backend + both timer pairs) when any of
# them have changed in the repo. ``daemon-reload`` only fires once
# per update; idempotent so re-runs are cheap.
units_changed=0
sync_unit() {
    local src="$1" dst="$2"
    if ! diff -q "$src" "$dst" >/dev/null 2>&1; then
        log "  unit changed: $(basename "$dst")"
        install -m 0644 "$src" "$dst"
        units_changed=1
    fi
}

sync_unit "${REPO_DIR}/deploy/trashpanda-backend.service" \
          "/etc/systemd/system/${SERVICE_NAME}.service"
sync_unit "${REPO_DIR}/deploy/${RETRY_UNIT}.service" \
          "/etc/systemd/system/${RETRY_UNIT}.service"
sync_unit "${REPO_DIR}/deploy/${RETRY_UNIT}.timer" \
          "/etc/systemd/system/${RETRY_UNIT}.timer"
sync_unit "${REPO_DIR}/deploy/${POLLER_UNIT}.service" \
          "/etc/systemd/system/${POLLER_UNIT}.service"
sync_unit "${REPO_DIR}/deploy/${POLLER_UNIT}.timer" \
          "/etc/systemd/system/${POLLER_UNIT}.timer"

if [[ "${units_changed}" -eq 1 ]]; then
    log "reloading systemd"
    systemctl daemon-reload
fi

# Make sure the timers are enabled even on hosts that were installed
# before this script learned about them. ``enable --now`` is
# idempotent: a no-op if already enabled+started.
systemctl enable --now "${RETRY_UNIT}.timer" >/dev/null
systemctl enable --now "${POLLER_UNIT}.timer" >/dev/null

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
