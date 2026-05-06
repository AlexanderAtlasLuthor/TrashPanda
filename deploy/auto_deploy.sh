#!/usr/bin/env bash
#
# Auto-deploy: poll origin/main and run update_vps.sh when there's a
# new commit. Designed to be triggered by the trashpanda-auto-deploy
# systemd timer (see trashpanda-auto-deploy.{service,timer}).
#
# Idempotent and safe to run on a tight loop. Default poll cadence is
# 60s — see trashpanda-auto-deploy.timer.
#
# Refuses to deploy when:
#   * the repo has uncommitted changes (operator might be debugging)
#   * the local branch is not ``main``
#   * the fast-forward merge would require a real merge
#
# All output goes to /var/log/trashpanda-auto-deploy.log.

set -euo pipefail

REPO_DIR="${TRASHPANDA_REPO_DIR:-/root/trashpanda}"
REPO_BRANCH="${TRASHPANDA_AUTO_DEPLOY_BRANCH:-main}"
LOG_FILE="${TRASHPANDA_AUTO_DEPLOY_LOG:-/var/log/trashpanda-auto-deploy.log}"
LOCK_FILE="${TRASHPANDA_AUTO_DEPLOY_LOCK:-/run/trashpanda-auto-deploy.lock}"

log() {
    printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" >> "${LOG_FILE}"
}

# Single-instance guard. flock is more robust than a PID file and
# never gets left behind.
exec 9>"${LOCK_FILE}" 2>/dev/null || {
    echo "auto_deploy: cannot open lock ${LOCK_FILE}" >&2
    exit 1
}
if ! flock -n 9; then
    log "skip: another auto_deploy is running"
    exit 0
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
    log "FAIL: no git repo at ${REPO_DIR}"
    exit 1
fi

cd "${REPO_DIR}"

# Refuse if uncommitted changes — operator may be mid-debug.
if ! git diff --quiet HEAD -- 2>/dev/null \
   || ! git diff --cached --quiet 2>/dev/null; then
    log "skip: uncommitted changes in ${REPO_DIR}"
    exit 0
fi

# Refuse if not on the target branch.
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [[ "${current_branch}" != "${REPO_BRANCH}" ]]; then
    log "skip: not on ${REPO_BRANCH} (current=${current_branch})"
    exit 0
fi

# Fetch and compare.
git fetch --quiet origin "${REPO_BRANCH}"
local_sha=$(git rev-parse HEAD)
remote_sha=$(git rev-parse "origin/${REPO_BRANCH}")

if [[ "${local_sha}" == "${remote_sha}" ]]; then
    # Up to date — exit silently. No log spam.
    exit 0
fi

# Verify the update would be a clean fast-forward. Refuse anything
# that would require a real merge — that's an operator concern.
if ! git merge-base --is-ancestor "${local_sha}" "${remote_sha}" 2>/dev/null; then
    log "FAIL: ${local_sha:0:7} is not an ancestor of origin/${REPO_BRANCH} (${remote_sha:0:7}); manual resolution required"
    exit 1
fi

log "DEPLOY: ${local_sha:0:7} -> ${remote_sha:0:7}"

# Delegate the heavy lifting to update_vps.sh — the canonical pull +
# venv refresh + service restart + healthz wait sequence. We don't
# duplicate that logic here.
if ! bash "${REPO_DIR}/deploy/update_vps.sh" >> "${LOG_FILE}" 2>&1; then
    log "FAIL: update_vps.sh non-zero exit; rolling state inspect required"
    exit 1
fi

log "OK: deployed ${remote_sha:0:7}"
exit 0
