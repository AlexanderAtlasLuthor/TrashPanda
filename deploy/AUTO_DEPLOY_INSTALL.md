# Auto-deploy install — VPS one-time setup

This is the one-time setup that closes the loop "code mergeado a
`main` → operador corre pilot → ve los mismos errores" by making the
VPS pull and restart automatically when there's a new commit on
`origin/main`.

After this is installed, every push to `main` lands on the VPS
within ~60 seconds without any manual `ssh` + `git pull`.

## What gets installed

* `/etc/systemd/system/trashpanda-auto-deploy.service` — oneshot unit
  that runs `/root/trashpanda/deploy/auto_deploy.sh`.
* `/etc/systemd/system/trashpanda-auto-deploy.timer` — fires the
  service every 60 seconds.
* The script `deploy/auto_deploy.sh` (in the repo) is the body. It
  refuses to deploy if the repo has uncommitted changes, isn't on
  `main`, or would require a real merge. Otherwise it delegates to
  the existing `deploy/update_vps.sh`.

## One-time install

Run on the VPS as root:

```bash
cd /root/trashpanda
git pull origin main   # make sure the new files are present locally

install -m 0644 deploy/trashpanda-auto-deploy.service \
    /etc/systemd/system/trashpanda-auto-deploy.service
install -m 0644 deploy/trashpanda-auto-deploy.timer \
    /etc/systemd/system/trashpanda-auto-deploy.timer

systemctl daemon-reload
systemctl enable --now trashpanda-auto-deploy.timer

# Sanity check
systemctl status trashpanda-auto-deploy.timer --no-pager
journalctl -u trashpanda-auto-deploy.service -n 20 --no-pager
tail -20 /var/log/trashpanda-auto-deploy.log
```

Expected first journal line within 60 seconds:

```
trashpanda-auto-deploy.service: Deactivated successfully.
```

(The log file `/var/log/trashpanda-auto-deploy.log` will be empty
when the local SHA matches `origin/main` — silence is the success
case.)

## Verify it works end-to-end

From your dev machine, push a trivial commit to `main`:

```bash
echo "# bump $(date -u +%FT%TZ)" >> deploy/AUTO_DEPLOY_INSTALL.md
git commit -am "chore: trigger auto-deploy"
git push origin main
```

On the VPS, within ~60 seconds:

```bash
tail -f /var/log/trashpanda-auto-deploy.log
# Expect:
#   2026-... DEPLOY: a65e30f -> e88c714
#   ... [update_vps.sh output] ...
#   2026-... OK: deployed e88c714

curl -fsS http://127.0.0.1:8000/version
# Expect:
#   {"commit":"e88c714...","short_commit":"e88c714",...,"source":"version_file"}
```

## Refusal cases (intentional)

The script will silently skip and log a `skip:` line — NOT deploy —
when:

* `git status` shows uncommitted changes. Operator might be
  debugging; we don't want to clobber their work tree.
* The repo isn't on `main`. Same reason.
* The remote branch is not a fast-forward of local. Real merges
  need operator review.

To recover, `ssh` in, resolve the situation, and the timer picks
up automatically on the next tick.

## Disable temporarily

```bash
systemctl stop trashpanda-auto-deploy.timer
# Re-enable when ready:
systemctl start trashpanda-auto-deploy.timer
```

## Override the cadence

Default is every 60 seconds. To change:

```bash
systemctl edit trashpanda-auto-deploy.timer
```

Add for example:

```ini
[Timer]
OnUnitActiveSec=
OnUnitActiveSec=5min
```

Then `systemctl daemon-reload && systemctl restart trashpanda-auto-deploy.timer`.

## How `/version` ties in

After every successful deploy, `update_vps.sh` writes the new SHA to
`/root/trashpanda/VERSION`. The backend reads that file at startup
(via `app/version.py`) and exposes it on `/version`. Operators / the
UI compare against `git log -1 origin/main --pretty=format:%H` to
detect drift.
