<#
.SYNOPSIS
    Set up passwordless SSH from this Windows laptop to the TrashPanda
    VPS so the tunnel.ps1 supervisor can run unattended.

.DESCRIPTION
    Walks through the four-step SSH-key bootstrap that everyone hits
    when they first deploy:

      1. Create ~/.ssh/id_ed25519 if it doesn't exist (no passphrase
         prompt — use ssh-keygen by hand if you want one).
      2. Read the public key.
      3. Append it to /root/.ssh/authorized_keys on the VPS via
         password-based ssh (the ONE password prompt you'll see).
      4. Verify passwordless ssh now works.

    Idempotent. Safe to re-run after a key rotation.

.PARAMETER VpsHost
    user@host string. Default root@192.3.105.145.

.EXAMPLE
    PS> .\deploy\setup_ssh_key.ps1
    PS> .\deploy\setup_ssh_key.ps1 -VpsHost root@1.2.3.4

.NOTES
    After this script finishes, run deploy/tunnel.ps1 to bring the
    tunnel up. tunnel.ps1 will no longer prompt for a password.
#>

[CmdletBinding()]
param(
    [string]$VpsHost = "root@192.3.105.145"
)

$ErrorActionPreference = "Stop"

function Section($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

function Ok($msg)   { Write-Host "    [OK] $msg"   -ForegroundColor Green }
function Warn($msg) { Write-Host "    [!]  $msg"   -ForegroundColor Yellow }
function Fail($msg) { Write-Host "    [X]  $msg"   -ForegroundColor Red; exit 1 }

# 1. Ensure local key exists -----------------------------------------------
$keyPath = Join-Path $env:USERPROFILE ".ssh\id_ed25519"
$pubPath = "${keyPath}.pub"

Section "Step 1/4: local SSH key"
if (-not (Test-Path $pubPath)) {
    if (Test-Path $keyPath) {
        Fail "Private key exists at $keyPath but no .pub. Generate the .pub with: ssh-keygen -y -f $keyPath > ${pubPath}"
    }
    Write-Host "    No key found at ${keyPath}. Generating an ed25519 key (no passphrase)."
    & ssh-keygen -t ed25519 -f $keyPath -N '""' -q
    if ($LASTEXITCODE -ne 0) {
        Fail "ssh-keygen failed (exit ${LASTEXITCODE})."
    }
    Ok "key generated"
} else {
    Ok "key already exists at ${keyPath}"
}

$pubKey = Get-Content $pubPath -Raw
if (-not $pubKey -or $pubKey.Trim().Length -lt 40) {
    Fail "Public key at $pubPath looks empty or malformed."
}
Ok "public key fingerprint: $((& ssh-keygen -lf $pubPath) -join ' ')"

# 2. Show what we're about to do ------------------------------------------
Section "Step 2/4: target VPS"
Write-Host "    Host: ${VpsHost}"
Write-Host "    The next step will ssh into the VPS and append this"
Write-Host "    laptop's public key to /root/.ssh/authorized_keys."
Write-Host "    You'll be prompted for the VPS password ONCE."

# 3. Append the key remotely. We do this in one ssh hop so the user only
#    types the password once. The remote command:
#      - ensures /root/.ssh exists with the right perms
#      - appends our pubkey only if it isn't already there
#      - prints "added" / "already present" so we can confirm
$keyValue = $pubKey.Trim()
$remote = "mkdir -p ~/.ssh; chmod 700 ~/.ssh; touch ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys; KEY='$keyValue'; if grep -qF `"'`$KEY`"' ~/.ssh/authorized_keys 2>/dev/null; then echo '[remote] key already present'; else echo `"`$KEY`" >> ~/.ssh/authorized_keys; echo '[remote] key added'; fi"

Section "Step 3/4: install key on VPS (one password prompt)"
& ssh -o PreferredAuthentications=password `
      -o PubkeyAuthentication=no `
      -o StrictHostKeyChecking=accept-new `
      $VpsHost $remote
if ($LASTEXITCODE -ne 0) {
    Fail "Remote command failed (exit ${LASTEXITCODE}). Did you mistype the password?"
}
Ok "key installed"

# 4. Verify passwordless login ---------------------------------------------
Section "Step 4/4: verifying passwordless SSH"
$verify = & ssh -o BatchMode=yes `
                -o PreferredAuthentications=publickey `
                -o StrictHostKeyChecking=accept-new `
                $VpsHost 'echo "[remote] passwordless ssh works"' 2>&1
if ($LASTEXITCODE -ne 0) {
    Warn "Passwordless ssh did not work yet."
    Warn "Output: $verify"
    Warn "Try: ssh -v ${VpsHost}  to see why the key wasn't accepted."
    exit 1
}
Ok $verify

Write-Host ""
Write-Host "All set. Next step:" -ForegroundColor Green
Write-Host "    .\deploy\tunnel.ps1 -VpsHost ${VpsHost}" -ForegroundColor Green
Write-Host ""
