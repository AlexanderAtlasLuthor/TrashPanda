#Requires -Version 5.1
<#
.SYNOPSIS
    One-shot Windows bootstrap for the TrashPanda operator laptop.

.DESCRIPTION
    Walks through the four prerequisites in order so the operator
    only has to run a single script and answer a couple of prompts:

      1. OpenSSH Client present on PATH (installs via Optional
         Features when missing — relaunches itself elevated only for
         that step, so the rest runs in the normal user profile).
      2. Passwordless SSH to the VPS (delegates to setup_ssh_key.ps1).
      3. Fetches TRASHPANDA_OPERATOR_TOKEN from the VPS over SSH and
         writes ``trashpanda-next/.env.local`` with it plus
         ``TRASHPANDA_BACKEND_URL=http://localhost:8001`` for the
         tunnel-pointed launcher.
      4. Creates the Desktop shortcut "TrashPanda (VPS)" via
         create_shortcut_vps.ps1.

    Idempotent. Re-running on a host that's already configured just
    re-confirms each step.

.PARAMETER VpsHost
    user@host string. Default: root@192.3.105.145.

.EXAMPLE
    PS> .\deploy\setup_windows.ps1
    PS> .\deploy\setup_windows.ps1 -VpsHost root@1.2.3.4
#>

[CmdletBinding()]
param(
    [string]$VpsHost = "root@192.3.105.145"
)

$ErrorActionPreference = "Stop"

# Resolve repo root from this script's location so the operator can
# launch from any cwd (including double-clicking it).
$repoRoot = Split-Path $PSScriptRoot -Parent

function Section($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}
function Ok($msg)   { Write-Host "    [OK] $msg"   -ForegroundColor Green }
function Warn($msg) { Write-Host "    [!]  $msg"   -ForegroundColor Yellow }
function Fail($msg) { Write-Host "    [X]  $msg"   -ForegroundColor Red; exit 1 }

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    return ([Security.Principal.WindowsPrincipal]$id).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator
    )
}

# --------------------------------------------------------------- #
# Step 1: OpenSSH Client
# --------------------------------------------------------------- #
Section "Step 1/4: OpenSSH Client (ssh.exe)"

$sshOnPath = $null
try {
    $sshOnPath = Get-Command ssh.exe -ErrorAction Stop
} catch {
    $sshOnPath = $null
}

if ($sshOnPath) {
    Ok "ssh.exe found at $($sshOnPath.Source)"
} else {
    Warn "ssh.exe not on PATH — installing OpenSSH Client (Optional Feature)"
    Warn "This requires admin rights; a UAC prompt may appear."

    # Add-WindowsCapability requires admin. Spawn an elevated child
    # only for this single command so the rest of the script keeps
    # running in the operator's normal profile (where setup_ssh_key
    # writes ~/.ssh/id_ed25519 to the right home directory).
    $installCmd = "Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0 | Out-Null"
    if (Test-Admin) {
        Invoke-Expression $installCmd
    } else {
        $proc = Start-Process powershell `
            -ArgumentList @(
                '-NoProfile', '-ExecutionPolicy', 'Bypass',
                '-Command', $installCmd
            ) `
            -Verb RunAs -PassThru -Wait
        if ($proc.ExitCode -ne 0) {
            Fail "OpenSSH Client install failed (exit $($proc.ExitCode)). Install it manually: Settings -> Apps -> Optional features -> Add -> 'OpenSSH Client'."
        }
    }

    # Refresh PATH for this session — Add-WindowsCapability puts ssh
    # under C:\Windows\System32\OpenSSH\, which is on the Machine
    # PATH but not on this process's PATH yet.
    $env:PATH = ([Environment]::GetEnvironmentVariable('PATH', 'Machine')) + ';' + ([Environment]::GetEnvironmentVariable('PATH', 'User'))

    $sshOnPath = Get-Command ssh.exe -ErrorAction SilentlyContinue
    if (-not $sshOnPath) {
        Fail "ssh.exe still not on PATH after install. Open a fresh PowerShell window and re-run this script."
    }
    Ok "OpenSSH Client installed: $($sshOnPath.Source)"
}

# --------------------------------------------------------------- #
# Step 2: SSH key on VPS (delegates to setup_ssh_key.ps1)
# --------------------------------------------------------------- #
Section "Step 2/4: passwordless SSH to ${VpsHost}"

# Quick probe — if BatchMode auth already works, skip the password
# prompt entirely. setup_ssh_key.ps1 is idempotent, but no need to
# rerun and re-prompt for the password on every bootstrap.
$probe = & ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new `
              $VpsHost 'echo ok' 2>$null
if ($LASTEXITCODE -eq 0 -and $probe -match 'ok') {
    Ok "passwordless SSH already works"
} else {
    $keyScript = Join-Path $repoRoot 'deploy\setup_ssh_key.ps1'
    if (-not (Test-Path $keyScript)) {
        Fail "setup_ssh_key.ps1 not found at $keyScript"
    }
    Write-Host "    Running setup_ssh_key.ps1 — you'll be asked for the VPS password ONCE."
    & $keyScript -VpsHost $VpsHost
    if ($LASTEXITCODE -ne 0) {
        Fail "ssh key bootstrap failed (exit $LASTEXITCODE)."
    }
}

# --------------------------------------------------------------- #
# Step 3: token + .env.local
# --------------------------------------------------------------- #
Section "Step 3/4: writing trashpanda-next\.env.local"

$envLocalDir  = Join-Path $repoRoot 'trashpanda-next'
$envLocalPath = Join-Path $envLocalDir '.env.local'

if (-not (Test-Path $envLocalDir)) {
    Fail "trashpanda-next/ folder not found at $envLocalDir. Did you clone the right repo?"
}

# Pull TRASHPANDA_OPERATOR_TOKEN from the VPS env file. Use grep
# with an anchored regex so a stray TRASHPANDA_OPERATOR_TOKENS=
# (rotation list) doesn't get picked up.
$remoteCmd = "grep -E '^TRASHPANDA_OPERATOR_TOKEN=' /etc/trashpanda/backend.env || true"
$tokenLine = & ssh -o BatchMode=yes $VpsHost $remoteCmd 2>$null
if ($LASTEXITCODE -ne 0 -or -not $tokenLine) {
    Fail "could not read TRASHPANDA_OPERATOR_TOKEN from ${VpsHost}:/etc/trashpanda/backend.env"
}
$token = ($tokenLine -split '=', 2)[1].Trim()
if (-not $token) {
    Fail "TRASHPANDA_OPERATOR_TOKEN on the VPS is empty. Re-run install_vps.sh on the server."
}
Ok "fetched token from VPS (length=$($token.Length))"

# Merge into existing .env.local (preserve any other vars the user
# already has — TRASHPANDA_DEPLOYMENT, custom keys, etc.). Replace
# the two keys we own; append if missing.
function Set-EnvVar {
    param([string]$Path, [string]$Key, [string]$Value)
    $existing = if (Test-Path $Path) { Get-Content $Path } else { @() }
    $matched  = $false
    $output   = foreach ($line in $existing) {
        if ($line -match "^\s*$([regex]::Escape($Key))\s*=") {
            $matched = $true
            "$Key=$Value"
        } else {
            $line
        }
    }
    if (-not $matched) {
        $output = @($output) + "$Key=$Value"
    }
    # UTF8 *without* BOM — Next.js' dotenv parser stumbles on a BOM
    # in the very first byte of the file.
    [IO.File]::WriteAllLines($Path, $output, (New-Object System.Text.UTF8Encoding $false))
}

Set-EnvVar -Path $envLocalPath -Key 'TRASHPANDA_OPERATOR_TOKEN' -Value $token
Set-EnvVar -Path $envLocalPath -Key 'TRASHPANDA_BACKEND_URL'   -Value 'http://localhost:8001'
Ok "wrote $envLocalPath"

# --------------------------------------------------------------- #
# Step 4: Desktop shortcut
# --------------------------------------------------------------- #
Section "Step 4/4: Desktop shortcut 'TrashPanda (VPS)'"

$shortcutScript = Join-Path $repoRoot 'scripts\create_shortcut_vps.ps1'
if (-not (Test-Path $shortcutScript)) {
    Warn "create_shortcut_vps.ps1 not found — skipping shortcut. Re-clone or pull main."
} else {
    & $shortcutScript
    if ($LASTEXITCODE -ne 0) {
        Warn "shortcut script returned non-zero — check the output above."
    }
}

# --------------------------------------------------------------- #
# Done
# --------------------------------------------------------------- #
Write-Host ""
Write-Host "All four steps complete." -ForegroundColor Green
Write-Host "  - ssh.exe on PATH"
Write-Host "  - passwordless SSH to ${VpsHost}"
Write-Host "  - $envLocalPath"
Write-Host "  - Desktop shortcut 'TrashPanda (VPS)'"
Write-Host ""
Write-Host "Double-click 'TrashPanda (VPS)' on your Desktop to launch the app." -ForegroundColor Cyan
Write-Host ""
