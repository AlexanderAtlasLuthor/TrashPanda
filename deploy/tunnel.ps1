<#
.SYNOPSIS
    Keep an SSH tunnel from your laptop to the TrashPanda VPS backend
    open at all times. Auto-restarts on disconnect.

.DESCRIPTION
    Replaces the manual ``ssh -N -L 8001:127.0.0.1:8000 root@<vps>``
    workflow with a supervised loop that restarts the tunnel within
    seconds if it drops. Logs to a rotating file so you can see why
    it disconnected.

    The tunnel maps:

        localhost:8001  →  VPS 127.0.0.1:8000

    Your Next.js dev server (TRASHPANDA_BACKEND_URL=http://localhost:8001)
    talks to the tunnel; the tunnel talks to the VPS backend over SSH.

.PARAMETER VpsHost
    User@host string. Default: root@192.3.105.145.

.PARAMETER LocalPort
    Local port the tunnel listens on. Default: 8001.

.PARAMETER VpsPort
    Port on the VPS the backend binds to. Default: 8000.

.PARAMETER LogPath
    Where to append tunnel logs. Default: $env:TEMP\trashpanda-tunnel.log.

.EXAMPLE
    PS> .\deploy\tunnel.ps1
    Starts the tunnel using the defaults.

.EXAMPLE
    PS> .\deploy\tunnel.ps1 -VpsHost root@192.3.105.145
    Equivalent to above; explicit form.
#>

[CmdletBinding()]
param(
    [string]$VpsHost = "root@192.3.105.145",
    [int]$LocalPort = 8001,
    [int]$VpsPort = 8000,
    [string]$LogPath = (Join-Path $env:TEMP "trashpanda-tunnel.log")
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Level, [string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] [$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogPath -Value $line
}

Write-Log "INFO" "starting tunnel supervisor"
Write-Log "INFO" "  local:${LocalPort} -> ${VpsHost}:127.0.0.1:${VpsPort}"
Write-Log "INFO" "  log file: $LogPath"
Write-Log "INFO" "press Ctrl+C to stop"

$attempt = 0
$backoffSeconds = 2
$maxBackoff = 60

while ($true) {
    $attempt++
    Write-Log "INFO" "attempt #${attempt}: opening tunnel"

    # ServerAliveInterval/CountMax: detect dead peers within ~30s.
    # ExitOnForwardFailure: bail out if the local port can't bind so
    # we don't silently leak a stale ssh process.
    $args = @(
        "-N",
        "-o", "ServerAliveInterval=10",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-L", "${LocalPort}:127.0.0.1:${VpsPort}",
        $VpsHost
    )

    $startedAt = Get-Date
    try {
        $proc = Start-Process -FilePath "ssh" -ArgumentList $args `
            -PassThru -NoNewWindow -Wait
        $exit = $proc.ExitCode
    } catch {
        Write-Log "ERROR" "ssh launch failed: $($_.Exception.Message)"
        $exit = -1
    }
    $elapsed = [int]((Get-Date) - $startedAt).TotalSeconds
    Write-Log "WARN" "tunnel exited (code=$exit) after ${elapsed}s"

    # Reset backoff if the tunnel ran for a meaningful duration —
    # otherwise we are in a tight crash loop and need to back off.
    if ($elapsed -ge 30) {
        $backoffSeconds = 2
    } else {
        $backoffSeconds = [math]::Min($maxBackoff, $backoffSeconds * 2)
    }
    Write-Log "INFO" "reconnecting in ${backoffSeconds}s"
    Start-Sleep -Seconds $backoffSeconds
}
