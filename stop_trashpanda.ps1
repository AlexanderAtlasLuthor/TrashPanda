#Requires -Version 5.1
<#
.SYNOPSIS
    Stop any processes currently holding ports 3000 or 8000.
.EXAMPLE
    .\stop_trashpanda.ps1
#>

Write-Host ""
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host "  TrashPanda - Stop Existing Servers" -ForegroundColor Cyan
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host ""

$any = $false

foreach ($port in 8000, 3000) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        try {
            $proc = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
            $name = if ($proc) { $proc.ProcessName } else { "unknown" }
            Write-Host " Stopping PID $($c.OwningProcess) ($name) on port $port ..." -ForegroundColor Yellow
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
            $any = $true
        } catch {
            Write-Host " Could not stop PID $($c.OwningProcess): $_" -ForegroundColor Red
        }
    }
}

if (-not $any) {
    Write-Host " No processes found on ports 3000 or 8000." -ForegroundColor Green
}

Write-Host ""
Write-Host " Done. You can now run start_trashpanda.ps1" -ForegroundColor Cyan
Write-Host ""
