@echo off
setlocal
title TrashPanda - Stop (VPS)

echo.
echo  =========================================
echo   TrashPanda - Stop VPS Launcher Processes
echo  =========================================
echo.

:: Kill the tunnel-side ssh.exe (LISTEN on 8001) and the Next.js
:: process (LISTEN on 3000). The "TrashPanda - Tunnel" supervisor
:: window will detect the dead ssh and try to reconnect — kill it
:: explicitly via window title to avoid that loop.

powershell -NoProfile -Command " ^
    $ports = 8001, 3000; ^
    $any = $false; ^
    foreach ($port in $ports) { ^
        $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue; ^
        foreach ($c in $conns) { ^
            Write-Host \"  Stopping PID $($c.OwningProcess) on port $port ...\"; ^
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue; ^
            $any = $true ^
        } ^
    }; ^
    if (-not $any) { Write-Host '  No processes found on ports 8001 or 3000.' } ^
"

:: Kill the supervisor window (powershell running tunnel.ps1) so it
:: doesn't keep restarting ssh after we just killed it. Match by
:: window title set in start_vps.bat.
taskkill /F /FI "WINDOWTITLE eq TrashPanda - Tunnel*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq TrashPanda - Frontend*" >nul 2>&1

echo.
echo  Done. You can now run start_vps.bat
echo.
pause
