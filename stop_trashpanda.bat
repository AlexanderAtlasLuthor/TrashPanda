@echo off
setlocal
title TrashPanda - Stop

echo.
echo  =========================================
echo   TrashPanda - Stop Existing Servers
echo  =========================================
echo.

powershell -NoProfile -Command " ^
    $ports = 8000, 3000; ^
    $any = $false; ^
    foreach ($port in $ports) { ^
        $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue; ^
        foreach ($c in $conns) { ^
            Write-Host \"  Stopping PID $($c.OwningProcess) on port $port ...\"; ^
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue; ^
            $any = $true ^
        } ^
    }; ^
    if (-not $any) { Write-Host '  No processes found on ports 3000 or 8000.' } ^
"

echo.
echo  Done. You can now run start_trashpanda.bat
echo.
pause
