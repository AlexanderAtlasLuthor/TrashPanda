@echo off
setlocal EnableDelayedExpansion
title TrashPanda Launcher (VPS)

:: ROOT = directory of this .bat file, regardless of where cmd was launched from
set "ROOT=%~dp0"
if "!ROOT:~-1!"=="\" set "ROOT=!ROOT:~0,-1!"

echo.
echo  =========================================
echo   TrashPanda - VPS Launcher
echo   tunnel  : localhost:8001 -> VPS:8000
echo   frontend: http://localhost:3000
echo  =========================================
echo.

:: --- Validate prerequisites ---

if not exist "!ROOT!\trashpanda-next" (
    echo [ERROR] trashpanda-next/ folder not found.
    pause
    exit /b 1
)

if not exist "!ROOT!\trashpanda-next\node_modules" (
    echo [WARN] node_modules not found. Running npm install...
    pushd "!ROOT!\trashpanda-next"
    call npm install
    popd
)

if not exist "!ROOT!\deploy\tunnel.ps1" (
    echo [ERROR] deploy\tunnel.ps1 not found.
    pause
    exit /b 1
)

where ssh >nul 2>&1
if errorlevel 1 (
    echo [ERROR] ssh.exe is not on PATH. Install OpenSSH Client from
    echo         Windows Settings -^> Apps -^> Optional Features.
    pause
    exit /b 1
)

:: --- Port checks ---

echo [Check]    Verifying ports are available...

powershell -NoProfile -Command "try{$t=New-Object Net.Sockets.TcpClient('localhost',8001);$t.Close();exit 1}catch{exit 0}" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Port 8001 is already in use ^(tunnel^).
    echo         Run stop_vps.bat first, then try again.
    echo.
    pause
    exit /b 1
)

powershell -NoProfile -Command "try{$t=New-Object Net.Sockets.TcpClient('localhost',3000);$t.Close();exit 1}catch{exit 0}" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Port 3000 is already in use ^(frontend^).
    echo         Run stop_vps.bat first, then try again.
    echo.
    pause
    exit /b 1
)

:: --- Start SSH tunnel (auto-restarting supervisor) ---
:: tunnel.ps1 reads VPS_HOST / LOCAL_PORT / VPS_PORT from its own
:: defaults; override here by exporting env vars before this line if
:: you ever change the VPS IP.
echo [Tunnel]   Opening SSH tunnel via deploy\tunnel.ps1 ...
start "TrashPanda - Tunnel" /d "!ROOT!" powershell -NoProfile -ExecutionPolicy Bypass -File "deploy\tunnel.ps1"

:: --- Wait for tunnel to bind localhost:8001 (max ~30 s) ---
echo  Waiting for tunnel to bind localhost:8001 ...
set /a _t=0
:_tunnel_poll
    set /a _t+=1
    if !_t! gtr 15 (
        echo.
        echo [ERROR] Tunnel did not bind port 8001 within 30 seconds.
        echo         Check the "TrashPanda - Tunnel" window for SSH errors.
        echo.
        pause
        exit /b 1
    )
    powershell -NoProfile -Command "try{$t=New-Object Net.Sockets.TcpClient('localhost',8001);$t.Close();exit 0}catch{exit 1}" >nul 2>&1
    if errorlevel 1 (
        timeout /t 2 /nobreak >nul
        goto _tunnel_poll
    )
echo [Tunnel]   ready on localhost:8001

:: --- Start Frontend ---
:: Force the backend URL to the tunnel for this Next.js process so
:: the launcher works even if .env.local is misconfigured. The child
:: cmd shell inherits this env var via SET.
::
:: IMPORTANT: use ``set "VAR=value"`` (Microsoft-recommended form).
:: Without the inner quotes, cmd captures the trailing space before
:: ``&&`` as part of the value — Next.js then builds URLs like
:: ``http://localhost:8001 /jobs`` (note the space) and every
:: /api/jobs request returns 500 with a URL parse error.
echo [Frontend] Starting Next.js on http://localhost:3000 ...
start "TrashPanda - Frontend" /d "!ROOT!\trashpanda-next" cmd /k "set ""TRASHPANDA_BACKEND_URL=http://localhost:8001"" && npm run dev"

:: --- Poll until frontend responds (max ~60 s, 2 s intervals) ---
echo.
echo  Waiting for http://localhost:3000 to be ready...
set /a _n=0
:_poll
    set /a _n+=1
    if !_n! gtr 30 goto _open
    powershell -NoProfile -Command "try{$null=Invoke-WebRequest -Uri 'http://localhost:3000' -UseBasicParsing -TimeoutSec 1;exit 0}catch{exit 1}" >nul 2>&1
    if errorlevel 1 (
        timeout /t 2 /nobreak >nul
        goto _poll
    )
:_open
start http://localhost:3000

echo.
echo  Done. Two windows are running:
echo    - TrashPanda - Tunnel    (SSH supervisor; auto-reconnects)
echo    - TrashPanda - Frontend  (Next.js dev server)
echo  Close both, or run stop_vps.bat, to stop the app.
echo.
