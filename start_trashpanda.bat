@echo off
setlocal EnableDelayedExpansion
title TrashPanda Launcher

:: ROOT = directory of this .bat file, regardless of where cmd was launched from
set "ROOT=%~dp0"
if "!ROOT:~-1!"=="\" set "ROOT=!ROOT:~0,-1!"

echo.
echo  =========================================
echo   TrashPanda - Local Dev Launcher
echo  =========================================
echo.

:: --- Validate prerequisites ---

if not exist "!ROOT!\.venv\Scripts\activate.bat" (
    echo [ERROR] .venv not found.
    echo         Run: python -m venv .venv ^&^& pip install -r requirements.txt
    pause
    exit /b 1
)

if not exist "!ROOT!\trashpanda-next" (
    echo [ERROR] trashpanda-next/ folder not found.
    pause
    exit /b 1
)

:: node_modules check: only install if the folder is missing
if not exist "!ROOT!\trashpanda-next\node_modules" (
    echo [WARN] node_modules not found. Running npm install...
    pushd "!ROOT!\trashpanda-next"
    call npm install
    popd
)

echo [Check]    Verifying Python backend dependencies...
"!ROOT!\.venv\Scripts\python.exe" -c "from app.db.dependencies import ensure_database_dependencies; ensure_database_dependencies()"
if errorlevel 1 (
    echo.
    echo [ERROR] Backend Python dependencies are incomplete.
    echo         Run: "!ROOT!\.venv\Scripts\python.exe" -m pip install -r requirements.txt
    pause
    exit /b 1
)

:: --- Port checks (abort early rather than launching silently on wrong port) ---
::
:: Strategy: try a TCP connect to localhost:PORT. If it succeeds, something is
:: already listening there and we must stop. If the connection is refused we
:: know the port is free. This is reliable and instant on localhost.

echo [Check]    Verifying ports are available...

powershell -NoProfile -Command "try{$t=New-Object Net.Sockets.TcpClient('localhost',8000);$t.Close();exit 1}catch{exit 0}" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Port 8000 is already in use.
    echo         Run stop_trashpanda.bat to stop existing servers, then try again.
    echo.
    pause
    exit /b 1
)

powershell -NoProfile -Command "try{$t=New-Object Net.Sockets.TcpClient('localhost',3000);$t.Close();exit 1}catch{exit 0}" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Port 3000 is already in use.
    echo         Run stop_trashpanda.bat to stop existing servers, then try again.
    echo.
    pause
    exit /b 1
)

:: --- Start Backend ---
:: /d sets working directory so relative paths work even with spaces in ROOT
echo [Backend]  Starting FastAPI on http://localhost:8000 ...
start "TrashPanda - Backend" /d "!ROOT!" cmd /k "call .venv\Scripts\activate.bat && uvicorn app.server:app --reload --port 8000"

:: --- Start Frontend ---
echo [Frontend] Starting Next.js on http://localhost:3000 ...
start "TrashPanda - Frontend" /d "!ROOT!\trashpanda-next" cmd /k "npm run dev"

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
echo  Done. Close the two terminal windows to stop the app.
echo.
