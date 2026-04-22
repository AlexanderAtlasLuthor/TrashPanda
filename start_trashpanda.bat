@echo off
setlocal EnableDelayedExpansion
title TrashPanda Launcher

echo.
echo  =========================================
echo   TrashPanda - Local Dev Launcher
echo  =========================================
echo.

:: --- Validate prerequisites ---

if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] .venv not found. Run: python -m venv .venv ^& pip install -r requirements.txt
    pause
    exit /b 1
)

if not exist "trashpanda-next" (
    echo [ERROR] trashpanda-next/ folder not found.
    pause
    exit /b 1
)

if not exist "trashpanda-next\node_modules" (
    echo [WARN] node_modules not found. Running npm install...
    cd trashpanda-next
    call npm install
    cd ..
)

:: --- Start Backend ---
echo [Backend] Starting FastAPI on http://localhost:8000 ...
start "TrashPanda - Backend" cmd /k "cd /d %CD% && .venv\Scripts\activate.bat && uvicorn app.server:app --reload --port 8000"

:: --- Start Frontend ---
echo [Frontend] Starting Next.js on http://localhost:3000 ...
start "TrashPanda - Frontend" cmd /k "cd /d %CD%\trashpanda-next && npm run dev"

:: --- Wait a moment then open browser ---
echo.
echo  Both servers are starting...
echo  Open: http://localhost:3000
echo.
timeout /t 4 /nobreak >nul
start http://localhost:3000

echo  Done. Close the two terminal windows to stop the app.
echo.
