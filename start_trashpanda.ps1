#Requires -Version 5.1
<#
.SYNOPSIS
    TrashPanda local dev launcher — starts FastAPI + Next.js
.EXAMPLE
    .\start_trashpanda.ps1
#>

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot   # always the script's own directory, handles spaces

Write-Host ""
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host "  TrashPanda - Local Dev Launcher" -ForegroundColor Cyan
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host ""

# --- Validate prerequisites ---

if (-not (Test-Path "$root\.venv\Scripts\Activate.ps1")) {
    Write-Host "[ERROR] .venv not found." -ForegroundColor Red
    Write-Host "        Run: python -m venv .venv && pip install -r requirements.txt" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

if (-not (Test-Path "$root\trashpanda-next")) {
    Write-Host "[ERROR] trashpanda-next/ folder not found." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# node_modules check: only install if the folder is missing
if (-not (Test-Path "$root\trashpanda-next\node_modules")) {
    Write-Host "[WARN]  node_modules not found. Running npm install..." -ForegroundColor Yellow
    Push-Location "$root\trashpanda-next"
    npm install
    Pop-Location
}

# --- Start Backend in a new window ---
Write-Host "[Backend]  Starting FastAPI on http://localhost:8000 ..." -ForegroundColor Green

# Paths with spaces are safe inside single-quotes passed to the child shell
$backendCmd = "Set-Location '$root'; .\.venv\Scripts\Activate.ps1; uvicorn app.server:app --reload --port 8000"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd -WindowStyle Normal

# --- Start Frontend in a new window ---
Write-Host "[Frontend] Starting Next.js on http://localhost:3000 ..." -ForegroundColor Green

$frontendCmd = "Set-Location '$root\trashpanda-next'; npm run dev"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCmd -WindowStyle Normal

# --- Poll until frontend responds (max 60 s) ---
Write-Host ""
Write-Host " Waiting for http://localhost:3000 to be ready..." -ForegroundColor Cyan

$maxWait = 60
$elapsed = 0
while ($elapsed -lt $maxWait) {
    try {
        $null = Invoke-WebRequest -Uri 'http://localhost:3000' -UseBasicParsing -TimeoutSec 1
        break
    } catch {
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
}

Start-Process "http://localhost:3000"

Write-Host ""
Write-Host " Done. Close the two PowerShell windows to stop the app." -ForegroundColor Cyan
Write-Host ""
