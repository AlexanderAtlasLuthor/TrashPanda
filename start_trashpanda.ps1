#Requires -Version 5.1
<#
.SYNOPSIS
    TrashPanda local dev launcher — starts FastAPI + Next.js
.EXAMPLE
    .\start_trashpanda.ps1
#>

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

Write-Host ""
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host "  TrashPanda - Local Dev Launcher" -ForegroundColor Cyan
Write-Host " =========================================" -ForegroundColor Cyan
Write-Host ""

# --- Validate prerequisites ---

if (-not (Test-Path "$root\.venv\Scripts\Activate.ps1")) {
    Write-Host "[ERROR] .venv not found." -ForegroundColor Red
    Write-Host "        Run: python -m venv .venv && pip install -r requirements.txt" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path "$root\trashpanda-next")) {
    Write-Host "[ERROR] trashpanda-next/ folder not found." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path "$root\trashpanda-next\node_modules")) {
    Write-Host "[WARN] node_modules not found. Running npm install..." -ForegroundColor Yellow
    Push-Location "$root\trashpanda-next"
    npm install
    Pop-Location
}

# --- Start Backend in a new window ---
Write-Host "[Backend]  Starting FastAPI on http://localhost:8000 ..." -ForegroundColor Green

$backendCmd = "Set-Location '$root'; .\.venv\Scripts\Activate.ps1; uvicorn app.server:app --reload --port 8000"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd -WindowStyle Normal

# --- Start Frontend in a new window ---
Write-Host "[Frontend] Starting Next.js on http://localhost:3000 ..." -ForegroundColor Green

$frontendCmd = "Set-Location '$root\trashpanda-next'; npm run dev"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCmd -WindowStyle Normal

# --- Open browser after short delay ---
Write-Host ""
Write-Host " Both servers are starting. Opening browser in 5 seconds..." -ForegroundColor Cyan
Start-Sleep -Seconds 5
Start-Process "http://localhost:3000"

Write-Host ""
Write-Host " Done. Close the two PowerShell windows to stop the app." -ForegroundColor Cyan
Write-Host ""
