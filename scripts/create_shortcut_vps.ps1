#Requires -Version 5.1
<#
.SYNOPSIS
    Creates a Desktop shortcut "TrashPanda (VPS)" that runs
    start_vps.bat — opens the SSH tunnel + Next.js frontend pointed
    at the remote VPS backend in one click.
.DESCRIPTION
    Companion to scripts/create_shortcut.ps1 which creates the
    *local-dev* shortcut. Run once after cloning, or after the
    underlying .bat file moves. Safe to re-run.
.EXAMPLE
    .\scripts\create_shortcut_vps.ps1
#>

$root    = Split-Path $PSScriptRoot -Parent
$pngPath = Join-Path $root "TrashPanda logo.png"
$icoDir  = Join-Path $root "assets"
$icoPath = Join-Path $icoDir "trashpanda.ico"
$batPath = Join-Path $root "start_vps.bat"

if (-not (Test-Path $batPath)) {
    Write-Host "[ERROR] start_vps.bat not found at $batPath" -ForegroundColor Red
    exit 1
}

# ---- Reuse the existing icon. If it isn't there yet (operator
# never ran create_shortcut.ps1), generate it now from the PNG so
# this script is self-sufficient.
if (-not (Test-Path $icoPath)) {
    if (-not (Test-Path $pngPath)) {
        Write-Host "[ERROR] Logo PNG not found: $pngPath" -ForegroundColor Red
        exit 1
    }
    Write-Host " Generating icon from PNG..." -ForegroundColor Cyan
    Add-Type -AssemblyName System.Drawing

    $src = [System.Drawing.Image]::FromFile($pngPath)
    $bmp = New-Object System.Drawing.Bitmap(256, 256, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    $g   = [System.Drawing.Graphics]::FromImage($bmp)
    $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
    $g.DrawImage($src, 0, 0, 256, 256)
    $g.Dispose(); $src.Dispose()

    $ms = New-Object System.IO.MemoryStream
    $bmp.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png)
    $bmp.Dispose()
    $pngData = $ms.ToArray(); $ms.Dispose()

    if (-not (Test-Path $icoDir)) {
        New-Item -ItemType Directory -Path $icoDir | Out-Null
    }
    $fs = [System.IO.File]::Create($icoPath)
    $w  = New-Object System.IO.BinaryWriter($fs)
    $w.Write([uint16]0); $w.Write([uint16]1); $w.Write([uint16]1)
    $w.Write([byte]0); $w.Write([byte]0); $w.Write([byte]0); $w.Write([byte]0)
    $w.Write([uint16]1); $w.Write([uint16]32)
    $w.Write([uint32]$pngData.Length); $w.Write([uint32]22)
    $w.Write($pngData)
    $w.Close(); $fs.Close()
    Write-Host " Icon saved: $icoPath" -ForegroundColor Green
}

# ---- Create Desktop shortcut ----
$desktop = (Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders').Desktop
$lnkPath = Join-Path $desktop "TrashPanda (VPS).lnk"

$wsh = New-Object -ComObject WScript.Shell
$lnk = $wsh.CreateShortcut($lnkPath)
$lnk.TargetPath       = $batPath
$lnk.WorkingDirectory = $root
$lnk.IconLocation     = "$icoPath,0"
$lnk.Description      = "Launch TrashPanda against the remote VPS (SSH tunnel + Next.js)"
$lnk.WindowStyle      = 1
$lnk.Save()

Write-Host " Shortcut created: $lnkPath" -ForegroundColor Green
Write-Host ""
Write-Host " Double-click 'TrashPanda (VPS)' on your Desktop to launch." -ForegroundColor Cyan
Write-Host ""
