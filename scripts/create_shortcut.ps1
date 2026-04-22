#Requires -Version 5.1
<#
.SYNOPSIS
    Creates a Desktop shortcut for TrashPanda with the project logo as icon.
    Run once after cloning. Safe to re-run — overwrites existing shortcut.
.EXAMPLE
    .\scripts\create_shortcut.ps1
#>

$root    = Split-Path $PSScriptRoot -Parent
$pngPath = Join-Path $root "TrashPanda logo.png"
$icoDir  = Join-Path $root "assets"
$icoPath = Join-Path $icoDir "trashpanda.ico"
$batPath = Join-Path $root "start_trashpanda.bat"

# ---- Convert PNG → ICO using System.Drawing (no extra dependencies) ----
Add-Type -AssemblyName System.Drawing

function ConvertTo-Ico {
    param([string]$Png, [string]$Ico)

    $src = [System.Drawing.Image]::FromFile($Png)

    # Render at 256×256 with high-quality resampling
    $bmp = New-Object System.Drawing.Bitmap(256, 256, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    $g   = [System.Drawing.Graphics]::FromImage($bmp)
    $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
    $g.DrawImage($src, 0, 0, 256, 256)
    $g.Dispose(); $src.Dispose()

    # Get the PNG-compressed bytes for the 256×256 frame
    $ms = New-Object System.IO.MemoryStream
    $bmp.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png)
    $bmp.Dispose()
    $pngData = $ms.ToArray(); $ms.Dispose()

    # Write ICO: ICONDIR (6 B) + ICONDIRENTRY (16 B) + PNG frame
    # Windows Vista+ accepts PNG-compressed 256×256 frames in ICO files
    if (-not (Test-Path (Split-Path $Ico))) {
        New-Item -ItemType Directory -Path (Split-Path $Ico) | Out-Null
    }
    $fs = [System.IO.File]::Create($Ico)
    $w  = New-Object System.IO.BinaryWriter($fs)

    # ICONDIR
    $w.Write([uint16]0)                    # reserved
    $w.Write([uint16]1)                    # type = ICO
    $w.Write([uint16]1)                    # image count

    # ICONDIRENTRY
    $w.Write([byte]0)                      # width  (0 means 256)
    $w.Write([byte]0)                      # height (0 means 256)
    $w.Write([byte]0)                      # palette colors
    $w.Write([byte]0)                      # reserved
    $w.Write([uint16]1)                    # color planes
    $w.Write([uint16]32)                   # bits per pixel
    $w.Write([uint32]$pngData.Length)      # bytes in image data
    $w.Write([uint32]22)                   # offset = 6 + 16

    # PNG frame data
    $w.Write($pngData)
    $w.Close(); $fs.Close()
}

# ---- Create ICO ----
if (-not (Test-Path $pngPath)) {
    Write-Host "[ERROR] Logo not found: $pngPath" -ForegroundColor Red
    exit 1
}

Write-Host " Converting logo to icon..." -ForegroundColor Cyan
ConvertTo-Ico -Png $pngPath -Ico $icoPath
Write-Host " Icon saved: $icoPath" -ForegroundColor Green

# ---- Create Desktop shortcut ----
# Use the registry to resolve the real Desktop — handles OneDrive-redirected desktops
$desktop  = (Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders').Desktop
$lnkPath  = Join-Path $desktop "TrashPanda.lnk"

$wsh = New-Object -ComObject WScript.Shell
$lnk = $wsh.CreateShortcut($lnkPath)
$lnk.TargetPath       = $batPath
$lnk.WorkingDirectory = $root
$lnk.IconLocation     = "$icoPath,0"
$lnk.Description      = "Launch TrashPanda local dev servers"
$lnk.WindowStyle      = 1    # normal window
$lnk.Save()

Write-Host " Shortcut created: $lnkPath" -ForegroundColor Green
Write-Host ""
Write-Host " Double-click 'TrashPanda' on your Desktop to launch the app." -ForegroundColor Cyan
Write-Host ""
