@echo off
REM Ride Sharing Surge Pricing Pipeline Starter
REM This batch file runs the PowerShell startup script

setlocal enabledelayedexpansion

REM Get the script directory
set "SCRIPT_DIR=%~dp0"

REM Check if PowerShell execution policy allows running scripts
powershell -Command "& {$ExecutionPolicy = Get-ExecutionPolicy; if ($ExecutionPolicy -eq 'Restricted') { Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned }}"

REM Run the PowerShell startup script
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%start_pipeline.ps1" %*

pause
