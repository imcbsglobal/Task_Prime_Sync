@echo off
title SQL Anywhere Sync Tool

echo.
echo ===============================================
echo    SQL Anywhere to Web API Sync Tool
echo ===============================================
echo.
echo Starting synchronization process...
echo.

REM Check if executable exists
if not exist "sync.exe" (
    echo ERROR: SyncTool.exe not found!
    echo Please ensure all files are in the same directory.
    echo.
    pause
    exit /b 1
)

REM Check if config file exists
if not exist "config.json" (
    echo ERROR: config.json not found!
    echo Please ensure the configuration file is present.
    echo.
    pause
    exit /b 1
)

REM Run the sync tool
SyncTool.exe

REM Check exit code
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ===============================================
    echo    Sync completed successfully!
    echo ===============================================
) else (
    echo.
    echo ===============================================
    echo    Sync encountered errors!
    echo    Check the log file for details.
    echo ===============================================
)

echo.
echo Press any key to exit...
pause >nul