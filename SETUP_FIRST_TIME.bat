@echo on
setlocal EnableExtensions
cd /d "%~dp0"
title Label Enricher - First Time Setup

echo ==========================================
echo Label Enricher First-Time Setup
echo Folder: %CD%
echo ==========================================

set "PY_EXE="
where python >nul 2>&1
if %ERRORLEVEL% EQU 0 set "PY_EXE=python"
if "%PY_EXE%"=="" (
  where py >nul 2>&1
  if %ERRORLEVEL% EQU 0 set "PY_EXE=py -3"
)
if "%PY_EXE%"=="" (
  echo.
  echo ERROR: Python was not found.
  echo Install Python 3.11+ and check "Add Python to PATH".
  echo Download: https://www.python.org/downloads/windows/
  pause
  exit /b 1
)

echo Using interpreter: %PY_EXE%
%PY_EXE% --version
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Python command failed.
  pause
  exit /b 1
)

echo.
echo Creating virtual environment...
%PY_EXE% -m venv .venv
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Failed creating .venv
  pause
  exit /b 1
)

echo Activating environment...
call .venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Failed activating .venv
  pause
  exit /b 1
)

echo Upgrading pip...
python -m pip install --upgrade pip
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: pip upgrade failed.
  pause
  exit /b 1
)

echo Installing requirements...
pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: dependency install failed.
  echo If this is a network issue, try again in a few minutes.
  pause
  exit /b 1
)

echo.
echo Setup complete.
echo Next: double-click START_LABEL_ENRICHER.bat
pause
