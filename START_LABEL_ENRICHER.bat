@echo on
setlocal EnableExtensions
cd /d "%~dp0"
title Label Enricher - Start

if not exist .venv (
  echo Virtual environment missing. Run SETUP_FIRST_TIME.bat first.
  pause
  exit /b 1
)

call .venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
  echo Failed to activate .venv.
  pause
  exit /b 1
)

set "PORT="
for %%P in (8081 8082 8083 8090 9000 10080) do (
  netstat -ano | findstr /R /C:":%%P .*LISTENING" >nul
  if errorlevel 1 (
    set "PORT=%%P"
    goto :port_found
  )
)

:port_found
if "%PORT%"=="" (
  echo Could not find a free default port.
  echo Close other local apps and try again.
  pause
  exit /b 1
)

echo Starting Label Enricher on http://127.0.0.1:%PORT%
start "" powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep -Milliseconds 900; Start-Process 'http://127.0.0.1:%PORT%'"
python -m uvicorn app.ui_server:app --host 127.0.0.1 --port %PORT%

echo.
