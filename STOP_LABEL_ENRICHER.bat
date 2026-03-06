@echo on
setlocal EnableExtensions
cd /d "%~dp0"
title Label Enricher - Stop

echo Stopping Label Enricher if running...
set "FOUND=0"
for %%P in (8081 8082 8083 8090 9000 10080) do (
  for /f "tokens=5" %%A in ('netstat -ano ^| findstr /R /C:":%%P .*LISTENING"') do (
    echo Found listener on port %%P with PID %%A
    taskkill /PID %%A /F
    set "FOUND=1"
  )
)

if "%FOUND%"=="0" (
  echo No running Label Enricher process found on default ports.
) else (
  echo Stop command completed.
)

echo.
pause
