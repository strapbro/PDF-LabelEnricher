@echo on
setlocal EnableExtensions
cd /d "%~dp0"

if not exist static\app_icon.ico (
  echo Missing static\app_icon.ico
  pause
  exit /b 1
)

echo Building native launcher EXE...
dotnet publish launcher-native\LabelEnricherLauncher.csproj -c Release -r win-x64 --self-contained false -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=false
if %ERRORLEVEL% NEQ 0 (
  echo Build failed.
  pause
  exit /b 1
)

copy /Y launcher-native\bin\Release\net7.0-windows\win-x64\publish\LabelEnricherLauncher.exe LabelEnricherLauncher.exe >nul
if %ERRORLEVEL% NEQ 0 (
  echo Built EXE, but failed to copy it into the app folder.
  pause
  exit /b 1
)

echo.
echo Build complete:
echo %CD%\LabelEnricherLauncher.exe
echo Keep this EXE next to START_LABEL_ENRICHER.bat, then pin it to the Windows taskbar.
pause

