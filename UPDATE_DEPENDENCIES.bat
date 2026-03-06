@echo off
cd /d "%~dp0"
if not exist .venv (
  echo Virtual environment not found. Run SETUP_FIRST_TIME.bat first.
  pause
  exit /b 1
)
call .venv\Scripts\activate
pip install -r requirements.txt
echo Dependencies updated.
pause
