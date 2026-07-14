@echo off
cd /d "%~dp0\.."

where python >nul 2>&1
if errorlevel 1 (
  echo python not found in PATH. Please install Python 3.10+ and retry.
  exit /b 1
)

if not exist ".venv\Scripts\activate.bat" (
  echo [.venv not found] Creating virtual environment with python ...
  python -m venv .venv
  if errorlevel 1 (
    echo Failed to create .venv
    exit /b 1
  )
)

call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
if errorlevel 1 (
  echo Failed to install requirements
  exit /b 1
)

streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501
