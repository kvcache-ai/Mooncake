@echo off
cd /d "%~dp0\.."
if exist ".venv\Scripts\activate.bat" (
  call .venv\Scripts\activate.bat
) else (
  echo [.venv not found] Creating with D:\python.exe ...
  D:\python.exe -m venv .venv
  call .venv\Scripts\activate.bat
  python -m pip install -r requirements.txt
)
streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501
