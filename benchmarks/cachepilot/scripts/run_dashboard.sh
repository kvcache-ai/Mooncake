#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

# Prefer project venv if present (avoids broken system/conda Python).
if [ -x ".venv/bin/python" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
elif [ -x ".venv/Scripts/python.exe" ]; then
  # Git Bash / MSYS on Windows
  # shellcheck disable=SC1091
  source .venv/Scripts/activate
fi

if ! command -v streamlit >/dev/null 2>&1; then
  echo "streamlit not found. Create venv and install deps first:"
  echo "  python3 -m venv .venv && source .venv/bin/activate"
  echo "  pip install -r requirements.txt"
  exit 1
fi

streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501
