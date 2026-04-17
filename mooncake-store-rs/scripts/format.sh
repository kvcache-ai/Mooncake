#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/format.sh
#
# Unified repository formatter for Rust and Python sources.
# Run this before every commit, or let the git pre-commit hook invoke it.
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"

CHECK_ONLY=0
RUN_RUST=1
RUN_PYTHON=1

usage() {
  cat <<'EOF'
Usage: scripts/format.sh [options]

Format tracked Rust and Python source files in this repository.

Options:
  --check         Verify formatting without modifying files
  --rust-only     Only run Rust formatting
  --python-only   Only run Python formatting
  -h, --help      Show this help

Notes:
  - Rust formatting uses `cargo fmt --all`
  - Python formatting prefers `ruff format`, and falls back to `black`
  - Install Python formatter support with: `python3 -m pip install -e '.[dev]'`
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      CHECK_ONLY=1
      shift
      ;;
    --rust-only)
      RUN_RUST=1
      RUN_PYTHON=0
      shift
      ;;
    --python-only)
      RUN_RUST=0
      RUN_PYTHON=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

run_rust_fmt() {
  mc_scripts_require_command cargo "Rust formatting"

  echo "==> formatting Rust"
  if ((CHECK_ONLY)); then
    cargo fmt --all --check
  else
    cargo fmt --all
  fi
}

select_python_formatter() {
  local repo_python=
  for repo_python in \
    "${REPO_ROOT}/.venv-wheel/bin/python" \
    "${REPO_ROOT}/.venv/bin/python"; do
    if [[ -x "${repo_python}" ]] && "${repo_python}" -m ruff --version >/dev/null 2>&1; then
      printf '%s\n' "${repo_python} -m ruff"
      return 0
    fi
  done

  if command -v ruff >/dev/null 2>&1; then
    printf '%s\n' "ruff"
    return 0
  fi

  if python3 -m ruff --version >/dev/null 2>&1; then
    printf '%s\n' "python3 -m ruff"
    return 0
  fi

  if command -v black >/dev/null 2>&1; then
    printf '%s\n' "black"
    return 0
  fi

  for repo_python in \
    "${REPO_ROOT}/.venv-wheel/bin/python" \
    "${REPO_ROOT}/.venv/bin/python"; do
    if [[ -x "${repo_python}" ]] && "${repo_python}" -m black --version >/dev/null 2>&1; then
      printf '%s\n' "${repo_python} -m black"
      return 0
    fi
  done

  if python3 -m black --version >/dev/null 2>&1; then
    printf '%s\n' "python3 -m black"
    return 0
  fi

  return 1
}

run_python_fmt() {
  local formatter
  local -a py_files=()

  mapfile -t py_files < <(git -C "${REPO_ROOT}" ls-files '*.py')
  if ((${#py_files[@]} == 0)); then
    echo "==> no tracked Python files to format"
    return 0
  fi

  if ! formatter=$(select_python_formatter); then
    cat >&2 <<'EOF'
missing Python formatter: install `ruff` (preferred) or `black`
hint: python3 -m pip install -e '.[dev]'
EOF
    exit 1
  fi

  echo "==> formatting Python with ${formatter}"
  case "${formatter}" in
    ruff|*" -m ruff")
      if ((CHECK_ONLY)); then
        ${formatter} format --check "${py_files[@]}"
      else
        ${formatter} format "${py_files[@]}"
      fi
      ;;
    black|*" -m black")
      if ((CHECK_ONLY)); then
        ${formatter} --check "${py_files[@]}"
      else
        ${formatter} "${py_files[@]}"
      fi
      ;;
    *)
      echo "unsupported Python formatter command: ${formatter}" >&2
      exit 1
      ;;
  esac
}

cd "${REPO_ROOT}"

if ((RUN_RUST)); then
  run_rust_fmt
fi

if ((RUN_PYTHON)); then
  run_python_fmt
fi

echo "formatting complete"
