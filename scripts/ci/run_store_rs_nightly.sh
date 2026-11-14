#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODE=${1:-all}

usage() {
  cat <<'EOF'
Usage: scripts/ci/run_store_rs_nightly.sh [all|smoke|local-e2e|python-compat|hot-cache|ttl-recovery|route-migration|eviction]

Run the Store-RS nightly suites against the existing CMake build and installed
Python component. The default `all` mode runs the installed-package smoke test
followed by the enabled heavier single-runner integration scenarios. The
hot-cache, TTL recovery, route migration, and eviction scenarios remain
available as explicit modes while known Classic TE TCP issues keep them out of
`all`.
EOF
}

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

case "${MODE}" in
  all | smoke | local-e2e | python-compat | hot-cache | ttl-recovery | route-migration | eviction) ;;
  *)
    echo "unsupported Store-RS nightly mode: ${MODE}" >&2
    usage >&2
    exit 1
    ;;
esac

ROOT_DIR=${MOONCAKE_ROOT_DIR:?MOONCAKE_ROOT_DIR must name the Mooncake source root}
STORE_RS_DIR=${MOONCAKE_STORE_RS_DIR:?MOONCAKE_STORE_RS_DIR must name the Store-RS source directory}
BUILD_DIR=${MOONCAKE_BUILD_DIR:?MOONCAKE_BUILD_DIR must name the configured CMake build directory}
PYTHON_PREFIX=${MOONCAKE_PYTHON_PREFIX:?MOONCAKE_PYTHON_PREFIX must name the CMake Python component install prefix}
BUILD_PYTHON_BIN=${MOONCAKE_PYTHON_BIN:-python3}

for required_dir in "${ROOT_DIR}" "${STORE_RS_DIR}" "${BUILD_DIR}" "${PYTHON_PREFIX}"; do
  [[ "${required_dir}" == /* && -d "${required_dir}" ]] || {
    echo "expected an absolute existing directory: ${required_dir}" >&2
    exit 1
  }
done
command -v "${BUILD_PYTHON_BIN}" >/dev/null || {
  echo "MOONCAKE_PYTHON_BIN is not executable from PATH: ${BUILD_PYTHON_BIN}" >&2
  exit 1
}
BUILD_PYTHON_BIN=$(command -v "${BUILD_PYTHON_BIN}")

TEMP_ROOT=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
mkdir -p "${TEMP_ROOT}"
NIGHTLY_TEMP_DIR=$(mktemp -d "${TEMP_ROOT}/store-rs-nightly.XXXXXX")

cleanup() {
  local status=$?
  rm -rf "${NIGHTLY_TEMP_DIR}"
  exit "${status}"
}
trap cleanup EXIT

run_suite() {
  local label=$1
  shift
  local started_at=${SECONDS}
  local status=0

  echo
  echo "==> ${label}"
  if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
    echo "::group::${label}"
  fi

  if "$@"; then
    status=0
  else
    status=$?
  fi

  if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
    echo "::endgroup::"
  fi
  if (( status != 0 )); then
    echo "FAILED: ${label} (exit ${status})" >&2
    return "${status}"
  fi
  echo "PASS: ${label} ($((SECONDS - started_at))s)"
}

allocate_port() {
  "${BUILD_PYTHON_BIN}" - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

run_with_redis_port() {
  local port_variable=$1
  shift
  local redis_port
  local status=0

  redis_port=$(allocate_port)
  if env "${port_variable}=${redis_port}" "$@"; then
    status=0
  else
    status=$?
  fi
  redis-cli -p "${redis_port}" shutdown nosave >/dev/null 2>&1 || true
  return "${status}"
}

prepare_installed_python() {
  local package_dir="${PYTHON_PREFIX}/mooncake"
  local venv_dir="${NIGHTLY_TEMP_DIR}/venv"
  local python_bin="${venv_dir}/bin/python"
  local site_packages
  local installed_package_dir
  local command
  local launcher

  for required_file in \
    "${package_dir}/__init__.py" \
    "${package_dir}/store/rs/store.py" \
    "${package_dir}/mooncake-store-rs-client" \
    "${package_dir}/mooncake-store-rs-admin" \
    "${package_dir}/mooncake-store-rs-bench"; do
    [[ -f "${required_file}" ]] || {
      echo "CMake Python component install is missing ${required_file}" >&2
      exit 1
    }
  done

  "${BUILD_PYTHON_BIN}" -m venv "${venv_dir}"
  "${python_bin}" -m pip install \
    --disable-pip-version-check aiohttp msgpack numpy requests
  site_packages=$("${python_bin}" -I -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
  installed_package_dir="${site_packages}/mooncake"
  mkdir -p "${installed_package_dir}"
  cp -a "${package_dir}/." "${installed_package_dir}/"

  # A built root wheel creates these console entry points. The nightly build
  # already installed the same native commands through the CMake Python
  # component, so expose small launchers without rebuilding the whole wheel.
  for command in \
    mooncake-store-rs-client \
    mooncake-store-rs-admin \
    mooncake-store-rs-bench; do
    launcher="${venv_dir}/bin/${command}"
    {
      echo '#!/usr/bin/env bash'
      printf 'exec %q "$@"\n' "${installed_package_dir}/${command}"
    } >"${launcher}"
    chmod +x "${launcher}"
  done

  MOONCAKE_PYTHON_BIN="${python_bin}"
  export MOONCAKE_PYTHON_BIN
}

run_smoke() {
  env \
    MOONCAKE_PYTHON_BIN="${BUILD_PYTHON_BIN}" \
    bash "${SCRIPT_DIR}/run_store_rs_smoke.sh"
}

run_local_e2e() {
  run_with_redis_port \
    MC_STORE_RS_REDIS_PORT \
    env CARGO_TARGET_DIR="${BUILD_DIR}/mooncake-store-rs/cargo-tests" \
    bash "${STORE_RS_DIR}/scripts/e2e/run-local-e2e.sh"
}

run_python_compat() {
  run_with_redis_port \
    MC_STORE_RS_REDIS_PORT \
    bash "${STORE_RS_DIR}/scripts/e2e/run-python-compat-e2e.sh"
}

run_hot_cache() {
  run_with_redis_port \
    MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT \
    bash "${STORE_RS_DIR}/scripts/e2e/run-local-hot-cache-e2e.sh" all
}

run_ttl_recovery() {
  run_with_redis_port \
    MC_STORE_RS_TTL_RECOVERY_REDIS_PORT \
    bash "${STORE_RS_DIR}/scripts/e2e/run-redis-ttl-recovery-e2e.sh" all
}

run_route_migration() {
  run_with_redis_port \
    MC_STORE_RS_REDIS_PORT \
    bash "${STORE_RS_DIR}/scripts/e2e/run-route-migration-e2e.sh" move
}

run_eviction() {
  run_with_redis_port \
    MC_STORE_RS_REDIS_PORT \
    bash "${STORE_RS_DIR}/scripts/tests/client/test-client-eviction-cli.sh"
}

run_e2e_mode() {
  local mode=$1
  case "${mode}" in
    local-e2e)
      run_suite "Store-RS local Rust/TENT E2E" run_local_e2e
      ;;
    python-compat)
      run_suite "Store-RS Python compatibility E2E" run_python_compat
      ;;
    hot-cache)
      run_suite "Store-RS local hot-cache E2E" run_hot_cache
      ;;
    ttl-recovery)
      run_suite "Store-RS Redis TTL recovery E2E" run_ttl_recovery
      ;;
    route-migration)
      run_suite "Store-RS route migration move E2E" run_route_migration
      ;;
    eviction)
      run_suite "Store-RS background eviction E2E" run_eviction
      ;;
  esac
}

if [[ "${MODE}" == "smoke" ]]; then
  run_suite "Store-RS Classic TE and TENT smoke" run_smoke
  exit 0
fi

if [[ "${MODE}" == "all" ]]; then
  run_suite "Store-RS Classic TE and TENT smoke" run_smoke
  prepare_installed_python
  # TODO: Re-enable this suite once the Store-RS Classic TE descriptor bridge
  # forwards the TCP data host and P2PHANDSHAKE remote connections succeed.
  echo "==> Skipping Store-RS local hot-cache E2E: Classic TE TCP P2PHANDSHAKE connection is currently unavailable"
  echo "==> Skipping Store-RS route migration E2E: Classic TE TCP P2PHANDSHAKE connection is currently unavailable"
  echo "==> Skipping Store-RS background eviction E2E: Classic TE TCP P2PHANDSHAKE connection is currently unavailable"
  # TODO: Re-enable this suite once Classic TE metadata recovery preserves its
  # P2PHANDSHAKE RPC endpoint instead of publishing a stale endpoint.
  echo "==> Skipping Store-RS Redis TTL recovery E2E: Classic TE republishes a stale P2PHANDSHAKE endpoint after metadata recovery"
  for suite in \
    local-e2e \
    python-compat; do
    run_e2e_mode "${suite}"
  done
  exit 0
fi

if [[ "${MODE}" != "local-e2e" ]]; then
  prepare_installed_python
fi
run_e2e_mode "${MODE}"
