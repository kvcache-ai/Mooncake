#!/usr/bin/env bash

# ---------------------------------------------------------------------------
# scripts/lib/common.sh
#
# Shared shell helpers for script entrypoints under scripts/.
# Keep this file boring: only repo/runtime bootstrap helpers that are truly
# repeated across many runners belong here.
# ---------------------------------------------------------------------------

mc_scripts_require_command() {
  local command_name=$1
  local context=${2:-}

  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${command_name}" == "cargo" ]]; then
    local cargo_env="${CARGO_HOME:-${HOME}/.cargo}/env"
    if [[ -f "${cargo_env}" ]]; then
      # shellcheck disable=SC1090
      source "${cargo_env}"
    fi
  fi

  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi

  if [[ -n "${context}" ]]; then
    echo "${command_name} is required for ${context}" >&2
  else
    echo "missing required command: ${command_name}" >&2
  fi
  exit 1
}

mc_scripts_resolve_upstream_build_dir() {
  local build_dir=${MOONCAKE_BUILD_DIR:?MOONCAKE_BUILD_DIR must be set to an explicit CMake build directory}
  [[ "${build_dir}" == /* ]] || { echo "MOONCAKE_BUILD_DIR must be an absolute path: ${build_dir}" >&2; exit 1; }
  if [[ ! -f "${build_dir}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
    || [[ ! -f "${build_dir}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
    echo "MOONCAKE_BUILD_DIR must contain the built Transfer Engine and TENT libraries: ${build_dir}" >&2
    exit 1
  fi
  printf '%s\n' "${build_dir}"
}

mc_scripts_prepend_env_path() {
  local var_name=$1
  shift

  local -a entries=()
  local value
  for value in "$@"; do
    [[ -n "${value}" ]] && entries+=("${value}")
  done

  if ((${#entries[@]} == 0)); then
    return 0
  fi

  local joined
  local current=${!var_name:-}
  joined=$(IFS=:; printf '%s' "${entries[*]}")
  if [[ -n "${current}" ]]; then
    printf -v "${var_name}" '%s:%s' "${joined}" "${current}"
  else
    printf -v "${var_name}" '%s' "${joined}"
  fi
  export "${var_name}"
}

mc_scripts_setup_upstream_runtime_env() {
  local pythonpath_mode=${1:-none}
  local source_root=${MOONCAKE_ROOT_DIR:?MOONCAKE_ROOT_DIR must be set to an explicit Mooncake source directory}
  local store_rs_root=${MOONCAKE_STORE_RS_DIR:?MOONCAKE_STORE_RS_DIR must be set to an explicit Store-RS source directory}
  local build_dir
  [[ "${source_root}" == /* ]] || { echo "MOONCAKE_ROOT_DIR must be an absolute path: ${source_root}" >&2; exit 1; }
  [[ "${store_rs_root}" == /* ]] || { echo "MOONCAKE_STORE_RS_DIR must be an absolute path: ${store_rs_root}" >&2; exit 1; }
  [[ -d "${source_root}" ]] || { echo "MOONCAKE_ROOT_DIR must be a directory: ${source_root}" >&2; exit 1; }
  [[ -d "${store_rs_root}" ]] || { echo "MOONCAKE_STORE_RS_DIR must be a directory: ${store_rs_root}" >&2; exit 1; }
  build_dir=$(mc_scripts_resolve_upstream_build_dir)
  mc_scripts_prepend_env_path \
    LD_LIBRARY_PATH \
    "${build_dir}/mooncake-transfer-engine/src" \
    "${build_dir}/mooncake-transfer-engine/tent/src"

  case "${pythonpath_mode}" in
    none)
      ;;
    python)
      mc_scripts_prepend_env_path PYTHONPATH "${store_rs_root}/python"
      ;;
    repo-python)
      mc_scripts_prepend_env_path PYTHONPATH "${store_rs_root}" "${store_rs_root}/python"
      ;;
    *)
      echo "unsupported python path mode: ${pythonpath_mode}" >&2
      exit 1
      ;;
  esac
}

mc_scripts_start_local_redis_if_needed() {
  local redis_port=$1
  local started_var=${2:-}
  local started=0

  if ! redis-cli -p "${redis_port}" ping >/dev/null 2>&1; then
    redis-server \
      --port "${redis_port}" \
      --bind 127.0.0.1 \
      --daemonize yes \
      --save '' \
      --appendonly no
    started=1
  fi

  if [[ -n "${started_var}" ]]; then
    printf -v "${started_var}" '%s' "${started}"
  fi
}
