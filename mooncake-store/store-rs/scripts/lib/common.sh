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

mc_scripts_list_upstream_dirs() {
  local repo_root=$1
  local primary_worktree

  if [[ -n "${MOONCAKE_UPSTREAM_DIR:-}" ]]; then
    printf '%s\n' "${MOONCAKE_UPSTREAM_DIR}"
  fi
  printf '%s\n' "${repo_root}/third_party/Mooncake"

  # Inside the Mooncake monorepo the upstream tree is the enclosing repository
  # rather than a submodule; identify it by its transfer-engine module so this
  # does not latch onto an unrelated parent directory.
  if [[ -d "${repo_root}/../../mooncake-transfer-engine" ]]; then
    (cd "${repo_root}/../.." && pwd)
  fi

  if primary_worktree=$(git -C "${repo_root}" worktree list --porcelain 2>/dev/null | awk '/^worktree / { print substr($0, 10); exit }'); then
    if [[ -n "${primary_worktree}" && "${primary_worktree}" != "${repo_root}" ]]; then
      printf '%s\n' "${primary_worktree}/third_party/Mooncake"
    fi
  fi
}

mc_scripts_resolve_upstream_build_dir() {
  local repo_root=$1
  local candidates=()
  local candidate
  local upstream_dir

  if [[ -n "${MOONCAKE_UPSTREAM_BUILD_DIR:-}" ]]; then
    candidates+=("${MOONCAKE_UPSTREAM_BUILD_DIR}")
  fi

  while IFS= read -r upstream_dir; do
    [[ -z "${upstream_dir}" ]] && continue
    candidates+=(
      "${upstream_dir}/build-rust"
      "${upstream_dir}/build-wheel-compat"
    )
  done < <(mc_scripts_list_upstream_dirs "${repo_root}")

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
      && [[ -f "${candidate}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "unable to find Mooncake runtime libraries under any known Mooncake upstream tree" >&2
  echo "checked candidates:" >&2
  printf '  %s\n' "${candidates[@]}" >&2
  echo "set MOONCAKE_UPSTREAM_BUILD_DIR to a built upstream directory" >&2
  exit 1
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
  local repo_root=$1
  local pythonpath_mode=${2:-none}
  local build_dir=${3:-}
  local upstream_dir

  if [[ -z "${build_dir}" ]]; then
    build_dir=$(mc_scripts_resolve_upstream_build_dir "${repo_root}")
  fi
  upstream_dir=$(cd -- "${build_dir}/.." && pwd)

  export MOONCAKE_UPSTREAM_DIR="${upstream_dir}"
  export MOONCAKE_UPSTREAM_BUILD_DIR="${build_dir}"
  mc_scripts_prepend_env_path \
    LD_LIBRARY_PATH \
    "${build_dir}/mooncake-transfer-engine/src" \
    "${build_dir}/mooncake-transfer-engine/tent/src"

  case "${pythonpath_mode}" in
    none)
      ;;
    python)
      mc_scripts_prepend_env_path PYTHONPATH "${repo_root}/python"
      ;;
    repo-python)
      mc_scripts_prepend_env_path PYTHONPATH "${repo_root}" "${repo_root}/python"
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
