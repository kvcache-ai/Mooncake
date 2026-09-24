#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/run-all-tests.sh
#
# Unified script-based regression entrypoint for CI and local runs.
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# store-rs is a subdirectory when it lives inside the Mooncake monorepo,
# where the git toplevel is the enclosing repository rather than this tree.
[ -f "${REPO_ROOT}/Cargo.toml" ] || REPO_ROOT="${REPO_ROOT}/mooncake-store/store-rs"
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
STAMP=$(date +%Y%m%d-%H%M%S)
LOG_DIR_DEFAULT="${REPO_ROOT}/target/regression-logs/scripts-ci-${STAMP}"

LIST_ONLY=0
DRY_RUN=0
FAIL_FAST=0
FAIL_ON_SKIP=0
JOBS="${MC_STORE_RS_JOBS:-1}"
LOG_DIR="${MC_STORE_RS_SCRIPT_CI_LOG_DIR:-${LOG_DIR_DEFAULT}}"

declare -a INCLUDE_PATTERNS=()
declare -a EXCLUDE_PATTERNS=()
declare -a REQUIRE_TAGS=()
declare -a EXCLUDE_TAGS=()

declare -a DISCOVERED_TESTS=()
declare -a SELECTED_TESTS=()
declare -a RESULT_STATUS=()
declare -a RESULT_PATH=()
declare -a RESULT_REASON=()
declare -a RESULT_LOG=()
declare -a RESULT_DURATION=()

usage() {
  cat <<'EOF'
Usage: scripts/run-all-tests.sh [options]

Auto-discover and run the shell-based validation entrypoints under `scripts/`.
This is the repository's unified scripts regression runner for CI and local use.

Discovery rules:
  - scans: `scripts/**/*.sh`
  - filename must match `run-*.sh` or `test-*.sh`
  - excludes: `scripts/build/`, `scripts/clients/`, and this runner itself

Options:
  --list               List discovered tests and exit
  --dry-run            Print the selected execution plan without running tests
  --include PATTERN    Keep only tests whose path contains PATTERN (repeatable)
  --exclude PATTERN    Drop tests whose path contains PATTERN (repeatable)
  --tag TAG            Keep only tests carrying TAG (repeatable)
  --skip-tag TAG       Drop tests carrying TAG (repeatable)
  --jobs N             Run up to N non-serial tests in parallel (default: 1)
  --fail-fast          Stop at the first failure (serial tests only)
  --fail-on-skip       Return non-zero if any test is skipped
  --log-dir PATH       Override the log directory
  -h, --help           Show this help

Examples:
  ./scripts/run-all-tests.sh
  ./scripts/run-all-tests.sh --list
  ./scripts/run-all-tests.sh --jobs 4
  ./scripts/run-all-tests.sh --include rolling --include hot-upgrade
  ./scripts/run-all-tests.sh --tag client --tag rolling

Environment:
  MC_STORE_RS_JOBS                          Default --jobs value (default: 1)
  MC_STORE_RS_SCRIPT_CI_LOG_DIR             Log directory override
  MC_STORE_RS_SCRIPT_TIMEOUT_DEFAULT        Default per-test timeout in seconds
                                            (default: 1800)
  MC_STORE_RS_SCRIPT_TIMEOUT_STRESS         Timeout for stress runners
                                            (default: 3600)
  MC_STORE_RS_SCRIPT_TIMEOUT_ROLLING        Timeout for rolling-upgrade runners
                                            (default: 2400)

Tag examples:
  e2e, client, rolling, serial, compat, stress, hot-cache,
  hot-upgrade, rollback, redis-recovery, real, dummy
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)
      LIST_ONLY=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --include)
      INCLUDE_PATTERNS+=("${2:?--include requires a value}")
      shift 2
      ;;
    --exclude)
      EXCLUDE_PATTERNS+=("${2:?--exclude requires a value}")
      shift 2
      ;;
    --tag)
      REQUIRE_TAGS+=("${2:?--tag requires a value}")
      shift 2
      ;;
    --skip-tag)
      EXCLUDE_TAGS+=("${2:?--skip-tag requires a value}")
      shift 2
      ;;
    --jobs)
      JOBS="${2:?--jobs requires a value}"
      shift 2
      ;;
    --fail-fast)
      FAIL_FAST=1
      shift
      ;;
    --fail-on-skip)
      FAIL_ON_SKIP=1
      shift
      ;;
    --log-dir)
      LOG_DIR="${2:?--log-dir requires a value}"
      shift 2
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

contains_tag() {
  local needle=$1
  shift || true
  local current
  for current in "$@"; do
    [[ "${current}" == "${needle}" ]] && return 0
  done
  return 1
}

script_tags() {
  local path=$1
  local -a tags=()

  case "${path}" in
    scripts/e2e/*) tags+=(e2e) ;;
    scripts/tests/client/*) tags+=(client) ;;
    scripts/tests/rolling/*) tags+=(rolling serial) ;;
  esac

  [[ "${path}" == *compat* ]] && tags+=(compat)
  [[ "${path}" == *stress* ]] && tags+=(stress)
  [[ "${path}" == *hot-cache* ]] && tags+=(hot-cache)
  [[ "${path}" == *hot-upgrade* ]] && tags+=(hot-upgrade)
  [[ "${path}" == *rollback* ]] && tags+=(rollback)
  if [[ "${path}" == *ttl-recovery* || "${path}" == *redis-ttl-recovery* ]]; then
    tags+=(redis-recovery)
  fi
  [[ "${path}" == *dummy* ]] && tags+=(dummy)
  [[ "${path}" == *real* ]] && tags+=(real)

  printf '%s\n' "${tags[*]}"
}

script_timeout_seconds() {
  local path=$1
  case "${path}" in
    *stress*)
      printf '%s\n' "${MC_STORE_RS_SCRIPT_TIMEOUT_STRESS:-3600}"
      ;;
    scripts/tests/rolling/*)
      printf '%s\n' "${MC_STORE_RS_SCRIPT_TIMEOUT_ROLLING:-2400}"
      ;;
    *)
      printf '%s\n' "${MC_STORE_RS_SCRIPT_TIMEOUT_DEFAULT:-1800}"
      ;;
  esac
}

path_matches_any() {
  local path=$1
  shift || true
  local pattern
  for pattern in "$@"; do
    [[ -z "${pattern}" ]] && continue
    if [[ "${path}" == *"${pattern}"* ]]; then
      return 0
    fi
  done
  return 1
}

is_selected_test() {
  local path=$1
  local tags_string
  local -a tags=()
  local required_tag
  local excluded_tag

  if ((${#INCLUDE_PATTERNS[@]} > 0)) && ! path_matches_any "${path}" "${INCLUDE_PATTERNS[@]}"; then
    return 1
  fi

  if ((${#EXCLUDE_PATTERNS[@]} > 0)) && path_matches_any "${path}" "${EXCLUDE_PATTERNS[@]}"; then
    return 1
  fi

  tags_string=$(script_tags "${path}")
  if [[ -n "${tags_string}" ]]; then
    read -r -a tags <<<"${tags_string}"
  fi

  for required_tag in "${REQUIRE_TAGS[@]}"; do
    if ! contains_tag "${required_tag}" "${tags[@]}"; then
      return 1
    fi
  done

  for excluded_tag in "${EXCLUDE_TAGS[@]}"; do
    if contains_tag "${excluded_tag}" "${tags[@]}"; then
      return 1
    fi
  done

  return 0
}

discover_tests() {
  local path
  while IFS= read -r path; do
    case "${path}" in
      scripts/run-all-tests.sh|scripts/build/*|scripts/clients/*) continue ;;
    esac
    case "$(basename "${path}")" in
      run-*.sh|test-*.sh) DISCOVERED_TESTS+=("${path}") ;;
    esac
  done < <(
    find "${REPO_ROOT}/scripts" \
      -type f -name '*.sh' -print \
      | sed "s#^${REPO_ROOT}/##" \
      | sort
  )
}

select_tests() {
  local path
  for path in "${DISCOVERED_TESTS[@]}"; do
    if is_selected_test "${path}"; then
      SELECTED_TESTS+=("${path}")
    fi
  done
}

allocate_port() {
  python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

tracked_worktree_state() {
  git -C "${REPO_ROOT}" status --short --untracked-files=no
}

sanitize_name() {
  local path=$1
  local name=${path//\//__}
  name=${name//./_}
  printf '%s\n' "${name}"
}

extract_skip_reason() {
  local log_file=$1
  local line
  line=$(grep -E '^SKIP:' "${log_file}" | head -n 1 || true)
  line=${line#SKIP:}
  line=${line# }
  printf '%s\n' "${line}"
}

cleanup_redis_port() {
  local redis_port=$1
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -p "${redis_port}" shutdown nosave >/dev/null 2>&1 || true
  fi
}

record_result() {
  RESULT_STATUS+=("$1")
  RESULT_PATH+=("$2")
  RESULT_REASON+=("$3")
  RESULT_LOG+=("$4")
  RESULT_DURATION+=("$5")
}

run_one_test() {
  local index=$1
  local path=$2
  local log_file="${LOG_DIR}/$(printf '%02d' "${index}")-$(sanitize_name "${path}").log"
  local redis_port
  local timeout_seconds
  local tags_string
  local tracked_before
  local tracked_after
  local start_epoch
  local end_epoch
  local duration
  local status
  local reason=""
  local rc=0

  mkdir -p "${LOG_DIR}"
  redis_port=$(allocate_port)
  timeout_seconds=$(script_timeout_seconds "${path}")
  tags_string=$(script_tags "${path}")
  tracked_before=$(tracked_worktree_state)
  start_epoch=$(date +%s)

  {
    echo "==> script: ${path}"
    echo "==> tags: ${tags_string}"
    echo "==> timeout_seconds: ${timeout_seconds}"
    echo "==> redis_port: ${redis_port}"
    echo "==> start: $(date -Is)"
  } | tee "${log_file}"

  if (( DRY_RUN == 1 )); then
    end_epoch=$(date +%s)
    duration=$((end_epoch - start_epoch))
    status="DRY"
    reason="selected"
    record_result "${status}" "${path}" "${reason}" "${log_file}" "${duration}"
    printf '%s\t%s\t%s\n' "${status}" "${reason}" "${duration}" > "${log_file}.result"
    echo "[DRY ] ${path}"
    return 0
  fi

  set +e
  if command -v timeout >/dev/null 2>&1 && [[ "${timeout_seconds}" != "0" ]]; then
    env \
      MC_STORE_RS_REDIS_PORT="${redis_port}" \
      MC_STORE_RS_REDIS_URL="redis://127.0.0.1:${redis_port}/0" \
      MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT="${redis_port}" \
      MC_STORE_RS_TTL_RECOVERY_REDIS_PORT="${redis_port}" \
      bash -lc "timeout --signal=TERM --kill-after=30s ${timeout_seconds}s ./${path}" \
      >>"${log_file}" 2>&1
    rc=$?
  else
    env \
      MC_STORE_RS_REDIS_PORT="${redis_port}" \
      MC_STORE_RS_REDIS_URL="redis://127.0.0.1:${redis_port}/0" \
      MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT="${redis_port}" \
      MC_STORE_RS_TTL_RECOVERY_REDIS_PORT="${redis_port}" \
      bash -lc "./${path}" \
      >>"${log_file}" 2>&1
    rc=$?
  fi
  set -e

  cleanup_redis_port "${redis_port}"

  tracked_after=$(tracked_worktree_state)
  if [[ "${tracked_before}" != "${tracked_after}" ]]; then
    {
      echo
      echo "==> tracked worktree changed during test"
      echo "--- before ---"
      printf '%s\n' "${tracked_before}"
      echo "--- after ---"
      printf '%s\n' "${tracked_after}"
    } >>"${log_file}"
    rc=97
  fi

  end_epoch=$(date +%s)
  duration=$((end_epoch - start_epoch))

  if [[ ${rc} -eq 0 ]]; then
    reason=$(extract_skip_reason "${log_file}")
    if [[ -n "${reason}" ]]; then
      status="SKIP"
    else
      status="PASS"
      reason="ok"
    fi
  else
    status="FAIL"
    reason="exit=${rc}"
  fi

  record_result "${status}" "${path}" "${reason}" "${log_file}" "${duration}"
  printf '%s\t%s\t%s\n' "${status}" "${reason}" "${duration}" > "${log_file}.result"

  case "${status}" in
    PASS) printf '[PASS] %s (%ss)\n' "${path}" "${duration}" ;;
    SKIP) printf '[SKIP] %s (%ss) %s\n' "${path}" "${duration}" "${reason}" ;;
    FAIL) printf '[FAIL] %s (%ss) %s\n' "${path}" "${duration}" "${reason}" ;;
    *)    printf '[%s] %s (%ss) %s\n' "${status}" "${path}" "${duration}" "${reason}" ;;
  esac

  if [[ "${status}" == "FAIL" ]]; then
    echo "  log: ${log_file}"
    echo "  tail:"
    tail -n 20 "${log_file}" | sed 's/^/    /'
    return 1
  fi

  return 0
}

print_list() {
  local path
  local timeout_seconds
  local tags_string
  for path in "${SELECTED_TESTS[@]}"; do
    timeout_seconds=$(script_timeout_seconds "${path}")
    tags_string=$(script_tags "${path}")
    printf '%s | timeout=%ss | tags=%s\n' "${path}" "${timeout_seconds}" "${tags_string}"
  done
}

print_summary() {
  local i
  local pass_count=0
  local fail_count=0
  local skip_count=0
  local dry_count=0

  echo
  echo "==> summary"
  for ((i = 0; i < ${#RESULT_STATUS[@]}; i += 1)); do
    printf '  %-4s %s (%ss) - %s\n' \
      "${RESULT_STATUS[i]}" \
      "${RESULT_PATH[i]}" \
      "${RESULT_DURATION[i]}" \
      "${RESULT_REASON[i]}"
    case "${RESULT_STATUS[i]}" in
      PASS) pass_count=$((pass_count + 1)) ;;
      FAIL) fail_count=$((fail_count + 1)) ;;
      SKIP) skip_count=$((skip_count + 1)) ;;
      DRY)  dry_count=$((dry_count + 1)) ;;
    esac
  done

  echo
  printf 'total=%d pass=%d fail=%d skip=%d dry=%d\n' \
    "${#RESULT_STATUS[@]}" \
    "${pass_count}" \
    "${fail_count}" \
    "${skip_count}" \
    "${dry_count}"
  echo "logs: ${LOG_DIR}"

  if (( fail_count > 0 )); then
    exit 1
  fi
  if (( FAIL_ON_SKIP == 1 && skip_count > 0 )); then
    exit 2
  fi
}

mc_scripts_require_command git
mc_scripts_require_command bash
mc_scripts_require_command python3

cd "${REPO_ROOT}"

discover_tests
select_tests

if ((${#SELECTED_TESTS[@]} == 0)); then
  echo "no tests matched the current selection" >&2
  exit 1
fi

if (( LIST_ONLY == 1 )); then
  print_list
  exit 0
fi

echo "==> scripts regression runner"
echo "==> repo: ${REPO_ROOT}"
echo "==> log dir: ${LOG_DIR}"
echo "==> selected: ${#SELECTED_TESTS[@]}"
echo "==> jobs: ${JOBS}"

if (( DRY_RUN == 1 )); then
  print_list
fi

# ---------------------------------------------------------------------------
# Partition into parallel and serial (tagged "serial") groups.
# ---------------------------------------------------------------------------
declare -a PARALLEL_TESTS=()
declare -a SERIAL_TESTS=()
for test_path in "${SELECTED_TESTS[@]}"; do
  if [[ "$(script_tags "${test_path}")" == *serial* ]]; then
    SERIAL_TESTS+=("${test_path}")
  else
    PARALLEL_TESTS+=("${test_path}")
  fi
done

index=0

# ---------------------------------------------------------------------------
# Parallel group — run up to JOBS tests concurrently.
# Results are written to ${log_file}.result and collected after all finish.
# ---------------------------------------------------------------------------
if ((JOBS > 1 && ${#PARALLEL_TESTS[@]} > 0)); then
  declare -a par_pids=()
  declare -a par_indices=()
  par_start=$((index + 1))

  for i in "${!PARALLEL_TESTS[@]}"; do
    index=$((par_start + i))
    par_indices+=("${index}")

    # Throttle: wait for a slot to free up before launching the next test.
    while ((${#par_pids[@]} >= JOBS)); do
      new_pids=()
      for pid in "${par_pids[@]}"; do
        kill -0 "${pid}" 2>/dev/null && new_pids+=("${pid}")
      done
      par_pids=("${new_pids[@]}")
      ((${#par_pids[@]} >= JOBS)) && sleep 0.1
    done

    run_one_test "${index}" "${PARALLEL_TESTS[$i]}" &
    par_pids+=($!)
  done

  # Wait for all parallel tests to finish.
  for pid in "${par_pids[@]}"; do
    wait "${pid}" || true
  done

  # Collect results from the .result files written by each subshell.
  for i in "${!PARALLEL_TESTS[@]}"; do
    idx="${par_indices[$i]}"
    path="${PARALLEL_TESTS[$i]}"
    log_file="${LOG_DIR}/$(printf '%02d' "${idx}")-$(sanitize_name "${path}").log"
    if [[ -f "${log_file}.result" ]]; then
      IFS=$'\t' read -r r_status r_reason r_duration < "${log_file}.result"
      record_result "${r_status}" "${path}" "${r_reason}" "${log_file}" "${r_duration}"
    else
      record_result "FAIL" "${path}" "result-file-missing" "${log_file}" "0"
    fi
  done

  index=$((par_start + ${#PARALLEL_TESTS[@]} - 1))
else
  # JOBS == 1: run parallel group sequentially (same as before).
  for test_path in "${PARALLEL_TESTS[@]}"; do
    index=$((index + 1))
    if ! run_one_test "${index}" "${test_path}"; then
      if (( FAIL_FAST == 1 )); then
        break
      fi
    fi
  done
fi

# ---------------------------------------------------------------------------
# Serial group — always sequential (rolling upgrade tests mutate source files).
# ---------------------------------------------------------------------------
if ((${#SERIAL_TESTS[@]} > 0)); then
  echo "==> serial tests: ${#SERIAL_TESTS[@]}"
fi
for test_path in "${SERIAL_TESTS[@]}"; do
  index=$((index + 1))
  if ! run_one_test "${index}" "${test_path}"; then
    if (( FAIL_FAST == 1 )); then
      break
    fi
  fi
done

print_summary
