#!/usr/bin/env bash

# Source from a suite and install `trap 'ci_cleanup_services "$?"' EXIT`.
# Commands must stay in the foreground so their PID remains owned by the suite.
declare -A CI_SERVICE_PIDS=()
declare -A CI_SERVICE_LOGS=()
declare -a CI_SERVICE_ORDER=()

ci_start_service() {
  local name=$1 log=$2
  shift 2
  "$@" >"$log" 2>&1 &
  CI_SERVICE_PIDS[$name]=$!
  CI_SERVICE_LOGS[$name]=$log
  CI_SERVICE_ORDER+=("$name")
}

ci_wait_service() {
  local name=$1 port ready _
  shift
  local pid=${CI_SERVICE_PIDS[$name]}
  for _ in {1..50}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "::error::$name exited before becoming ready"
      return 1
    fi
    ready=true
    for port in "$@"; do
      if ! ss -H -ltn "sport = :$port" | grep -q .; then
        ready=false
        break
      fi
    done
    if "$ready"; then
      return 0
    fi
    sleep 0.1
  done
  echo "::error::$name did not listen on ports $* within 5 seconds"
  return 1
}

ci_stop_service() {
  local name=$1
  local pid=${CI_SERVICE_PIDS[$name]:-}
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    unset 'CI_SERVICE_PIDS[$name]'
  fi
}

ci_cleanup_services() {
  local status=$1 name index
  # Stop dependents before services started earlier (e.g. the metadata server).
  for ((index=${#CI_SERVICE_ORDER[@]}-1; index>=0; index--)); do
    ci_stop_service "${CI_SERVICE_ORDER[$index]}"
  done
  if [ "$status" -ne 0 ]; then
    for name in "${CI_SERVICE_ORDER[@]}"; do
      echo "=== $name ==="
      cat "${CI_SERVICE_LOGS[$name]}" 2>/dev/null || true
    done
  fi
  return "$status"
}
