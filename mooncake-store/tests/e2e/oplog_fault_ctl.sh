#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<EOF
Usage:
  $0 failpoint <arm|wait|release> --run-dir DIR --name NAME [--timeout-sec N]
  $0 process <stop|continue|kill> --run-dir DIR --name NAME
EOF
}

parse_options() {
  RUN_DIR=""
  NAME=""
  TIMEOUT_SEC=30
  while (($#)); do
    case "$1" in
      --run-dir)
        (($# >= 2)) || die "--run-dir requires a value"
        RUN_DIR=$2
        shift 2
        ;;
      --name)
        (($# >= 2)) || die "--name requires a value"
        NAME=$2
        shift 2
        ;;
      --timeout-sec)
        (($# >= 2)) || die "--timeout-sec requires a value"
        TIMEOUT_SEC=$2
        shift 2
        ;;
      *) die "unknown option: $1" ;;
    esac
  done
  [[ -d "$RUN_DIR" ]] || die "run directory does not exist: $RUN_DIR"
  [[ "$NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || die "invalid name: $NAME"
  [[ "$TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] || die "timeout-sec must be positive"
}

record_event() {
  mkdir -p "$RUN_DIR/faults"
  printf '%s\t%s\t%s\n' "$(date +%s)" "$1" "$NAME" >>"$RUN_DIR/faults/events.tsv"
}

failpoint_ctl() {
  local action=$1
  [[ "$NAME" =~ ^[a-z0-9][a-z0-9_]*$ ]] || die "invalid failpoint name: $NAME"
  local directory=${FAILPOINT_DIR:-"$RUN_DIR/failpoints"}
  mkdir -p "$directory"
  local base="$directory/$NAME"
  case "$action" in
    arm)
      [[ ! -e "$base.hit" && ! -e "$base.release" ]] ||
        die "stale failpoint files exist for $NAME"
      : >"$base.arm"
      record_event "failpoint_arm"
      ;;
    wait)
      local deadline=$((SECONDS + TIMEOUT_SEC))
      while [[ ! -e "$base.hit" ]] && ((SECONDS < deadline)); do
        sleep 0.01
      done
      [[ -e "$base.hit" ]] || die "timed out waiting for failpoint: $NAME"
      record_event "failpoint_hit"
      ;;
    release)
      [[ -e "$base.hit" ]] || die "failpoint has not been hit: $NAME"
      : >"$base.release"
      record_event "failpoint_release"
      ;;
    *) die "unknown failpoint action: $action" ;;
  esac
  echo "ok: failpoint $action $NAME"
}

process_ctl() {
  local action=$1
  local pid_file="$RUN_DIR/pids/$NAME.pid"
  local cmd_file="$RUN_DIR/pids/$NAME.cmd"
  [[ -r "$pid_file" && -r "$cmd_file" ]] || die "missing process files for $NAME"
  local pid
  pid=$(<"$pid_file")
  [[ "$pid" =~ ^[0-9]+$ && -r "/proc/$pid/cmdline" ]] ||
    die "process is not running: $NAME"
  local actual
  actual=$(tr '\0' ' ' <"/proc/$pid/cmdline")
  grep -Fqx "$actual" "$cmd_file" || die "PID command does not match: $NAME"

  local signal
  case "$action" in
    stop) signal=STOP ;;
    continue) signal=CONT ;;
    kill) signal=KILL ;;
    *) die "unknown process action: $action" ;;
  esac
  kill -"$signal" "$pid"
  record_event "process_$action"
  echo "ok: process $action $NAME pid=$pid"
}

main() {
  (($# >= 2)) || {
    usage
    exit 1
  }
  local kind=$1
  local action=$2
  shift 2
  parse_options "$@"
  case "$kind" in
    failpoint) failpoint_ctl "$action" ;;
    process) process_ctl "$action" ;;
    *) die "unknown control type: $kind" ;;
  esac
}

main "$@"
