#!/usr/bin/env bash
set -euo pipefail

check_environment() {
  if (( EUID != 0 )); then
    echo "Run as root; kernel nvmet configuration requires it." >&2
    exit 1
  fi
  repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
  build_dir=${BUILD_DIR:-"$repo_root/build-linux"}
  binary="$build_dir/mooncake-store/tests/e2e/nof_linux_initiator"
  if [[ ! -x "$binary" ]]; then
    echo "Build the nof_linux_initiator target first: $binary" >&2
    exit 1
  fi
  for command in modprobe nvme truncate timeout udevadm; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
  done
  modprobe nvmet-tcp
  modprobe nvme-tcp
  config=/sys/kernel/config/nvmet
  if [[ ! -d "$config/subsystems" ]]; then
    echo "Kernel nvmet configfs is unavailable: $config" >&2
    exit 1
  fi
}

create_target() {
  truncate -s 64M "$work/namespace.img"
  mkdir "$subsystem"
  subsystem_created=1
  echo 1 > "$subsystem/attr_allow_any_host"
  mkdir "$subsystem/namespaces/1"
  printf '%s' "$work/namespace.img" > "$subsystem/namespaces/1/device_path"
  echo 1 > "$subsystem/namespaces/1/enable"
  mkdir "$port_dir"
  port_created=1
  echo tcp > "$port_dir/addr_trtype"
  echo ipv4 > "$port_dir/addr_adrfam"
  echo 127.0.0.1 > "$port_dir/addr_traddr"
  printf '%s' "$port" > "$port_dir/addr_trsvcid"
  ln -s "$subsystem" "$port_dir/subsystems/$nqn"
}

connect_namespace() {
  nvme connect -t tcp -a 127.0.0.1 -s "$port" -n "$nqn"
  connected=1
  udevadm settle --timeout=30
}

run_test() {
  export MC_TEST_NOF_ENDPOINT="trtype:tcp traddr:127.0.0.1 trsvcid:$port subnqn:$nqn ns:1"
  export LD_LIBRARY_PATH="$build_dir/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  printf 'Testing endpoint: %s\n' "$MC_TEST_NOF_ENDPOINT"
  timeout --kill-after=5s 30s "$binary"
}

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  if (( connected )); then
    nvme disconnect -n "$nqn" || status=1
  fi
  if (( port_created )); then
    if [[ -L "$port_dir/subsystems/$nqn" ]]; then
      rm "$port_dir/subsystems/$nqn" || status=1
    fi
    rmdir "$port_dir" || status=1
  fi
  if (( subsystem_created )); then
    if [[ -d "$subsystem/namespaces/1" ]]; then
      echo 0 > "$subsystem/namespaces/1/enable" || status=1
      rmdir "$subsystem/namespaces/1" || status=1
    fi
    rmdir "$subsystem" || status=1
  fi
  rm -f "$work/namespace.img" || status=1
  rmdir "$work" || status=1
  exit "$status"
}

check_environment

work=$(mktemp -d /tmp/mooncake-linux-nof.XXXXXX)
nqn="nqn.2026-10.io.mooncake:test.$(basename "$work")"
port=$((24000 + $$ % 20000))
subsystem="$config/subsystems/$nqn"
port_dir="$config/ports/$port"
subsystem_created=0
port_created=0
connected=0

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

create_target
connect_namespace
run_test
