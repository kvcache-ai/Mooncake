#!/usr/bin/env bash
set -euo pipefail

fail() { echo "::error::$*" >&2; exit 1; }

cleanup_container() {
  local status=0
  docker stop --time 10 "$container_id" || status=1
  docker inspect "$container_id" > "$run_directory/output/container-final.json" || status=1
  if [ "$(docker inspect --format '{{.State.Running}}' "$container_id")" != false ]; then
    echo "Container processes were not released" >&2
    status=1
  fi
  docker rm "$container_id" || status=1
  docker info >/dev/null || status=1
  if docker inspect "$container_id" >/dev/null 2>&1; then
    echo "Current run container was not removed" >&2
    status=1
  fi
  return "$status"
}

# Actions may interrupt the shell trap during cancellation. Recheck only this
# run's containers in an always() step before the runner can be reused.
if [ "${1:-}" = --cleanup ]; then
  run_directory=$(realpath "$2")
  remaining=$(docker ps -aq --no-trunc \
    --filter "label=mooncake.xpu.run=$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT")
  docker info >/dev/null
  if [ -n "$remaining" ]; then
    while read -r container_id; do
      [[ "$container_id" =~ ^[0-9a-f]{64}$ ]] || fail "Invalid current-run container ID"
      cleanup_container
    done <<< "$remaining"
  fi
  echo '{"cleanup_recheck":"passed"}' > "$run_directory/output/cleanup-recheck.json"
  exit 0
fi

run_directory=$(realpath "$1")
source_directory=$(realpath "$2")
control_directory=$(realpath "$3")
mkdir -p "$run_directory/output"
exec > >(tee "$run_directory/output/runner.log") 2>&1

[ "$(id -u)" -ne 0 ] || { echo "XPU runner must not run as root" >&2; exit 1; }
: "${ZE_AFFINITY_MASK:?Set a host-owned two-GPU ZE_AFFINITY_MASK}"
: "${XPU_IMAGE:?Missing pinned runtime image}"
: "${CHECKOUT_SHA:?Missing authorized SHA}"
[[ "$ZE_AFFINITY_MASK" =~ ^(0|[1-9][0-9]*),(0|[1-9][0-9]*)$ ]] || {
  echo "ZE_AFFINITY_MASK must select two whole devices, not subdevices" >&2; exit 1;
}
[ "${BASH_REMATCH[1]}" != "${BASH_REMATCH[2]}" ] || fail "Affinity must select distinct GPUs"
[[ "$XPU_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] || fail "Runtime image must be digest-pinned"
for command in docker python3 timeout; do
  command -v "$command" >/dev/null || fail "Missing runner command: $command"
done

# Each runner owns a fixed pair. Preserve host enumeration so masks such as
# 2,3 and 6,7 stay valid; the mask is scheduling, not a device security boundary.
shopt -s nullglob
devices=(/dev/dri/renderD*)
[ "${#devices[@]}" -ge 2 ] || fail "At least two GPU render nodes are required"
device_args=()
for device in "${devices[@]}"; do
  [[ "$device" =~ ^/dev/dri/renderD[0-9]+$ ]] && [ -c "$device" ] \
    || fail "Not a GPU render character device: $device"
  device_args+=(--device "$device" --group-add "$(stat -c %g "$device")")
done
python3 "$control_directory/ci/xpu_core.py" seccomp "$run_directory/output/seccomp.json"

container_id=
cleanup() {
  local status=$? cleanup_status=0
  trap - EXIT INT TERM
  if [ -n "$container_id" ]; then
    cleanup_container || cleanup_status=1
  fi
  printf '{"cleanup_status":%d,"suite_status":%d}\n' "$cleanup_status" "$status" \
    > "$run_directory/output/cleanup.json"
  if [ "$cleanup_status" -ne 0 ]; then exit 1; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker pull "$XPU_IMAGE"
docker image inspect "$XPU_IMAGE" > "$run_directory/output/image.json"
container_id=$(docker create \
  --label "mooncake.xpu.run=$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT" \
  --user "$(id -u):$(id -g)" --cap-drop ALL \
  --security-opt no-new-privileges \
  --security-opt "seccomp=$run_directory/output/seccomp.json" \
  --network none --ipc private --shm-size 2g \
  --pids-limit 1024 --ulimit core=0 \
  "${device_args[@]}" \
  --mount "type=bind,src=$run_directory/bundle,dst=/bundle,readonly" \
  --mount "type=bind,src=$source_directory,dst=/source,readonly" \
  --mount "type=bind,src=$control_directory,dst=/control,readonly" \
  --mount "type=bind,src=$run_directory/output,dst=/output" \
  --env "ZE_AFFINITY_MASK=$ZE_AFFINITY_MASK" \
  --env ONEAPI_DEVICE_SELECTOR=level_zero:gpu \
  --env ZE_FLAT_DEVICE_HIERARCHY=FLAT \
  --env MC_USE_TENT=1 --env MOONCAKE_STORE_BACKEND=cpp \
  --env PROTOCOL=tcp --env MC_METADATA_SERVER=P2PHANDSHAKE \
  --env LOCAL_HOSTNAME=127.0.0.1 --env MASTER_SERVER=127.0.0.1:50051 \
  --env PYTHONOPTIMIZE=0 --env PYTHONDONTWRITEBYTECODE=1 \
  --env TEST_TMPDIR=/output \
  --env "XPU_SOURCE_SHA=$CHECKOUT_SHA" --env "XPU_IMAGE=$XPU_IMAGE" \
  --env HOME=/tmp/home --workdir /tmp \
  --entrypoint /bin/bash "$XPU_IMAGE" -c '
    set -eo pipefail
    mkdir -p "$HOME"
    python3 /control/ci/xpu_core.py run /bundle /source /output
  ')
timeout --signal=TERM --kill-after=30s 70m docker start --attach "$container_id"
