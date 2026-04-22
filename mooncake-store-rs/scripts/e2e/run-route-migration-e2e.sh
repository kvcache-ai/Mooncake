#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"

MODE="${1:-move}"

usage() {
  cat <<'EOF'
Usage: scripts/e2e/run-route-migration-e2e.sh [move|copy|copy-multi]

Bring up a local Redis metadata backend, two or three standalone store clients, and the
standalone admin HTTP server. Then use a Python helper to:

  1. submit an admin HTTP route-migration task
  2. poll the task state until it succeeds
  3. verify query_route reflects the expected route shape
  4. verify the migrated value remains readable

Default mode:
  move  - move a key from the source client segment to the target segment

Alternative mode:
  copy  - copy a key from the source client segment to one target segment
  copy-multi
        - copy a key from the source client segment to two target segments

Environment:
  MC_STORE_RS_REDIS_PORT            Fixed Redis port; auto-allocates when empty
  MC_STORE_RS_KEEP_TEMP             Keep temp dir on failure when set to 1
  MC_STORE_RS_ROUTE_MIGRATION_MODE  Optional mode override when no CLI arg is
                                   passed (`move`, `copy`, or `copy-multi`)
  MC_STORE_RS_ROUTE_MIGRATION_SUBMITTER
                                   Task submitter used by the Python driver:
                                   `http` (default) or `cli`
  MC_STORE_RS_ROUTE_MIGRATION_LEASE_TTL_MS
                                   Lease TTL for the standalone clients
                                   (default: 15000)
  MC_STORE_RS_ROUTE_MIGRATION_STORAGE_BYTES
                                   Storage bytes per storage client
                                   (default: 64 MiB)
  MC_STORE_RS_ROUTE_MIGRATION_SCRATCH_BYTES
                                   Scratch bytes per storage client
                                   (default: 16 MiB)
  MC_STORE_RS_ROUTE_MIGRATION_REQUEST_TIMEOUT_MS
                                   Python helper HTTP timeout in milliseconds
                                   (default: 2000)
  CARGO_TARGET_DIR                 Cargo target dir override. Defaults to a
                                   fresh temp dir under the e2e temp root so
                                   host and container builds do not fight over
                                   permissions or stale artifacts.
  MC_STORE_RS_ROUTE_MIGRATION_BIN_DIR
                                   Optional prebuilt bin dir. When set, the
                                   e2e reuses existing standalone binaries and
                                   skips `cargo build`.
  MC_STORE_RS_ROUTE_MIGRATION_CLIENT_BIN
                                   Optional explicit client binary path.
  MC_STORE_RS_ROUTE_MIGRATION_ADMIN_BIN
                                   Optional explicit admin server binary path.
  MC_STORE_RS_ROUTE_MIGRATION_ADMIN_CLI_BIN
                                   Optional explicit admin CLI binary path.
  MOONCAKE_UPSTREAM_DIR            Mooncake upstream checkout override
  MOONCAKE_UPSTREAM_BUILD_DIR      Built upstream directory override
EOF
}

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -n "${MC_STORE_RS_ROUTE_MIGRATION_MODE:-}" && "${MODE}" == "move" ]]; then
  MODE="${MC_STORE_RS_ROUTE_MIGRATION_MODE}"
fi

if [[ "${MODE}" != "move" && "${MODE}" != "copy" && "${MODE}" != "copy-multi" ]]; then
  echo "unsupported mode: ${MODE}" >&2
  usage >&2
  exit 1
fi

SUBMITTER="${MC_STORE_RS_ROUTE_MIGRATION_SUBMITTER:-http}"
if [[ "${SUBMITTER}" != "http" && "${SUBMITTER}" != "cli" ]]; then
  echo "unsupported submitter: ${SUBMITTER}" >&2
  usage >&2
  exit 1
fi

allocate_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_log() {
  local log_file=$1
  local needle=$2
  local timeout_seconds=${3:-20}
  local attempts=$((timeout_seconds * 10))
  local attempt

  for ((attempt = 0; attempt < attempts; attempt += 1)); do
    if [[ -f "${log_file}" ]] && grep -Fq "${needle}" "${log_file}"; then
      return 0
    fi
    sleep 0.1
  done

  echo "timed out waiting for log line: ${needle}" >&2
  echo "--- ${log_file} ---" >&2
  cat "${log_file}" >&2
  return 1
}

wait_for_http_healthz() {
  local base_url=$1
  local timeout_seconds=${2:-20}
  python3 - "${base_url}" "${timeout_seconds}" <<'PY'
import sys
import time
import urllib.request

base_url = sys.argv[1]
timeout_seconds = float(sys.argv[2])
deadline = time.time() + timeout_seconds
last_error = None

while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"{base_url}/healthz", timeout=2) as response:
            if response.read().decode() == "ok\n":
                raise SystemExit(0)
    except Exception as error:
        last_error = error
        time.sleep(0.1)

raise SystemExit(f"admin health check failed for {base_url}: {last_error!r}")
PY
}

have_command() {
  command -v "$1" >/dev/null 2>&1
}

wait_for_docker_redis() {
  local container_name=$1
  local timeout_seconds=${2:-20}
  python3 - "${container_name}" "${timeout_seconds}" <<'PY'
import subprocess
import sys
import time

container_name = sys.argv[1]
timeout_seconds = float(sys.argv[2])
deadline = time.time() + timeout_seconds
last_error = None

while time.time() < deadline:
    try:
        completed = subprocess.run(
            ["docker", "exec", container_name, "redis-cli", "ping"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        if completed.stdout.strip() == "PONG":
            raise SystemExit(0)
    except Exception as error:
        last_error = error
        time.sleep(0.2)

raise SystemExit(f"docker redis did not become ready: {last_error!r}")
PY
}

start_local_or_docker_redis_if_needed() {
  local redis_port=$1
  local started_var=${2:-}
  local container_var=${3:-}
  local container_name=${4:-}
  local started=0
  local container_started=

  if have_command redis-cli && redis-cli -p "${redis_port}" ping >/dev/null 2>&1; then
    started=0
  elif have_command redis-cli && have_command redis-server; then
    mc_scripts_start_local_redis_if_needed "${redis_port}" started
  else
    mc_scripts_require_command docker "docker redis fallback"
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
    docker run -d --rm \
      --name "${container_name}" \
      -p "${redis_port}:6379" \
      --entrypoint redis-server \
      "${MC_STORE_RS_ROUTE_MIGRATION_REDIS_IMAGE:-mooncake-store-nightly-builder:20260421}" \
      --port 6379 \
      --bind 0.0.0.0 \
      --save '' \
      --appendonly no \
      >/dev/null
    wait_for_docker_redis "${container_name}" 20
    started=1
    container_started="${container_name}"
  fi

  if [[ -n "${started_var}" ]]; then
    printf -v "${started_var}" '%s' "${started}"
  fi
  if [[ -n "${container_var}" ]]; then
    printf -v "${container_var}" '%s' "${container_started}"
  fi
}

REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-$(allocate_port)}"
SOURCE_TRANSPORT_PORT="$(allocate_port)"
TARGET_TRANSPORT_PORT="$(allocate_port)"
EXTRA_TARGET_TRANSPORT_PORT="$(allocate_port)"
ADMIN_PORT="$(allocate_port)"

SOURCE_STABLE_ID="route-migration-source-$(date +%s%N)"
TARGET_STABLE_ID="route-migration-target-$(date +%s%N)"
EXTRA_TARGET_STABLE_ID="route-migration-target-extra-$(date +%s%N)"
KEYSPACE="mc/store-rs/e2e/route-migration/$(date +%s%N)"
KEY="route-migration-key"
TENANT="default"
SOURCE_SEGMENT="route-migration-source-segment-${SOURCE_STABLE_ID}"
TARGET_SEGMENT="route-migration-target-segment-${TARGET_STABLE_ID}"
EXTRA_TARGET_SEGMENT="route-migration-target-segment-${EXTRA_TARGET_STABLE_ID}"
PAYLOAD="route-migration-payload-${SOURCE_STABLE_ID}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
ADMIN_URL="http://127.0.0.1:${ADMIN_PORT}"
TMP_BASE="${TMPDIR:-/nvme/tmp}"
mkdir -p "${TMP_BASE}"
TMP_DIR="$(mktemp -d "${TMP_BASE}/mc-route-migration-e2e.XXXXXX")"
SOURCE_LOG="${TMP_DIR}/source-client.log"
TARGET_LOG="${TMP_DIR}/target-client.log"
EXTRA_TARGET_LOG="${TMP_DIR}/target-extra-client.log"
ADMIN_LOG="${TMP_DIR}/admin-server.log"
PIDS=()
REDIS_STARTED=0
REDIS_CONTAINER_NAME="mc-route-migration-e2e-redis-${SOURCE_STABLE_ID}"
REDIS_CONTAINER_STARTED=

cleanup() {
  local status=$?
  local pid

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -TERM "${pid}" >/dev/null 2>&1 || true
    fi
  done
  sleep 0.2
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -KILL "${pid}" >/dev/null 2>&1 || true
    fi
  done

  if [[ "${REDIS_STARTED}" == "1" ]]; then
    redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi
  if [[ -n "${REDIS_CONTAINER_STARTED}" ]]; then
    docker rm -f "${REDIS_CONTAINER_STARTED}" >/dev/null 2>&1 || true
  fi

  if [[ "${status}" != "0" && "${MC_STORE_RS_KEEP_TEMP:-0}" == "1" ]]; then
    echo "preserving temp dir: ${TMP_DIR}" >&2
  else
    rm -rf "${TMP_DIR}"
  fi

  exit "${status}"
}
trap cleanup EXIT

mc_scripts_require_command cargo
mc_scripts_require_command python3

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1
export MC_STORE_RS_REDIS_URL="${REDIS_URL}"
export MC_STORE_RS_REDIS_PORT="${REDIS_PORT}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMP_DIR}/cargo-target}"

CLIENT_BIN_OVERRIDE=${MC_STORE_RS_ROUTE_MIGRATION_CLIENT_BIN:-}
ADMIN_BIN_OVERRIDE=${MC_STORE_RS_ROUTE_MIGRATION_ADMIN_BIN:-}
ADMIN_CLI_BIN_OVERRIDE=${MC_STORE_RS_ROUTE_MIGRATION_ADMIN_CLI_BIN:-}
if [[ -n "${MC_STORE_RS_ROUTE_MIGRATION_BIN_DIR:-}" ]]; then
  CLIENT_BIN_OVERRIDE=${CLIENT_BIN_OVERRIDE:-${MC_STORE_RS_ROUTE_MIGRATION_BIN_DIR}/mooncake-store-client}
  ADMIN_BIN_OVERRIDE=${ADMIN_BIN_OVERRIDE:-${MC_STORE_RS_ROUTE_MIGRATION_BIN_DIR}/mooncake-store-admin-server}
  ADMIN_CLI_BIN_OVERRIDE=${ADMIN_CLI_BIN_OVERRIDE:-${MC_STORE_RS_ROUTE_MIGRATION_BIN_DIR}/mooncake-store-admin}
fi

start_local_or_docker_redis_if_needed \
  "${REDIS_PORT}" \
  REDIS_STARTED \
  REDIS_CONTAINER_STARTED \
  "${REDIS_CONTAINER_NAME}"

cd "${REPO_ROOT}"

if [[ -n "${CLIENT_BIN_OVERRIDE}" || -n "${ADMIN_BIN_OVERRIDE}" ]]; then
  echo "==> reusing prebuilt standalone binaries"
else
  echo "==> building standalone mooncake-store-client/admin binaries"
  cargo build -p mooncake-store-py
fi

TARGET_DIR="${CARGO_TARGET_DIR}"
BIN="${CLIENT_BIN_OVERRIDE:-${TARGET_DIR}/debug/mooncake-store-client}"
ADMIN_BIN="${ADMIN_BIN_OVERRIDE:-${TARGET_DIR}/debug/mooncake-store-admin-server}"
ADMIN_CLI_BIN="${ADMIN_CLI_BIN_OVERRIDE:-${TARGET_DIR}/debug/mooncake-store-admin}"

if [[ ! -x "${BIN}" ]]; then
  echo "expected client binary was not produced at ${BIN}" >&2
  exit 1
fi
if [[ ! -x "${ADMIN_BIN}" ]]; then
  echo "expected admin binary was not produced at ${ADMIN_BIN}" >&2
  exit 1
fi
if [[ "${SUBMITTER}" == "cli" && ! -x "${ADMIN_CLI_BIN}" ]]; then
  echo "expected admin CLI binary was not produced at ${ADMIN_CLI_BIN}" >&2
  exit 1
fi

CLIENT_BASE_ARGS=(
  --local-hostname 127.0.0.1
  --metadata-url "${REDIS_URL}"
  --storage-bytes "${MC_STORE_RS_ROUTE_MIGRATION_STORAGE_BYTES:-$((64 * 1024 * 1024))}"
  --scratch-bytes "${MC_STORE_RS_ROUTE_MIGRATION_SCRATCH_BYTES:-$((16 * 1024 * 1024))}"
  --protocol tcp
  --transport-backend classic-te
  --route-control metadata-only
  --keyspace "${KEYSPACE}"
  --lease-ttl-ms "${MC_STORE_RS_ROUTE_MIGRATION_LEASE_TTL_MS:-15000}"
  --heartbeat-interval-ms 500
  --label pool=pool-a
  --label storage=true
)

echo "==> starting source store client"
"${BIN}" \
  "${CLIENT_BASE_ARGS[@]}" \
  --stable-id "${SOURCE_STABLE_ID}" \
  --epoch 1 \
  --transport-rpc-port "${SOURCE_TRANSPORT_PORT}" \
  --local-segment-name "${SOURCE_SEGMENT}" \
  >"${SOURCE_LOG}" 2>&1 &
PIDS+=($!)
wait_for_log "${SOURCE_LOG}" "mooncake-store-client started stable_id=${SOURCE_STABLE_ID}" 30

echo "==> starting target store client"
"${BIN}" \
  "${CLIENT_BASE_ARGS[@]}" \
  --stable-id "${TARGET_STABLE_ID}" \
  --epoch 1 \
  --transport-rpc-port "${TARGET_TRANSPORT_PORT}" \
  --local-segment-name "${TARGET_SEGMENT}" \
  >"${TARGET_LOG}" 2>&1 &
PIDS+=($!)
wait_for_log "${TARGET_LOG}" "mooncake-store-client started stable_id=${TARGET_STABLE_ID}" 30

if [[ "${MODE}" == "copy-multi" ]]; then
  echo "==> starting extra target store client"
  "${BIN}" \
    "${CLIENT_BASE_ARGS[@]}" \
    --stable-id "${EXTRA_TARGET_STABLE_ID}" \
    --epoch 1 \
    --transport-rpc-port "${EXTRA_TARGET_TRANSPORT_PORT}" \
    --local-segment-name "${EXTRA_TARGET_SEGMENT}" \
    >"${EXTRA_TARGET_LOG}" 2>&1 &
  PIDS+=($!)
  wait_for_log "${EXTRA_TARGET_LOG}" "mooncake-store-client started stable_id=${EXTRA_TARGET_STABLE_ID}" 30
fi

echo "==> starting admin HTTP server"
"${ADMIN_BIN}" \
  --metadata-url "${REDIS_URL}" \
  --keyspace "${KEYSPACE}" \
  --bind-addr "127.0.0.1:${ADMIN_PORT}" \
  >"${ADMIN_LOG}" 2>&1 &
PIDS+=($!)
wait_for_http_healthz "${ADMIN_URL}" 30

TARGET_SEGMENTS_JSON=$(python3 - "${MODE}" "${TARGET_SEGMENT}" "${EXTRA_TARGET_SEGMENT}" <<'PY'
import json
import sys

mode = sys.argv[1]
segments = [sys.argv[2]]
if mode == "copy-multi":
    segments.append(sys.argv[3])
print(json.dumps(segments))
PY
)

python3 - "${ADMIN_URL}" "${REDIS_URL}" "${MODE}" "${TENANT}" "${KEYSPACE}" "${KEY}" "${PAYLOAD}" "${SOURCE_SEGMENT}" "${TARGET_SEGMENTS_JSON}" "${SOURCE_STABLE_ID}" "${MC_STORE_RS_ROUTE_MIGRATION_REQUEST_TIMEOUT_MS:-2000}" "${SUBMITTER}" "${ADMIN_CLI_BIN}" <<'PY'
import json
import re
import subprocess
import sys
import time
import urllib.request

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

admin_url = sys.argv[1]
redis_url = sys.argv[2]
mode = sys.argv[3]
tenant = sys.argv[4]
keyspace = sys.argv[5]
key = sys.argv[6]
payload = sys.argv[7].encode()
source_segment = sys.argv[8]
target_segments = json.loads(sys.argv[9])
source_stable_id = sys.argv[10]
request_timeout_ms = int(sys.argv[11])
submitter = sys.argv[12]
admin_cli_bin = sys.argv[13]
request_timeout_s = max(request_timeout_ms / 1000.0, 1.0)
submit_mode = "copy" if mode == "copy-multi" else mode


def request_json(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f"{admin_url}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(request, timeout=request_timeout_s) as response:
        return json.loads(response.read().decode())


def run_admin_cli(*args):
    completed = subprocess.run(
        [admin_cli_bin, *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"admin cli failed rc={completed.returncode}: {completed.stdout}"
        )
    return completed.stdout


def parse_cli_fields(output):
    fields = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if ": " not in line:
            continue
        key_name, value = line.split(": ", 1)
        fields[key_name] = value
    return fields


def wait_for_task(task_id):
    deadline = time.time() + 60.0
    last_status = None
    while time.time() < deadline:
        if submitter == "cli":
            output = run_admin_cli(
                "--metadata-url",
                redis_url,
                "--admin-url",
                admin_url,
                "--keyspace",
                keyspace,
                "migrate",
                "task",
                "get",
                "--task-id",
                task_id,
            )
            status = parse_cli_fields(output)
        else:
            status = request_json("GET", f"/v1/route-migrations/{task_id}")
        last_status = status
        state = status["state"]
        if state in {"succeeded", "failed", "cancelled"}:
            return status
        time.sleep(0.2)
    raise AssertionError(f"route migration task {task_id} did not finish: {last_status!r}")


def route_segments(route):
    return [replica["segment_name"] for replica in route["replicas"]]


def assert_route_shape(route, expected_source, expected_targets):
    segments = route_segments(route)
    has_source = source_segment in segments
    assert has_source is expected_source, (route, segments, expected_source)
    for target_segment in target_segments:
        assert (target_segment in segments) is expected_targets, (
            route,
            segments,
            target_segment,
            expected_targets,
        )


store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1",
    redis_url,
    4 * 1024 * 1024,
    1 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id=f"route-migration-driver-{int(time.time() * 1000)}",
    keyspace=keyspace,
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    routed_writes=True,
    replica_count=1,
    route_control="metadata_only",
) == 0

initial_config = ReplicateConfig(replica_num=1, preferred_segment=source_segment)
assert store.put(key, payload, tenant=tenant, config=initial_config) == 0
initial_route = store.query_route(key, tenant=tenant)
assert initial_route is not None
assert_route_shape(initial_route, expected_source=True, expected_targets=False)
assert store.get(key, tenant=tenant) == payload

if submitter == "cli":
    submit_args = [
        "--metadata-url",
        redis_url,
        "--admin-url",
        admin_url,
        "--keyspace",
        keyspace,
        "migrate",
        submit_mode,
        "--authority",
        source_stable_id,
        "--tenant",
        tenant,
        "--key",
        key,
        "--source-segment",
        source_segment,
    ]
    for target_segment in target_segments:
        submit_args.extend(["--target-segment", target_segment])
    submit_args.extend(["--task-executor", source_stable_id])
    submit_output = run_admin_cli(*submit_args)
    submit_response = parse_cli_fields(submit_output)
    task_id = submit_response["task_id"]

    listed_output = run_admin_cli(
        "--metadata-url",
        redis_url,
        "--admin-url",
        admin_url,
        "--keyspace",
        keyspace,
        "migrate",
        "task",
        "list",
    )
    listed = parse_cli_fields(listed_output)
    assert int(listed["count"]) >= 1
else:
    submit_body = {
        "authority": source_stable_id,
        "tenant": tenant,
        "key": key,
        "mode": submit_mode,
        "source_segment": source_segment,
        "target_segments": target_segments,
        "task_executor": source_stable_id,
    }
    submit_response = request_json("POST", "/v1/route-migrations", submit_body)
    task_id = submit_response["task_id"]

    listed = request_json("GET", "/v1/route-migrations")
    assert listed["count"] >= 1
    assert any(task["task_id"] == task_id for task in listed["tasks"])

status = wait_for_task(task_id)
assert status["state"] == "succeeded", status
assert status["task_executor"] == source_stable_id

final_route = None
deadline = time.time() + 30.0
while time.time() < deadline:
    final_route = store.query_route(key, tenant=tenant)
    if final_route is not None:
        segments = route_segments(final_route)
        if submit_mode == "move":
            if segments == target_segments:
                break
        else:
            if source_segment in segments and all(
                target_segment in segments for target_segment in target_segments
            ):
                break
    time.sleep(0.2)

assert final_route is not None
if submit_mode == "move":
    assert_route_shape(final_route, expected_source=False, expected_targets=True)
else:
    assert_route_shape(final_route, expected_source=True, expected_targets=True)

assert store.get(key, tenant=tenant) == payload

print(
    json.dumps(
        {
            "task_id": task_id,
            "state": status["state"],
            "mode": mode,
            "route_segments": route_segments(final_route),
        },
        sort_keys=True,
    )
)

store.close()
PY

echo "route migration e2e ok"
