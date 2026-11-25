#!/usr/bin/env bash
set -euo pipefail

# Redis Cluster integration runs in a subshell so its process cleanup and
# environment stay separate from the original offline script tests below.
run_redis_cluster_tests() (
build_dir=${1:-build}
redis_server=${REDIS_SERVER:-redis-server}
redis_cli=${REDIS_CLI:-redis-cli}
base_port=${REDIS_TEST_BASE_PORT:-17300}
test_dir=$(mktemp -d /tmp/mooncake-redis-routing.XXXXXX)
pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    echo "Redis test logs retained in $test_dir"
}
trap cleanup EXIT
nodes=()
for i in {0..10}; do
    port=$((base_port + i))
    mkdir "$test_dir/$port"
    extra=()
    if ((i < 9)); then
        extra=(--cluster-enabled yes --cluster-config-file nodes.conf)
        nodes+=("127.0.0.1:$port")
    elif ((i == 10)); then
        extra=(--replicaof 127.0.0.1 "$((base_port + 9))")
    fi
    "$redis_server" --bind 127.0.0.1 --port "$port" --save "" --appendonly no \
        --dir "$test_dir/$port" --logfile "$test_dir/$port/redis.log" \
        "${extra[@]}" &
    pids+=("$!")
done
for i in {0..10}; do
    ready=0
    for attempt in {1..100}; do
        kill -0 "${pids[$i]}"
        if "$redis_cli" -p "$((base_port + i))" ping 2>/dev/null | grep -q PONG; then
            ready=1
            break
        fi
        sleep 0.1
    done
    [[ $ready == 1 ]]
done
"$redis_cli" --cluster create "${nodes[@]}" --cluster-replicas 2 --cluster-yes
ready=0
for attempt in {1..100}; do
    synced=1
    for i in 3 4 5 6 7 8 10; do
        if ! "$redis_cli" -p "$((base_port + i))" info replication |
            grep -q master_link_status:up; then
            synced=0
        fi
    done
    if [[ $synced == 1 ]] &&
       "$redis_cli" -p "$base_port" cluster info | grep -q cluster_state:ok &&
       "$redis_cli" -p "$base_port" --json cluster slots |
           python3 -c 'import json,sys; slots=json.load(sys.stdin); sys.exit(not (len(slots)==3 and all(len(s)==5 for s in slots)))'; then
        ready=1
        break
    fi
    sleep 0.2
done
if [[ $ready != 1 ]]; then
    "$redis_cli" -p "$base_port" --json cluster slots
    "$redis_cli" -p "$base_port" cluster nodes
    exit 1
fi
# Allow the initial replica synchronizations to complete before asserting ACKs.
sleep 2
export MC_TEST_REDIS_CLUSTER_SEEDS="redis+cluster://127.0.0.1:$base_port,127.0.0.1:$((base_port + 1))"
"$build_dir/mooncake-store/tests/redis_ha_kv_backend_test" \
    --redis_endpoint="127.0.0.1:$((base_port + 9))"
GTEST_FILTER='*Redis*' "$build_dir/mooncake-store/tests/high_availability_test" \
    --redis_endpoint="127.0.0.1:$((base_port + 9))"
python3 - "$build_dir" "$test_dir" "$redis_cli" "$base_port" <<'PY_REDIS_SERVICE_TEST'
"""Run only against the private Redis fixture created by the routing harness."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request


def main():
    build, run_dir, redis_cli, base = sys.argv[1:]
    build, run_dir, base = Path(build).resolve(), Path(run_dir), int(base)
    seeds = os.environ["MC_TEST_REDIS_CLUSTER_SEEDS"]
    env = dict(os.environ, MC_STORE_CLUSTER_ID="service-routing-test")
    processes = []
    logs = []

    def start(name, args):
        log = (run_dir / f"{name}.log").open("w")
        logs.append(log)
        process = subprocess.Popen(args, stdout=log, stderr=log, env=env)
        processes.append(process)
        return process

    def wait_for(predicate, seconds=30):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            result = predicate()
            if result:
                return result
            time.sleep(0.1)
        raise AssertionError("Timed out; inspect service logs in " + str(run_dir))

    def health(index):
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{base + 30 + index}/health", timeout=0.5
            ) as response:
                return json.load(response)
        except (OSError, ValueError):
            return {}

    def redis(*args):
        return subprocess.check_output(
            [redis_cli, "-p", str(base), "--json", *args], text=True, timeout=5
        )

    try:
        masters = []
        for index in range(2):
            masters.append(
                start(
                    f"master-{index}",
                    [
                        str(build / "mooncake-store/src/mooncake_master"),
                        "--enable_ha=true",
                        "--ha_backend_type=redis",
                        f"--ha_backend_connstring={seeds}",
                        "--ha_redis_expected_replicas_per_shard=2",
                        "--cluster_id=service-routing-test",
                        "--enable_oplog=true",
                        "--rpc_address=127.0.0.1",
                        f"--rpc_port={base + 20 + index}",
                        f"--metrics_port={base + 30 + index}",
                        "--rpc_thread_num=2",
                        "--enable_http_metadata_server=false",
                        "--logtostderr=true",
                    ],
                )
            )

        def one_leader():
            states = [health(i) for i in range(2)]
            if all(states) and sum(s.get("service_ready", False) for s in states) == 1:
                return [i for i, state in enumerate(states) if state["service_ready"]]
            return None

        leader = wait_for(one_leader)[0]
        client = str(build / "mooncake-store/tests/e2e/oplog_ha_client")
        common = [
            client,
            f"--master_server_entry={seeds}",
            "--protocol=tcp",
            "--engine_meta_url=P2PHANDSHAKE",
            "--count=8",
            "--key_prefix=redis-routing-service",
            "--payload_size=128",
            f"--manifest={run_dir / 'manifest'}",
        ]
        provider = start(
            "provider", common + ["--mode=provider", f"--port={base + 40}"]
        )
        wait_for(lambda: "provider_ready" in (run_dir / "provider.log").read_text())
        for mode in ("seed", "verify"):
            process = start(mode, common + [f"--mode={mode}", f"--port={base + 41}"])
            assert process.wait(timeout=40) == 0, f"{mode} failed"
        assert provider.poll() is None
        # OpLog was enabled above: verify batches reached the Redis shard,
        # independently of the client's acknowledged put/get operations.
        keys = []
        slots = json.loads(redis("CLUSTER", "SLOTS"))
        for shard in slots:
            keys.extend(
                json.loads(
                    subprocess.check_output(
                        [
                            redis_cli,
                            "-p",
                            str(shard[2][1]),
                            "--json",
                            "KEYS",
                            "*service-routing-test*",
                        ],
                        text=True,
                        timeout=5,
                    )
                )
            )
        assert any("oplog" in key for key in keys), keys

        # Pause every replica, not the primaries: topology remains complete,
        # but neither Master can obtain the configured WAIT 2 confirmation.
        replica_ports = {node[1] for shard in slots for node in shard[3:]}
        for port in replica_ports:
            subprocess.run(
                [redis_cli, "-p", str(port), "CLIENT", "PAUSE", "10000", "ALL"],
                check=True,
                stdout=subprocess.DEVNULL,
                timeout=5,
            )
        started = time.monotonic()
        wait_for(
            lambda: masters[leader].poll() is not None
            or health(leader).get("service_ready") is False,
            seconds=4,
        )
        assert time.monotonic() - started < 5
        assert not any(health(i).get("service_ready", False) for i in range(2))
        print(
            "PASS: two Masters, Redis OpLog put/get, and serving revoked on missing ACKs"
        )
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
PY_REDIS_SERVICE_TEST
)

case "${1:-}" in
  --redis-cluster)
    shift
    if (( $# > 1 )); then
      echo "Usage: $0 --redis-cluster [build-dir]" >&2
      exit 2
    fi
    run_redis_cluster_tests "$@"
    exit 0
    ;;
  --help|-h)
    echo "Usage: $0 [--redis-cluster [build-dir]]"
    echo "No arguments: offline OpLog script tests; --redis-cluster: private Redis integration."
    exit 0
    ;;
  "") ;;
  *)
    echo "Usage: $0 [--redis-cluster [build-dir]]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$SCRIPT_DIR/run_oplog_batch_cluster.sh"
TEST_ROOT=${TEST_ROOT:-"/tmp/mooncake-oplog-cluster-script-test-$$"}
mkdir -p "$TEST_ROOT"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_failure() {
  local expected_status=$1
  local expected_text=$2
  shift 2

  local output_file="$TEST_ROOT/output-$RANDOM.log"
  local status=0
  "$@" >"$output_file" 2>&1 || status=$?
  [[ "$status" -eq "$expected_status" ]] ||
    fail "expected status $expected_status, got $status: $(<"$output_file")"
  grep -Fq "$expected_text" "$output_file" ||
    fail "expected '$expected_text': $(<"$output_file")"
}

expect_success() {
  local expected_text=$1
  shift

  local output_file="$TEST_ROOT/output-$RANDOM.log"
  "$@" >"$output_file" 2>&1 ||
    fail "expected success: $(<"$output_file")"
  grep -Fq "$expected_text" "$output_file" ||
    fail "expected '$expected_text': $(<"$output_file")"
}

expect_failure 1 "unknown command" "$SCRIPT" unknown
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" up --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" failpoint-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" failpoint-crash-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" remove-boundary-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" standby-read-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" promotion-catchup-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" allocator-recovery-matrix --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "master config does not exist" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT" \
  --master-config "$TEST_ROOT/missing-master.yaml"
expect_failure 1 "non-ha-workers must be positive" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT" --non-ha-workers 0
expect_failure 1 "ha-objects must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-objects 0
expect_failure 1 "ha-payload-bytes must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-payload-bytes 0
expect_failure 1 "ha-pressure-sec must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-pressure-sec 0
expect_failure 1 "snapshot-chunk-object-count must be positive" \
  "$SCRIPT" up --build-dir "$TEST_ROOT" --snapshot-chunk-object-count 0
expect_failure 1 "memory-allocator must be offset or cachelib" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --memory-allocator invalid
expect_failure 1 "recovery-seed-objects must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-seed-objects 0
expect_failure 1 "recovery-refill-objects must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-refill-objects 0
expect_failure 1 "recovery-pressure-sec must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-pressure-sec 0
expect_failure 1 "recovery-segment-bytes must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-segment-bytes 0
expect_failure 1 "recovery-payload-sizes must be positive CSV integers" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-payload-sizes 64,0,4096
expect_failure 1 "allocator recovery requires exactly 3 masters" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" --masters 2
EMPTY_BUILD="$TEST_ROOT/empty-build"
mkdir -p "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" up --build-dir "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" allocator-recovery-smoke \
  --build-dir "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" non-ha-smoke \
  --build-dir "$EMPTY_BUILD" --no-etcd-observer
expect_failure 1 "run directory does not exist" \
  "$SCRIPT" status --run-dir "$TEST_ROOT/missing-run"
expect_failure 1 "run directory does not exist" \
  "$SCRIPT" restart --run-dir "$TEST_ROOT/missing-run"

EMPTY_RUN="$TEST_ROOT/empty-run"
mkdir -p "$EMPTY_RUN/pids"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$EMPTY_RUN"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$EMPTY_RUN"

MISMATCH_RUN="$TEST_ROOT/mismatch-run"
mkdir -p "$MISMATCH_RUN/pids"
sleep 30 &
SLEEP_PID=$!
trap 'kill "$SLEEP_PID" 2>/dev/null || true' EXIT
printf '%s\n' "$SLEEP_PID" >"$MISMATCH_RUN/pids/master-0.pid"
printf '%s\n' "definitely-not-the-sleep-command" \
  >"$MISMATCH_RUN/pids/master-0.cmd"
expect_success "refusing to stop PID" \
  "$SCRIPT" down --run-dir "$MISMATCH_RUN"
kill -0 "$SLEEP_PID" 2>/dev/null || fail "mismatched PID was stopped"

MATCH_RUN="$TEST_ROOT/match-run"
mkdir -p "$MATCH_RUN/pids"
sleep 30 &
MATCH_PID=$!
printf '%s\n' "$MATCH_PID" >"$MATCH_RUN/pids/master-0.pid"
tr '\0' ' ' <"/proc/$MATCH_PID/cmdline" >"$MATCH_RUN/pids/master-0.cmd"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$MATCH_RUN"
kill -0 "$MATCH_PID" 2>/dev/null && fail "matching PID was not stopped"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$MATCH_RUN"

# Execute start_master with a fake binary, including both environment paths.
FAKE_MASTER="$TEST_ROOT/fake-master"
cat >"$FAKE_MASTER" <<'MASTER'
#!/usr/bin/env bash
[[ "$MOONCAKE_SNAPSHOT_LOCAL_PATH" == "$EXPECTED_SNAPSHOT_PATH" ]]
[[ "${MOONCAKE_TEST_FAILPOINT_DIR:-}" == "$EXPECTED_FAILPOINT_PATH" ]]
[[ "$*" == *"--enable_oplog_snapshot=true"* ]]
MASTER
chmod +x "$FAKE_MASTER"
for failpoints in "" "$TEST_ROOT/failpoints"; do
  (
    source "$SCRIPT"
    parse_up_options --build-dir "$TEST_ROOT" --run-dir "$TEST_ROOT/env-run" \
      --enable-oplog-snapshot --snapshot-chunk-object-count 2
    MASTER_BIN="$FAKE_MASTER"
    ETCD_ENDPOINTS=127.0.0.1:1
    RPC_PORTS=(10001)
    ADMIN_PORTS=(10002)
    FAILPOINT_DIR="$failpoints"
    export EXPECTED_SNAPSHOT_PATH="$RUN_DIR/snapshots"
    export EXPECTED_FAILPOINT_PATH="$failpoints"
    start_process() { shift; "$@"; }
    start_master 0
  ) || fail "snapshot environment propagation failed"
done

(
  source "$SCRIPT"
  RUN_DIR="$TEST_ROOT/old-run"
  mkdir -p "$RUN_DIR"
  printf 'RPC_PORTS=10001\nADMIN_PORTS=10002\nBUILD_DIR=/unused\n' >"$RUN_DIR/cluster.env"
  unset ENABLE_OPLOG_SNAPSHOT SNAPSHOT_CHUNK_OBJECT_COUNT
  load_cluster_env
  [[ "$ENABLE_OPLOG_SNAPSHOT" == false ]]
  [[ "$SNAPSHOT_CHUNK_OBJECT_COUNT" == 1000000 ]]
) || fail "older cluster.env did not retain pure OpLog defaults"

for command in capacity-soak capacity-nospace; do
  expect_failure 1 "build directory does not exist" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT/missing-build"
  expect_failure 1 "external endpoints are forbidden" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT" --etcd-endpoints localhost:2379
  expect_failure 1 "capacity-seconds must be positive" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT" --capacity-seconds 0
  expect_failure 1 "capacity-seconds must be at least 30" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT" --capacity-seconds 29
  expect_failure 1 "capacity-max-batches must be positive" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT" --capacity-max-batches invalid
  expect_failure 1 "fresh run directory" \
    "$SCRIPT" "$command" --build-dir "$TEST_ROOT" --run-dir "$EMPTY_RUN"
done
python3 "$SCRIPT_DIR/../ha/snapshot/batch_oplog/capacity_test.py"
echo "PASS"
