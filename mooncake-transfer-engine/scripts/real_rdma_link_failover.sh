#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
BENCH="${BENCH:-$BUILD_DIR/mooncake-transfer-engine/example/transfer_engine_bench}"
VALIDATOR="${VALIDATOR:-$BUILD_DIR/mooncake-transfer-engine/example/transfer_engine_validator}"
LOG_DIR="${LOG_DIR:-/tmp/mooncake-real-rdma-link-failover-$(date +%s)}"

FAULT_SIDE="receiver"
DOWN_AFTER_SEC=5
DOWN_FOR_SEC=7
DURATION_SEC=200
TARGET_PRIMARY="mlx5_3"
TARGET_AVAILABLE="mlx5_4"
SENDER_PRIMARY="mlx5_2"
SENDER_AVAILABLE="mlx5_1"
NETDEV_OVERRIDE=""
VALIDATE_DATA="${VALIDATE_DATA:-1}"

usage() {
    cat <<EOF
Usage:
  $0 [options]

Options:
  --fault-side sender|receiver   Which side's primary RNIC to link down.
                                 Default: receiver
  --target-primary DEV           Target primary RNIC. Default: mlx5_3
  --target-available DEV         Target available/fallback RNIC. Default: mlx5_4
  --sender-primary DEV           Sender primary RNIC. Default: mlx5_2
  --sender-available DEV         Sender available/fallback RNIC. Default: mlx5_1
  --target-pref DEV              Alias for --target-primary.
  --target-fallback DEV          Alias for --target-available.
  --sender-pref DEV              Alias for --sender-primary.
  --sender-fallback DEV          Alias for --sender-available.
  --netdev IFACE                 Override Linux netdev to down/up.
  --down-after SEC               Seconds after initiator starts before down.
                                 Default: 5
  --down-for SEC                 Seconds to keep the link down. Default: 7
  --duration SEC                 Initiator transfer duration. Default: 200
  --log-dir DIR                  Log/output directory.
  --validate-data                Use transfer_engine_validator, which performs
                                 write/read/memcmp integrity checks.
  --no-validate-data             Use transfer_engine_bench.

Examples:
  sudo $0 --fault-side receiver
  sudo $0 --fault-side sender
  sudo $0 --fault-side sender
  sudo $0 --fault-side sender --sender-primary mlx5_2 --sender-available mlx5_1
  sudo $0 --fault-side receiver --target-primary mlx5_3 --target-available mlx5_4
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fault-side) FAULT_SIDE="$2"; shift 2 ;;
        --target-primary|--target-pref) TARGET_PRIMARY="$2"; shift 2 ;;
        --target-available|--target-fallback) TARGET_AVAILABLE="$2"; shift 2 ;;
        --sender-primary|--sender-pref) SENDER_PRIMARY="$2"; shift 2 ;;
        --sender-available|--sender-fallback) SENDER_AVAILABLE="$2"; shift 2 ;;
        --netdev) NETDEV_OVERRIDE="$2"; shift 2 ;;
        --down-after) DOWN_AFTER_SEC="$2"; shift 2 ;;
        --down-for) DOWN_FOR_SEC="$2"; shift 2 ;;
        --duration) DURATION_SEC="$2"; shift 2 ;;
        --log-dir) LOG_DIR="$2"; shift 2 ;;
        --validate-data) VALIDATE_DATA=1; shift ;;
        --no-validate-data) VALIDATE_DATA=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

if [[ "$FAULT_SIDE" != "sender" && "$FAULT_SIDE" != "receiver" ]]; then
    echo "--fault-side must be sender or receiver" >&2
    exit 2
fi

validate_single_rnic() {
    local label="$1"
    local value="$2"
    if [[ -z "$value" ]]; then
        echo "$label RNIC must not be empty" >&2
        exit 2
    fi
    if [[ "$value" == *,* ]]; then
        echo "$label RNIC must be exactly one device for this failover validation: $value" >&2
        exit 2
    fi
}

validate_pair() {
    local side="$1"
    local primary="$2"
    local available="$3"
    validate_single_rnic "$side primary" "$primary"
    validate_single_rnic "$side available" "$available"
    if [[ "$primary" == "$available" ]]; then
        echo "$side primary and available RNIC must be different: $primary" >&2
        exit 2
    fi
}

validate_pair "target" "$TARGET_PRIMARY" "$TARGET_AVAILABLE"
validate_pair "sender" "$SENDER_PRIMARY" "$SENDER_AVAILABLE"

run_privileged() {
    if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

require_privilege() {
    if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        return
    fi
    if ! sudo -n true 2>/dev/null; then
        echo "This test must change link state. Run it with sudo, for example:" >&2
        echo "  sudo $0 --fault-side $FAULT_SIDE" >&2
        exit 1
    fi
}

rdma_to_netdev() {
    local rdma_dev="$1"
    local ndev_dir="/sys/class/infiniband/$rdma_dev/ports/1/gid_attrs/ndevs"
    if [[ ! -d "$ndev_dir" ]]; then
        echo "Cannot find $ndev_dir for RDMA device $rdma_dev" >&2
        return 1
    fi

    local netdev=""
    local entry value
    while IFS= read -r entry; do
        value="$(cat "$entry" 2>/dev/null || true)"
        value="${value%%[[:space:]]*}"
        if [[ -n "$value" && "$value" != "(null)" &&
              -e "/sys/class/net/$value" ]]; then
            netdev="$value"
            break
        fi
    done < <(find "$ndev_dir" -maxdepth 1 -type f -printf '%f %p\n' |
             sort -n |
             awk '{ print $2 }')
    if [[ -z "$netdev" ]]; then
        echo "No Linux netdev found under $ndev_dir for RDMA device $rdma_dev" >&2
        return 1
    fi
    printf '%s\n' "$netdev"
}

make_matrix() {
    local preferred="$1"
    local fallback="$2"
    printf '{"cpu:0":[[%s],[%s]],"cpu:1":[[%s],[%s]]}\n' \
        "$(quote_csv "$preferred")" "$(quote_csv "$fallback")" \
        "$(quote_csv "$preferred")" "$(quote_csv "$fallback")"
}

quote_csv() {
    local csv="$1"
    local out=""
    local item
    IFS=',' read -ra parts <<< "$csv"
    for item in "${parts[@]}"; do
        [[ -z "$item" ]] && continue
        if [[ -n "$out" ]]; then out+=","; fi
        out+="\"$item\""
    done
    printf '%s' "$out"
}

wait_for_log() {
    local file="$1"
    local pattern="$2"
    local timeout="$3"
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        if [[ -f "$file" ]] && grep -q "$pattern" "$file"; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

RUNNER="$BENCH"
BUILD_TARGET="transfer_engine_bench"
if [[ "$VALIDATE_DATA" == "1" ]]; then
    RUNNER="$VALIDATOR"
    BUILD_TARGET="transfer_engine_validator"
fi

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    echo "Building $BUILD_TARGET at $RUNNER ..."
    cmake --build "$BUILD_DIR" --target "$BUILD_TARGET" -j"$(nproc)"
elif [[ ! -x "$RUNNER" ]]; then
    echo "$BUILD_TARGET not found at $RUNNER and SKIP_BUILD=1" >&2
    exit 1
fi

require_privilege

FAULT_RDMA="$TARGET_PRIMARY"
if [[ "$FAULT_SIDE" == "sender" ]]; then
    FAULT_RDMA="$SENDER_PRIMARY"
fi

FAULT_NETDEV="$NETDEV_OVERRIDE"
if [[ -z "$FAULT_NETDEV" ]]; then
    FAULT_NETDEV="$(rdma_to_netdev "$FAULT_RDMA")"
fi

mkdir -p "$LOG_DIR"
TARGET_MATRIX_FILE="$LOG_DIR/target_nic_priority.json"
SENDER_MATRIX_FILE="$LOG_DIR/sender_nic_priority.json"
TARGET_LOG="$LOG_DIR/target.log"
INITIATOR_LOG="$LOG_DIR/initiator.log"

make_matrix "$TARGET_PRIMARY" "$TARGET_AVAILABLE" > "$TARGET_MATRIX_FILE"
make_matrix "$SENDER_PRIMARY" "$SENDER_AVAILABLE" > "$SENDER_MATRIX_FILE"

TARGET_PORT="${TARGET_PORT:-$((30000 + RANDOM % 10000))}"
INITIATOR_PORT="${INITIATOR_PORT:-$((40000 + RANDOM % 10000))}"
TARGET_SERVER="127.0.0.1:$TARGET_PORT"
INITIATOR_SERVER="127.0.0.1:$INITIATOR_PORT"

COMMON_FLAGS=(
    --metadata_server=P2PHANDSHAKE
    --protocol=rdma
    --buffer_size=$((256 * 1024 * 1024))
    --block_size=$((4 * 1024 * 1024))
    --batch_size=1
    --threads=1
    --duration="$DURATION_SEC"
)

if [[ "$VALIDATE_DATA" != "1" ]]; then
    COMMON_FLAGS+=(--operation=write)
fi

if "$RUNNER" --help 2>&1 | grep -q -- '--use_vram'; then
    COMMON_FLAGS+=(--use_vram=false)
fi

TARGET_PID=""
LINK_WAS_DOWN=0

cleanup() {
    local rc=$?
    if [[ "$LINK_WAS_DOWN" -eq 1 ]]; then
        echo "[cleanup] Restoring $FAULT_NETDEV up"
        run_privileged ip link set "$FAULT_NETDEV" up || true
    fi
    if [[ -n "$TARGET_PID" ]] && kill -0 "$TARGET_PID" 2>/dev/null; then
        kill -TERM "$TARGET_PID" 2>/dev/null || true
        wait "$TARGET_PID" 2>/dev/null || true
    fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

echo "=== Real RDMA link failover test ==="
echo "fault_side       : $FAULT_SIDE"
echo "fault_rdma       : $FAULT_RDMA"
echo "fault_netdev     : $FAULT_NETDEV"
echo "target primary   : $TARGET_PRIMARY"
echo "target available : $TARGET_AVAILABLE"
echo "sender primary   : $SENDER_PRIMARY"
echo "sender available : $SENDER_AVAILABLE"
echo "target matrix    : $(cat "$TARGET_MATRIX_FILE")"
echo "sender matrix    : $(cat "$SENDER_MATRIX_FILE")"
echo "target server    : $TARGET_SERVER"
echo "initiator server : $INITIATOR_SERVER"
echo "runner           : $RUNNER"
echo "validate_data    : $VALIDATE_DATA"
echo "logs             : $LOG_DIR"
echo
echo "WARNING: this will run: ip link set $FAULT_NETDEV down; then up."
echo

"$RUNNER" "${COMMON_FLAGS[@]}" \
    --mode=target \
    --local_server_name="$TARGET_SERVER" \
    --device_name="$TARGET_PRIMARY,$TARGET_AVAILABLE" \
    --nic_priority_matrix="$TARGET_MATRIX_FILE" \
    >"$TARGET_LOG" 2>&1 &
TARGET_PID=$!

if ! wait_for_log "$TARGET_LOG" "RDMA device" 15; then
    echo "Target did not start successfully. See $TARGET_LOG" >&2
    exit 1
fi

TARGET_SEGMENT="$(
    awk '/Transfer Engine RPC using P2P handshake, listening on / {
        print $NF;
        exit;
    }' "$TARGET_LOG"
)"
if [[ -z "$TARGET_SEGMENT" ]]; then
    TARGET_SEGMENT="$TARGET_SERVER"
fi
echo "target segment  : $TARGET_SEGMENT"

"$RUNNER" "${COMMON_FLAGS[@]}" \
    --mode=initiator \
    --local_server_name="$INITIATOR_SERVER" \
    --segment_id="$TARGET_SEGMENT" \
    --device_name="$SENDER_PRIMARY,$SENDER_AVAILABLE" \
    --nic_priority_matrix="$SENDER_MATRIX_FILE" \
    >"$INITIATOR_LOG" 2>&1 &
INITIATOR_PID=$!

sleep "$DOWN_AFTER_SEC"

echo "[$(date '+%F %T')] Bringing $FAULT_NETDEV down ($FAULT_RDMA)"
run_privileged ip link set "$FAULT_NETDEV" down
LINK_WAS_DOWN=1

sleep "$DOWN_FOR_SEC"

echo "[$(date '+%F %T')] Bringing $FAULT_NETDEV up ($FAULT_RDMA)"
run_privileged ip link set "$FAULT_NETDEV" up
LINK_WAS_DOWN=0

set +e
wait "$INITIATOR_PID"
INITIATOR_RC=$?
set -e

kill -TERM "$TARGET_PID" 2>/dev/null || true
wait "$TARGET_PID" 2>/dev/null || true
TARGET_PID=""

echo
echo "=== Result ==="
echo "initiator exit code: $INITIATOR_RC"
echo "target log         : $TARGET_LOG"
echo "initiator log      : $INITIATOR_LOG"

if grep -E "FAILED|failed|Transport retry counter exceeded|Cannot make connection|KVTransferError|AddressNotRegistered|Detect data integrity problem" "$INITIATOR_LOG" "$TARGET_LOG" >/tmp/mooncake-real-link-failover-grep.$$ 2>/dev/null; then
    echo
    echo "Important failure-like log lines:"
    tail -n 80 /tmp/mooncake-real-link-failover-grep.$$
    rm -f /tmp/mooncake-real-link-failover-grep.$$
fi

if [[ "$FAULT_SIDE" == "sender" ]]; then
    CHECK_LOG="$INITIATOR_LOG"
    CHECK_PRIMARY="$SENDER_PRIMARY"
    CHECK_AVAILABLE="$SENDER_AVAILABLE"
else
    CHECK_LOG="$TARGET_LOG"
    CHECK_PRIMARY="$TARGET_PRIMARY"
    CHECK_AVAILABLE="$TARGET_AVAILABLE"
fi

if ! grep -q "RDMA device: $CHECK_PRIMARY" "$CHECK_LOG"; then
    echo "FAIL: $FAULT_SIDE primary RNIC $CHECK_PRIMARY was not initialized" >&2
    exit 1
fi
if ! grep -q "RDMA device: $CHECK_AVAILABLE" "$CHECK_LOG"; then
    echo "FAIL: $FAULT_SIDE available RNIC $CHECK_AVAILABLE was not initialized" >&2
    exit 1
fi
if ! grep -q "Context $CHECK_PRIMARY is now inactive" "$CHECK_LOG"; then
    echo "FAIL: did not observe $FAULT_SIDE primary RNIC $CHECK_PRIMARY going inactive" >&2
    exit 1
fi
if grep -q "Context $CHECK_AVAILABLE is now inactive" "$CHECK_LOG"; then
    echo "FAIL: $FAULT_SIDE available RNIC $CHECK_AVAILABLE also went inactive" >&2
    exit 1
fi

if [[ "$INITIATOR_RC" -ne 0 ]]; then
    echo "FAIL: initiator exited non-zero"
    exit "$INITIATOR_RC"
fi

if ! grep -q "Test completed" "$INITIATOR_LOG"; then
    echo "FAIL: initiator did not report completion"
    exit 1
fi

echo "PASS: transfer completed across real $FAULT_RDMA/$FAULT_NETDEV down/up"
