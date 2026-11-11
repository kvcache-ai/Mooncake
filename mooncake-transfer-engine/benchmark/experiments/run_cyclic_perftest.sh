#!/usr/bin/env bash
# Execute once per host, only inside an approved idle-machine reservation.
set -euo pipefail
ROLE=${1:?target or initiator}
BINARY=${2:?absolute path to patched ib_write_bw}
NUMACTL=${3:?absolute path to numactl}
# Eight comma-separated CPU pairs, in mlx5_0..mlx5_7 order; semicolon separates pairs.
CPU_PAIRS=${4:?approved CPU pairs}
TARGET_IP=${5:-}
OUT=${6:?new output directory}
mkdir "$OUT"
IFS=';' read -r -a PAIRS <<< "$CPU_PAIRS"
[[ ${#PAIRS[@]} == 8 ]]
export PERFTEST_EXPERIMENTAL_CYCLE_BYTES=134217728
[[ "$ROLE" == target || "$ROLE" == initiator ]]
pids=()
cleanup() { local live; live=$(jobs -pr); [[ -z "$live" ]] || kill $live 2>/dev/null || true; wait || true; }
trap cleanup EXIT INT TERM
for n in {0..7}; do
  peer=(); [[ "$ROLE" != initiator ]] || peer=("${TARGET_IP:?target IP required}")
  "$NUMACTL" --physcpubind="${PAIRS[$n]}" --membind="$((n / 4))" \
    "$BINARY" -d "mlx5_$n" -i 1 -c RC -x 3 -p "$((24000+n))" \
    -s 262144 -q 8 -t 64 -D 24 -f 2 -l 1 -Q 1 --report_gbits \
    --out_json --out_json_file="$OUT/nic$n.json" "${peer[@]}" \
    >"$OUT/nic$n.log" 2>&1 &
  pids+=("$!")
done
for p in "${pids[@]}"; do wait "$p"; done
pids=()
