#!/usr/bin/env bash
# Run inside a MACA development environment with two available GPUs and HCAs.
set -euo pipefail
if (( $# < 3 || $# > 5 )); then
    echo "Usage: $0 BINARY NIC0 NIC1 [GPU0=0] [GPU1=1]" >&2
    exit 2
fi
binary=$1
nic0=$2
nic1=$3
gpu0=${4:-0}
gpu1=${5:-1}
[[ -x "$binary" ]] || { echo "Not executable: $binary" >&2; exit 2; }
[[ "$gpu0" != "$gpu1" ]] || { echo "Choose two distinct GPUs" >&2; exit 2; }
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/mooncake-maca-ibgda.XXXXXX")
sha256sum "$binary" > "$run_dir/binary.sha256"
printf 'binary=%s nic0=%s nic1=%s gpu0=%s gpu1=%s\n' "$binary" "$nic0" "$nic1" "$gpu0" "$gpu1" > "$run_dir/options"
timeout -k 5 240 "$binary" 0 "$gpu0" "$nic0" "$run_dir/meta" > "$run_dir/rank0.log" 2>&1 & pid0=$!
timeout -k 5 240 "$binary" 1 "$gpu1" "$nic1" "$run_dir/meta" > "$run_dir/rank1.log" 2>&1 & pid1=$!
trap 'kill "$pid0" "$pid1" 2>/dev/null || true' INT TERM
echo "Logs: $run_dir"
rc0=0; rc1=0
wait "$pid0" || rc0=$?
wait "$pid1" || rc1=$?
printf 'rank0=%s rank1=%s\n' "$rc0" "$rc1" | tee "$run_dir/status"
cat "$run_dir/rank0.log" "$run_dir/rank1.log"
test "$rc0" -eq 0 && test "$rc1" -eq 0
