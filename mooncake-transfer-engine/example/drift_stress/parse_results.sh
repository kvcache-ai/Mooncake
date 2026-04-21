#!/usr/bin/env bash
# parse_results.sh <log_dir>
# Aggregate drift.csv + initiator.log into a one-page summary.
set -euo pipefail

DIR="${1:-.}"
CSV="$DIR/drift.csv"
LOG="$DIR/initiator.log"

if [[ ! -s "$CSV" ]]; then
  echo "no drift.csv in $DIR — initiator never wrote a row"
  exit 0
fi

TOTAL=$(( $(wc -l < "$CSV") - 1 ))   # subtract header
OK=$(awk -F, 'NR>1 && $8=="ok"' "$CSV" | wc -l | tr -d ' ')
OPEN_FAIL=$(awk -F, 'NR>1 && $8=="open_fail"' "$CSV" | wc -l | tr -d ' ')
WARMUP_FAIL=$(awk -F, 'NR>1 && $8=="warmup_fail"' "$CSV" | wc -l | tr -d ' ')
XFER_FAIL=$(awk -F, 'NR>1 && $8=="transfer_fail"' "$CSV" | wc -l | tr -d ' ')

# Stats on the timing columns (open_ms, warmup_ms, burst_ms) over OK rows.
stats() {
  awk -F, -v col="$1" 'NR>1 && $8=="ok" { v=$col; sum+=v; if (!n++||v<mn) mn=v; if (v>mx) mx=v } END { if (n>0) printf "avg=%.1f min=%d max=%d n=%d", sum/n, mn, mx, n; else printf "n=0" }' "$CSV"
}

echo "=== drift-stress summary ($DIR) ==="
echo "iterations recorded: $TOTAL"
echo "  ok           : $OK"
echo "  open_fail    : $OPEN_FAIL"
echo "  warmup_fail  : $WARMUP_FAIL"
echo "  transfer_fail: $XFER_FAIL"
echo ""
echo "timings (ms, ok rows only):"
echo "  open_segment : $(stats 3)"
echo "  warmup       : $(stats 4)"
echo "  burst        : $(stats 5)"
echo ""
if [[ -s "$LOG" ]]; then
  ENOMEM_HITS=$(grep -c 'fi_enable.*Cannot allocate memory' "$LOG" || true)
  EVICTED=$(grep -c 'Evicting stale EFA endpoint' "$LOG" || true)
  DESTROYED=$(grep -c 'Peer reconnected with new address, re-establishing' "$LOG" || true)
  echo "signals in initiator.log:"
  echo "  'fi_enable ENOMEM'                      : $ENOMEM_HITS"
  echo "  'Evicting stale EFA endpoint'           : $EVICTED"
  echo "  'Peer reconnected with new address'     : $DESTROYED"
fi
echo ""
echo "first 5 failure rows (if any):"
awk -F, 'NR>1 && $8!="ok"' "$CSV" | head -n 5 || true
