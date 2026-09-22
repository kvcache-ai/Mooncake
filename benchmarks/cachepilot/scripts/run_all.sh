#!/usr/bin/env bash
# Run the full CachePilot experiment pipeline.
# Store benchmarks may fail if Mooncake is unavailable; later stages still run.
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

echo "=============================================="
echo " CachePilot — full experiment pipeline"
echo " Root: ${ROOT}"
echo "=============================================="

mkdir -p results/csv results/logs results/figures

echo ""
echo "[1/6] Store verify_write ..."
if bash scripts/run_store_verify.sh; then
  echo "  -> store verify finished"
else
  echo "  -> store verify failed (continuing). Start mooncake_master for real Store metrics."
fi

echo ""
echo "[2/6] Store read_perf ..."
if bash scripts/run_store_perf.sh; then
  echo "  -> store perf finished"
else
  echo "  -> store perf failed (continuing). Start mooncake_master for real Store metrics."
fi

echo ""
echo "[3/6] Prefix reuse simulation ..."
bash scripts/run_prefix_reuse.sh

echo ""
echo "[4/6] Retrieval scheduler simulation ..."
bash scripts/run_scheduler_sim.sh

echo ""
echo "[5/6] Plot results ..."
bash scripts/plot_all.sh

echo ""
echo "[6/6] Generated artifacts"
echo "----------------------------------------------"
echo "CSV files:"
find results/csv -type f \( -name '*.csv' -o -name '.gitkeep' \) | sort || true
echo ""
echo "Figures:"
find results/figures -type f \( -name '*.png' -o -name '.gitkeep' \) | sort || true
echo ""
echo "Logs:"
find results/logs -type f \( -name '*.log' -o -name '*.csv' -o -name '.gitkeep' \) | sort || true
echo "----------------------------------------------"
echo "Done."
