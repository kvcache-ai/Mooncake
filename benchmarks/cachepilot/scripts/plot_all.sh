#!/usr/bin/env bash
# Generate all figures from results/csv.
set -euo pipefail
cd "$(dirname "$0")/.."
python benchmark/plot_results.py
