#!/usr/bin/env bash
# Run Prefix Cache reuse simulation.
set -euo pipefail
cd "$(dirname "$0")/.."
python benchmark/prefix_reuse_benchmark.py
