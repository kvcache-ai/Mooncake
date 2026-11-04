#!/usr/bin/env bash
# Run retrieval scheduler simulation.
set -euo pipefail
cd "$(dirname "$0")/.."
python benchmark/retrieval_scheduler_sim.py
