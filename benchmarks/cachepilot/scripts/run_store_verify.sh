#!/usr/bin/env bash
# Run Mooncake store verify_write scenario (soft-fail friendly).
set -u
cd "$(dirname "$0")/.."
python benchmark/mooncake_store_runner.py \
  --scenario verify_write \
  --io-api plain \
  --value-sizes 4096 \
  --batch-sizes 4 \
  --nr-objects 16 \
  --skip-on-fail
