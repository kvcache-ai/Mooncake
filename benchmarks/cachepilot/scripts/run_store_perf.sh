#!/usr/bin/env bash
# Run Mooncake store read_perf scenario (soft-fail friendly).
set -u
cd "$(dirname "$0")/.."
python benchmark/mooncake_store_runner.py \
  --scenario read_perf \
  --io-api plain \
  --value-sizes 4096,65536,1048576,4194304 \
  --batch-sizes 1,4,8,16 \
  --runtime 5 \
  --nr-objects 64 \
  --skip-on-fail
