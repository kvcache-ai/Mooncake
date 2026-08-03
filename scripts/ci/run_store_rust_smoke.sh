#!/usr/bin/env bash

set -e -o pipefail

: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE must be set}"
: "${MOONCAKE_STORE_CLUSTER_ID:?MOONCAKE_STORE_CLUSTER_ID must be set}"

# ASan-enabled CI libraries require the Rust package to link libasan first.
# Release-style nightly builds intentionally leave the ASan runtime unlinked.
case "${MOONCAKE_STORE_RUST_LINK_ASAN:-0}" in
    0) unset MOONCAKE_LINK_ASAN ;;
    1) export MOONCAKE_LINK_ASAN=1 ;;
    *)
        echo "MOONCAKE_STORE_RUST_LINK_ASAN must be 0 or 1" >&2
        exit 2
        ;;
esac

"$GITHUB_WORKSPACE/build/mooncake-store/src/mooncake_master" \
    --eviction_high_watermark_ratio=0.95 \
    --cluster_id="$MOONCAKE_STORE_CLUSTER_ID" \
    --port 50051 &
master_pid=$!
sleep 3

cd "$GITHUB_WORKSPACE/mooncake-store/rust"
export LD_LIBRARY_PATH="$GITHUB_WORKSPACE/build/mooncake-asio:$GITHUB_WORKSPACE/build/mooncake-store/src:$GITHUB_WORKSPACE/build/mooncake-store/src/cachelib_memory_allocator:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src/common/base:$GITHUB_WORKSPACE/build/mooncake-common/etcd:${LD_LIBRARY_PATH:-}"
export MOONCAKE_BUILD_DIR="$GITHUB_WORKSPACE/build"
export MOONCAKE_STORE_LIB_DIR="$GITHUB_WORKSPACE/build/mooncake-store/src"
export MOONCAKE_STORE_INCLUDE_DIR="$GITHUB_WORKSPACE/mooncake-store/include"
export MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
export MC_RUST_STORE_RUN_INTEGRATION=true
export MC_RUST_STORE_MASTER_ADDR=127.0.0.1:50051
export MC_RUST_STORE_LOCAL_HOSTNAME=127.0.0.1
export MC_RUST_STORE_PROTOCOL=tcp
export MC_RUST_STORE_DEVICE_NAME=

cargo test --test minimal_smoke -- --nocapture
MC_RUST_BENCH_ITERATIONS=4 \
    MC_RUST_BENCH_VALUE_SIZE=4096 \
    MC_RUST_BENCH_WARMUP=1 \
    cargo run --release --example store_benchmark

kill "$master_pid" 2>/dev/null || true
