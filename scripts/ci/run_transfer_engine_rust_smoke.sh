#!/usr/bin/env bash

set -e -o pipefail

: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE must be set}"

# ASan-enabled CI libraries require the Rust package to link libasan first.
# Release-style nightly builds intentionally leave the ASan runtime unlinked.
case "${MOONCAKE_TE_RUST_LINK_ASAN:-0}" in
    0) unset MOONCAKE_LINK_ASAN ;;
    1) export MOONCAKE_LINK_ASAN=1 ;;
    *)
        echo "MOONCAKE_TE_RUST_LINK_ASAN must be 0 or 1" >&2
        exit 2
        ;;
esac

cd "$GITHUB_WORKSPACE/mooncake-transfer-engine/rust"

export LD_LIBRARY_PATH="$GITHUB_WORKSPACE/build/mooncake-asio:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src/common/base:$GITHUB_WORKSPACE/build/mooncake-common:$GITHUB_WORKSPACE/build/mooncake-common/etcd:${LD_LIBRARY_PATH:-}"
if [ -d /usr/local/cuda/lib64/stubs ]; then
    export LD_LIBRARY_PATH="/usr/local/cuda/lib64/stubs:${LD_LIBRARY_PATH}"
fi
if [ -d /usr/local/cuda/lib64 ]; then
    export LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH}"
fi

export MOONCAKE_BUILD_DIR="$GITHUB_WORKSPACE/build"
export MOONCAKE_TE_LIB_DIR="$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src"
export MOONCAKE_TE_INCLUDE_DIR="$GITHUB_WORKSPACE/mooncake-transfer-engine/include"
export MC_METADATA_SERVER="${MC_METADATA_SERVER:-http://127.0.0.1:8080/metadata}"
export MC_RUST_TE_RUN_INTEGRATION=1
export MC_RUST_TE_PROTOCOL="${MC_RUST_TE_PROTOCOL:-tcp}"
export MC_RUST_TE_LOCAL_HOSTNAME="${MC_RUST_TE_LOCAL_HOSTNAME:-127.0.0.1:$((20000 + $$ % 10000))}"

cargo test --lib
cargo test --test minimal_smoke -- --nocapture
cargo test --examples --tests --no-run
