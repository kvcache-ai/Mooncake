#!/bin/bash
# Install this directory as tone-cli/tests/mooncake-rdma-perf.
# Dependencies (including the matching RDMA provider) are provisioned on the pair.
DEP_PKG_LIST=""

extract_src() (
    set -euo pipefail
    [[ ${HARNESS_SHA:-} =~ ^[0-9a-f]{40}$ ]]
    [[ ${RDMA_RUN_ID:-} =~ ^[a-zA-Z0-9_-]+$ ]]
    echo 2 > "$TONE_BM_RUN_DIR/rdma-exit-$RDMA_RUN_ID"
    source_dir="$TONE_BM_RUN_DIR/rdma-$RDMA_RUN_ID"
    mkdir "$source_dir"
    git -C "$source_dir" init -q
    git -C "$source_dir" remote add origin https://github.com/kvcache-ai/Mooncake.git
    git -C "$source_dir" fetch --depth=1 origin "$HARNESS_SHA"
    git -C "$source_dir" checkout --detach FETCH_HEAD
    test "$(git -C "$source_dir" rev-parse HEAD)" = "$HARNESS_SHA"
)

build() { :; }
install() { :; }
