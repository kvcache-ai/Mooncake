#!/usr/bin/env bash

set -e -o pipefail

# shellcheck source=scripts/ci/common/services.sh
source "$(dirname "${BASH_SOURCE[0]}")/../common/services.sh"
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"
trap 'ci_cleanup_services "$?"' EXIT

: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE must be set}"
: "${MOONCAKE_STORE_CLUSTER_ID:?MOONCAKE_STORE_CLUSTER_ID must be set}"

case "${MOONCAKE_STORE_GO_SANITIZED:-0}" in
    0) sanitized=false ;;
    1) sanitized=true ;;
    *)
        echo "MOONCAKE_STORE_GO_SANITIZED must be 0 or 1" >&2
        exit 2
        ;;
esac

# The Go test client publishes rpc_meta over HTTP to the master's embedded
# metadata server (see integration_test.go's run instructions). Older runs
# got 8080 served by an unrelated shared master that no longer exists in this
# job, so this master must serve metadata itself.
ci_start_service master "$RUNNER_TEMP/mooncake-master.log" \
    "$GITHUB_WORKSPACE/build/mooncake-store/src/mooncake_master" \
    --eviction_high_watermark_ratio=0.95 \
    --cluster_id="$MOONCAKE_STORE_CLUSTER_ID" \
    --port 50051 \
    --enable_http_metadata_server=true
ci_wait_service master 50051

cd "$GITHUB_WORKSPACE/mooncake-store/go"
export LD_LIBRARY_PATH="$GITHUB_WORKSPACE/build/mooncake-common:$GITHUB_WORKSPACE/build/mooncake-store/src:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src:$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src/common/base:$GITHUB_WORKSPACE/build/mooncake-common/etcd:${LD_LIBRARY_PATH:-}"
export CGO_ENABLED=1
export CGO_CFLAGS="-I$GITHUB_WORKSPACE/mooncake-store/include -I$GITHUB_WORKSPACE/mooncake-transfer-engine/include"

# master_service.cpp (inside libmooncake_store.a) calls into LocalSsdManager,
# which lives in its own static archive under local_ssd/. It is built
# unconditionally and is a required dependency of mooncake_store, so link it
# inside the group where cyclic references resolve.
linker_flags=(
    "-L$GITHUB_WORKSPACE/build/mooncake-store/src"
    "-L$GITHUB_WORKSPACE/build/mooncake-store/src/cachelib_memory_allocator"
    "-L$GITHUB_WORKSPACE/build/mooncake-store/src/local_ssd"
    "-L$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src"
    "-L$GITHUB_WORKSPACE/build/mooncake-transfer-engine/src/common/base"
    "-L$GITHUB_WORKSPACE/build/mooncake-common"
    "-L$GITHUB_WORKSPACE/build/mooncake-common/src"
    "-L$GITHUB_WORKSPACE/build/mooncake-common/etcd"
    -Wl,--start-group
    -lmooncake_store -lmooncake_local_ssd -lcachelib_memory_allocator -ltransfer_engine -lbase
    -lmooncake_common
    -Wl,--end-group
)
linker_flags+=(
    -lasio -letcd_wrapper -lstdc++ -lnuma -lglog -lgflags -libverbs -lmlx5
    -ljsoncpp -lzstd -lcurl -luring
)
if $sanitized; then
    linker_flags+=(-lasan)
fi
linker_flags+=(-lm)
if $sanitized; then
    linker_flags+=(-lgcov)
fi
linker_flags+=(-lxxhash -lyaml-cpp)
export CGO_LDFLAGS="${linker_flags[*]}"

# OSS adapter request signing uses OpenSSL HMAC (EVP_sha256). Static archives
# carry no transitive deps, so the Go link needs libcrypto explicitly when the
# adapter was compiled in (CURL + OpenSSL found at CMake time).
if grep -q '^MOONCAKE_OSS_ADAPTER_ENABLED:INTERNAL=TRUE$' "$GITHUB_WORKSPACE/build/CMakeCache.txt"; then
    export CGO_LDFLAGS="$CGO_LDFLAGS -lcrypto"
fi

# Link cudart if CUDA is available (needed for D2H staging in mooncake_store).
if [ -d /usr/local/cuda/lib64 ]; then
    export CGO_LDFLAGS="$CGO_LDFLAGS -L/usr/local/cuda/lib64 -lcudart"
fi
# USE_CUDA links transfer_engine against the CUDA Driver API in addition to the
# runtime API. Reuse the exact driver library that CMake selected.
if grep -q '^USE_CUDA:BOOL=ON$' "$GITHUB_WORKSPACE/build/CMakeCache.txt"; then
    cuda_driver_library=$(sed -n \
        's/^CUDA_cuda_driver_LIBRARY:FILEPATH=//p' \
        "$GITHUB_WORKSPACE/build/CMakeCache.txt")
    if [ -z "$cuda_driver_library" ] || [ ! -f "$cuda_driver_library" ]; then
        echo "CMake did not resolve the CUDA driver library" >&2
        exit 1
    fi
    export CGO_LDFLAGS="$CGO_LDFLAGS -L$(dirname "$cuda_driver_library") -lcuda"
fi
# The KV events publisher is optional and linked when libzmq is installed.
if ldconfig -p 2>/dev/null | grep -q libzmq; then
    export CGO_LDFLAGS="$CGO_LDFLAGS -lzmq"
fi

test_env=(MC_METADATA_SERVER=http://127.0.0.1:8080/metadata)
if $sanitized; then
    test_env=(ASAN_OPTIONS=detect_leaks=0:verify_asan_link_order=0 "${test_env[@]}")
fi
env "${test_env[@]}" go test -v ./tests/...
