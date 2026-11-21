#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "Usage: $0 <build-flags|cuda-off|cuda-off-metrics-on|ub-mock|memfabric-mock>" >&2
}

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

profile="$1"

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
GITHUB_WORKSPACE="${GITHUB_WORKSPACE:-$(pwd)}"
export GITHUB_WORKSPACE
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"

if [[ "$profile" == "build-flags" ]]; then
  build_dir="$GITHUB_WORKSPACE/mooncake-transfer-engine/build-flags"
  install_dir="$RUNNER_TEMP/install-build-flags"
  relwithdebinfo_flags="${CMAKE_RELWITHDEBINFO_FLAGS:--O2 -DNDEBUG}"

  rm -rf "$build_dir" "$install_dir"
  PIP_NO_CACHE_DIR=1 cmake -S "$GITHUB_WORKSPACE/mooncake-transfer-engine" -B "$build_dir" -G Ninja \
    -DUSE_ETCD=OFF \
    -DUSE_CXL=ON \
    -DUSE_REDIS=ON \
    -DUSE_HTTP=ON \
    -DWITH_METRICS=ON \
    -DBUILD_UNIT_TESTS=ON \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_BENCHMARK=OFF \
    -DENABLE_SCCACHE=ON \
    -DUSE_CUDA=OFF \
    -DUSE_MNNVL=OFF \
    -DUSE_UB=OFF \
    -DUSE_LLD=ON \
    "-DCMAKE_C_FLAGS_RELWITHDEBINFO=${relwithdebinfo_flags}" \
    "-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${relwithdebinfo_flags}" \
    -DENABLE_DEBUG_SYMBOLS=OFF
  cmake --build "$build_dir"
  DESTDIR="$install_dir" cmake --install "$build_dir"
  rm -rf "$build_dir" "$install_dir"
  df -h
  exit 0
fi

build_dir="$GITHUB_WORKSPACE/build-tent-${profile}"
install_dir="$RUNNER_TEMP/install-tent-${profile}"

common_flags=(
  -G Ninja
  -DUSE_TENT=ON
  -DUSE_HTTP=ON
  -DENABLE_SCCACHE=ON
  -DBUILD_UNIT_TESTS=ON
  -DBUILD_EXAMPLES=ON
  -DBUILD_BENCHMARK=OFF
  -DUSE_LLD=ON
  -DENABLE_DEBUG_SYMBOLS=OFF
)

extra_flags=()
build_targets=()
ctest_args=()

case "$profile" in
  cuda-off)
    extra_flags=(-DUSE_CUDA=OFF -DUSE_EVENT_DRIVEN_COMPLETION=ON)
    build_targets=(mooncake-transfer-engine/all transfer_task_test standalone_client_test)
    ;;
  cuda-off-metrics-on)
    extra_flags=(-DUSE_CUDA=OFF -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DTENT_METRICS_ENABLED=ON)
    ;;
  ub-mock)
    extra_flags=(-DUSE_CUDA=OFF -DUSE_UB=ON -DCMAKE_BUILD_TYPE=Debug -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF)
    ;;
  memfabric-mock)
    extra_flags=(-DUSE_CUDA=OFF -DUSE_MEMFABRIC=ON -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DCMAKE_BUILD_TYPE=Debug)
    build_targets=(tent_memfabric_transport_test transfer_engine)
    ctest_args=(-R '^tent_memfabric_transport_test$' --no-tests=error --timeout 120)
    ;;
  *)
    usage
    exit 2
    ;;
esac

rm -rf "$build_dir" "$install_dir"
cmake -S "$GITHUB_WORKSPACE" -B "$build_dir" "${common_flags[@]}" "${extra_flags[@]}"

if [[ ${#build_targets[@]} -gt 0 ]]; then
  cmake --build "$build_dir" --target "${build_targets[@]}"
else
  cmake --build "$build_dir"
  DESTDIR="$install_dir" cmake --install "$build_dir"
fi

if [[ "$profile" == "cuda-off" ]]; then
  ctest --test-dir "$build_dir" --output-on-failure --no-tests=error \
    -R '^(transfer_task_tent_completion_test|standalone_client_tent_test)$'
  ctest --test-dir "$build_dir" --output-on-failure --no-tests=error \
    -R '^tent_compat_notify_test$'
  ctest --test-dir "$build_dir" --output-on-failure --no-tests=error \
    -R '^transfer_task_tent_shared_segment_test$'
fi

mkdir -p "$build_dir/test-results"
(
  cd "$build_dir"
  ctest --test-dir mooncake-transfer-engine/tent/tests -j \
    --output-on-failure \
    --output-junit "$build_dir/test-results/ctest.xml" \
    ${ctest_args[@]+"${ctest_args[@]}"}
)

source "$GITHUB_WORKSPACE/scripts/ci/services.sh"
trap 'ci_cleanup_services "$?"' EXIT

ci_start_service "tent-metadata-${profile}" "$RUNNER_TEMP/tent-metadata-${profile}.log" \
  python3 "$GITHUB_WORKSPACE/mooncake-transfer-engine/example/http-metadata-server-python/bootstrap_server.py"
ci_wait_service "tent-metadata-${profile}" 8080

rust_smoke_script="${MOONCAKE_TE_RUST_SMOKE_SCRIPT:-$GITHUB_WORKSPACE/scripts/ci/run_transfer_engine_rust_smoke.sh}"
MOONCAKE_BUILD_DIR="$build_dir" \
  MOONCAKE_TE_RUST_LINK_ASAN="0" \
  "$rust_smoke_script"

ci_stop_service "tent-metadata-${profile}"

rm -rf "$build_dir" "$install_dir"
df -h
