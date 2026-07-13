#!/bin/bash
# Prepare mooncake-wheel/mooncake/ for local testing without building a wheel.
# This mirrors the artifact-copy phase of build_wheel.sh but skips the
# expensive python -m build / auditwheel repair / pip install steps.
#
# Usage:
#   cmake --build build -j$(nproc)
#   BUILD_DIR=build ./scripts/prepare_wheel_dir.sh
#   PYTHONPATH=$(pwd)/mooncake-wheel:$PYTHONPATH python mooncake-pg/tests/test_pg_elastic.py ...

set -e

BUILD_DIR="${BUILD_DIR:-build}"
BUILD_DIR_ABS="$(pwd)/${BUILD_DIR}"
WHEEL_DIR="mooncake-wheel/mooncake"

if [ ! -d "${BUILD_DIR}" ]; then
    echo "Error: build directory '${BUILD_DIR}' not found. Run cmake --build first."
    exit 1
fi

echo "Preparing ${WHEEL_DIR} from ${BUILD_DIR}..."

# Clean old artifacts
rm -f ${WHEEL_DIR}/*.so
rm -f ${WHEEL_DIR}/mooncake_master
rm -f ${WHEEL_DIR}/mooncake_client
rm -f ${WHEEL_DIR}/transfer_engine_bench

echo "Copying Python helpers..."
cp mooncake-integration/fabric_allocator_utils.py ${WHEEL_DIR}/fabric_allocator_utils.py

# engine.so (versioned in build tree, bare name expected by EP/PG extensions)
if compgen -G "${BUILD_DIR}/mooncake-integration/engine.*.so" >/dev/null; then
    cp ${BUILD_DIR}/mooncake-integration/engine.*.so ${WHEEL_DIR}/engine.so
else
    echo "Warning: engine.*.so not found"
fi

# libasio.so runtime dependency of engine.so
if [ -f "${BUILD_DIR}/mooncake-common/libasio.so" ]; then
    cp ${BUILD_DIR}/mooncake-common/libasio.so ${WHEEL_DIR}/libasio.so
fi

# store.so and store-related binaries/scripts (only when WITH_STORE=ON)
if compgen -G "${BUILD_DIR}/mooncake-integration/store.*.so" >/dev/null; then
    echo "Copying store.so..."
    cp ${BUILD_DIR}/mooncake-integration/store.*.so ${WHEEL_DIR}/store.so
    cp ${BUILD_DIR}/mooncake-store/src/mooncake_master ${WHEEL_DIR}/mooncake_master
    cp ${BUILD_DIR}/mooncake-store/src/mooncake_client ${WHEEL_DIR}/mooncake_client
    cp mooncake-integration/store/async_store.py ${WHEEL_DIR}/async_store.py
fi

# Optional shared libraries built by the main CMake project
for lib in \
    ${BUILD_DIR}/mooncake-store/src/libmooncake_store.so \
    ${BUILD_DIR}/mooncake-common/src/libmooncake_common.so \
    ${BUILD_DIR}/mooncake-common/etcd/libetcd_wrapper.so \
    ${BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so
do
    if [ -f "${lib}" ]; then
        cp "${lib}" ${WHEEL_DIR}/
    fi
done

# Ascend transport (NPU builds only)
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so" ]; then
    cp ${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so ${WHEEL_DIR}/ascend_transport.so
fi

# CUDA nvlink allocator
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so" ]; then
    echo "Copying nvlink_allocator.so..."
    cp ${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so ${WHEEL_DIR}/nvlink_allocator.so
    cp mooncake-integration/allocator.py ${WHEEL_DIR}/allocator.py
fi

# NPU ubshmem allocator
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/ubshmem-allocator/ubshmem_fabric_allocator.so" ]; then
    echo "Copying ubshmem_fabric_allocator.so..."
    cp ${BUILD_DIR}/mooncake-transfer-engine/ubshmem-allocator/ubshmem_fabric_allocator.so ${WHEEL_DIR}/ubshmem_fabric_allocator.so
    cp mooncake-integration/allocator_ascend_npu.py ${WHEEL_DIR}/allocator_ascend_npu.py
fi

# Benchmark binary
cp ${BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench ${WHEEL_DIR}/transfer_engine_bench

# Ascend HCCL transport helper (NPU builds only)
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so" ]; then
    cp ${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so ${WHEEL_DIR}/
fi

# EP/PG CUDA extensions staged by the main build (when WITH_EP=ON)
CUDA_EP_STAGING_DIR="${BUILD_DIR_ABS}/ep_pg_staging"
if [ -d "${CUDA_EP_STAGING_DIR}" ] && ls "${CUDA_EP_STAGING_DIR}"/*.so &>/dev/null; then
    echo "Copying EP/PG CUDA extensions..."
    cp ${CUDA_EP_STAGING_DIR}/*.so ${WHEEL_DIR}/
fi

echo "Done. ${WHEEL_DIR} is ready for PYTHONPATH-based testing."

