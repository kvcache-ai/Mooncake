#!/bin/bash

set -e

source "$(dirname "$(readlink -f "$0")")/../scripts/allocator_build_common.sh"

# Check for flags
USE_NVCC=false
USE_HIPCC=false
USE_MCC=false
USE_MACA=false
CI_BUILD=false
WITH_HYLINK=false

HIP_LIB=""
HIP_DEFINES=""

while [[ "$1" == --* ]]; do
    case "$1" in
        --use-nvcc) USE_NVCC=true ;;
        --use-hipcc) USE_HIPCC=true ;;
        --use-mcc) USE_MCC=true ;;
        --use-maca) USE_MACA=true ;;
        --ci-build) CI_BUILD=true ;;
        --with-hylink) WITH_HYLINK=true ;;
        # Full path of the libamdhip64 CMake resolved (ROCm SDK wheels only
        # ship the unversioned .so in the devel package).
        --hip-lib=*) HIP_LIB="${1#*=}" ;;
        --hip-fabric) HIP_DEFINES="-DMC_HIP_HAS_FABRIC_HANDLE" ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
    shift
done

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

prepare_allocator_build_env "$OUTPUT_DIR" "${2:-}"

echo "Building nvlink allocator to: $OUTPUT_DIR"

CPP_FILE="${SCRIPT_DIR}/nvlink_allocator.cpp"

# Choose build command based on flags
if [ "$CI_BUILD" = true ]; then
    # CI build: use nvcc without linking cuda
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -I/usr/local/cuda/include ${INCLUDE_FLAGS} -L/usr/local/cuda-12.8/targets/x86_64-linux/lib -lcuda -DUSE_CUDA=1
elif [ "$USE_NVCC" = true ]; then
    # Regular nvcc build with cuda linking
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -lcuda -I/usr/local/cuda/include ${INCLUDE_FLAGS} -DUSE_CUDA=1
elif [ "$USE_HIPCC" = true ]; then
    HIPCC_DEFINES="-DUSE_HIP=1"
    [ "$WITH_HYLINK" = true ] && HIPCC_DEFINES="$HIPCC_DEFINES -DUSE_HYLINK=1"
    hipify-perl "$CPP_FILE" > "${OUTPUT_DIR}/nvlink_allocator.cpp"
    if [ -n "$HIP_LIB" ]; then
        # hipcc links "$HIP_PATH/lib/libamdhip64.so" itself; point it at the
        # SDK that provides the library CMake found.
        HIP_ROOT=$(dirname "$(dirname "$HIP_LIB")")
        export HIP_PATH="$HIP_ROOT" ROCM_PATH="$HIP_ROOT"
    else
        HIP_ROOT=${ROCM_PATH:-$(hipconfig --path 2>/dev/null || echo /opt/rocm)}
    fi
    hipcc "$OUTPUT_DIR/nvlink_allocator.cpp" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -fPIC -L"${HIP_ROOT}/lib" -lamdhip64 -I"${HIP_ROOT}/include" ${INCLUDE_FLAGS} ${HIPCC_DEFINES} ${HIP_DEFINES}
elif [ "$USE_MCC" = true ]; then
    mcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lmusa -I/usr/local/musa/include ${INCLUDE_FLAGS} -DUSE_MUSA=1
elif [ "$USE_MACA" = true ]; then
    MACA_ROOT=${MACA_HOME:-/opt/maca}
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC \
        -I"${MACA_ROOT}/include" ${INCLUDE_FLAGS} \
        -L"${MACA_ROOT}/lib64" -L"${MACA_ROOT}/lib" -lmcruntime -DUSE_MACA=1
else
    # Default g++ build
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lcuda -I/usr/local/cuda/include ${INCLUDE_FLAGS} -DUSE_CUDA=1
fi

if [ $? -eq 0 ]; then
    echo "Successfully built nvlink_allocator.so in $OUTPUT_DIR"
else
    echo "Failed to build nvlink_allocator.so"
    exit 1
fi
