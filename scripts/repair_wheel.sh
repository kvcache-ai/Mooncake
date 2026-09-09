#!/bin/bash
# Repair a scikit-build-core wheel. Never stage artifacts in tracked sources.
set -euo pipefail
PYTHON=$1
OUTPUT_DIR=$2
BUILD_DIR=$3
NPU_BUILD=${NPU_BUILD:-0}
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

wheels=("$OUTPUT_DIR"/*.whl)
if [ "${#wheels[@]}" -ne 1 ] || [ ! -f "${wheels[0]}" ]; then
    echo "Expected exactly one wheel in $OUTPUT_DIR" >&2
    exit 1
fi
"$PYTHON" -m wheel unpack "${wheels[0]}" -d "$WORK_DIR/unpacked"
packages=("$WORK_DIR"/unpacked/*)
PACKAGE_DIR=${packages[0]}
CUDA_EP_STAGING_DIR="$WORK_DIR/device"
mkdir -p "$CUDA_EP_STAGING_DIR" "$WORK_DIR/raw" "$WORK_DIR/repaired"

# The backend installs the complete wheel. Temporarily remove CUDA EP/PG files
# from that wheel before auditwheel/patchelf can corrupt their fatbins. The
# staging manifest comes from CMake, not a second packaging file list.
for staged in "$BUILD_DIR"/ep_pg_staging/*.so; do
    [ -f "$staged" ] || continue
    artifact="$PACKAGE_DIR/mooncake/$(basename "$staged")"
    if [ ! -f "$artifact" ]; then
        echo "Backend wheel is missing staged CUDA artifact: $artifact" >&2
        exit 1
    fi
    mv "$artifact" "$CUDA_EP_STAGING_DIR/"
done
export LD_LIBRARY_PATH="$CUDA_EP_STAGING_DIR:$BUILD_DIR/mooncake-common:$BUILD_DIR/mooncake-common/etcd:$BUILD_DIR/mooncake-common/k8s-lease:/usr/local/lib:${LD_LIBRARY_PATH:-}"

if [ "$NPU_BUILD" = "1" ]; then
    find "$PACKAGE_DIR/mooncake" -name '*.so' -exec strip --strip-unneeded {} \;
    find "$PACKAGE_DIR/mooncake" -type f -exec sh -c 'file "$1" | grep -q ELF' _ {} \; -print0 |
        xargs -0 -r patchelf --force-rpath --set-rpath '$ORIGIN'
fi
# CI needs the native build's disk space back before repacking large EP wheels.
# All runtime artifacts now live in the unpacked wheel/device directory. Never
# remove a source ancestor or a build directory containing the requested output.
if [ "${CI:-}" = "true" ] || [ "${FREE_BUILD_DIR:-}" = "1" ]; then
    build=$(realpath "$BUILD_DIR")
    output=$(realpath "$OUTPUT_DIR")
    root=$(cd "$(dirname "$0")/.." && pwd)
    if [ -f "$build/.skbuild-info.json" ] && [ -f "$build/CMakeCache.txt" ] &&
       [[ "$root/" != "$build/"* && "$output/" != "$build/"* ]]; then
        rm -rf "$build"
    fi
fi
"$PYTHON" -m wheel pack "$PACKAGE_DIR" -d "$WORK_DIR/raw"
# Avoid retaining two complete unpacked copies during the final repack.
rm -rf "$WORK_DIR/unpacked"

# Detect the baseline in the build container, never on the CI host.
GLIBC_VERSION=$(getconf GNU_LIBC_VERSION | awk '{print $2}' | tr . _)
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|aarch64) ;;
    arm64) ARCH=aarch64 ;;
    *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
PLATFORM_TAG=${PLATFORM_TAG:-manylinux_${GLIBC_VERSION}_${ARCH}}

# These are externally supplied runtime libraries, not wheel-owned artifacts.
# In particular MPComm is independently installed and may contain CUDA fatbins.
EXCLUDED_LIBRARIES='libcurl.so* libfabric.so* libefa.so* libibverbs.so* libmlx5.so*
libnuma.so* libstdc++.so* libgcc_s.so* libc.so* libnghttp2.so* libidn2.so*
librtmp.so* libssh.so* libpsl.so* libssl.so* libcrypto.so* libgssapi_krb5.so*
libldap.so* liblber.so* libbrotlidec.so* libz.so* libnl-route-3.so* libnl-3.so*
libm.so* liblzma.so* libunistring.so* libgnutls.so* libhogweed.so* libnettle.so*
libgmp.so* libkrb5.so* libk5crypto.so* libcom_err.so* libkrb5support.so*
libsasl2.so* libbrotlicommon.so* libp11-kit.so* libtasn1.so* libkeyutils.so*
libresolv.so* libffi.so* libcuda.so* libcudart.so* libmooncake_ep_device.so*
libmooncake_pg_device.so* libnccl.so* libmusa.so* libmusart.so* libamdhip64.so*
libhsa-runtime64.so* librocprofiler-register.so* libc10.so* libc10_cuda.so*
libtorch.so* libtorch_cpu.so* libtorch_cuda.so* libtorch_python.so*
libascendcl.so* libhccl.so* libmsprofiler.so* libgert.so* libascendcl_impl.so*
libge_executor.so* libascend_dump.so* libgraph.so* libruntime.so*
libascend_watchdog.so* libprofapi.so* liberror_manager.so* libascendalog.so*
libc_sec.so* libhccl_alg.so* libhccl_plf.so* libascend_protobuf.so*
libhybrid_executor.so* libdavinci_executor.so* libge_common.so* libge_common_base.so*
liblowering.so* libregister.so* libexe_graph.so* libmmpa.so* libplatform.so*
libgraph_base.so* libruntime_common.so* libqos_manager.so* libascend_trace.so*
libmetadef*.so libllm_datadist*.so ascend_transport*.so libaccl_barex.so*
liburma.so* libmpcomm.so*'
# Word splitting is intentional; glob expansion is not.
set -f
excludes=()
for library in $EXCLUDED_LIBRARIES; do
    excludes+=(--exclude "$library")
done
set +f
"$PYTHON" -m auditwheel repair "$WORK_DIR"/raw/*.whl \
    "${excludes[@]}" -w "$WORK_DIR/repaired" --plat "$PLATFORM_TAG"

"$PYTHON" -m wheel unpack "$WORK_DIR"/repaired/*.whl -d "$WORK_DIR/final"
packages=("$WORK_DIR"/final/*)
PACKAGE_DIR=${packages[0]}
for artifact in "$CUDA_EP_STAGING_DIR"/*.so; do
    [ -f "$artifact" ] || continue
    cp "$artifact" "$PACKAGE_DIR/mooncake/"
done
for host in "$PACKAGE_DIR"/mooncake/_ep*.so; do
    [ -f "$host" ] || continue
    patchelf --add-rpath '$ORIGIN' "$host"
done

if [ "$NPU_BUILD" = "1" ]; then
    for vendored in "$PACKAGE_DIR"/*.libs; do
        [ -d "$vendored" ] || continue
        cp "$vendored"/*.so* "$PACKAGE_DIR/mooncake/"
        rm -rf "$vendored"
        rm -f "$PACKAGE_DIR/$(basename "$vendored").pth"
    done
    find "$PACKAGE_DIR/mooncake" -type f -exec sh -c 'file "$1" | grep -q ELF' _ {} \; -print0 |
        xargs -0 -r patchelf --force-rpath --set-rpath '$ORIGIN'
fi
mkdir "$WORK_DIR/result"
"$PYTHON" -m wheel pack "$PACKAGE_DIR" -d "$WORK_DIR/result"
# Replace the input only after every repair/repack step succeeds.
rm "${wheels[0]}"
mv "$WORK_DIR"/result/*.whl "$OUTPUT_DIR/"
