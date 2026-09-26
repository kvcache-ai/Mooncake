#!/bin/bash
# Build Mooncake with Intel XPU (Level Zero) support
# Usage: ./scripts/build_xpu.sh [build_dir]
#
# Prerequisites:
#   - Level Zero development files: headers + libze_loader, from the distro
#     package (level-zero-devel / libze-dev) or any prefix on the search path.
#     Needs a version providing zeCommandListHostSynchronize,
#     ZE_COMMAND_QUEUE_FLAG_IN_ORDER, zeMemGetIpcHandle/zeMemPutIpcHandle,
#     zeMemGetAddressRange and zeDevicePciGetPropertiesExt. Verified against
#     level-zero 1.28.0 (Ubuntu, API 1.15) and 1.33.1 (conda-forge).
#   - The usual Mooncake build dependencies (see dependencies.sh).
#
# oneAPI is NOT required: this code is plain C++ against the Level Zero API,
# with no SYCL, so a stock GCC builds it and setvars.sh need not be sourced.
#
# Environment:
#   LEVEL_ZERO_ROOT  optional; prefix to search for Level Zero first
#   CONDA_PREFIX     optional; added to the CMake search paths when set
#   CC / CXX         optional; honoured if already set

set -e
set -x

BUILD_DIR="${1:-build-xpu}"

# ── Locate Level Zero ───────────────────────────────────────────────────
# Fail early here rather than deep inside CMake.
ZE_SEARCH_PREFIXES=()
[ -n "$LEVEL_ZERO_ROOT" ] && ZE_SEARCH_PREFIXES+=("$LEVEL_ZERO_ROOT")
[ -n "$CONDA_PREFIX" ] && ZE_SEARCH_PREFIXES+=("$CONDA_PREFIX")
ZE_SEARCH_PREFIXES+=(/usr /usr/local)

ZE_PREFIX=""
for prefix in "${ZE_SEARCH_PREFIXES[@]}"; do
    if [ -f "$prefix/include/level_zero/ze_api.h" ]; then
        ZE_PREFIX="$prefix"
        break
    fi
done

if [ -z "$ZE_PREFIX" ]; then
    echo "ERROR: Level Zero headers (level_zero/ze_api.h) not found."
    echo "Searched: ${ZE_SEARCH_PREFIXES[*]}"
    echo "Install the Level Zero development package, e.g.:"
    echo "  apt install level-zero-devel      # or libze-dev"
    echo "  dnf install oneapi-level-zero-devel"
    echo "Or set LEVEL_ZERO_ROOT to a prefix containing include/level_zero/."
    exit 1
fi

# ── Toolchain ───────────────────────────────────────────────────────────
# Prefer the conda cross-compiler when present (newer GCC than some distros).
if [ -z "$CC" ]; then
    CC=$(command -v x86_64-conda-linux-gnu-gcc 2>/dev/null || command -v gcc)
fi
if [ -z "$CXX" ]; then
    CXX=$(command -v x86_64-conda-linux-gnu-g++ 2>/dev/null || command -v g++)
fi
export CC CXX

# The conda cross-compiler has its own sysroot and may miss system headers and
# libraries, so point it at them. Only for that compiler: with a system GCC,
# `-isystem /usr/include` moves /usr/include ahead of the compiler's own C++
# headers and breaks libstdc++'s `#include_next <stdlib.h>` (seen on GCC 15).
if [[ "$(basename "$CXX")" == x86_64-conda-linux-gnu-* ]]; then
    SYS_INCLUDE_FLAGS="-isystem /usr/include"
    SYS_LINK_FLAGS="-L/usr/lib64 -Wl,-rpath-link,/usr/lib64"
else
    SYS_INCLUDE_FLAGS=""
    SYS_LINK_FLAGS=""
fi

# ── CMake search paths ──────────────────────────────────────────────────
join_unique() {
    # Join with ';', dropping empties and duplicates (prefixes may coincide).
    local out="" item
    for item in "$@"; do
        [ -n "$item" ] || continue
        case ";$out;" in *";$item;"*) continue ;; esac
        out="${out:+$out;}$item"
    done
    printf '%s' "$out"
}

CMAKE_PREFIXES=$(join_unique "$CONDA_PREFIX" "$ZE_PREFIX")
CMAKE_INCLUDES=$(join_unique "${CONDA_PREFIX:+$CONDA_PREFIX/include}" \
                             "$ZE_PREFIX/include" /usr/include)
CMAKE_LIBS=$(join_unique "${CONDA_PREFIX:+$CONDA_PREFIX/lib}" \
                         "$ZE_PREFIX/lib" /usr/lib64 /usr/lib/x86_64-linux-gnu)

echo "Using CC=$CC CXX=$CXX"
echo "Level Zero prefix: $ZE_PREFIX"
echo "CONDA_PREFIX=${CONDA_PREFIX:-<none>}"

cmake -B "$BUILD_DIR" -S . \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DUSE_XPU=ON \
    -DUSE_TCP=ON \
    -DUSE_HTTP=ON \
    -DBUILD_UNIT_TESTS=ON \
    -DBUILD_EXAMPLES=ON \
    -DWITH_STORE=OFF \
    -DWITH_P2P_STORE=OFF \
    -DWITH_STORE_RUST=OFF \
    -DCMAKE_PREFIX_PATH="$CMAKE_PREFIXES" \
    -DCMAKE_INCLUDE_PATH="$CMAKE_INCLUDES" \
    -DCMAKE_LIBRARY_PATH="$CMAKE_LIBS" \
    -DCMAKE_CXX_FLAGS="$SYS_INCLUDE_FLAGS" \
    -DCMAKE_C_FLAGS="$SYS_INCLUDE_FLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$SYS_LINK_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$SYS_LINK_FLAGS" \
    -DCMAKE_MODULE_LINKER_FLAGS="$SYS_LINK_FLAGS"

cmake --build "$BUILD_DIR" -j "$(nproc)"

echo ""
echo "Build complete: $BUILD_DIR"
echo "Run tests with: cd $BUILD_DIR && ctest --output-on-failure"
