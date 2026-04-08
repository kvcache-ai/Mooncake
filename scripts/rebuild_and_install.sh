#!/usr/bin/env bash
# Mooncake rebuild & install script
# Usage: bash scripts/rebuild_and_install.sh [--clean]
#
# --clean: remove existing build directory before building
#
# Prerequisites:
#   - cmake >= 3.16, ninja (or make), python3
#   - System dependencies: libibverbs, libjsoncpp, libcurl, libnuma, gflags, glog
#   - ROCm (for AMD GPU) or CUDA (for NVIDIA GPU)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOONCAKE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${MOONCAKE_ROOT}/build"
INSTALL_PREFIX="/usr/local"
PYTHON_SITE_PACKAGES=$(python3 -c "import sys; print([s for s in sys.path if 'packages' in s][0])")
MOONCAKE_PKG_DIR="${PYTHON_SITE_PACKAGES}/mooncake"
NPROC=$(nproc)

echo "============================================"
echo " Mooncake Rebuild & Install"
echo "============================================"
echo "Source:       ${MOONCAKE_ROOT}"
echo "Build:        ${BUILD_DIR}"
echo "Python pkgs:  ${MOONCAKE_PKG_DIR}"
echo "Parallel:     ${NPROC} jobs"
echo ""

# -----------------------------------------------
# Parse arguments
# -----------------------------------------------
CLEAN=false
for arg in "$@"; do
    case "$arg" in
        --clean) CLEAN=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# -----------------------------------------------
# Step 1: Remove existing installation
# -----------------------------------------------
echo "[1/5] Removing existing Mooncake installation..."

# Remove Python package
if [ -d "${MOONCAKE_PKG_DIR}" ]; then
    echo "  Removing ${MOONCAKE_PKG_DIR}"
    rm -rf "${MOONCAKE_PKG_DIR}"
fi

# Remove binaries
for bin in mooncake_master mooncake_client; do
    if [ -f "${INSTALL_PREFIX}/bin/${bin}" ]; then
        echo "  Removing ${INSTALL_PREFIX}/bin/${bin}"
        rm -f "${INSTALL_PREFIX}/bin/${bin}"
    fi
done

echo "  Done."

# -----------------------------------------------
# Step 2: Clean build directory (if --clean)
# -----------------------------------------------
if [ "$CLEAN" = true ]; then
    if [ -d "${BUILD_DIR}" ]; then
        echo "[2/5] Cleaning build directory..."
        rm -rf "${BUILD_DIR}"
        echo "  Done."
    else
        echo "[2/5] Clean requested but build dir does not exist, skipping."
    fi
else
    echo "[2/5] Incremental build (use --clean for full rebuild)"
fi

# -----------------------------------------------
# Step 3: Init submodules
# -----------------------------------------------
echo "[3/5] Initializing submodules..."
cd "${MOONCAKE_ROOT}"
# Add safe.directory for submodules to avoid dubious ownership errors
git config --global --add safe.directory "${MOONCAKE_ROOT}"
git config --global --add safe.directory "${MOONCAKE_ROOT}/extern/pybind11"
git submodule update --init
echo "  Done."

# -----------------------------------------------
# Step 4: CMake configure & build
# -----------------------------------------------
echo "[4/5] Building Mooncake..."
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

cmake "${MOONCAKE_ROOT}" \
    -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DWITH_STORE=ON \
    -DWITH_TE=ON \
    -DBUILD_UNIT_TESTS=OFF

ninja -j"${NPROC}"
echo "  Build complete."

# -----------------------------------------------
# Step 5: Install
# -----------------------------------------------
echo "[5/5] Installing..."
ninja install
echo "  Done."

# -----------------------------------------------
# Verify
# -----------------------------------------------
echo ""
echo "============================================"
echo " Verification"
echo "============================================"

# Check Python import
if python3 -c "from mooncake.store import MooncakeDistributedStore; print('  Python import: OK')" 2>/dev/null; then
    true
else
    echo "  Python import: FAILED"
    exit 1
fi

# Check enable_offload parameter is exposed
if python3 -c "
from mooncake.store import MooncakeDistributedStore
doc = MooncakeDistributedStore.setup.__doc__ or ''
assert 'enable_offload' in doc, 'enable_offload not found in setup() docstring'
print('  enable_offload param: OK')
" 2>/dev/null; then
    true
else
    echo "  enable_offload param: NOT FOUND (rebuild may have used old source)"
    exit 1
fi

# Check binaries
if command -v mooncake_master &>/dev/null; then
    echo "  mooncake_master: OK"
else
    echo "  mooncake_master: NOT FOUND"
fi

echo ""
echo "Mooncake rebuilt and installed successfully."
echo "Branch: $(cd "${MOONCAKE_ROOT}" && git branch --show-current 2>/dev/null || echo 'detached')"
echo "Commit: $(cd "${MOONCAKE_ROOT}" && git log --oneline -1)"
