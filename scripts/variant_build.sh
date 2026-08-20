#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"
BUILD_DIR="${REPO_ROOT}/build"
FINAL_DIST_DIR="${FINAL_DIST_DIR:-${REPO_ROOT}/mooncake-wheel/dist}"
PYPROJECT_FILE="${REPO_ROOT}/mooncake-wheel/pyproject.toml"

RUN_DEPS=1
DRY_RUN=0
BUILD_VARIANT=""
PYPROJECT_BACKUP=""
BUILD_TYPE="Release"

usage() {
    cat <<'EOF'
Usage: ./scripts/build_transfer_engine_variants.sh [options] <standard|rpc|shca>

Build variants:
  standard  mooncake_transfer_engine       (normal NIC, no hylink; USE_HYGON=ON, USE_FAKE_HIP_RPC=ON, USE_ETCD=ON, STORE_USE_ETCD=ON)
  rpc       mooncake_transfer_engine_rpc   (normal NIC, hylink enabled; USE_HYGON=ON, USE_ETCD=ON, STORE_USE_ETCD=ON)
  shca      mooncake_transfer_engine_shca  (TianLong NIC, no hylink; USE_HYGON=ON, USE_SHCA=ON, USE_FAKE_HIP_RPC=ON, USE_ETCD=ON, STORE_USE_ETCD=ON)

Options:
  --skip-deps   Skip `bash dependencies.sh`
  --dry-run     Print commands without executing them
  --jobs N      Override build parallelism
  -h, --help    Show this help

Exactly one build variant must be specified.
EOF
}

restore_pyproject() {
    if [ -n "${PYPROJECT_BACKUP}" ] && [ -f "${PYPROJECT_BACKUP}" ]; then
        mv "${PYPROJECT_BACKUP}" "${PYPROJECT_FILE}"
    fi
}

patch_pyproject() {
    local package_name="$1"
    local package_description="$2"
    local package_keywords="$3"

    if [ -n "${PYPROJECT_BACKUP}" ]; then
        restore_pyproject
    fi

    PYPROJECT_BACKUP="$(mktemp)"
    cp "${PYPROJECT_FILE}" "${PYPROJECT_BACKUP}"

    python - "${PYPROJECT_FILE}" "${package_name}" "${package_description}" "${package_keywords}" <<'PY'
import re
import sys

path, package_name, package_description, package_keywords = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

content = re.sub(
    r'^name = ".*"$',
    f'name = "{package_name}"',
    content,
    count=1,
    flags=re.MULTILINE,
)
content = re.sub(
    r'^description = ".*"$',
    f'description = "{package_description}"',
    content,
    count=1,
    flags=re.MULTILINE,
)
content = re.sub(
    r'^keywords = \[.*\]$',
    f'keywords = {package_keywords}',
    content,
    count=1,
    flags=re.MULTILINE,
)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
PY
}

run_cmd() {
    printf '+'
    printf ' %q' "$@"
    printf '\n'
    if [ "${DRY_RUN}" -eq 0 ]; then
        "$@"
    fi
}

refresh_go_env() {
    if [ -d "/usr/local/go/bin" ] && [[ ":${PATH}:" != *":/usr/local/go/bin:"* ]]; then
        export PATH="${PATH}:/usr/local/go/bin"
    fi
    hash -r
}

run_wheel_build() {
    printf '+'
    printf ' %q' bash "${REPO_ROOT}/scripts/build_wheel.sh"
    printf '\n'

    if [ "${DRY_RUN}" -eq 0 ]; then
        (
            cd "${REPO_ROOT}"
            bash ./scripts/build_wheel.sh
        )
    fi
}

setup_shca_env() {
    echo "==> Preparing TianLong SHCA environment..."

    # 检查驱动头文件是否已存在
    if [ -f /usr/include/infiniband/shca_17b_types.h ]; then
        echo "SHCA driver header found (/usr/include/infiniband/shca_17b_types.h), skipping setup."
        return 0
    fi

    if [ "${DRY_RUN}" -eq 1 ]; then
        echo "[dry-run] Would download and install SHCA driver."
        return 0
    fi

    local SETUP_DIR="/tmp/shca_setup"
    mkdir -p "${SETUP_DIR}"
    cd "${SETUP_DIR}"

    # 下载执行脚本
    wget ${RESOURCE_SERVER_URL}/Jenkins/CompileDep/mooncake/mlxtoshca.sh

    # 下载驱动安装包
    wget ${RESOURCE_SERVER_URL}/Jenkins/CompileDep/mooncake/shca-tools_2.500.4.B068-Ubuntu22.04_amd64.deb

    # 安装驱动
    bash "${SETUP_DIR}/mlxtoshca.sh"

    # 验证安装
    ibv_devinfo
    echo "SHCA environment setup complete."
}

build_variant() {
    local variant="$1"
    local package_basename=""
    local -a cmake_args=(-DUSE_ETCD=ON -DSTORE_USE_ETCD=ON)

    case "${variant}" in
        standard)
            package_basename="mooncake_transfer_engine"
            cmake_args+=(-DUSE_HYGON=ON -DUSE_FAKE_HIP_RPC=ON)
            if [ "${DRY_RUN}" -eq 0 ]; then
                restore_pyproject
            fi
            ;;
        rpc)
            package_basename="mooncake_transfer_engine_rpc"
            cmake_args+=(-DUSE_HYGON=ON -DUSE_FAKE_HIP_RPC=OFF)
            if [ "${DRY_RUN}" -eq 0 ]; then
                patch_pyproject \
                    "mooncake-transfer-engine-rpc" \
                    "Python binding of a Mooncake library using pybind11 (Hylink-enabled version)" \
                    '["mooncake", "data transfer", "kv cache", "llm inference", "hylink", "rpc"]'
            fi
            ;;
        shca)
            package_basename="mooncake_transfer_engine_shca"
            cmake_args+=(-DUSE_HYGON=ON -DUSE_SHCA=ON -DUSE_FAKE_HIP_RPC=ON)
            setup_shca_env
            if [ "${DRY_RUN}" -eq 0 ]; then
                patch_pyproject \
                    "mooncake-transfer-engine-shca" \
                    "Python binding of a Mooncake library using pybind11 (TianLong SHCA version)" \
                    '["mooncake", "data transfer", "kv cache", "llm inference", "tianlong", "shca"]'
            fi
            ;;
        *)
            echo "Unknown build variant: ${variant}" >&2
            exit 1
            ;;
    esac

    echo "==> Building ${package_basename}"
    run_cmd mkdir -p "${BUILD_DIR}" "${FINAL_DIST_DIR}"
    unset VERBOSE || true
    run_cmd cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" "${cmake_args[@]}" \
        -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_VERBOSE_MAKEFILE=OFF -DCMAKE_CXX_FLAGS="" \
        -DCMAKE_IGNORE_PREFIX_PATH="${CONDA_PREFIX:-/opt/hyhal/hbm/Miniconda3}"
    run_cmd cmake --build "${BUILD_DIR}" -j"${JOBS}"
    run_wheel_build
}

while [ $# -gt 0 ]; do
    case "$1" in
        standard|rpc|shca)
            if [ -n "${BUILD_VARIANT}" ]; then
                echo "Error: exactly one build variant must be specified." >&2
                usage >&2
                exit 1
            fi
            BUILD_VARIANT="$1"
            ;;
        --skip-deps)
            RUN_DEPS=0
            ;;
        --dry-run)
            DRY_RUN=1
            ;;
        --jobs)
            JOBS="$2"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

if [ -z "${BUILD_VARIANT}" ]; then
    echo "Error: missing build variant." >&2
    usage >&2
    exit 1
fi

trap restore_pyproject EXIT

run_cmd rm -rf "${FINAL_DIST_DIR}"
run_cmd mkdir -p "${FINAL_DIST_DIR}"

if [ "${RUN_DEPS}" -eq 1 ]; then
    run_cmd bash "${REPO_ROOT}/dependencies.sh" -y
fi

refresh_go_env

build_variant "${BUILD_VARIANT}"

run_cmd ls -1 "${FINAL_DIST_DIR}"
