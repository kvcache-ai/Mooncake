#!/bin/bash
# Copyright 2024-2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEPS_BUILD_DIR="${REPO_ROOT}/.mooncake-openeuler-deps"
GOVER=1.25.9
ASCEND_GITHUB_MIRROR_URLS="${ASCEND_GITHUB_MIRROR_URLS:-https://ghfast.top/}"

print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

print_warn() {
    echo -e "${YELLOW}! $1${NC}"
}

check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

SKIP_CONFIRM=false
for arg in "$@"; do
    case $arg in
        -y|--yes)
            SKIP_CONFIRM=true
            ;;
        -h|--help)
            echo -e "${YELLOW}Mooncake openEuler Dependencies Installer${NC}"
            echo "Usage: bash scripts/ascend/dependencies_openeuler.sh [-y]"
            exit 0
            ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    print_error "Require root permission, try: sudo bash scripts/ascend/dependencies_openeuler.sh -y"
fi

echo -e "${YELLOW}Mooncake openEuler Dependencies Installer${NC}"
echo "Repository root: ${REPO_ROOT}"
echo "This script installs (same scope as root dependencies.sh):"
echo "  - openEuler/RHEL system packages (dnf/yum)"
echo "  - Git submodules (pybind11, yalantinglibs, ...)"
echo "  - yalantinglibs (from extern/ submodule)"
echo "  - Go ${GOVER} (for USE_ETCD / libetcd_wrapper.so)"
echo "Run scripts/ascend/dependencies_ascend_installation.sh afterward for Ascend extras."
echo

if [ "$SKIP_CONFIRM" = false ]; then
    read -p "Do you want to continue? [Y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! $REPLY = "" ]]; then
        echo -e "${YELLOW}Installation cancelled.${NC}"
        exit 0
    fi
fi

detect_pkg_manager() {
    if command -v dnf >/dev/null 2>&1; then
        echo dnf
    elif command -v yum >/dev/null 2>&1; then
        echo yum
    else
        print_error "Neither dnf nor yum found. This script is for openEuler/RHEL only."
    fi
}

git_with_github_mirror_fallback() {
    local repo_dir="$1"
    local repo_url="$2"
    shift 2

    if git clone "$repo_url" "$repo_dir" "$@"; then
        return 0
    fi

    print_warn "Direct clone failed, retrying with mirror https://ghfast.top/"
    rm -rf "$repo_dir"
    git clone "https://ghfast.top/${repo_url}" "$repo_dir" "$@"
}

clone_repo_if_not_exists() {
    local repo_dir="$1"
    local repo_url="$2"
    shift 2

    if [ -d "$repo_dir" ]; then
        echo "Directory $repo_dir already exists, skipping clone."
        return 0
    fi
    git_with_github_mirror_fallback "$repo_dir" "$repo_url" "$@"
}

install_rpm_packages() {
    local pkg_mgr="$1"
    shift
    local packages=("$@")
    local missing=()

    print_section "Installing system packages via ${pkg_mgr}"
    ${pkg_mgr} makecache -y || ${pkg_mgr} makecache
    check_success "Failed to refresh ${pkg_mgr} metadata"

    for pkg in "${packages[@]}"; do
        if ! ${pkg_mgr} install -y "$pkg"; then
            missing+=("$pkg")
        fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
        print_warn "Some packages were not installed (may be optional on this openEuler release):"
        printf '  - %s\n' "${missing[@]}"
    fi
}

PKG_MGR="$(detect_pkg_manager)"

# Core build dependencies aligned with root dependencies.sh (Debian names mapped to RHEL/openEuler).
CORE_PACKAGES=(
    gcc gcc-c++ make cmake ninja-build git wget unzip
    gflags-devel glog-devel libibverbs-devel numactl-devel
    gtest gtest-devel boost-devel openssl-devel hiredis-devel
    libcurl-devel jsoncpp-devel libunwind-devel python3-devel
    zstd-devel xxhash-devel pkgconf pkgconf-pkg-config patchelf
    mpich mpich-devel glibc glibc-common
)

# Often provided by repos on openEuler; install best-effort.
OPTIONAL_PACKAGES=(
    grpc-devel grpc-plugins protobuf-devel protobuf-compiler
    liburing-devel jemalloc-devel
)

install_rpm_packages "$PKG_MGR" "${CORE_PACKAGES[@]}"
install_rpm_packages "$PKG_MGR" "${OPTIONAL_PACKAGES[@]}" || true

# openEuler images may ship MPICH; remove OpenMPI if present to avoid conflicts.
${PKG_MGR} remove -y openmpi openmpi-devel 2>/dev/null || true

print_success "System package installation finished"

export CPLUS_INCLUDE_PATH="$(
    echo "${CPLUS_INCLUDE_PATH:-}" | tr ':' '\n' | grep -v "/usr/local/Ascend" | paste -sd: -
)"

print_section "Initializing Git submodules"
cd "${REPO_ROOT}"
if [ ! -f "${REPO_ROOT}/.gitmodules" ]; then
    print_error "No .gitmodules found under ${REPO_ROOT}"
fi

submodule_updated=false
if git submodule sync --recursive && git submodule update --init --recursive; then
    submodule_updated=true
elif [ -n "${ASCEND_GITHUB_MIRROR_URLS:-}" ]; then
  while IFS= read -r raw; do
    base="${raw#"${raw%%[![:space:]]*}"}"
    base="${base%"${base##*[![:space:]]}"}"
    [ -n "$base" ] || continue
    [ "$base" = "https://github.com/" ] && continue
    [ "$base" != "https://github.com/" ] && base="${base%/}/"
    echo "Retrying submodule update with mirror ${base}"
    if git -c url."${base}https://github.com/".insteadOf=https://github.com/ \
        submodule sync --recursive && \
       git -c url."${base}https://github.com/".insteadOf=https://github.com/ \
        submodule update --init --recursive; then
        submodule_updated=true
        break
    fi
  done < <(printf '%s\n' "$ASCEND_GITHUB_MIRROR_URLS" | tr ',;' '\n')
fi

if [ "$submodule_updated" != true ]; then
    print_error "git submodule update failed (direct GitHub and mirrors exhausted)"
fi
print_success "Git submodules initialized"

print_section "Installing yalantinglibs from extern/ submodule"
if [ -d "${REPO_ROOT}/extern/yalantinglibs" ]; then
    cd "${REPO_ROOT}/extern/yalantinglibs"
    rm -rf build
    mkdir -p build && cd build
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
    check_success "Failed to configure yalantinglibs"
    cmake --build . -j"$(nproc)"
    check_success "Failed to build yalantinglibs"
    cmake --install .
    check_success "Failed to install yalantinglibs"
    print_success "yalantinglibs installed from submodule"
    cd "${REPO_ROOT}"
else
    print_warn "extern/yalantinglibs missing, building from upstream clone"
    mkdir -p "${DEPS_BUILD_DIR}"
    cd "${DEPS_BUILD_DIR}"
    clone_repo_if_not_exists "yalantinglibs" "https://github.com/alibaba/yalantinglibs.git"
    cd yalantinglibs
    git checkout 0.5.5
    rm -rf build && mkdir -p build && cd build
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
    cmake --build . -j"$(nproc)"
    cmake --install .
    cd "${REPO_ROOT}"
    print_success "yalantinglibs installed from git clone"
fi

print_section "Verifying essential build tools"
for tool in getconf ldd patchelf cmake git; do
    command -v "$tool" >/dev/null || print_error "${tool} not found after dependency installation"
    print_success "${tool} found: $(command -v "$tool")"
done

print_section "Installing Go ${GOVER}"
USED_CN_MIRROR=false

install_go() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        aarch64) arch=arm64 ;;
        x86_64) arch=amd64 ;;
        *) print_error "Unsupported architecture: $arch" ;;
    esac

    local tarball="go${GOVER}.linux-${arch}.tar.gz"
    local urls=(
        "https://go.dev/dl/${tarball}"
        "https://golang.google.cn/dl/${tarball}"
        "https://mirrors.aliyun.com/golang/${tarball}"
    )

    local url downloaded=false
    for url in "${urls[@]}"; do
        echo "Downloading Go ${GOVER} from ${url}..."
        if wget -q --show-progress --timeout=30 --tries=2 -O "${tarball}" "${url}"; then
            downloaded=true
            [[ "$url" != "https://go.dev/dl/${tarball}" ]] && USED_CN_MIRROR=true
            break
        fi
        rm -f "${tarball}"
    done

    [ "$downloaded" = true ] || print_error "Failed to download Go ${GOVER}"

    rm -rf /usr/local/go
    tar -C /usr/local -xzf "${tarball}"
    rm -f "${tarball}"
    export PATH="/usr/local/go/bin:${PATH}"
    print_success "Go ${GOVER} installed to /usr/local/go"
}

if command -v go >/dev/null 2>&1; then
    current_go="$(go version | awk '{print $3}')"
    if [[ "$current_go" == "go${GOVER}" ]]; then
        print_success "Go ${GOVER} already installed (${current_go})"
    else
        print_warn "Found ${current_go}, installing Go ${GOVER}"
        install_go
    fi
else
    install_go
fi

export PATH="/usr/local/go/bin:${PATH}"
if [ "$USED_CN_MIRROR" = true ] && [ -z "${GOPROXY:-}" ]; then
    export GOPROXY=https://goproxy.cn,https://goproxy.io,direct
    print_warn "GOPROXY set to ${GOPROXY} (restricted network detected)"
fi

go version
print_success "Go is available: $(command -v go)"

print_section "Installation complete"
echo -e "${GREEN}openEuler dependencies are ready.${NC}"
echo "Next steps (example NPU release build):"
echo "  source /usr/local/Ascend/cann-9.0.0/set_env.sh"
echo "  mkdir -p build && cd build"
echo "  cmake .. -DUSE_ASCEND_DIRECT=ON -DUSE_CUDA=OFF -DWITH_EP=OFF -DUSE_HTTP=ON -DUSE_ETCD=ON -DSTORE_USE_ETCD=ON"
echo "  cmake --build . -j\$(nproc) && cmake --install ."