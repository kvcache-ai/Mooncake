#!/bin/bash
# Copyright 2024 KVCache.AI
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

# Color definitions
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Configuration
REPO_ROOT=`pwd`
GITHUB_PROXY=${GITHUB_PROXY:-"https://github.com"}
OS_RELEASE_FILE=${OS_RELEASE_FILE:-/etc/os-release}

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error messages and exit
print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# Function to check command success
check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

read_os_release_value() {
    local key="$1"
    awk -F= -v key="$key" '
        $1 == key {
            value = $0
            sub(/^[^=]*=/, "", value)
            gsub(/^"|"$/, "", value)
            print value
            exit
        }
    ' "$OS_RELEASE_FILE"
}

# Function to detect OS
detect_os() {
    if [ -f "$OS_RELEASE_FILE" ]; then
        ID=$(read_os_release_value ID)
        VERSION_ID=$(read_os_release_value VERSION_ID)
        OS=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
        OS_VERSION=$VERSION_ID
    elif [ -f /etc/redhat-release ]; then
        OS="centos"
    else
        print_error "Cannot detect OS. Supported OS: Ubuntu, Debian, CentOS, RHEL, Rocky, AlmaLinux, EulerOS, and openEuler."
    fi

    echo -e "${GREEN}Detected OS: $OS ${OS_VERSION:-unknown}${NC}"
}

if [ $(id -u) -ne 0 ]; then
	print_error "Require root permission, try sudo ./dependencies.sh"
fi

# Parse command line arguments
SKIP_CONFIRM=false
INSTALL_SPDK=false
for arg in "$@"; do
    case $arg in
        -y|--yes)
            SKIP_CONFIRM=true
            ;;
        --with-spdk)
            INSTALL_SPDK=true
            ;;
        -h|--help)
            echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
            echo -e "Usage: ./dependencies.sh [OPTIONS]"
            echo -e "\nOptions:"
            echo -e "  -y, --yes       Skip confirmation and install all dependencies"
            echo -e "  --with-spdk     Install SPDK for NVMe-oF support"
            echo -e "  -h, --help      Show this help message and exit"
            exit 0
            ;;
    esac
done

# Print welcome message
echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
echo -e "This script will install all required dependencies for Mooncake."
echo -e "The following components will be installed:"
echo -e "  - System packages (libraries)"
echo -e "  - Git submodules (pybind11)"
echo -e "  - Toolchain (build tools)"
if [ "$INSTALL_SPDK" = true ]; then
    echo -e "  - SPDK (for NVMe-oF support)"
fi
echo

# Ask for confirmation unless -y flag is used
if [ "$SKIP_CONFIRM" = false ]; then
    read -p "Do you want to continue? [Y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! $REPLY = "" ]]; then
        echo -e "${YELLOW}Installation cancelled.${NC}"
        exit 0
    fi
fi

# Detect OS
detect_os

# Update package lists
print_section "Updating package lists"
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    apt-get update
    check_success "Failed to update package lists"
elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ]; then
    yum install -y dnf-plugins-core epel-release || true
    yum config-manager --set-enabled powertools || yum config-manager --set-enabled crb || true
    yum clean all
    yum makecache
    check_success "Failed to update package lists"
else
    print_error "Unsupported OS: $OS"
fi

# Install system packages
print_section "Installing system packages"
echo -e "${YELLOW}This may take a few minutes...${NC}"

if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    SYSTEM_PACKAGES="build-essential \
                     git \
                     wget \
                     unzip \
                     libibverbs-dev \
                     libgoogle-glog-dev \
                     libjsoncpp-dev \
                     libunwind-dev \
                     libnuma-dev \
                     libpython3-dev \
                     libboost-all-dev \
                     libssl-dev \
                     libgrpc-dev \
                     libgrpc++-dev \
                     libprotobuf-dev \
                     libyaml-cpp-dev \
                     protobuf-compiler-grpc \
                     libcurl4-openssl-dev \
                     libhiredis-dev \
                     liburing-dev \
                     libjemalloc-dev \
                     libmsgpack-dev \
                     libzmq3-dev \
                     libzstd-dev \
                     libxxhash-dev \
                     pkg-config \
                     patchelf \
                     python3-venv \
                     libc6-dev \
                     libc-bin"

    apt-get install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"

elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ]; then
    SYSTEM_PACKAGES="@development \
                     git \
                     wget \
                     rdma-core-devel \
                     glog-devel \
                     gflags-devel \
                     jsoncpp-devel \
                     libunwind-devel \
                     numactl-devel \
                     python3-devel \
                     boost1.78-devel \
                     openssl-devel \
                     protobuf-devel \
                     yaml-cpp-devel \
                     libcurl-devel \
                     hiredis-devel \
                     liburing-devel \
                     jemalloc-devel \
                     msgpack-devel \
                     libzstd-devel \
                     pkgconf-pkg-config \
                     elfutils-libelf-devel \
                     patchelf  \
                     xxhash-devel \
                     libbsd-devel"

    yum install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"
else
    print_error "Unsupported OS: $OS"
fi

print_success "System packages installed successfully"

# Initialize and update git submodules
print_section "Initializing Git Submodules"

# Check if .gitmodules exists
if [ -f "${REPO_ROOT}/.gitmodules" ]; then
    echo "Enter repository root: ${REPO_ROOT}"
    cd "${REPO_ROOT}"
    check_success "Failed to change to repository root directory"

    echo "Initializing git submodules..."
    git submodule sync --recursive
    check_success "Failed to sync git submodules"
    git submodule update --init --recursive
    check_success "Failed to initialize git submodules"

    print_success "Git submodules initialized and updated successfully"
else
    echo -e "${YELLOW}No .gitmodules file found. Skipping...${NC}"
    exit 1
fi

print_section "Verifying essential build tools"

# Verify getconf and ldd (required for glibc version detection in build_wheel.sh)
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    if ! command -v getconf >/dev/null 2>&1; then
        print_error "getconf not found after installing system packages. This should not happen."
    fi
    if ! command -v ldd >/dev/null 2>&1; then
        print_error "ldd not found after installing system packages. This should not happen."
    fi
    print_success "getconf found: $(getconf --version 2>&1 | head -1)"
    print_success "ldd found: $(ldd --version 2>&1 | head -1)"
fi

print_section "Installing Toolchain: CMake, Ninja and Go"

sh ${REPO_ROOT}/scripts/install_toolchain.sh
check_success "Failed to install toolchain"

print_success "Toolchain installed successfully"

# Install SPDK if requested
if [ "$INSTALL_SPDK" = true ]; then
    print_section "Installing SPDK"

    cd "${REPO_ROOT}/extern"
    check_success "Failed to change to extern directory"

    # Remove existing SPDK if present
    if [ -d "spdk" ]; then
        echo -e "${YELLOW}SPDK directory already exists. Removing for fresh install...${NC}"
        rm -rf spdk
        check_success "Failed to remove existing SPDK directory"
    fi

    # Clone SPDK
    echo "Cloning SPDK from ${GITHUB_PROXY}/spdk/spdk.git..."
    git clone ${GITHUB_PROXY}/spdk/spdk.git
    check_success "Failed to clone SPDK"

    cd spdk
    check_success "Failed to change to SPDK directory"

    # Checkout specific version
    echo "Checking out SPDK version v23.01.1..."
    git checkout v23.01.1
    check_success "Failed to checkout SPDK version v23.01.1"

    # Initialize submodules
    echo "Initializing SPDK submodules..."
    git submodule update --init
    check_success "Failed to initialize SPDK submodules"

    # Install SPDK dependencies
    echo "Installing SPDK dependencies..."
    ./scripts/pkgdep.sh
    check_success "Failed to install SPDK dependencies"

    # Configure SPDK with RDMA support
    echo "Configuring SPDK with RDMA support..."
    ./configure --with-rdma
    check_success "Failed to configure SPDK"

    # Build SPDK
    echo "Building SPDK (using $(nproc) cores)..."
    make -j$(nproc)
    check_success "Failed to build SPDK"

    # Install SPDK
    echo "Installing SPDK..."
    make install
    check_success "Failed to install SPDK"

    # Copy DPDK libraries to system library path
    if ls dpdk/build/lib/*.a >/dev/null 2>&1; then
        echo "Copying DPDK libraries to /usr/local/lib..."
        cp dpdk/build/lib/*.a /usr/local/lib/
        check_success "Failed to copy DPDK libraries"
    fi

    print_success "SPDK installed successfully"
    cd "${REPO_ROOT}"
fi

# Return to the repository root
cd "${REPO_ROOT}"

# Print summary
print_section "Installation Complete"
echo -e "${GREEN}All dependencies have been successfully installed!${NC}"
echo -e "The following components were installed:"
echo -e "  ${GREEN}✓${NC} System packages"
echo -e "  ${GREEN}✓${NC} Git submodules"
echo -e "  ${GREEN}✓${NC} Toolchain"
if [ "$INSTALL_SPDK" = true ]; then
    echo -e "  ${GREEN}✓${NC} SPDK (v23.01.1)"
fi
echo
echo -e "You can now build and run Mooncake."
echo -e "${YELLOW}Note: You may need to restart your terminal or run 'source ~/.bashrc' to use toolchain.${NC}"

if [ "$INSTALL_SPDK" = true ]; then
    echo -e "${YELLOW}Note: SPDK requires hugepages and RDMA configuration. Please refer to SPDK documentation for setup.${NC}"
fi
