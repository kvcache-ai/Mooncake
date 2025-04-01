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

# TODO: remove cpprestsdk, etcd-cpp-apiv3, grpc, grpc++ and protobuf installation later

set -e
set -o pipefail

# Configuration
# Fetch the root directory of the repository
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
# Directory to store third-party dependencies
THIRD_PARTY_DIR="${REPO_ROOT}/thirdparties"
# Go version to install
GO_VERSION="1.22.10"
# Go tarball name
GO_TARBALL="go${GO_VERSION}.linux-amd64.tar.gz"
# Go download URL
GO_URL="https://go.dev/dl/${GO_TARBALL}"
# Go installation directory
GO_INSTALL_DIR="/usr/local"
# Check if yalantinglibs already installed
YALANTING_HEADER_CHECK="/usr/local/include/ylt/reflection/user_reflect_macro.hpp"

# Output functions
info() { echo -e "\e[34mINFO: $1\e[0m"; }
success() { echo -e "\e[32mSUCCESS: $1\e[0m"; }
warn() { echo -e "\e[33mWARN: $1\e[0m"; }
error() { echo -e "\e[31mERROR: $1\e[0m" >&2; }

# Check if command exists
check_command() {
    command -v "$1" &> /dev/null || { error "Command '$1' not found. Please install it first."; exit 1; }
}

install_apt_packages() {
    info "Updating package lists..."
    sudo apt-get update -y || { error "apt-get update failed."; exit 1; }

    info "Installing required packages..."
    sudo apt-get install -y build-essential cmake git wget tar libibverbs-dev libunwind-dev \
        libgoogle-glog-dev libgtest-dev libjsoncpp-dev libnuma-dev libpython3-dev \
        libboost-all-dev libssl-dev libgrpc-dev libgrpc++-dev libprotobuf-dev \
        protobuf-compiler-grpc pybind11-dev libcurl4-openssl-dev libhiredis-dev pkg-config \
        || { error "Failed to install apt packages."; exit 1; }
    success "System packages installed."
}

install_cpprest() {
    local repo_url="https://github.com/microsoft/cpprestsdk.git"
    local repo_path="${THIRD_PARTY_DIR}/cpprestsdk"
    info "Installing [cpprestsdk]..."
    
    [ -d "$repo_path" ] || git clone "$repo_url" "$repo_path" || { error "Failed to clone cpprestsdk."; exit 1; }
    
    cd "$repo_path" || exit 1
    mkdir -p build && cd build || exit 1
    
    cmake .. -DCPPREST_EXCLUDE_WEBSOCKETS=ON || { error "CMake configuration failed."; exit 1; }
    make -j$(nproc) || { error "Build failed."; exit 1; }
    sudo make install || { error "Installation failed."; exit 1; }
    
    success "[cpprestsdk] installed successfully."
    cd "${REPO_ROOT}"
}

install_etcd_cpp() {
    local repo_url="https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git"
    local repo_path="${THIRD_PARTY_DIR}/etcd-cpp-apiv3"
    info "Installing [etcd-cpp-apiv3]..."
    
    [ -d "$repo_path" ] || git clone "$repo_url" "$repo_path" || { error "Failed to clone etcd-cpp-apiv3."; exit 1; }
    
    cd "$repo_path" || exit 1
    mkdir -p build && cd build || exit 1
    
    cmake .. || { error "CMake configuration failed."; exit 1; }
    make -j$(nproc) || { error "Build failed."; exit 1; }
    sudo make install || { error "Installation failed."; exit 1; }
    
    success "[etcd-cpp-apiv3] installed successfully."
    cd "${REPO_ROOT}"
}

install_yalanting() {
    info "Checking for existing [yalantinglibs] installation..."
    if [ -f "${YALANTING_HEADER_CHECK}" ]; then
        success "[yalantinglibs] header found. Skipping installation."
        return 0
    fi

    local repo_url="https://github.com/alibaba/yalantinglibs.git"
    local repo_path="${THIRD_PARTY_DIR}/yalantinglibs"
    
    [ -d "$repo_path" ] || git clone "$repo_url" "$repo_path" || { error "Failed to clone yalantinglibs."; exit 1; }
    
    cd "$repo_path" || exit 1
    mkdir -p build && cd build || exit 1
    
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF || { error "CMake configuration failed."; exit 1; }
    cmake --build . -j$(nproc) || { error "Build failed."; exit 1; }
    sudo cmake --install . || { error "Installation failed."; exit 1; }
    
    [ -f "${YALANTING_HEADER_CHECK}" ] || { error "Installation finished, but header not found."; exit 1; }
    success "[yalantinglibs] installed successfully."
    cd "${REPO_ROOT}"
}

install_go() {
    if command -v go >/dev/null 2>&1; then
        local installed_version=$(go version | awk '{print $3}' | sed 's/go//')
        
        if { [ "$(printf '%s\n' "$installed_version" "$GO_VERSION" | sort -V | head -n1)" = "$GO_VERSION" ] && 
             [ "$installed_version" != "$GO_VERSION" ]; } || 
           [ "$installed_version" = "$GO_VERSION" ]; then
            success "Go version ${installed_version} is sufficient. Skipping installation."
            return 0
        else
            error "Installed Go version (${installed_version}) is older than required (${GO_VERSION})."
            error "Please install Go ${GO_VERSION} manually."
            exit 1
        fi
    fi

    info "Downloading Go ${GO_VERSION}..."
    local temp_dir=$(mktemp -d)
    
    wget -q --show-progress -P "${temp_dir}" "${GO_URL}" || 
        { error "Download failed."; rm -rf "${temp_dir}"; exit 1; }
    
    sudo tar -C "${GO_INSTALL_DIR}" -xzf "${temp_dir}/${GO_TARBALL}" || 
        { error "Extraction failed."; rm -rf "${temp_dir}"; exit 1; }
    
    rm -rf "${temp_dir}"
    
    [ -x "${GO_INSTALL_DIR}/go/bin/go" ] || { error "Go binary not found after installation."; exit 1; }
    success "Go ${GO_VERSION} installed successfully."
    info "IMPORTANT: Add ${GO_INSTALL_DIR}/go/bin to your PATH."
}

# Main execution
info "Starting dependency installation..."
mkdir -p "${THIRD_PARTY_DIR}" || { error "Failed to create ${THIRD_PARTY_DIR}"; exit 1; }

# Check prerequisites
for cmd in git cmake wget tar gcc g++; do
    check_command $cmd
done

# Install dependencies
install_apt_packages
install_cpprest
install_etcd_cpp
install_yalanting
install_go

success "All dependencies installed successfully!"
exit 0
