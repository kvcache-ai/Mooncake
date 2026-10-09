#!/bin/bash
# Copyright 2025 Huawei Technologies Co., Ltd
# Copyright 2024 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install system dependencies for the Ascend transport build.
# Debian/Ubuntu uses apt packages; openEuler/RHEL uses yum/dnf packages.
#
# NOTE: the former from-source build of msgpack-c was removed: no Mooncake
# target on the Ascend path consumes msgpack (mooncake-conductor is
# WITH_CONDUCTOR=OFF by default and locates msgpack on its own when enabled),
# and building msgpack-c with its default MSGPACK_USE_BOOST=ON forced an extra
# Boost requirement that broke on newer CMake (>= 3.30) with older boost-devel.
# yaml-cpp is a hard requirement of mooncake-common (find_package(yaml-cpp
# REQUIRED)); it is now satisfied by the distro package (yaml-cpp-devel /
# libyaml-cpp-dev) instead of a from-source build.

print_error() {
    echo "[ERROR] $1"
    exit 1
}

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "[WARN] Not running as root; system package installation will likely fail. Try: sudo bash $0"
fi

# System detection and dependency installation
if command -v apt-get &> /dev/null; then
    echo "Detected apt-get. Using Debian-based package manager."
    apt-get update
    apt-get install -y build-essential \
            cmake \
            git \
            wget \
            libibverbs-dev \
            libgoogle-glog-dev \
            libjsoncpp-dev \
            libunwind-dev \
            libnuma-dev \
            libpython3-dev \
            libboost-dev \
            libssl-dev \
            libgrpc-dev \
            libgrpc++-dev \
            libprotobuf-dev \
            libyaml-cpp-dev \
            protobuf-compiler-grpc \
            libcurl4-openssl-dev \
            libhiredis-dev \
            pkg-config \
            patchelf \
            mpich \
            libmpich-dev \
            libzstd-dev \
            libxxhash-dev \
            libmsgpack-dev
    apt purge -y openmpi-bin libopenmpi-dev || true
elif command -v yum &> /dev/null; then
    echo "Detected yum. Using Red Hat-based package manager."
    yum makecache
    # Required packages; keep in sync with scripts/ascend/dependencies_openeuler.sh.
    yum install -y cmake \
            gcc gcc-c++ make git wget unzip \
            gflags-devel \
            glog-devel \
            libibverbs-devel \
            numactl-devel \
            boost-devel \
            openssl-devel \
            hiredis-devel \
            libcurl-devel \
            jsoncpp-devel \
            yaml-cpp-devel \
            zstd-devel \
            xxhash-devel
    # Best-effort packages: provided by most openEuler releases but safe to skip.
    yum install -y mpich mpich-devel 2>/dev/null \
        || echo "[WARN] mpich not available from repo, skip."
    yum install -y libunwind-devel python3-devel pkgconf pkgconf-pkg-config patchelf glibc glibc-common 2>/dev/null \
        || echo "[WARN] some optional packages are not available from repo, skip."
    # openEuler images may ship OpenMPI; remove it to avoid conflicts with MPICH.
    yum remove -y openmpi openmpi-devel 2>/dev/null || true
else
    echo "Unsupported package manager. Please install the dependencies manually."
    exit 1
fi

echo -e "system packages installed successfully."

export CPLUS_INCLUDE_PATH="$(echo "${CPLUS_INCLUDE_PATH:-}" | tr ':' '\n' | grep -v "/usr/local/Ascend" | paste -sd: - || true)"
