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

# If git clone fails, you can place dependencies and the script in a directory for compilation and installation.
# Define a function to handle the git clone operation

#!/bin/bash

clone_repo_if_not_exists() {
    local repo_dir=$1
    local repo_url=$2

    if [ ! -d "$repo_dir" ]; then
        git clone "$repo_url"
    else
        echo "Directory $repo_dir already exists, skipping clone."
    fi
}

# Function to check command success
check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

set +e

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
            libgtest-dev \
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
            pkg-config \
            patchelf \
            mpich \
            libmpich-dev
    apt purge -y openmpi-bin libopenmpi-dev || true
elif command -v yum &> /dev/null; then
    echo "Detected yum. Using Red Hat-based package manager."
    yum makecache
    yum install -y cmake \
            gflags-devel \
            glog-devel \
            libibverbs-devel \
            numactl-devel \
            gtest \
            gtest-devel \
            boost-devel \
            openssl-devel \
            hiredis-devel \
            libcurl-devel \
            jsoncpp-devel \
            mpich \
            mpich-devel
    # Install yaml-cpp
    cd "$TARGET_DIR"
    clone_repo_if_not_exists "yaml-cpp" https://github.com/jbeder/yaml-cpp.git
    cd yaml-cpp || exit
    rm -rf build
    mkdir -p build && cd build
    cmake ..
    make -j$(nproc)
    make install
    cd ../..
else
    echo "Unsupported package manager. Please install the dependencies manually."
    exit 1
fi

check_success "Failed to install system packages"
echo -e "system packages installed successfully."

export CPLUS_INCLUDE_PATH=$(echo $CPLUS_INCLUDE_PATH | tr ':' '\n' | grep -v "/usr/local/Ascend" | paste -sd: -)

# Install yalantinglibs
clone_repo_if_not_exists "yalantinglibs" "https://github.com/alibaba/yalantinglibs.git"
cd yalantinglibs || exit
git checkout 0.5.5
rm -rf build
mkdir -p build && cd build
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
make -j$(nproc)
make install
cd ../..

echo -e "yalantinglibs installed successfully."

# Add the so package to the environment variables
cp libascend_transport_mem.so /usr/local/Ascend/ascend-toolkit/latest/python/site-packages

# Pip install whl
pip install mooncake_transfer_engine*.whl --force