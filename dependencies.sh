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

REPO_ROOT=`pwd`
GITHUB_PROXY="https://github.com"

sudo apt-get install -y build-essential \
                        cmake \
                        libunwind-dev \
                        libgoogle-glog-dev \
                        libgtest-dev \
                        libjsoncpp-dev \
                        libnuma-dev \
                        libpython3-dev \
                        libboost-all-dev \
                        libssl-dev \
                        pybind11-dev \
                        libcurl4-openssl-dev \
                        libhiredis-dev \
                        pkg-config \
                        patchelf


echo "*** Download and installing [setuptools & wheel] ***"            
pip install build setuptools wheel

echo "*** Download and installing [yalantinglibs] ***"
cd ${REPO_ROOT}/thirdparties
git clone ${GITHUB_PROXY}/alibaba/yalantinglibs.git
cd yalantinglibs
mkdir -p build
cd build
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
cmake --build . -j$(nproc)
sudo cmake --install .

echo "*** Download and installing [golang-1.22] ***"
wget https://go.dev/dl/go1.22.10.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.10.linux-amd64.tar.gz

echo "*** Dependencies Installed! ***"
