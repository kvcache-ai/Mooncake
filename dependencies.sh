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
set -ex

REPO_ROOT=`pwd`
GITHUB_PROXY="https://mirror.ghproxy.com/github.com"

sudo apt-get install -y build-essential \
                        cmake \
                        libibverbs-dev \
                        libgoogle-glog-dev \
                        libgtest-dev \
                        libjsoncpp-dev \
                        libnuma-dev \
                        libpython3-dev \
                        libboost-all-dev \
                        libssl-dev \
                        libgrpc-dev \
                        libgrpc++-dev \
                        libprotobuf-dev \
                        protobuf-compiler-grpc \
                        pybind11-dev

echo "*** Download and installing [cpprest sdk] ***"
mkdir ${REPO_ROOT}/thirdparties
cd ${REPO_ROOT}/thirdparties
git clone ${GITHUB_PROXY}/microsoft/cpprestsdk.git
cd cpprestsdk
mkdir -p build
cd build
cmake .. -DCPPREST_EXCLUDE_WEBSOCKETS=ON
make -j$(nproc) && sudo make install

echo "*** Download and installing [etcd-cpp-apiv3] ***"
cd ${REPO_ROOT}/thirdparties
git clone ${GITHUB_PROXY}/etcd-cpp-apiv3/etcd-cpp-apiv3.git
cd etcd-cpp-apiv3
mkdir -p build
cd build
cmake ..
make -j$(nproc) && sudo make install

echo "*** Dependencies Installed! ***"
