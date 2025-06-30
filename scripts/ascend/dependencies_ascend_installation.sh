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

#!/bin/bash
# 默认的git clone的依赖目录，如果git clone失败，可以把依赖和Mooncake源码放到该目录下进行编译安装
# 如果提供了第一个参数（$1），则使用它作为TARGET_DIR；否则，默认使用当前目录。
TARGET_DIR=${1:-$(pwd)}

# 定义一个函数来处理git clone操作
clone_repo_if_not_exists() {
    local repo_dir=$1
    local repo_url=$2

    if [ ! -d "$repo_dir" ]; then
        git clone "$repo_url"
    else
        echo "Directory $repo_dir already exists, skipping clone."
    fi
}

set +e  # 允许脚本在某条命令失败后继续执行

# 安装基础依赖库
yum install -y cmake \
gflags-devel \
glog-devel \
libibverbs-devel \
numactl-devel \
gtest \
gtest-devel \
boost-devel \
openssl-devel --allowerasing \
hiredis-devel \
libcurl-devel \
jsoncpp-devel

# 安装 MPI 相关依赖，ASCEND依赖
yum install -y mpich mpich-devel

# 进入目标目录
cd "$TARGET_DIR" || { echo "Failed to enter directory"; exit 1; }

# 处理 yalantinglibs
clone_repo_if_not_exists "yalantinglibs" "https://github.com/alibaba/yalantinglibs.git"
cd yalantinglibs || exit
mkdir -p build && cd build
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
make -j$(nproc)
make install
cd ../..

cp libascend_transport_mem.so /usr/local/Ascend/ascend-toolkit/latest/python/site-packages
pip install *.whl --force-reinstall