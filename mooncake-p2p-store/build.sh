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

if [ "$#" -ne 6 ]; then
    echo "Usage: $0 TARGET_PATH USE_ETCD USE_REDIS USE_HTTP USE_ETCD_LEGACY BUILD_DIR"
    exit 1
fi

TARGET=$1
USE_ETCD=$2
USE_REDIS=$3
USE_HTTP=$4
USE_ETCD_LEGACY=$5
BUILD_DIR=$6

cd "src/p2pstore"
if [ $? -ne 0 ]; then
    echo "Error: Directory src/p2pstore does not exist."
    exit 1
fi

EXT_LDFLAGS="-L$BUILD_DIR/mooncake-transfer-engine/src"
EXT_LDFLAGS+=" -L$BUILD_DIR/mooncake-transfer-engine/src/common/base"
EXT_LDFLAGS+=" -L$BUILD_DIR/mooncake-asio"
EXT_LDFLAGS+=" -ltransfer_engine -lbase -lasio -lstdc++ -lnuma -lglog -libverbs -ljsoncpp"

if [ -d "/usr/local/cuda/lib64" ]; then
    EXT_LDFLAGS+=" -L/usr/local/cuda/lib64 -lcudart"
fi

if [ -d "/opt/rocm/lib" ]; then
    EXT_LDFLAGS+=" -L/opt/rocm/lib64 -L/opt/rocm/lib -lamdhip64"
fi

if [ -d "/usr/local/musa/lib" ]; then
    EXT_LDFLAGS+=" -L/usr/local/musa/lib -lmusart"
fi

if [ "$USE_ETCD" = "ON" ]; then
    if [ "$USE_ETCD_LEGACY" = "ON" ]; then
        EXT_LDFLAGS+=" -letcd-cpp-api -lprotobuf -lgrpc++ -lgrpc"
    else
        EXT_LDFLAGS+=" -L$BUILD_DIR/mooncake-common/etcd -letcd_wrapper"
    fi
fi

if [ "$USE_REDIS" = "ON" ]; then
    EXT_LDFLAGS+=" -lhiredis"
fi

if [ "$USE_HTTP" = "ON" ]; then
    EXT_LDFLAGS+=" -lcurl"
fi

go get
go build -o "$TARGET/p2p-store-example" -ldflags="-extldflags '$EXT_LDFLAGS'" "../example/p2p-store-example.go"
if [ $? -ne 0 ]; then
    echo "Error: Failed to build the example."
    exit 1
fi

echo "P2P Store: build successfully"
