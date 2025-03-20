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

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 TARGET"
    exit 1
fi

TARGET=$1
PROJECT_ROOT_DIRECTORY="`pwd`/../"

cd "src/p2pstore"
if [ $? -ne 0 ]; then
    echo "Error: Directory src/p2pstore does not exist."
    exit 1
fi

# go get
# if [ $? -ne 0 ]; then
#     echo "Error: Failed to get dependencies."
#     exit 1
# fi

EXT_LDFLAGS="-L$PROJECT_ROOT_DIRECTORY/build/mooncake-transfer-engine/src"
EXT_LDFLAGS+=" -L$PROJECT_ROOT_DIRECTORY/thirdparties/lib"
EXT_LDFLAGS+=" -ltransfer_engine -lstdc++ -lnuma -lglog -libverbs -ljsoncpp -letcd-cpp-api -lprotobuf -lgrpc++ -lgrpc"
# EXT_LDFLAGS+=" -lhiredis"     // if USE_REDIS is enabled
# EXT_LDFLAGS+=" -lcurl"        // if USE_HTTP is enabled

go get
go build -o "$TARGET/p2p-store-example" -ldflags="-extldflags '$EXT_LDFLAGS'" "../example/p2p-store-example.go"
if [ $? -ne 0 ]; then
    echo "Error: Failed to build the example."
    exit 1
fi

echo "Build completed successfully."
