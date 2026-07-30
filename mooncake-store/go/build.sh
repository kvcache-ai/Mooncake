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

set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 BUILD_DIR TARGET_PATH [USE_ETCD=OFF] [USE_REDIS=OFF] [USE_HTTP=OFF] [USE_ETCD_LEGACY=OFF]"
    exit 1
fi

BUILD_DIR=$(cd "$1" && pwd)
TARGET=$(cd "$2" && pwd)
# Legacy feature arguments are accepted for command-line compatibility. Their
# libraries are now owned by libmooncake_store.so.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# CGo compiler flags — point at the store_c.h header
CGO_CFLAGS="-I${REPO_ROOT}/mooncake-store/include"

# CGo links only the project-level Store SDK. Its portable and hardware/system
# dependencies are owned by libmooncake_store.so rather than duplicated here.
CGO_LDFLAGS="-L${BUILD_DIR}/mooncake-store/src"
CGO_LDFLAGS+=" -Wl,-rpath,${BUILD_DIR}/mooncake-store/src -lmooncake_store"

export CGO_CFLAGS
export CGO_LDFLAGS
export CGO_ENABLED=1

cd "$SCRIPT_DIR"

echo "Building Mooncake Store Go example..."
echo "  CGO_CFLAGS=$CGO_CFLAGS"
echo "  CGO_LDFLAGS=$CGO_LDFLAGS"

go build -o "${TARGET}/mooncake-store-go-example" ./examples/basic/

echo "Mooncake Store Go bindings: build successful"
