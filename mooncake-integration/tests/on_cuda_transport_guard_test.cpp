// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <string>

#include "transfer_engine/transfer_engine_py.h"

int main() {
    int failures = 0;
    const auto expect = [&failures](bool condition, const char* message) {
        if (!condition) {
            std::cerr << message << '\n';
            ++failures;
        }
    };

    expect(isCudaHostFuncUnsupportedProtocol("nvlink_intra"),
           "nvlink_intra must be rejected before cudaLaunchHostFunc");
    for (const std::string& protocol : {"", "tcp", "rdma", "shm"}) {
        expect(!isCudaHostFuncUnsupportedProtocol(protocol),
               "non-NVLink transport must remain available");
    }

    return failures == 0 ? 0 : 1;
}
