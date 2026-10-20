// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <new>

#include "tent/transport/rdma/endpoint.h"

TEST(RdmaConsumerAbiTest, EndpointConstructionRespectsCallerStorage) {
    using mooncake::tent::RdmaEndPoint;
    constexpr size_t kGuardBytes = 256;
    constexpr std::byte kGuard{0xA5};
    alignas(RdmaEndPoint)
        std::array<std::byte, sizeof(RdmaEndPoint) + kGuardBytes>
            storage;
    storage.fill(kGuard);

    auto* endpoint = ::new (storage.data()) RdmaEndPoint;
    const auto guard = storage.begin() + sizeof(RdmaEndPoint);
    EXPECT_TRUE(std::all_of(guard, storage.end(), [](std::byte value) {
        return value == kGuard;
    })) << "Library constructor wrote beyond the caller's object layout";
    endpoint->~RdmaEndPoint();
}
