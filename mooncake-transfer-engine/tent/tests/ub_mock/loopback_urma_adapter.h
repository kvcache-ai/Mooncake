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

#ifndef TENT_TESTS_UB_MOCK_LOOPBACK_URMA_ADAPTER_H_
#define TENT_TESTS_UB_MOCK_LOOPBACK_URMA_ADAPTER_H_

#include <cstdint>
#include <memory>

#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace mock {

// Test-only URMA adapter for builds without UB hardware. It reports one fake
// UB device and carries READ/WRITE work requests with memcpy, so it only works
// when both ends live in the same process (e.g. in-process Store tests).
//
// It is compiled only with -DUSE_UB_MOCK=ON. In such a build the default
// adapter factory returns it when MC_TENT_UB_MOCK=1 is set; otherwise the
// factory behaves as in a normal build.
std::shared_ptr<UrmaAdapter> createLoopbackUrmaAdapter();

// Name of the environment variable that turns the loopback adapter on.
inline constexpr const char* kLoopbackEnv = "MC_TENT_UB_MOCK";

// True when kLoopbackEnv is set to a non-empty value other than "0".
bool loopbackRequested();

// Process-wide counters across all loopback adapter instances, so tests can
// check that data really went through the UB transport.
struct LoopbackStats {
    uint64_t read_requests = 0;
    uint64_t write_requests = 0;
    uint64_t bytes = 0;
};
LoopbackStats loopbackStats();
void resetLoopbackStats();

}  // namespace mock
}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TESTS_UB_MOCK_LOOPBACK_URMA_ADAPTER_H_
