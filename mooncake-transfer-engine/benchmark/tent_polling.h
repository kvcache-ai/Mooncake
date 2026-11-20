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

#ifndef MOONCAKE_BENCHMARK_TENT_POLLING_H
#define MOONCAKE_BENCHMARK_TENT_POLLING_H

#include <cstdint>

namespace mooncake::tent {

// Bound lock traffic for concurrent, large host-RDMA batches. Small batches,
// low concurrency and device-memory transports retain their hot wait path.
inline bool useTentPollingBackoff(bool host_rdma, int threads,
                                  uint64_t block_size, uint64_t batch_size) {
    constexpr uint64_t kMinBytes = 8ull * 1024 * 1024;
    if (!host_rdma || threads < 32 || !batch_size) return false;
    // Compare the total payload without multiplying untrusted sizes.
    return block_size >= kMinBytes / batch_size + (kMinBytes % batch_size != 0);
}

}  // namespace mooncake::tent

#endif  // MOONCAKE_BENCHMARK_TENT_POLLING_H
