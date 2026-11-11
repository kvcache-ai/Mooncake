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

#ifndef TENT_MEMORY_ENGINE_MEMFABRIC_BACKEND_H
#define TENT_MEMORY_ENGINE_MEMFABRIC_BACKEND_H

#include <cstdint>
#include <vector>

#include "tent/memory_engine.h"

namespace mooncake {
namespace tent {

// Placeholder backend for the API-only change. The real MemFabric
// implementation is added in a follow-up.
class MemfabricBackend {
   public:
    MemfabricBackend() = default;
    ~MemfabricBackend() = default;

    MemfabricBackend(const MemfabricBackend&) = delete;
    MemfabricBackend& operator=(const MemfabricBackend&) = delete;

    Status create(const MemoryEngineConfig& config);
    Status join();
    Status leave();
    void shutdown();

    Status getLocalRank(uint32_t* rank_out) const;
    Status gvaBase(MemoryEngineMemType type, uint32_t rank,
                   void** gva_out) const;
    Status rankOfGva(const void* gva, uint32_t* rank_out) const;

    Status allocate(uint64_t size, MemoryEngineMemType type, void** gva_out);
    Status free(void* gva);
    Status batchCopy(const std::vector<MemoryEngineCopyEntry>& entries,
                     std::vector<int32_t>* results);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_MEMORY_ENGINE_MEMFABRIC_BACKEND_H
