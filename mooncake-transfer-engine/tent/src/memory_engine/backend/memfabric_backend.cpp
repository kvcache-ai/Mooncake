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

#include "tent/memory_engine/backend/memfabric_backend.h"

namespace mooncake {
namespace tent {
namespace {

Status Unsupported() {
    return Status::MemoryEngineUnsupported(
        "MemFabric backend is not included in this change" LOC_MARK);
}

}  // namespace

Status MemfabricBackend::create(const MemoryEngineConfig&) {
    return Unsupported();
}

Status MemfabricBackend::join() { return Unsupported(); }

Status MemfabricBackend::leave() { return Unsupported(); }

void MemfabricBackend::shutdown() {}

Status MemfabricBackend::getLocalRank(uint32_t*) const { return Unsupported(); }

Status MemfabricBackend::gvaBase(MemoryEngineMemType, uint32_t, void**) const {
    return Unsupported();
}

Status MemfabricBackend::rankOfGva(const void*, uint32_t*) const {
    return Unsupported();
}

Status MemfabricBackend::allocate(uint64_t, MemoryEngineMemType, void**) {
    return Unsupported();
}

Status MemfabricBackend::free(void*) { return Unsupported(); }

Status MemfabricBackend::batchCopy(const std::vector<MemoryEngineCopyEntry>&,
                                   std::vector<int32_t>*) {
    return Unsupported();
}

}  // namespace tent
}  // namespace mooncake
