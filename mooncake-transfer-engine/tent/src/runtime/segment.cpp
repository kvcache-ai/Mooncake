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

#include "tent/runtime/segment_registry.h"

#include <cassert>
#include <set>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {
BufferDesc *SegmentDesc::findBuffer(uint64_t base, uint64_t length) {
    if (type != SegmentType::Memory) return nullptr;
    auto &cast = std::get<MemorySegmentDesc>(detail);
    for (auto &entry : cast.buffers) {
        // Check for integer overflow in base + length
        if (base + length < base) return nullptr;
        if (entry.addr + entry.length < entry.addr) continue;

        if (entry.addr <= base && base + length <= entry.addr + entry.length)
            return &entry;
    }
    return nullptr;
}

DeviceDesc *SegmentDesc::findDevice(const std::string &name) {
    if (type != SegmentType::Memory) return nullptr;
    auto &cast = std::get<MemorySegmentDesc>(detail);
    for (auto &entry : cast.devices) {
        if (entry.name == name) return &entry;
    }
    return nullptr;
}
}  // namespace tent
}  // namespace mooncake
