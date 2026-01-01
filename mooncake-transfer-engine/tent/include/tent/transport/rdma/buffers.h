// Copyright 2025 KVCache.AI
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

#ifndef TENT_BUFFERS_H
#define TENT_BUFFERS_H

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "tent/runtime/transport.h"
// #include "tent/common/concurrent/thread_pool.h"

namespace mooncake {
namespace tent {
class RdmaContext;

struct AddressRange {
    void *addr;
    size_t length;

    AddressRange(void *addr = nullptr, size_t length = 0)
        : addr(addr), length(length) {}

    bool operator<(const AddressRange &rhs) const {
        if ((char *)addr < (char *)rhs.addr) return true;
        if ((char *)addr > (char *)rhs.addr) return false;
        return length < rhs.length;
    }

    bool operator==(const AddressRange &rhs) const {
        return addr == rhs.addr && length == rhs.length;
    }

    bool empty() const { return length == 0; }

    bool contains(const AddressRange &rhs) const {
        return ((char *)addr <= (char *)rhs.addr) &&
               ((char *)addr + length >= (char *)rhs.addr + rhs.length);
    }

    AddressRange intersect(const AddressRange &rhs) const {
        char *a_start = static_cast<char *>(addr);
        char *a_end = a_start + length;
        char *b_start = static_cast<char *>(rhs.addr);
        char *b_end = b_start + rhs.length;

        char *inter_start = std::max(a_start, b_start);
        char *inter_end = std::min(a_end, b_end);

        if (inter_start < inter_end) {
            return AddressRange(static_cast<void *>(inter_start),
                                inter_end - inter_start);
        } else {
            return AddressRange(nullptr, 0);
        }
    }
};

struct BufferQueryResult {
    void *addr;
    size_t length;
    uint32_t lkey;
    uint32_t rkey;
    int device_id;
};

class LocalBufferManager {
   public:
    LocalBufferManager();

    ~LocalBufferManager();

    void setTopology(std::shared_ptr<Topology> &topology) {
        topology_ = topology;
        context_list_.resize(topology->getNicCount(), nullptr);
    }

    Status addBuffer(BufferDesc &desc, const MemoryOptions &options);

    Status addBuffer(std::vector<BufferDesc> &desc_list,
                     const MemoryOptions &options);

    Status removeBuffer(BufferDesc &desc);

    Status addDevice(RdmaContext *context);

    Status removeDevice(RdmaContext *context, bool do_unreg = true);

    Status clear();

   private:
    struct BufferEntryForRdma {
        MemoryOptions options;
        std::unordered_map<RdmaContext *, void *> mem_reg_map;
    };

   private:
    RWSpinlock lock_;
    std::vector<RdmaContext *> context_list_;
    std::map<AddressRange, BufferEntryForRdma> buffer_list_;
    std::shared_ptr<Topology> topology_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_BUFFERS_H