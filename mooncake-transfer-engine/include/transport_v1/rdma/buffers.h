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

#ifndef RDMA_BUFFERS_H
#define RDMA_BUFFERS_H

#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "transport_v1/transport.h"

namespace mooncake {
namespace v1 {
class RdmaContext;

struct AddressRange {
    void *addr;
    size_t length;

    AddressRange(void *addr = nullptr, size_t length = 0)
        : addr(addr), length(length) {}

    bool operator<(const AddressRange &rhs) const {
        return (char *)addr < (char *)rhs.addr;
    }

    bool operator==(const AddressRange &rhs) const {
        return addr == rhs.addr && length == rhs.length;
    }

    bool empty() const { return length == 0; }

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

class LocalBufferSet {
   public:
    LocalBufferSet();

    ~LocalBufferSet();

    int addBuffer(const Transport::BufferEntry &entry);

    int removeBuffer(const AddressRange &range);

    int removeBufferLegacy(void *addr);

    struct Result {
        void *addr;
        size_t length;
        uint32_t lkey, rkey;
    };

    int findBuffer(const AddressRange &range, RdmaContext *context,
                   Transport::BufferVisibility visibility,
                   std::vector<Result> &result);

    int findBufferLegacy(void *addr, RdmaContext *context, uint32_t &lkey,
                         uint32_t &rkey);

    int addDevice(RdmaContext *context);

    int removeDevice(RdmaContext *context);

    int clear();

   private:
    struct BufferItem {
        int ref_cnt;
        Transport::BufferEntry entry;
        std::unordered_map<RdmaContext *, void *> mem_reg_map;
    };

    int registerMemReg(BufferItem &item);

    int unregisterMemReg(BufferItem &item,
                         RdmaContext *context_filter = nullptr);

   private:
    RWSpinlock lock_;
    std::map<AddressRange, BufferItem>
        buffer_lists_[3];  // each represents one visibility level
    std::unordered_set<RdmaContext *> context_list_;
};

class PeerBuffers {
   public:
    PeerBuffers();

    ~PeerBuffers();

    int reload(const std::shared_ptr<SegmentDesc> &segment_desc);

    struct Result {
        void *addr;
        size_t length;
        uint32_t lkey, rkey;
    };

    int query(const AddressRange &target_range, int device_id,
              std::vector<Result> &result);

   private:
    RWSpinlock lock_;
    std::shared_ptr<SegmentDesc> segment_desc_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_BUFFERS_H