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

#include <memory>

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

class LocalBuffers {
   public:
    LocalBuffers(std::shared_ptr<RdmaContext> &context);

    ~LocalBuffers();

    int add(const Transport::BufferEntry &buffer, uint32_t &lkey,
            uint32_t &rkey);

    int remove(const AddressRange &range);

    int remove(void *addr);  // legacy remove

    struct Result {
        void *addr;
        size_t length;
        uint32_t lkey, rkey;
    };

    int query(const AddressRange &target_range, std::vector<Result> &result);

   private:
    struct BufferItem {
        int mem_reg_index;
        uint32_t lkey, rkey;
    };

    std::shared_ptr<RdmaContext> context_;
    RWSpinlock lock_;
    std::map<AddressRange, BufferItem> sorted_buffers_;
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