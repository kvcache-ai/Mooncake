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
#include <unordered_map>
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

class AddressRangeManager {
   private:
    struct AddressRangeRC {
        void *addr;
        size_t length;
        int ref_cnt;
        AddressRangeRC(void *addr = nullptr, size_t length = 0, int ref_cnt = 0)
            : addr(addr), length(length), ref_cnt(ref_cnt) {}
    };

    std::vector<AddressRangeRC> addr_list;

    // Helper function to find the position where a range would be inserted
    std::vector<AddressRangeRC>::iterator findInsertPosition(
        const AddressRange &range);

    // Helper function to check if two ranges overlap
    bool rangesOverlap(const AddressRangeRC &a, const AddressRange &b);

   public:
    void add(const AddressRange &range, std::vector<AddressRange> &reg_parts);

    void remove(const AddressRange &range,
                std::vector<AddressRange> &dereg_parts);
};

class LocalBufferManager {
   public:
    LocalBufferManager();

    ~LocalBufferManager();

    void setTopology(std::shared_ptr<Topology> &topology) {
        topology_ = topology;
    }

    int addBuffer(const Transport::BufferEntry &buffer_entry);

    int removeBuffer(const AddressRange &range);

    struct Result {
        void *addr;
        size_t length;
        uint32_t lkey, rkey;
        int device_id;
    };

    int addDevice(RdmaContext *context);

    int removeDevice(RdmaContext *context);

    int clear();

    int fillBufferDesc(std::shared_ptr<SegmentDesc> &segment_desc);

    int query(const AddressRange &range, std::vector<Result> &result,
              int retry_count = 0);

    const std::string deviceName(int id);

   private:
    struct BufferEntry {
        Transport::BufferEntry entry;
        std::unordered_map<RdmaContext *, void *> mem_reg_map;
    };

   private:
    RWSpinlock lock_;
    AddressRangeManager manager_;
    std::vector<RdmaContext *> context_list_;
    std::map<AddressRange, BufferEntry> buffer_list_;
    std::shared_ptr<Topology> topology_;
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
        int device_id;
    };

    bool valid() const { return segment_desc_ != nullptr; }

    int query(const AddressRange &range, std::vector<Result> &result,
              int retry_count = 0);

    const std::string &segmentName() const { return segment_desc_->name; }

    const std::string &deviceName(int id);

   private:
    RWSpinlock lock_;
    std::shared_ptr<SegmentDesc> segment_desc_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_BUFFERS_H