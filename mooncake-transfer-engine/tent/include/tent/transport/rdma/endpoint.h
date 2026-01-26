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

#ifndef TENT_ENDPOINT_H
#define TENT_ENDPOINT_H

#include <queue>

#include "context.h"

namespace mooncake {
namespace tent {
class RdmaEndPoint {
    struct WrDepthBlock {
        volatile int value;
        uint64_t padding[7];
    };

    struct BoundedSliceQueue {
        size_t head, tail, capacity, count;
        std::vector<RdmaSlice *> entries;
        std::unordered_set<RdmaSlice *> slice_set;

        BoundedSliceQueue(size_t num_wr)
            : head(0),
              tail(0),
              capacity(num_wr * 2),
              count(0),
              entries(capacity) {
            assert(capacity > 0);
        }

        ~BoundedSliceQueue() {}

        bool empty() const { return count == 0; }

        bool full() const { return count == capacity; }

        void push(RdmaSlice *slice) {
            if (count == capacity) throw std::runtime_error("queue overflow");
            entries[tail] = slice;
            tail = (tail + 1) % capacity;
            slice_set.insert(slice);
            count++;
        }

        RdmaSlice *peek() {
            if (count == 0) return nullptr;
            return entries[head];
        }

        RdmaSlice *pop() {
            if (count == 0) return nullptr;
            RdmaSlice *cur = entries[head];
            head = (head + 1) % capacity;
            slice_set.erase(cur);
            count--;
            return cur;
        }

        bool contains(RdmaSlice *slice) { return slice_set.count(slice); }
    };

   public:
    RdmaEndPoint();

    ~RdmaEndPoint();

   public:
    enum EndPointStatus { EP_UNINIT, EP_HANDSHAKING, EP_READY, EP_RESET };

    int reset();

    int construct(RdmaContext *context, EndPointParams *params,
                  const std::string &endpoint_name,
                  std::atomic<int> *endpoints_count = nullptr);

    int deconstruct();

    Status connect(const std::string &peer_server_name,
                   const std::string &peer_nic_name);

    Status accept(const BootstrapDesc &peer_desc, BootstrapDesc &local_desc);

    EndPointStatus status() const { return status_; }

    std::vector<uint32_t> qpNum();

    int getInflightSlices() const;

    RdmaContext &context() const { return *context_; }

    std::string name() const { return endpoint_name_; }

    // Notification QP operations
    uint32_t notifyQpNum() const { return notify_qp_ ? notify_qp_->qp_num : 0; }

    bool sendNotification(const std::string& name, const std::string& msg);

    bool receiveNotification(std::string& name, std::string& msg);

    // Process RECV completion: parse message and add to internal queue
    bool handleNotifyRecv(size_t buffer_idx, size_t byte_len);

   public:
    struct Request {
        struct SglEntry {
            uint32_t length;
            uint64_t addr;
            uint32_t lkey;
        };
        ibv_wr_opcode opcode;
        std::vector<SglEntry> local;
        uint64_t remote_addr;
        uint32_t remote_key;
        uint32_t imm_data;
        void *user_context;
        bool failed;
    };

    int resetUnlocked();

    int submitSlices(std::vector<RdmaSlice *> &slice_list, int qp_index);

    int submitRecvImmDataRequest(int qp_index, uint64_t id);

    size_t acknowledge(RdmaSlice *slice, TransferStatusEnum status);

    volatile int *getQuotaCounter(int qp_index) const {
        return &wr_depth_list_[qp_index].value;
    }

   private:
    int setupAllQPs(const std::string &peer_gid, uint16_t peer_lid,
                    std::vector<uint32_t> peer_qp_num_list,
                    std::string *reply_msg = nullptr);

    int setupOneQP(int qp_index, const std::string &peer_gid, uint16_t peer_lid,
                   uint32_t peer_qp_num, std::string *reply_msg = nullptr);

    bool reserveQuota(int qp_index, int num_entries);

    void cancelQuota(int qp_index, int num_entries);

   private:
    void resetInflightSlices();

    void postNotifyRecv(size_t idx);

   private:
    std::atomic<EndPointStatus> status_;
    RdmaContext *context_;
    EndPointParams *params_;
    std::string endpoint_name_;

    std::vector<ibv_qp *> qp_list_;
    std::vector<BoundedSliceQueue> slice_queue_;
    WrDepthBlock *wr_depth_list_;
    volatile int inflight_slices_;
    uint32_t padding_[7];
    RWSpinlock lock_;

    std::string peer_server_name_;
    std::string peer_nic_name_;
    std::atomic<int> *endpoints_count_;

    // Notification QP (one per endpoint for control plane operations)
    ibv_qp *notify_qp_ = nullptr;

    // Notification buffers
    static constexpr size_t kNotifyBufferSize = 4096;
    static constexpr size_t kNotifyNumBuffers = 64;
    std::vector<std::vector<char>> notify_recv_buffers_;
    std::vector<std::vector<char>> notify_pending_sends_;
    std::mutex notify_recv_mutex_;
    std::queue<std::pair<std::string, std::string>> notify_received_messages_;
    bool notify_connected_ = false;
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_ENDPOINT_H