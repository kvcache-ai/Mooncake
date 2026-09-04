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

#ifndef RDMA_MSG_CHANNEL_H_
#define RDMA_MSG_CHANNEL_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transfer_metadata.h"
#include "transport/rdma_twosided/bounce_pool.h"
#include "transport/rdma_twosided/msg_header.h"

namespace mooncake {

class RdmaContext;
class RdmaTwoSidedTransport;

// Per-peer RC QP for two-sided data (SEND/RECV with TE bounce buffers).
// Always owned by a shared_ptr; RdmaContext holds only weak references.
class MsgChannel : public std::enable_shared_from_this<MsgChannel> {
   public:
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    MsgChannel(RdmaTwoSidedTransport &transport, RdmaContext &context,
               std::string peer_server_name);
    ~MsgChannel();

    MsgChannel(const MsgChannel &) = delete;
    MsgChannel &operator=(const MsgChannel &) = delete;

    int construct();
    int connectActive();
    int acceptPassive(const HandShakeDesc &peer_desc,
                      HandShakeDesc &local_desc);

    bool connected() const {
        return connected_.load(std::memory_order_acquire);
    }

    uint32_t msgQpNum() const { return qp_ ? qp_->qp_num : 0; }
    // Advertised RQ capacity (QP max / pool_max), not current active slots.
    uint16_t msgRqDepth() const { return static_cast<uint16_t>(pool_max_); }

    RdmaContext &context() { return context_; }
    const RdmaContext &context() const { return context_; }
    std::string nicPath() const;

    size_t activeSlots() const { return pool_.activeCount(); }
    size_t freeSendSlots() const { return pool_.freeSendCount(); }
    size_t allocatedSlots() const { return pool_.slotCount(); }
    size_t waitingHints() const {
        return expand_hint_.load(std::memory_order_relaxed);
    }
    void requestExpand() {
        expand_hint_.fetch_add(1, std::memory_order_relaxed);
    }

    // Background manager: grow pool then post_recv new slots. Returns 0 ok.
    int expandPool(size_t extra);
    // Background manager: shrink toward target (>= pool_base).
    size_t shrinkPoolToward(size_t target_active);

    const std::string &peerServerName() const { return peer_server_name_; }

    // Send DATA_WRITE with payload copied from src. total_chunks is how many
    // chunks the whole task is split into, so the receiver can retire its ACK
    // bookkeeping once it has seen them all. Returns 0 on post success.
    int sendDataWrite(uint64_t task_id, uint32_t slice_seq, uint64_t dest_addr,
                      const void *src, uint32_t length, uint32_t total_chunks);

    // Send READ_REQ (header only).
    int sendReadReq(uint64_t task_id, uint32_t slice_seq, uint64_t src_addr,
                    uint32_t length);

    // Send READ_RESP with payload.
    int sendReadResp(uint64_t task_id, uint32_t slice_seq, uint64_t dest_addr,
                     const void *src, uint32_t length);

    // Send READ_RESP, or hold it for replay when the bounce pool is momentarily
    // full. Never drops the response: a dropped READ_RESP would strand the
    // requester, which waits for that exact slice forever. The payload is read
    // from `addr` (a validated local managed buffer) at (re)send time. Returns
    // 0 unless the channel is down.
    int sendOrQueueReadResp(uint64_t task_id, uint32_t slice_seq, uint64_t addr,
                            uint32_t length);

    int pollCompletions(int max_entries = 16);
    void disconnect();

   private:
    int createResources();
    void destroyResources();
    int postRecv(size_t idx);
    int repostAllRecvs();
    int connectQp(const std::string &peer_gid, uint16_t peer_lid,
                  uint32_t peer_qp_num);
    void fillLocalDesc(HandShakeDesc &local_desc) const;
    int postSend(const MsgHeader &hdr, const void *payload, uint32_t length);
    void dispatchRecv(size_t idx, size_t byte_len);
    void handleSendComplete(uint64_t wr_id);
    // Replay READ_RESPs held back by a full bounce pool. Called whenever a send
    // slot frees up (SEND completion) or the pool grows (expandPool).
    void drainPendingReadResps();

    RdmaTwoSidedTransport &transport_;
    RdmaContext &context_;
    std::string peer_server_name_;

    ibv_cq *cq_ = nullptr;
    ibv_qp *qp_ = nullptr;
    BouncePool pool_;

    size_t pool_base_ = 0;
    size_t pool_max_ = 0;
    size_t max_pending_sends_ = 0;
    std::mutex send_mutex_;
    size_t pending_sends_ = 0;
    uint64_t send_wr_id_ = 0;
    // wr_id -> send slot index for completion release
    std::mutex inflight_mutex_;
    std::vector<int> inflight_slots_;  // indexed by wr_id % capacity

    // READ_RESP deferred because the bounce pool was full; replayed in order on
    // the next slot recycle or pool expansion. Guarded by resp_mutex_.
    struct PendingReadResp {
        uint64_t task_id;
        uint64_t addr;
        uint32_t length;
        uint32_t slice_seq;
    };
    std::mutex resp_mutex_;
    std::deque<PendingReadResp> pending_resps_;

    std::mutex resource_mutex_;
    std::atomic<bool> connected_{false};
    std::atomic<uint64_t> expand_hint_{0};
    bool registered_with_context_ = false;
};

}  // namespace mooncake

#endif  // RDMA_MSG_CHANNEL_H_
