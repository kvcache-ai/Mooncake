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

#ifndef RDMA_CTRL_CHANNEL_H_
#define RDMA_CTRL_CHANNEL_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transfer_metadata.h"
#include "transport/rdma_twosided/ctrl_frame.h"

namespace mooncake {

class RdmaContext;
class RdmaTwoSidedTransport;

// Per-peer RDMA control channel: one RC QP dedicated to two-sided SEND/RECV
// typed CtrlFrames. Owned by RdmaTwoSidedTransport; not part of EndpointStore.
class CtrlChannel {
   public:
    using NotifyDesc = TransferMetadata::NotifyDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    CtrlChannel(RdmaTwoSidedTransport &transport, RdmaContext &context,
                std::string peer_server_name);
    ~CtrlChannel();

    CtrlChannel(const CtrlChannel &) = delete;
    CtrlChannel &operator=(const CtrlChannel &) = delete;

    // Allocate QP/CQ/buffers and move QP to INIT. Does not connect.
    int construct();

    // Active: exchange ctrl handshake and bring QP to RTS.
    int connectActive();

    // Passive: complete ctrl handshake against peer_desc; fill local_desc.
    int acceptPassive(const HandShakeDesc &peer_desc,
                      HandShakeDesc &local_desc);

    bool connected() const {
        return connected_.load(std::memory_order_acquire);
    }

    uint32_t notifyQpNum() const { return qp_ ? qp_->qp_num : 0; }
    uint16_t notifyRqDepth() const {
        return static_cast<uint16_t>(recv_count_);
    }

    const std::string &peerServerName() const { return peer_server_name_; }
    RdmaContext &context() const { return context_; }

    // Async: post typed CtrlFrame SEND; returns 0 on success.
    int sendCtrlFrame(const CtrlFrame &frame);

    // Compatibility wrapper: encode NotifyDesc as NOTIFY_COMPAT frame.
    int sendNotify(const NotifyDesc &notify);

    // Poll notify CQ; dispatch inbound frames via transport callbacks.
    // Returns number of WC processed.
    int pollCompletions(int max_entries = 16);

    void disconnect();

   private:
    int createResources();
    void destroyResources();
    int postRecv(size_t idx);
    int repostAllRecvs();
    int connectQp(const std::string &peer_gid, uint16_t peer_lid,
                  uint32_t peer_qp_num);
    void dispatchRecvPayload(const uint8_t *data, size_t byte_len);
    void handleSendComplete();
    void handleRecvComplete(const ibv_wc &wc);
    // Shared CQ drain for worker poll and send-path opportunistic poll.
    // Returns successfully handled SEND/RECV count, or <0 on poll failure.
    int drainCompletions(int max_entries, bool log_poll_error);
    // Drain SEND (and any stolen RECV) WCs without blocking on send slots.
    int pollSendCompletions(int max_entries = 16);
    void fillLocalDesc(HandShakeDesc &local_desc) const;
    int postSessionOpen();

    RdmaTwoSidedTransport &transport_;
    RdmaContext &context_;
    std::string peer_server_name_;

    ibv_cq *cq_ = nullptr;
    ibv_qp *qp_ = nullptr;
    ibv_mr *send_mr_ = nullptr;
    std::vector<ibv_mr *> recv_mrs_;
    std::vector<char> send_buffer_;
    std::vector<std::vector<char>> recv_buffers_;

    size_t recv_count_ = 0;
    size_t buffer_size_ = 0;
    size_t max_pending_sends_ = 0;

    std::mutex send_mutex_;
    std::condition_variable send_cv_;
    size_t pending_sends_ = 0;
    uint64_t send_wr_id_ = 0;
    uint64_t next_frame_seq_ = 1;

    std::mutex resource_mutex_;
    std::atomic<bool> connected_{false};
};

}  // namespace mooncake

#endif  // RDMA_CTRL_CHANNEL_H_
