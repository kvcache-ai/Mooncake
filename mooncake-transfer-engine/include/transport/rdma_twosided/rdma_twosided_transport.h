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

#ifndef RDMA_TWOSIDED_TRANSPORT_H_
#define RDMA_TWOSIDED_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_twosided/ctrl_frame.h"
#include "transport/rdma_twosided/msg_header.h"
#include "transport/rdma_twosided/sender_credit.h"

namespace mooncake {

class CtrlChannel;
class MsgChannel;

// RDMA transport with a dedicated CtrlChannel for typed notify frames and a
// per-peer MsgChannel for the two-sided (SEND/RECV + bounce) data path. The
// one-sided data path is inherited from RdmaTransport; install name is
// "rdma_twosided" (mutually exclusive with classic "rdma" in MultiTransport).
class RdmaTwoSidedTransport : public RdmaTransport {
    friend class CtrlChannel;
    friend class MsgChannel;

   public:
    using NotifyDesc = TransferMetadata::NotifyDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    RdmaTwoSidedTransport();
    ~RdmaTwoSidedTransport() override;

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "rdma_twosided"; }

    int onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                               HandShakeDesc &local_desc) override;

    // TE-managed / upper-layer buffer for the default two-sided path (no
    // ibv_reg_mr on the peer side, hence no rkey exchange).
    // registerManagedBuffer: caller owns memory (owned=false).
    void *allocateManagedBuffer(size_t length);
    int registerManagedBuffer(void *addr, size_t length);
    int releaseManagedBuffer(void *addr);

    // Splits the batch: managed source/target pairs go through the two-sided
    // MsgChannel path, everything else falls back to the inherited one-sided
    // path.
    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    int sendRdmaNotify(const std::string &peer_server_name,
                       const NotifyDesc &notify);

    void onCtrlNotifyReceived(const NotifyDesc &notify);
    void onCtrlFrameReceived(const std::string &peer_server_name,
                             const CtrlFrame &frame);
    void onMsgReceived(const std::string &peer_server_name,
                       const MsgHeader &hdr, const void *payload,
                       MsgChannel *channel = nullptr);

    uint64_t localCtrlSessionId() const { return local_ctrl_session_id_; }

    SenderCreditLedger &senderCreditLedger() { return sender_credit_; }

    uint64_t peerGrantedBounceSlots(const std::string &peer_server_name);

    // Local MsgChannel active bounce slot count for peer (0 if none).
    size_t msgBounceActiveSlots(const std::string &peer_server_name);

    int sendInitialCreditGrant(const std::string &peer_server_name);

   protected:
    // Peers select the two-sided path from this bit in our SegmentDesc, so it
    // tracks whether the msg path is actually enabled on this transport.
    bool supportsTwoSidedMsg() const override {
        return globalConfig().rdma_msg_enabled;
    }

   private:
    int onSetupCtrlChannel(const HandShakeDesc &peer_desc,
                           HandShakeDesc &local_desc);
    int onSetupMsgChannel(const HandShakeDesc &peer_desc,
                          HandShakeDesc &local_desc);
    int registerManagedBufferInternal(void *addr, size_t length, bool owned);
    std::shared_ptr<CtrlChannel> ensureCtrlChannel(
        const std::string &peer_server_name);
    std::shared_ptr<MsgChannel> ensureMsgChannel(
        const std::string &peer_server_name);
    // One MsgChannel per local RdmaContext (NIC rail). Partial success OK if
    // at least one rail connects. Empty vector on total failure.
    std::vector<std::shared_ptr<MsgChannel>> ensureMsgRails(
        const std::string &peer_server_name);
    std::shared_ptr<RdmaContext> selectMsgContext(
        const HandShakeDesc &peer_desc, size_t existing_rail_count);

    // RAII publisher for one connect attempt. On construction it installs the
    // placeholder entry and marks the peer as having a connect in flight; on
    // destruction it always retracts the marker and wakes waiters, and drops
    // the placeholder unless markSuccess() was called. Callers must hold
    // ctrl_mutex_ for the whole lifetime of the scope, so a blocking handshake
    // has to be wrapped in an UnlockGuard rather than releasing the lock by
    // hand.
    class ConnectScope {
       public:
        ConnectScope(RdmaTwoSidedTransport &transport, std::string peer,
                     std::shared_ptr<CtrlChannel> channel);
        ~ConnectScope();

        ConnectScope(const ConnectScope &) = delete;
        ConnectScope &operator=(const ConnectScope &) = delete;

        void markSuccess() { success_ = true; }

       private:
        RdmaTwoSidedTransport &transport_;
        std::string peer_;
        std::shared_ptr<CtrlChannel> channel_;
        bool success_ = false;
    };

    void startCtrlWorker();
    void stopCtrlWorker();
    void ctrlWorkerLoop();

    bool shouldUseTwoSided(const TransferRequest &req);
    bool isLocalManaged(uint64_t addr, size_t length) const;
    bool isRemoteTwoSided(SegmentID target_id, uint64_t offset,
                          size_t length) const;
    Status submitTwoSidedTasks(const std::vector<TransferTask *> &tasks);
    int dispatchTwoSidedTask(TransferTask *task);
    void redispatchWaitingTasks();
    void completeTwoSidedAck(uint64_t task_id, uint64_t acked_bytes);
    int sendDataAck(const std::string &peer, uint64_t task_id,
                    uint64_t acked_bytes);
    bool validateLocalManagedDest(uint64_t dest_addr, uint32_t length) const;

    struct PeerCtrlState {
        uint64_t peer_session = 0;
        uint64_t epoch = 1;
        uint64_t next_grant_seq = 1;
        uint32_t peer_bounce_slots = 0;
        uint32_t peer_bounce_slot_size = 0;
        bool session_open_received = false;
        bool initial_grant_sent = false;
        bool grant_pending = false;
        // Peer asked us to expand bounce capacity (CREDIT_REQUEST).
        bool expand_requested = false;
        uint64_t last_credit_request_ms = 0;
        // Last bounce slots advertised in CREDIT_GRANT to this peer.
        uint64_t granted_bounce_slots = 0;
        uint64_t high_watermark_since_ms = 0;
    };

    struct ManagedBuffer {
        void *addr = nullptr;
        size_t length = 0;
        bool owned = false;  // allocated by TE
    };

    struct TwoSidedTaskState {
        TransferTask *task = nullptr;
        uint64_t task_id = 0;
        uint64_t total_bytes = 0;
        uint64_t acked_bytes = 0;
        std::string peer;
        uint64_t peer_session = 0;
        // Session generation the reservation was made under; the ledger
        // rejects a rollback carrying a stale epoch.
        uint64_t peer_epoch = 1;
        bool waiting_credit = false;
        size_t slices_posted = 0;
        bool credit_reserved = false;
        uint64_t reserved_slots = 0;
        uint64_t reserved_bytes = 0;
        // Copied from TransferRequest: submitTransfer({req}) leaves
        // task->request dangling after the temporary vector is destroyed.
        TransferRequest::OpCode opcode = TransferRequest::WRITE;
        void *local_buf = nullptr;
    };

    int sendCreditGrant(const std::string &peer_server_name, uint64_t epoch,
                        uint64_t bounce_slots = 0);
    void handleSessionOpen(const std::string &peer_server_name,
                           const CtrlFrame &frame);
    void handleCreditGrant(const std::string &peer_server_name,
                           const CtrlFrame &frame);
    void handleCreditRequest(const std::string &peer_server_name,
                             const CtrlFrame &frame);
    void handleDataAck(const std::string &peer_server_name,
                       const CtrlFrame &frame);

    void startBounceManager();
    void stopBounceManager();
    void bounceManagerLoop();
    void manageBouncePoolsTick();
    size_t waitingTaskCount();
    int sendCreditRequest(const std::string &peer_server_name);

    std::mutex ctrl_mutex_;
    std::condition_variable ctrl_cv_;
    std::unordered_map<std::string, std::shared_ptr<CtrlChannel>>
        ctrl_channels_;
    // Ordered MsgChannel rails per peer (one per local NIC / RdmaContext).
    std::unordered_map<std::string, std::vector<std::shared_ptr<MsgChannel>>>
        msg_channels_;
    std::unordered_map<std::string, PeerCtrlState> peer_ctrl_state_;
    // Peers with a connect handshake in flight. Only such peers are worth
    // waiting for: an entry in ctrl_channels_ that is not connected and not
    // listed here is a dead channel, which the next caller reclaims instead of
    // waiting for a wakeup that never comes. Refcounted because an active and
    // a passive connect to the same peer can overlap.
    std::unordered_map<std::string, int> ctrl_connecting_;
    // Set once by stopCtrlWorker() to release waiters during shutdown.
    // Guarded by ctrl_mutex_.
    bool ctrl_stopping_ = false;
    std::thread ctrl_worker_;
    std::atomic<bool> ctrl_worker_running_{false};
    std::thread bounce_manager_;
    std::atomic<bool> bounce_manager_running_{false};
    uint64_t local_ctrl_session_id_ = 0;
    SenderCreditLedger sender_credit_;

    mutable std::mutex managed_mutex_;
    std::unordered_map<uint64_t, ManagedBuffer> managed_buffers_;  // addr ->

    std::mutex twosided_mutex_;
    std::unordered_map<uint64_t, TwoSidedTaskState> twosided_tasks_;
    std::deque<TransferTask *> waiting_tasks_;
    std::atomic<uint64_t> next_task_id_{1};
    // Tasks dispatched on MsgChannel awaiting DATA_ACK. Non-zero keeps the
    // ctrl worker spinning so ACKs are drained without the idle sleep.
    std::atomic<size_t> twosided_inflight_{0};
    // Wakes the ctrl worker when a task is dispatched: the worker may be in
    // its idle sleep, which would otherwise delay the DATA_ACK drain by the
    // whole sleep period.
    std::mutex ctrl_idle_mutex_;
    std::condition_variable ctrl_idle_cv_;
    // Receiver-side cumulative bytes per remote task_id (per-transport).
    std::mutex recv_ack_mutex_;
    std::unordered_map<uint64_t, uint64_t> recv_acked_bytes_;
};

}  // namespace mooncake

#endif  // RDMA_TWOSIDED_TRANSPORT_H_
