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

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include <mutex>
#include <queue>
#include <unordered_set>

#include "rdma_context.h"

namespace mooncake {

// RdmaEndPoint represents all QP connections between the local NIC1 (identified
// by its RdmaContext) and the remote NIC2 (identified by peer_nic_path).
// 1. After construct, resources are allocated without specifying the peers.
// 2. Handshake information needs to be exchanged with remote RdmaEndPoint.
//    - Local side calls the setupConnectionsByActive() function, passing in the
//    peer_nic_path of the remote side
//      peer_nic_path := peer_server_name@nic_name, e.g. 192.168.3.76@mlx5_3,
//      which can be obtained from RdmaContext::nicPath() on the remote side
//    - Remote side calls the setupConnectionsByPassive() function in its RPC
//    service.
//   After above steps, the RdmaEndPoint state is set to CONNECTED
//
// If the user initiates a disconnect() call or an error is detected after the
// endpoint has connected, the endpoint is marked for destruction and cannot be
// reused. A new endpoint must be created for future connections.
class RdmaEndPoint {
   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTING,
        CONNECTED,
        DESTROYING,
        DESTROYED,
    };

   public:
    RdmaEndPoint(RdmaContext &context);

    ~RdmaEndPoint();

    int construct(ibv_cq *cq, size_t num_qp_list = 2, size_t max_sge = 4,
                  size_t max_wr = 256, size_t max_inline = 64);

   private:
    int deconstruct();
    int deconstructLocked();

   public:
    void setPeerNicPath(const std::string &peer_nic_path);

    int setupConnectionsByActive();

    int setupConnectionsByActive(const std::string &peer_nic_path) {
        setPeerNicPath(peer_nic_path);
        return setupConnectionsByActive();
    }

    using HandShakeDesc = TransferMetadata::HandShakeDesc;
    int setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                  HandShakeDesc &local_desc);

    struct CompletionToken {
        std::atomic<RdmaEndPoint *> endpoint{nullptr};
        std::atomic<Transport::Slice *> slice{nullptr};
        std::atomic<volatile int *> qp_depth{nullptr};
        std::atomic<bool> fallback_completed{false};
    };

    bool active() const { return active_.load(std::memory_order_acquire); }

    void set_active(bool flag) {
        RWSpinlock::WriteGuard guard(lock_);
        active_.store(flag, std::memory_order_release);
        if (!flag)
            inactive_time_.store(getCurrentTimeInNano(),
                                 std::memory_order_release);
    }

    double inactiveTime() {
        if (active_.load(std::memory_order_acquire)) return 0.0;
        return (getCurrentTimeInNano() -
                inactive_time_.load(std::memory_order_acquire)) /
               1000000000.0;
    }

   public:
    bool connected() const {
        return status_.load(std::memory_order_relaxed) == CONNECTED;
    }

    Status status() const { return status_.load(std::memory_order_relaxed); }

    // Interrupts the connection. Endpoints have unidirectional lifecycle: once
    // a connected endpoint is disconnected, it is marked for destruction and
    // cannot be reused.
    void disconnect();

    // Destroy QPs before CQs (in RDMA Context)
    int destroyQP();

    // Two-phase QP destruction to avoid use-after-free in concurrent
    // submitPostSend. Phase 1 (beginDestroy): sets active_=false and
    // status_=DESTROYING, transitions QPs to ERR state so hardware flushes
    // inflight WRs to CQ. Does not block. Phase 2 (finishDestroy): called
    // after all outstanding WRs have been drained (wr_depth_list_ all zero),
    // actually destroys QPs and frees resources. Returns true if destruction
    // is complete, false if outstanding WRs remain.
    void beginDestroy();
    bool finishDestroy();

    // Claims completion ownership for polled WRs. Each result is false if the
    // corresponding WR was already completed by the destruction fallback.
    std::vector<uint8_t> claimPendingSlices(
        const std::vector<CompletionToken *> &tokens);

   private:
    // Same as beginDestroy but must be called while already holding lock_.
    void beginDestroyNoLock();

    // Resets only an unestablished endpoint during handshake retry. Connected
    // endpoints must use beginDestroyNoLock() and be replaced instead.
    int disconnectUnlocked();

    void addPendingTokens(const std::vector<CompletionToken *> &tokens);
    void removePendingTokens(const std::vector<CompletionToken *> &tokens,
                             size_t start, size_t count);
    std::vector<CompletionToken *> claimAllPendingTokens();
    void completePendingSlicesAsFailed(
        const std::vector<CompletionToken *> &pending_tokens);

    uint64_t beginHandshakeAttempt();
    bool verifyHandshakeVersion(uint64_t response_version) const;
    bool validateHandshakeTimestamp(uint64_t timestamp) const;
    void recordSuccessfulHandshake(const std::vector<uint32_t> &peer_qp_num,
                                   uint64_t version);

    // Resets a handshake attempt, or destroys the endpoint if it has already
    // connected. This is mainly used in setupConnectionsByActive/Passive.
    int resetConnection(const std::string &reason);

   public:
    const std::string toString() const;

   public:
    // Submit some work requests to HW
    // Submitted tasks (success/failed) are removed in slice_list
    // Failed tasks (which must be submitted) are inserted in failed_slice_list
    int submitPostSend(std::vector<Transport::Slice *> &slice_list,
                       std::vector<Transport::Slice *> &failed_slice_list);

    // Get the number of QPs in this endpoint
    size_t getQPNumber() const;

   private:
    enum class SetupConnectionFailureStage {
        kNone,
        kPeerValidation,
        kReset,
        kInit,
        kRtr,
        kRts,
    };

    struct SetupConnectionFailureInfo {
        SetupConnectionFailureStage stage = SetupConnectionFailureStage::kNone;
        int sys_errno = 0;
    };

    std::vector<uint32_t> qpNum() const;

    int doSetupConnection(const std::string &peer_gid, uint16_t peer_lid,
                          std::vector<uint32_t> peer_qp_num_list,
                          std::string *reply_msg = nullptr,
                          SetupConnectionFailureInfo *failure_info = nullptr);

    int doSetupConnection(int qp_index, const ibv_gid &peer_gid,
                          uint16_t peer_lid, uint32_t peer_qp_num,
                          int local_gid_index, std::string *reply_msg = nullptr,
                          SetupConnectionFailureInfo *failure_info = nullptr);

   private:
    static constexpr uint64_t kWaitExistingHandshakeTimeoutNano =
        10 * 1000000000ull;  // 10 seconds
    static constexpr uint32_t kWaitExistingHandshakeSpinCount = 500;
    static constexpr uint32_t kWaitExistingHandshakeInitialSleepUs = 50;
    static constexpr uint32_t kWaitExistingHandshakeMaxSleepUs = 2000;

    // Maximum time (in seconds) to wait for outstanding WRs to drain in
    // finishDestroy before forcing QP destruction. This guards against
    // ibv_modify_qp-to-ERR failures that prevent WR flushing.
    static constexpr double kFinishDestroyTimeoutSec = 30.0;

    static constexpr uint64_t kHandshakeTimeoutNs =
        30 * 1000000000ULL;  // 30 seconds
    static constexpr uint64_t kHandshakeStalenessNs =
        10 * 1000000000ULL;  // 10 seconds

    // Maximum number of deconstructLocked retries in finishDestroy before
    // giving up and marking the endpoint as DESTROYED. Prevents infinite
    // retry loops and log flooding when ibv_destroy_qp fails permanently.
    static constexpr int kFinishDestroyMaxRetries = 3;

    RdmaContext &context_;
    std::atomic<Status> status_;

    RWSpinlock lock_;
    std::vector<ibv_qp *> qp_list_;

    std::string peer_nic_path_;
    std::vector<uint32_t> peer_qp_num_list_;

    volatile int *wr_depth_list_;
    int max_wr_depth_;
    size_t max_sge_per_wr_;
    size_t max_inline_bytes_;

    std::atomic<bool> active_;
    volatile int *cq_outstanding_;
    std::atomic<uint64_t> inactive_time_{0};
    int finish_destroy_retries_ = 0;
    bool needs_manual_completion_ = false;

    std::atomic<uint64_t> handshake_version_{0};
    struct PeerInfo {
        uint32_t qp_num = 0;
        uint64_t handshake_version = 0;
        uint64_t timestamp = 0;
    } established_peer_;

    std::mutex pending_slices_mutex_;
    std::unordered_set<CompletionToken *> pending_tokens_;
};

}  // namespace mooncake

#endif  // RDMA_ENDPOINT_H
