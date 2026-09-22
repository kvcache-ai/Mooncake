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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>

#include "rdma_context.h"

namespace mooncake {

class RdmaEndPointTestPeer;

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
//   After above steps, the RdmaEndPoint state is set to CONNECTED. With RDMA
//   ready ACK enabled, QPs first enter CONNECTED_WAIT_READY_ACK after reaching
//   RTS and become CONNECTED only after the ready-ACK phase completes.
//
// If the user initiates a disconnect() call or an error is detected internally,
// the connection is closed and the RdmaEndPoint state is set to UNCONNECTED.
// The handshake can be restarted at this point.
class RdmaEndPoint : public std::enable_shared_from_this<RdmaEndPoint> {
   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTING,
        CONNECTED_WAIT_READY_ACK,
        CONNECTED,
        DESTROYING,
        DESTROYED,
    };

    friend class RdmaEndPointTestPeer;
    friend class RdmaNotificationTestPeer;
    friend class RdmaContext;
    friend class RdmaTransport;

   public:
    RdmaEndPoint(RdmaContext &context);

    ~RdmaEndPoint();

    int construct(ibv_cq *cq, size_t num_qp_list = 2, size_t max_sge = 4,
                  size_t max_wr = 256, size_t max_inline = 64);

   private:
    int reconstruct();
    int deconstruct();
    int deconstructLocked();
    void beginDestroyLocked();

   public:
    void setPeerNicPath(const std::string &peer_nic_path);
    std::string peerNicPath() const;

    std::optional<AutoGidConnectionIdentity> autoGidConnection() const;

    int setupConnectionsByActive();

    int setupConnectionsByActive(const std::string &peer_nic_path) {
        setPeerNicPath(peer_nic_path);
        return setupConnectionsByActive();
    }

    using HandShakeDesc = TransferMetadata::HandShakeDesc;
    int setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                  HandShakeDesc &local_desc);

    int sendNotification(const TransferMetadata::NotifyDesc &notify);
    bool notificationNeedsReconnect() const;

    bool active() const { return active_.load(std::memory_order_acquire); }

    void set_active(bool flag) {
        RWSpinlock::WriteGuard guard(lock_);
        if (!flag)
            inactive_time_.store(getCurrentTimeInNano(),
                                 std::memory_order_relaxed);
        active_.store(flag, std::memory_order_release);
    }

    double inactiveTime() {
        if (active_.load(std::memory_order_acquire)) return 0.0;
        return (getCurrentTimeInNano() -
                inactive_time_.load(std::memory_order_relaxed)) /
               1000000000.0;
    }

   public:
    bool connected() const { return isConnectedStatus(status()); }

    // CONNECTED_WAIT_READY_ACK means local QPs have reached RTS but the RDMA
    // ready-ACK phase has not completed yet. Only CONNECTED can post WRs.
    bool readyToSend() const { return status() == CONNECTED; }

    bool readyAckTimedOut() const;

    bool retired() const {
        auto status = status_.load(std::memory_order_relaxed);
        return status == DESTROYING || status == DESTROYED;
    }

    // Interrupts the connection, which can be triggered by user or by internal
    // error. Use setupConnectionsByActive or setupConnectionsByPassive to
    // reconnect
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

   private:
    // Match TENT: fixed capacity and native-endian length-prefixed strings.
    // Layout: uint32 name_length, name, uint32 message_length, message.
    static constexpr size_t kNotifySlots = 256;
    static constexpr size_t kNotifySlotBytes = 65536;
    static constexpr size_t kNotifyHeaderBytes = 8;
    static bool notificationFits(const TransferMetadata::NotifyDesc &notify);
    static size_t encodeNotification(
        char *slot, const TransferMetadata::NotifyDesc &notify);
    static bool decodeNotification(const char *slot, size_t bytes,
                                   TransferMetadata::NotifyDesc &notify);
    int constructNotification();
    int connectNotification(const ibv_gid &gid, uint32_t lid, uint32_t peer_qp,
                            int local_gid_index);
    uint32_t notificationQpNum() const;
    int postNotificationReceive(size_t slot);
    void handleNotificationCompletion(
        const ibv_wc &wc, std::vector<TransferMetadata::NotifyDesc> &received);
    void stopNotification();
    int closeNotification();
    void describeNotification(HandShakeDesc &desc) const;
    int disconnectUnlocked();

    // Resets only pre-connected handshake attempts. Once an endpoint has ever
    // reached CONNECTED, it is retired instead of being reused.
    int resetConnection(const std::string &reason);
    int sendReadyAck(const std::string &peer_server_name,
                     const HandShakeDesc &local_desc);
    void rememberConnectedAutoGid(const GidSelectionSnapshot &local_selection,
                                  const HandShakeDesc &peer_desc);

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

    Status status() const { return status_.load(std::memory_order_relaxed); }

    static bool isConnectedStatus(Status status) {
        return status == CONNECTED_WAIT_READY_ACK || status == CONNECTED;
    }

    std::vector<uint32_t> qpNum() const;

    int doSetupConnection(const std::string &peer_gid, uint32_t peer_lid,
                          std::vector<uint32_t> peer_qp_num_list,
                          Status connected_status = CONNECTED,
                          std::string *reply_msg = nullptr,
                          SetupConnectionFailureInfo *failure_info = nullptr,
                          uint32_t notify_qp_num = 0);

    int doSetupConnection(int qp_index, const ibv_gid &peer_gid,
                          uint32_t peer_lid, uint32_t peer_qp_num,
                          int local_gid_index, std::string *reply_msg = nullptr,
                          SetupConnectionFailureInfo *failure_info = nullptr);

   private:
    static constexpr uint64_t kWaitExistingHandshakeTimeoutNano =
        10 * 1000000000ull;  // 10 seconds
    static constexpr uint64_t kReadyAckTimeoutNano =
        10 * 1000000000ull;  // 10 seconds
    static constexpr uint32_t kWaitExistingHandshakeSpinCount = 500;
    static constexpr uint32_t kWaitExistingHandshakeInitialSleepUs = 50;
    static constexpr uint32_t kWaitExistingHandshakeMaxSleepUs = 2000;

    // Maximum time (in seconds) to wait for outstanding WRs to drain before
    // treating the endpoint as leaked. Timed-out endpoints stay in the
    // EndpointStore waiting list so stale in-flight references cannot turn
    // into UAF.
    static constexpr double kFinishDestroyTimeoutSec = 30.0;

    // Maximum number of deconstructLocked retries in finishDestroy before
    // giving up and marking the endpoint as DESTROYED. Prevents infinite
    // retry loops and log flooding when ibv_destroy_qp fails permanently.
    static constexpr int kFinishDestroyMaxRetries = 3;

    RdmaContext &context_;
    std::atomic<Status> status_;

    mutable RWSpinlock lock_;
    std::vector<ibv_qp *> qp_list_;
    uint64_t qp_generation_;
    // Protected by its own mutex so send-slot waits never hold lock_.
    struct NotifyState {
        int fail(int code, bool reconnect = false) {
            if (!error) error = code;
            reconnect_needed = reconnect_needed || reconnect;
            connected = false;
            cv.notify_all();
            return error;
        }
        mutable std::mutex mutex;
        std::condition_variable cv;
        ibv_qp *qp = nullptr;
        ibv_mr *send_mr = nullptr, *recv_mr = nullptr;
        std::unique_ptr<char[]> send_buffer, recv_buffer;
        uint64_t next_send = 0;
        size_t pending = 0;
        uint32_t peer_qp = 0, inline_bytes = 0;
        int error = 0;
        bool enabled = false, connected = false, reconnect_needed = false;
    } notify_;
    uint32_t peer_notify_qp_num_ = 0;

    std::string peer_nic_path_;
    std::vector<uint32_t> peer_qp_num_list_;
    std::optional<AutoGidConnectionIdentity> auto_gid_connection_;
    bool has_connected_;
    std::atomic<uint64_t> ready_wait_start_ts_;

    std::atomic<int> *wr_depth_list_;
    int max_wr_depth_;
    size_t max_sge_per_wr_;
    size_t max_inline_bytes_;

    std::atomic<bool> active_;
    ibv_cq *cq_;
    std::atomic<int> *cq_outstanding_;
    std::atomic<uint64_t> inactive_time_;
    bool finish_destroy_timeout_logged_ = false;
    int finish_destroy_retries_ = 0;
};

}  // namespace mooncake

#endif  // RDMA_ENDPOINT_H
