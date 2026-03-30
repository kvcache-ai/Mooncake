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

#include <queue>

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
// If the user initiates a disconnect() call or an error is detected internally,
// the connection is closed and the RdmaEndPoint state is set to UNCONNECTED.
// The handshake can be restarted at this point.
class RdmaEndPoint {
   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTING,
        CONNECTED,
    };

   public:
    RdmaEndPoint(RdmaContext &context);

    ~RdmaEndPoint();

    int construct(ibv_cq *cq, size_t num_qp_list = 2, size_t max_sge = 4,
                  size_t max_wr = 256, size_t max_inline = 64);

   private:
    int reconstruct();
    int deconstruct();

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

    bool hasOutstandingSlice() const;

    bool active() const { return active_; }

    void set_active(bool flag) {
        RWSpinlock::WriteGuard guard(lock_);
        active_ = flag;
        if (!flag) inactive_time_ = getCurrentTimeInNano();
    }

    double inactiveTime() {
        if (active_) return 0.0;
        return (getCurrentTimeInNano() - inactive_time_) / 1000000000.0;
    }

   public:
    bool connected() const {
        return status_.load(std::memory_order_relaxed) == CONNECTED;
    }

    // Interrupts the connection, which can be triggered by user or by internal
    // error. Use setupConnectionsByActive or setupConnectionsByPassive to
    // reconnect
    void disconnect();

    // Destroy QPs before CQs (in RDMA Context)
    int destroyQP();

   private:
    int disconnectUnlocked();

    // Resets the connection.
    //
    // The main difference between this function and `disconnectUnlocked`
    // is that it will reconstruct QPs when `CONFIG_ERDMA` is defined.
    // Without `CONFIG_ERDMA`, it is essentially the same as
    // `disconnectUnlocked` but with additional logging.
    //
    // This serves as a workaround for Aliyun eRDMA devices (i.e., once a QP is
    // transitioned to the RTS state, it cannot be reset to RTS again directly).
    // For more details:
    // https://github.com/kvcache-ai/Mooncake/pull/1733#discussion_r2992088663
    //
    // In practice:
    // - Call `resetConnection` if the QPs' state may have transitioned to RTS.
    // - Call `disconnectUnlocked` otherwise.
    //
    // This is mainly used in `setupConnectionsByActive` or
    // `setupConnectionsByPassive`. It is NOT invoked in the normal execution
    // flow, so a `reason` argument is passed for internal logging purposes.
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
    std::vector<uint32_t> qpNum() const;

    int doSetupConnection(const std::string &peer_gid, uint16_t peer_lid,
                          std::vector<uint32_t> peer_qp_num_list,
                          std::string *reply_msg = nullptr);

    int doSetupConnection(int qp_index, const std::string &peer_gid,
                          uint16_t peer_lid, uint32_t peer_qp_num,
                          std::string *reply_msg = nullptr);

   private:
    static constexpr uint64_t kWaitExistingHandshakeTimeoutNano =
        10 * 1000000000ull;  // 10 seconds
    static constexpr uint32_t kWaitExistingHandshakeSpinCount = 500;
    static constexpr uint32_t kWaitExistingHandshakeInitialSleepUs = 50;
    static constexpr uint32_t kWaitExistingHandshakeMaxSleepUs = 2000;

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

    volatile bool active_;
    volatile int *cq_outstanding_;
    volatile uint64_t inactive_time_;
};

}  // namespace mooncake

#endif  // RDMA_ENDPOINT_H
