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

#ifndef EFA_ENDPOINT_H
#define EFA_ENDPOINT_H

#include <queue>

#include "efa_context.h"

namespace mooncake {

// EfaEndPoint represents all QP connections between the local EFA (identified
// by its EfaContext) and the remote EFA (identified by peer_nic_path).
// 1. After construct, resources are allocated without specifying the peers.
// 2. Handshake information needs to be exchanged with remote EfaEndPoint.
//    - Local side calls the setupConnectionsByActive() function, passing in the
//    peer_nic_path of the remote side
//      peer_nic_path := peer_server_name@nic_name, e.g. 192.168.3.76@rdmap0s2,
//      which can be obtained from EfaContext::nicPath() on the remote side
//    - Remote side calls the setupConnectionsByPassive() function in its RPC
//    service.
//   After above steps, the EfaEndPoint state is set to CONNECTED
//
// If the user initiates a disconnect() call or an error is detected internally,
// the connection is closed and the EfaEndPoint state is set to UNCONNECTED.
// The handshake can be restarted at this point.
class EfaEndPoint {
   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTED,
    };

   public:
    EfaEndPoint(EfaContext &context);

    ~EfaEndPoint();

    int construct(ibv_cq *cq, size_t num_qp_list = 2, size_t max_sge = 4,
                  size_t max_wr = 256, size_t max_inline = 64);

   private:
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

    // Destroy QPs before CQs (in EFA Context)
    int destroyQP();

   private:
    int connectQP(struct ibv_qp *qp, uint32_t remote_qpn, uint32_t remote_psn,
                  const union ibv_gid &remote_gid);

   public:
    int postSlice(Transport::Slice *slice);

    int pollCompletion();

   public:
    std::string peer_nic_path() const { return peer_nic_path_; }

   private:
    EfaContext &context_;
    std::string peer_nic_path_;

    std::atomic<Status> status_;
    std::vector<struct ibv_qp *> qp_list_;
    std::vector<volatile int> qp_depth_list_;

    mutable RWSpinlock lock_;
    bool active_;
    int64_t inactive_time_;

    size_t max_sge_;
    size_t max_wr_;
    size_t max_inline_;
};

}  // namespace mooncake

#endif  // EFA_ENDPOINT_H
