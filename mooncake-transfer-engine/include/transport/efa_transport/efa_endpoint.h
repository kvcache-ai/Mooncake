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

#include <glog/logging.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>

#include <atomic>
#include <cstdint>
#include <queue>
#include <string>
#include <vector>

#include "common.h"
#include "efa_context.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class EfaContext;

// Custom context for libfabric operations - stores slice pointer for completion
// handling This struct MUST have fi_context as its first member
struct EfaOpContext {
    struct fi_context fi_ctx;  // Must be first member
    Transport::Slice *slice;   // Slice pointer for completion handling
    volatile int *wr_depth;    // Pointer to endpoint's wr_depth_ for CQ
                               // completion decrement
};

// EfaEndPoint represents a libfabric endpoint for EFA communication.
// Unlike RDMA QPs, EFA uses RDM (Reliable Datagram) endpoints with
// an address vector for peer addressing.
class EfaEndPoint {
   public:
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    enum Status { INITIALIZING, UNCONNECTED, CONNECTED };

    EfaEndPoint(EfaContext &context);
    ~EfaEndPoint();

    // Construct endpoint with specified completion queue
    int construct(struct fid_cq *cq, size_t num_qp_list = 1, size_t max_sge = 4,
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

    void disconnect();
    int destroyQP();

   private:
    void disconnectUnlocked();

   public:
    const std::string toString() const;

    // Submit RDMA write/read operations via libfabric
    int submitPostSend(std::vector<Transport::Slice *> &slice_list,
                       std::vector<Transport::Slice *> &failed_slice_list);

    // Get the number of endpoints (always 1 for EFA RDM)
    size_t getQPNumber() const { return 1; }

    // Get local endpoint address for handshake
    std::string getLocalAddr() const;

    // Get peer's fi_addr
    fi_addr_t getPeerFiAddr() const { return peer_fi_addr_; }

    EfaContext &context() { return context_; }

   private:
    // Setup connection using peer's address from handshake
    int doSetupConnection(const std::string &peer_addr,
                          std::string *reply_msg = nullptr);

    // Insert peer address into address vector
    int insertPeerAddr(const std::string &peer_addr);

   private:
    EfaContext &context_;
    std::atomic<Status> status_;

    RWSpinlock lock_;
    std::string peer_nic_path_;

    // Libfabric endpoint
    struct fid_ep *ep_;
    struct fid_cq *tx_cq_;
    struct fid_cq *rx_cq_;
    fi_addr_t peer_fi_addr_;  // Peer's address in the AV

    // Local endpoint address (for handshake)
    std::vector<uint8_t> local_addr_;
    size_t local_addr_len_;

    volatile int wr_depth_;
    int max_wr_depth_;
    volatile int *cq_outstanding_;

    // Spinlock to serialize fi_write calls on this endpoint.
    // libfabric RDM endpoints are not thread-safe by default.
    std::atomic_flag post_lock_ = ATOMIC_FLAG_INIT;

    volatile bool active_;
    volatile uint64_t inactive_time_;
};

}  // namespace mooncake

#endif  // EFA_ENDPOINT_H
