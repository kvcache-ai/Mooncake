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
#include <string>
#include <vector>

#include "common.h"
#include "efa_context.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class EfaContext;

// Custom context for libfabric operations - stores slice pointer for
// completion handling.  This struct MUST have fi_context as its first member.
struct EfaOpContext {
    struct fi_context fi_ctx;  // Must be first member
    Transport::Slice* slice;   // Slice pointer for completion handling
    // Pointer to the context's wr_depth_ for CQ completion decrement.
    // std::atomic<int> (not volatile int) so the CQ-poller decrement
    // and the submit-path fetch_add play by the C++ memory model.
    std::atomic<int>* wr_depth;
    // The AV slot this operation was posted against, plus the slot number
    // itself; the CQ poller settles slot->inflight and any deferred
    // fi_av_remove on completion.  See EfaContext::av_slots_.
    EfaContext::AvSlotState* slot;
    fi_addr_t slot_addr;
};

// Per-peer handle in the shared-endpoint SRD model.
//
// This class does NOT own an fid_ep.  A single `shared_ep_` on the owning
// EfaContext services every peer; each EfaEndPoint just carries one
// fi_addr_t (AV index) plus the handshake state needed to populate it.
//
// Lifecycle:
//   1. construct()            : trivial; no libfabric resources allocated.
//   2. setupConnectionsByActive(peer): handshake -> fi_av_insert -> CONNECTED.
//   3. submitPostSend()       : delegates to EfaContext::submitSlicesOnPeer.
//   4. disconnect() / dtor    : fi_av_remove(peer_fi_addr_).
class EfaEndPoint {
   public:
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    enum Status { INITIALIZING, UNCONNECTED, CONNECTED };

    explicit EfaEndPoint(EfaContext& context);
    ~EfaEndPoint();

   public:
    void setPeerNicPath(const std::string& peer_nic_path);

    int setupConnectionsByActive();

    int setupConnectionsByActive(const std::string& peer_nic_path) {
        setPeerNicPath(peer_nic_path);
        return setupConnectionsByActive();
    }

    int setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                  HandShakeDesc& local_desc);

    // True if the AV slot this handle points at still has operations awaiting a
    // CQ entry.  Asks the context for the slot inflight count, which is shared
    // across endpoints (see EfaContext::av_slots_).
    bool hasOutstandingSlice() const;

    bool connected() const {
        return status_.load(std::memory_order_relaxed) == CONNECTED;
    }

    void disconnect();

    // Refuse to disconnect this handle unless the write lock is free (no
    // submitter inside submitPostSend) AND the shared AV slot has no inflight
    // operations; the decision is taken under the same write lock that
    // submitPostSend() holds as a reader, so a submitter cannot slip in
    // between the check and the teardown.  AV removal may still be deferred by
    // refcount / inflight inside removePeerAddr().  Returns false if the peer
    // was left alone because it is busy.
    bool disconnectIfIdle();

    // Called during EfaContext teardown: forget the AV slot WITHOUT calling
    // fi_av_remove().  The AV itself is about to be closed, which invalidates
    // every slot in one shot; calling fi_av_remove after the shared endpoint
    // has been closed trips an assertion inside the EFA provider.
    void markDetachedForTeardown();

   private:
    void disconnectUnlocked();

   public:
    const std::string toString() const;

    // Submit a batch of slices bound for this peer.  Internally establishes
    // the connection if needed, then delegates to
    // EfaContext::submitSlicesOnPeer using this peer's fi_addr_t.
    int submitPostSend(std::vector<Transport::Slice*>& slice_list,
                       std::vector<Transport::Slice*>& failed_slice_list);

    // Always 1: the shared endpoint backs every peer.  Kept so callers that
    // sum "QP count" across endpoints don't break.
    size_t getQPNumber() const { return 1; }

    fi_addr_t getPeerFiAddr() const { return peer_fi_addr_; }

    EfaContext& context() { return context_; }

   private:
    EfaContext& context_;
    std::atomic<Status> status_;

    // protects peer_nic_path_, status_ and peer_fi_addr_.  mutable so const
    // observers (hasOutstandingSlice) can read peer_fi_addr_ safely.
    mutable RWSpinlock lock_;
    std::string peer_nic_path_;
    fi_addr_t peer_fi_addr_;  // slot in context_.av()
    // Last peer EFA address successfully inserted into the AV.  Used ONLY to
    // classify a passive handshake as same-address (benign symmetric handshake
    // under bilateral sglang PD traffic) versus a real reconnect, which picks
    // the log level.  The AV reinsert is still mandatory on every handshake --
    // see the classify comment in setupConnectionsByPassive().
    std::string cached_peer_addr_;
};

}  // namespace mooncake

#endif  // EFA_ENDPOINT_H
