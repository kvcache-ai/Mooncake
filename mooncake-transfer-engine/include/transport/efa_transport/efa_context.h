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

#ifndef EFA_CONTEXT_H
#define EFA_CONTEXT_H

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <map>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

class EfaEndPoint;
class EfaTransport;

struct EfaCq {
    EfaCq() : cq(nullptr), outstanding(0) {}
    struct fid_cq* cq;
    std::atomic<int> outstanding;
};

struct EfaMemoryRegionMeta {
    void* addr;
    size_t length;
    struct fid_mr* mr;
    uint64_t key;
};

// EfaContext represents the set of resources controlled by each local EFA
// device: one libfabric domain, one address vector (AV), one shared endpoint
// (fid_ep), one or more CQs, and the MR table.
//
// Key design point (SRD shared-endpoint model): there is exactly ONE fid_ep
// per local NIC for the entire process.  Every peer lives as an fi_addr_t
// entry inside the AV.  Adding a peer is an O(1) `fi_av_insert` into the
// existing AV — it does NOT consume a QP slot.  Consequences:
//   * QP usage is a constant 1 per local NIC, regardless of peer count.
//   * Cold warmup is ~ms per peer (handshake + fi_av_insert) instead of
//     ~35 ms per peer (fi_endpoint + fi_enable).
//   * Scale-out is bounded only by AV capacity (65536) instead of 768/NIC.
class EfaContext {
   public:
    EfaContext(EfaTransport& engine, const std::string& device_name);

    ~EfaContext();

    int construct(size_t num_cq_list = 1, size_t max_cqe = 4096,
                  int max_endpoints = 65536);

   private:
    int deconstruct();
    int buildSharedEndpoint(size_t max_wr, size_t max_inline);

   public:
    // Memory Region Management
    int registerMemoryRegion(void* addr, size_t length, int access);
    int unregisterMemoryRegion(void* addr);
    int preTouchMemory(void* addr, size_t length);
    uint64_t rkey(void* addr);
    uint64_t lkey(void* addr);
    void* mrDesc(void* addr);  // Get MR descriptor for fi_write local_desc

   private:
    int registerMemoryRegionInternal(void* addr, size_t length, int access,
                                     EfaMemoryRegionMeta& mrMeta);

   public:
    bool active() const { return active_; }
    void set_active(bool flag) { active_ = flag; }

   public:
    // Get or create a per-peer handle.  Does NOT open an fid_ep or call
    // fi_enable — the shared endpoint was created once at construct() time.
    // The returned EfaEndPoint only carries {peer_fi_addr_t, status, mutex}
    // and, when connected, routes sends through this context's shared_ep_.
    std::shared_ptr<EfaEndPoint> endpoint(const std::string& peer_nic_path);

    // Non-creating lookup under the normalized key.  Returns nullptr if the
    // peer handle does not yet exist.  Safe for idempotency checks.
    std::shared_ptr<EfaEndPoint> peekEndpoint(const std::string& peer_nic_path);

    int deleteEndpoint(const std::string& peer_nic_path);
    int disconnectAllEndpoints();

    // Number of live peer handles.  Historically named "QP number"; with the
    // shared endpoint model the actual QP count is always 1 per context.
    size_t getTotalQPNumber() const;

   public:
    // Access to engine for endpoint handshake
    EfaTransport& engine() { return engine_; }
    const EfaTransport& engine() const { return engine_; }

    // Submit slices for transfer
    int submitPostSend(const std::vector<Transport::Slice*>& slice_list);

    // Hot-path submit: post a batch of slices to `peer_fi_addr` via the
    // shared endpoint.  Handles WR / CQ reservation, MR descriptor prep,
    // and the fi_write / fi_read burst under post_lock_.  Called by
    // EfaEndPoint::submitPostSend once the peer is connected.
    int submitSlicesOnPeer(fi_addr_t peer_fi_addr,
                           std::vector<Transport::Slice*>& slice_list,
                           std::vector<Transport::Slice*>& failed_slice_list);

    // Poll completion queue for completed operations
    int pollCq(int max_entries, int cq_index = 0);

    // Get CQ count
    size_t cqCount() const { return cq_list_.size(); }

   public:
    // Device name, such as `rdmap0s2`
    std::string deviceName() const { return device_name_; }

    // NIC Path, such as `192.168.3.76@rdmap0s2`
    std::string nicPath() const;

   public:
    // Libfabric accessors
    struct fid_fabric* fabric() const { return fabric_; }
    struct fid_domain* domain() const { return domain_; }
    struct fid_av* av() const { return av_; }
    struct fi_info* info() const { return fi_info_; }
    std::string localAddr() const;

    // Local (shared-endpoint) address in hex, for inclusion in handshake.
    // Populated by construct() -> buildSharedEndpoint().
    std::string localEpAddr() const;

    // Raw bytes of the local endpoint address.  Use this for loopback
    // (skip the hex encode/decode round-trip) or for any caller that
    // already has the bytes.
    const std::vector<uint8_t>& localEpAddrBytes() const {
        return local_ep_addr_;
    }

    // Insert a peer's hex-encoded EFA address into this context's AV and
    // return the resulting fi_addr_t.  Thread-safe (fi_av_insert is safe
    // under libfabric's domain-level threading).
    int insertPeerAddr(const std::string& peer_hex_addr, fi_addr_t& out);

    // Binary variant — avoids the hex-decode when the caller already
    // has the raw address bytes (e.g. loopback).
    int insertPeerAddrBytes(const uint8_t* addr, size_t len, fi_addr_t& out);

    // Remove a peer from the AV.  No-op if fi_addr is FI_ADDR_UNSPEC.
    void removePeerAddr(fi_addr_t fi_addr);

    // Compatibility methods (libfabric doesn't use lid/gid like ibverbs)
    uint16_t lid() const { return 0; }
    std::string gid() const { return localAddr(); }

   private:
    EfaTransport& engine_;
    std::string device_name_;

    // Libfabric objects
    struct fi_info* fi_info_;
    struct fi_info* hints_;
    struct fid_fabric* fabric_;
    struct fid_domain* domain_;
    struct fid_av* av_;  // Address vector for peer addressing

    bool active_;

    // ---- Shared endpoint (one per local NIC, serves ALL peers) ----
    struct fid_ep* shared_ep_;
    std::vector<uint8_t> local_ep_addr_;  // bytes returned by fi_getname()
    // Pacing for outstanding work requests on the shared endpoint.  Shared
    // across all peers routed through this context.  std::atomic<int> so the
    // submit-path fetch_add and the CQ-poller fetch_sub obey the C++ memory
    // model; plain `volatile int` + __sync_* was UB under the current
    // standard.
    std::atomic<int> wr_depth_;
    int max_wr_depth_;
    // CQ that shared_ep_ is bound to (FI_TRANSMIT|FI_RECV).  Points into
    // cq_list_[0]; kept here to avoid re-indexing on the hot path.
    std::shared_ptr<EfaCq> shared_cq_;
    // Serializes fi_write / fi_read calls on shared_ep_.  libfabric's RDM
    // endpoints are not thread-safe for concurrent post, even with
    // FI_THREAD_SAFE at the domain level.
    std::atomic_flag post_lock_;

    std::vector<std::shared_ptr<EfaCq>> cq_list_;

    // ---- Peer handles (one entry per peer, each ~constant size) ----
    mutable RWSpinlock peer_map_lock_;
    std::unordered_map<std::string, std::shared_ptr<EfaEndPoint>> peer_map_;

    RWSpinlock mr_lock_;
    std::map<uint64_t, EfaMemoryRegionMeta> mr_map_;
};

}  // namespace mooncake

#endif  // EFA_CONTEXT_H
