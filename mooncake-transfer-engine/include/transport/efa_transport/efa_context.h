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
// FI_OPT_EFA_* provider-specific setopt values (FI_OPT_EFA_HOMOGENEOUS_PEERS).
// Not a mandatory libfabric header: the AWS EFA installer ships it, but the
// distro libfabric-dev package -- what the wheel CI builds against -- does not.
// Probe for it instead of hard-failing the compile; see
// kEfaOptHomogeneousPeers for what we do when it is absent.
#if defined(__has_include)
#if __has_include(<rdma/fi_ext.h>)
#include <rdma/fi_ext.h>
#define MC_HAVE_FI_EXT_H 1
#endif
#endif

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

// fi_setopt() value that tells the EFA provider every peer is homogeneous with
// us, letting it skip the per-peer RDM handshake (see buildSharedEndpoint()).
//
// libfabric declares this as an *enum*, not a macro, so #ifdef cannot probe it,
// and it lives in rdma/fi_ext.h -- a header the AWS EFA installer ships but the
// distro libfabric-dev package does not.  Whether the option works is a
// property of the libfabric we LOAD, not the headers we COMPILE against, and
// those differ in the build that matters: the EFA wheel compiles against distro
// headers and then runs against the EFA installer's libfabric.  So resolve the
// value at compile time but always issue the fi_setopt, and let the provider
// reject it (-FI_ENOPROTOOPT / -FI_EOPNOTSUPP) when it is too old to know it.
//
// The vendored fallback is safe because the enum's layout is append-only: the
// first seven values are byte-identical in every libfabric release from 1.22
// through 2.3, with HOMOGENEOUS_PEERS added eighth in 2.2.  A libfabric
// predating it hits efa_rdm_ep_setopt()'s default case and returns
// -FI_ENOPROTOOPT, which is exactly the "keep the handshake" path.
#ifdef MC_HAVE_FI_EXT_H
constexpr int kEfaOptHomogeneousPeers = FI_OPT_EFA_HOMOGENEOUS_PEERS;
#else
constexpr int kEfaOptHomogeneousPeers = -(0xefa << 16) + 7;
#endif

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
//   * Scale-out is bounded by peer_map_max_ (MC_MAX_EP_PER_CTX) rather than
//     by 768 QPs/NIC; the AV is sized to match (max_endpoints + headroom).
class EfaContext {
   public:
    // Per-AV-slot bookkeeping.  Public because EfaOpContext holds a pointer to
    // one so the CQ poller can settle the slot on completion; see av_slots_ for
    // why both counters have to live on the slot rather than on the endpoint.
    struct AvSlotState {
        // EfaEndPoint handles currently holding this slot.
        std::atomic<int> refcount{0};
        // Operations posted against this slot with no CQ entry yet.
        std::atomic<int> inflight{0};
        // Last holder released the slot while inflight > 0; the CQ poller
        // performs the fi_av_remove once it drains.
        std::atomic<bool> remove_pending{false};
    };

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

    // Like deleteEndpoint(), but refuses to disconnect the handle and erase its
    // peer_map_ entry while the peer is busy -- either a submitter holds the
    // handle's lock or the shared AV slot still has inflight operations.
    // Returns false in that case, leaving the entry in place.  Nothing
    // schedules a retry: teardown is only reattempted if a later call path
    // (another CQ error, eviction, submit failure) reaches this peer again.
    // The fi_av_remove itself is deferred inside removePeerAddr(), not here.
    bool deleteEndpointIfIdle(const std::string& peer_nic_path);
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

    // Same-process self-loopback fast path.  When a slice's resolved peer
    // NIC path equals our own (same server_name — which embeds the
    // per-process RPC port — AND same device), source and destination are
    // both valid pointers in THIS process's address space.  We satisfy the
    // copy with a local memcpy / cudaMemcpy instead of issuing it over EFA.
    //
    // This avoids libfabric's EFA SHM intra-node path, which performs a
    // host memcpy into FI_HMEM_CUDA device buffers and segfaults
    // (ofiwg/libfabric#12328).  Returns true if the slice was handled here
    // (and marked success/failed); false if it should fall through to the
    // normal EFA submit path.
    bool tryLoopbackCopy(Transport::Slice* slice);

    // Hot-path submit: post a batch of slices to `peer_fi_addr` via the
    // shared endpoint.  Handles WR / CQ reservation, MR descriptor prep,
    // and the fi_write / fi_read burst under post_lock_.  Called by
    // EfaEndPoint::submitPostSend once the peer is connected.
    //
    // Posted operations are counted on the AV slot, not the endpoint (see
    // av_slots_).
    int submitSlicesOnPeer(fi_addr_t peer_fi_addr,
                           std::vector<Transport::Slice*>& slice_list,
                           std::vector<Transport::Slice*>& failed_slice_list);

    // True if any operation posted against `fi_addr` is still awaiting a CQ
    // entry (shared across every endpoint holding the slot).
    bool slotHasInflight(fi_addr_t fi_addr);

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
    // return the resulting fi_addr_t.  Thread-safe: the insert and its slot
    // reference claim are taken together under post_lock_, which is also what
    // serializes them against fi_av_remove (see insertPeerAddrBytes).
    int insertPeerAddr(const std::string& peer_hex_addr, fi_addr_t& out);

    // Binary variant — avoids the hex-decode when the caller already
    // has the raw address bytes (e.g. loopback).
    int insertPeerAddrBytes(const uint8_t* addr, size_t len, fi_addr_t& out);

    // Release this holder's reference on a peer's AV slot.  No-op if fi_addr is
    // FI_ADDR_UNSPEC.  The fi_av_remove happens only on the last release, and
    // is deferred to the CQ poller while the slot still has inflight
    // operations (see av_slots_).
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

    // Event queue bound to shared_ep_.  libfabric's EFA provider funnels every
    // fatal error through efa_base_ep_write_eq_error(); without an EQ bound,
    // that function calls abort() and kills the process.  Binding our own EQ
    // turns those events into queued entries we can drain via fi_eq_readerr
    // and recover from instead of crashing.
    struct fid_eq* eq_;
    std::thread eq_poller_thread_;
    std::atomic<bool> eq_poller_stop_;

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
    // Ceilings for the two pacing counters (wr_depth_ above, EfaCq::outstanding
    // on shared_cq_).  Both are the depths the provider gave us for this
    // device, not GlobalConfig values: a counter that disagrees with the queue
    // it paces either stalls submission early or hands out credit fi_write has
    // to refuse.  Set in buildSharedEndpoint() and construct() respectively.
    int max_wr_depth_;
    size_t max_cqe_ = 0;
    // CQ that shared_ep_ is bound to (FI_TRANSMIT|FI_RECV).  Points into
    // cq_list_[0]; kept here to avoid re-indexing on the hot path.
    std::shared_ptr<EfaCq> shared_cq_;
    // Serializes fi_write / fi_read calls on shared_ep_.  libfabric's RDM
    // endpoints are not thread-safe for concurrent post, even with
    // FI_THREAD_SAFE at the domain level.
    std::atomic_flag post_lock_;

    // ---- Credit-flow diagnostics (observability only) ----
    //
    // wr_depth_ is released only by the CQ poller, so when a submitter runs out
    // of credit the question that actually narrows things down is whether
    // completions are still arriving at all.  These answer it without touching
    // the healthy submit path: the poller stamps them once per non-empty
    // fi_cq_read batch rather than once per completion, and only failure paths
    // ever read them.  Every access is memory_order_relaxed -- these counters
    // order nothing, they only get printed.
    //
    // Because the starvation logs print the cumulative values, two events hours
    // apart can be diffed to separate a slow leak (posted - completed - errors
    // drifting up) from ordinary saturation, with no periodic logging thread.
    //
    // alignas(64) is load-bearing, not decoration: the poller writes
    // last_completion_ns_ on every batch, and without its own cache line these
    // counters would share one with wr_depth_ / post_lock_ just above -- the
    // exact words every submitter spins on.  Diagnostics must not add coherence
    // traffic to the pacing hot path.
    alignas(64) std::atomic<uint64_t> last_completion_ns_{0};
    std::atomic<uint64_t> total_posted_{0};
    std::atomic<uint64_t> total_completions_{0};
    std::atomic<uint64_t> total_cq_errors_{0};
    // Completions whose op_context or slice is null.  Both pollCq branches
    // return the CQ reservation for these (they are counted in `ret` /
    // `err_count`) but cannot return the matching wr_depth_ reservation, whose
    // owner is reached only through op_ctx.  A nonzero value here is therefore
    // a wr_depth_ leak, and its signature is wr_depth_ drifting above
    // cq_outstanding -- the two are otherwise reserved and released in pairs.
    std::atomic<uint64_t> orphan_completions_{0};

    // Top-N AV slots by inflight, plus the total across all slots.  wr_depth_
    // is per local NIC and shared by every peer, so one stalled peer can pin
    // all of it and starve the rest; and a total far below wr_depth_ means the
    // credit is held by operations with no slot, i.e. an accounting bug rather
    // than a slow peer.  Walks av_slots_ under av_ref_lock_, so callers must
    // not hold post_lock_ (keeps the post_lock_ -> av_ref_lock_ order intact).
    std::string describeInflightBySlot(size_t top_n) const;

    // Milliseconds since the CQ poller last reaped anything, or -1 if it never
    // has.  Reads last_completion_ns_; failure paths only.
    int64_t msSinceLastCompletion() const;

    std::vector<std::shared_ptr<EfaCq>> cq_list_;

    // ---- Peer handles (one entry per peer, each ~constant size) ----
    mutable RWSpinlock peer_map_lock_;

    // FIFO eviction cap.  Bounds peer_map_ so that volatile peer_nic_path
    // schemes (ip:port:timestamp@nic) cannot accumulate stale entries past
    // the configured limit.  When peer_map_ would exceed peer_map_max_, the
    // oldest inserted entry is disconnected and erased from peer_map_ before
    // the new one is added; the AV remove may be deferred (see av_slots_), and
    // busy victims are skipped, so the map can transiently exceed the cap.
    // This keeps libfabric's AV table bounded without the per-process aliasing
    // that a host+nic "stable key" would impose on sglang DP>1 workloads (each
    // DP worker has its own RPC port and must retain its own peer_map_ slot).
    size_t peer_map_max_;  // set from MC_MAX_EP_PER_CTX, 0 = unlimited

    // peer_map_ entry storing both the endpoint and an iterator into the
    // FIFO eviction list, so deletion from either side is O(1).
    struct PeerMapEntry {
        std::shared_ptr<EfaEndPoint> ep;
        std::list<std::string>::iterator lru_it;
    };
    std::unordered_map<std::string, PeerMapEntry> peer_map_;
    // FIFO eviction list (named lru_ historically): insertion order only, no
    // move-to-back on lookup.  Front = oldest.  Guarded by peer_map_lock_.
    std::list<std::string> peer_lru_;

    // Per-AV-slot bookkeeping, keyed by fi_addr_t.  CANONICAL NOTE for the
    // refcount / inflight / remove_pending machinery; other sites point here.
    //
    // fi_av_insert() returns the SAME table index for the same peer address and
    // fi_av_remove() frees that index for reuse, so several EfaEndPoint objects
    // can legitimately hold one fi_addr_t at once -- with a volatile peer key
    // (ip:port:timestamp@nic) the same peer is re-admitted under a new key
    // while the old handle is still alive.  Hence both counters live on the
    // SLOT, not the endpoint, which can only see its own operations:
    //   * refcount -- fi_av_remove only on the LAST holder's release.
    //     Otherwise the first teardown invalidates a slot its siblings are
    //     still posting on (SIGSEGV inside libfabric, freed dest_addr).
    //   * inflight -- fi_av_remove with operations still posted makes the
    //     provider drop their completions, so the slices stay PENDING and the
    //     caller's status loop spins on WAITING forever (hang).  When the last
    //     holder leaves while inflight > 0, removal is deferred via
    //     remove_pending and the CQ poller retires the slot on drain.
    // Removal is additionally serialized against the post burst on post_lock_,
    // since a freed index can be re-handed to another peer; removeSlotNow() is
    // the authoritative gate and re-validates both counters under that lock.
    //
    // Entries are never erased, which keeps the AvSlotState* stored in each
    // EfaOpContext valid for as long as libfabric may hand that context back.
    // Bounded by the number of distinct AV indices the provider hands out
    // (at most the AV size), and each entry is three ints.
    mutable RWSpinlock av_ref_lock_;
    std::unordered_map<fi_addr_t, std::unique_ptr<AvSlotState>> av_slots_;

    // Get-or-create the state for a slot.  Never returns null.
    AvSlotState* avSlot(fi_addr_t fi_addr);
    // Look up existing slot state, or null if the slot was never inserted.
    AvSlotState* avSlotIfPresent(fi_addr_t fi_addr);
    // Retire a slot that has quiesced.  Called by the CQ poller for slots
    // whose removal was deferred.
    void retireSlotIfPending(AvSlotState* slot, fi_addr_t fi_addr);
    // fi_av_remove serialized against the post burst.  Re-validates the slot's
    // counters under post_lock_ and declines (re-arming remove_pending if
    // needed) when a concurrent handshake re-claimed the slot or new operations
    // were posted -- the callers' checks are advisory, this one is decisive.
    void removeSlotNow(AvSlotState* slot, fi_addr_t fi_addr);

    RWSpinlock mr_lock_;
    std::map<uint64_t, EfaMemoryRegionMeta> mr_map_;
};

}  // namespace mooncake

#endif  // EFA_CONTEXT_H
