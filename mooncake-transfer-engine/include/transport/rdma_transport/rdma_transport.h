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

#ifndef RDMA_TRANSPORT_H_
#define RDMA_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

struct DmabufExport;
class RdmaContext;
class RdmaEndPoint;
class TransferMetadata;
class RdmaTransportTestPeer;
class WorkerPool;

class RdmaTransport : public Transport {
    friend class RdmaContext;
    friend class RdmaEndPoint;
    friend class RdmaTransportTestPeer;
    friend class RdmaNotificationTestPeer;
    friend class WorkerPool;

   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    RdmaTransport();

    ~RdmaTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "rdma"; }

    bool supportsBatchCompletionCounter() const override { return true; }

    // Ordinary RDMA's native notification channel. Success means the SEND
    // was posted locally, not that the remote application consumed it.
    int sendNativeNotify(const std::string &peer_server_name,
                         const TransferMetadata::NotifyDesc &notify);

    // HugeTLB host memory is tried over the caller's own range first; only an
    // EINVAL from ibv_reg_mr (a hugetlb VMA cannot be split for the
    // MADV_DONTFORK ibv_fork_init installs) retries it over the enclosing huge
    // pages. The widened MR covers that padding and so do its keys; see the
    // caller contract on TransferEngine::registerLocalMemory.
    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

   private:
    // Internal version with force_sequential option to avoid nested parallelism
    int registerLocalMemoryInternal(void *addr, size_t length,
                                    const std::string &location,
                                    bool remote_accessible,
                                    bool update_metadata,
                                    bool force_sequential);

    // defer_publish_to_caller: used by unregisterLocalMemoryBatch, whose
    // members must not each publish the local segment desc. A member that has
    // finished everything but the publish records that in pending_republish_
    // and returns, leaving the batch to publish once and then clearPending().
    int unregisterLocalMemoryInternal(void *addr, bool update_metadata,
                                      bool force_sequential,
                                      bool defer_publish_to_caller = false);

    // Pending-cleanup bookkeeping (see the members below). Each helper takes
    // pending_cleanup_mutex_ exactly once.
    bool hasPendingDereg(uint64_t addr);
    std::optional<std::vector<uint64_t>> lookupPendingRepublish(uint64_t addr);
    void markPendingDereg(uint64_t addr);
    void markPendingRepublish(uint64_t addr, std::vector<uint64_t> keys);
    void clearPending(uint64_t addr);

    // amdgpu before 6.19.14 charges a NIC's dma-buf pin of KFD-allocated VRAM
    // against the KFD free pool a second time; only pins taken through its
    // peer-memory client are offset. Holding one such pin per allocation keeps
    // hipMemGetInfo and later device allocations exact while the data MRs stay
    // on dma-buf. MC_HIP_KFD_PIN_FIX and a probe at the first registration
    // decide whether the driver needs it.
    bool acquireKfdPinAnchor(void *addr, const DmabufExport &exp);
    void releaseKfdPinAnchor(void *addr);

   public:
    // TRANSFER
    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    // Submit fresh tasks without an external submission seal owner. No slices
    // may have been published before this call; all return statuses seal tasks.
    Status submitTransferTaskWithSeal(
        const std::vector<TransferTask *> &task_list);

    bool supportsGroupedScatter() const override { return true; }

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    SegmentID getSegmentID(const std::string &segment_name);

   private:
    int allocateLocalSegmentID();

    int refreshLocalDeviceDesc(const std::string &device_name, uint32_t lid,
                               const std::string &gid);

    int preTouchMemory(void *addr, size_t length);

   public:
    virtual int onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                       HandShakeDesc &local_desc);

    virtual int sendHandshake(const std::string &peer_server_name,
                              const HandShakeDesc &local_desc,
                              HandShakeDesc &peer_desc) {
        return metadata_->sendHandshake(peer_server_name, local_desc,
                                        peer_desc);
    }

   private:
    int initializeRdmaResources();

    int startHandshakeDaemon(std::string &local_server_name);

    void notifyWorkerThread();

   public:
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            int &buffer_id, int &device_id, int retry_cnt = 0,
                            int hint_buffer_id = -1, int hint_device_id = -1);
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            std::string_view hint, int &buffer_id,
                            int &device_id, int retry_cnt = 0,
                            int hint_buffer_id = -1, int hint_device_id = -1);
    static int selectDeviceByLocalHca(SegmentDesc *desc, uint64_t offset,
                                      size_t length, std::string_view local_hca,
                                      int &buffer_id, int &device_id,
                                      int retry_cnt = 0,
                                      int hint_buffer_id = -1,
                                      int hint_device_id = -1);

    const std::vector<std::shared_ptr<RdmaContext>> &getContextList() const {
        return context_list_;
    }

   private:
    std::vector<std::shared_ptr<RdmaContext>> context_list_;
    std::atomic<bool> notify_running_{false};
    std::thread notify_worker_;
    std::shared_ptr<Topology> local_topology_;
    // When MC_RDMA_BIND_ADDRESS is set in a dual-NIC environment,
    // rdma_server_name_ holds the RDMA-reachable address (e.g.
    // "192.168.0.y:port") for NIC path construction, while
    // local_server_name_ keeps the TCP-reachable address for P2P routing.
    std::string rdma_server_name_;
    std::mutex local_desc_lock_;
    // Mooncake#2017: buffers larger than the device max_mr_size are split into
    // multiple sub-max_mr_size MRs (one BufferDesc per chunk) so that
    // ibv_reg_mr is never silently truncated. unregisterLocalMemory() only
    // receives the base addr, so remember each base buffer's chunk
    // start-addresses for cleanup.
    std::mutex chunk_map_mutex_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> chunk_map_;

    // A registration of HugeTLB host memory is attempted over the caller's own
    // range first; only when ibv_reg_mr() rejects it with EINVAL — the kernel
    // refuses to split a hugetlb VMA when ibv_fork_init() makes the
    // registration madvise() it — is it retried over the enclosing huge pages
    // (see registerLocalMemoryInternal). Such an MR is registered under a base
    // the caller never sees, while the published BufferDesc keeps the caller's
    // range. Two things are tracked:
    //
    // hugetlb_registered_regions_ holds the range of every live hugetlb-backed
    // registration, keyed by MR base (the narrow range where that worked, the
    // widened one otherwise). Overlap is what has to be refused, not just a
    // shared base: RdmaContext's MR map would silently drop an MR registered at
    // a duplicate base, and unregisterMemoryRegion() madvises MADV_DOFORK over
    // the whole MR range (issue #3639), which must not be re-opened while a
    // second MR still covers part of it. Overlapping the interior of a larger
    // registration has exactly that effect, so every hugetlb-backed
    // registration is claimed and checked against the live ranges rather than
    // their bases. The check, the claim, both attempts and the release are
    // serialized by hugetlb_registry_mutex_ so two workers of a batch cannot
    // both pass the check before either records its range.
    //
    // hugepage_widen_aliases_ maps a widened registration's caller-visible
    // address to the base its MR actually lives at, so unregisterLocalMemory
    // (which only receives the caller's address) can deregister it. A
    // registration that succeeded over the caller's own range has no alias.
    std::mutex hugetlb_registry_mutex_;
    std::map<uint64_t, size_t> hugetlb_registered_regions_;
    std::unordered_map<uint64_t, uint64_t> hugepage_widen_aliases_;

    // A failed unregister is resumed in one of two states, never both:
    //
    // pending_dereg_: MR deregistration is NOT confirmed (an ibv_dereg_mr
    // failed, or the metadata removal/publish failed while the MRs were still
    // live). A retry must re-enter the per-context MR cleanup, whose MR lookup
    // is safe here because the address still belongs to our registration.
    //
    // pending_republish_: the MRs are confirmed gone, the claim and widen
    // alias are released and chunk_map_ was erased; only the final
    // updateLocalSegmentDesc() (and possibly a leftover local metadata
    // removal) is outstanding. The value holds the metadata keys the
    // registration was added under (the caller's address for a single buffer,
    // the chunk addresses for a chunked one) so a retry can finish the
    // removal idempotently. A retry in this state MUST NOT perform any MR
    // lookup: a disjoint view of the same range may have registered in the
    // meantime, and RdmaContext::unregisterMemoryRegion() resolves by
    // containing range, so it would deregister that view's live MR (and
    // madvise MADV_DOFORK over it).
    //
    // The batch path uses pending_republish_ to defer its single collective
    // publish; it must never seed pending_dereg_ for a member it already
    // cleaned.
    std::mutex pending_cleanup_mutex_;
    std::unordered_set<uint64_t> pending_dereg_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> pending_republish_;

    struct KfdPinAnchor {
        ibv_mr *mr = nullptr;
        size_t refs = 0;
    };
    std::mutex kfd_pin_anchor_mutex_;
    std::unordered_map<uintptr_t, KfdPinAnchor> kfd_pin_anchors_;
    std::unordered_map<uintptr_t, uintptr_t> kfd_pin_anchor_of_buffer_;
};

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;

}  // namespace mooncake

#endif  // RDMA_TRANSPORT_H_
