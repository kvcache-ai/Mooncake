// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <utility>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

class NvlinkTransport : public Transport {
   public:
    NvlinkTransport();

    ~NvlinkTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    static void* allocatePinnedLocalMemory(size_t length);

    static void freePinnedLocalMemory(void* addr);

    // Release all import mappings this process holds for a remote segment,
    // reclaiming the exporter's pinned pages at runtime. Returns the count
    // actually released (all backing CUDA calls succeeded), not merely erased.
    // Caller must ensure no transfer to target_id is in flight.
    int unregisterRemoteSegment(SegmentID target_id) override;

    // Release every imported segment idle (no write) longer than ttl_s; returns
    // the count actually reclaimed. A peer that goes down by any means stops
    // being written to and ages out, so no explicit release message is needed.
    int evictIdleSegments(double ttl_s);

   protected:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata = true) override;

    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    int relocateSharedMemoryAddress(uint64_t& dest_addr, uint64_t length,
                                    uint64_t target_id);

    const char* getName() const override { return "nvlink"; }

   private:
    std::atomic_bool running_{false};

    struct OpenedShmEntry {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        // Imported fabric segments only: handle from
        // cuMemImportFromShareableHandle, kept alive for the mapping's lifetime
        // so releaseImportedEntry can drop it via cuMemRelease (it cannot be
        // recovered from an imported allocation). Unused on the cudaIpc path.
        CUmemGenericAllocationHandle import_handle{};
        // Last write activity (steady_clock ns), stamped by
        // relocateSharedMemoryAddress under the remap_lock_ shared lock and
        // read by evictIdleSegments under the write lock. Per-entry + atomic so
        // the hot path stamps a relaxed store with no extra mutex; 0 = not yet
        // stamped. The lease dies with the entry, so there is no separate map
        // to grow or clean up.
        std::atomic<int64_t> last_active_ns{0};
        OpenedShmEntry() = default;
        // std::atomic is non-copyable; provide value copies so the entry can
        // live in the map and be collected into a release vector.
        OpenedShmEntry(const OpenedShmEntry& o)
            : shm_addr(o.shm_addr),
              length(o.length),
              import_handle(o.import_handle),
              last_active_ns(o.last_active_ns.load(std::memory_order_relaxed)) {
        }
        OpenedShmEntry& operator=(const OpenedShmEntry& o) {
            shm_addr = o.shm_addr;
            length = o.length;
            import_handle = o.import_handle;
            last_active_ns.store(
                o.last_active_ns.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
            return *this;
        }
    };

    // Tear down one imported mapping (unmap + free VA + release the stored
    // handle, or cudaIpcCloseMemHandle on the IPC path), reclaiming the
    // exporter's pinned pages. True only if every backing release succeeded.
    bool releaseImportedEntry(const OpenedShmEntry& entry);

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;

    std::mutex register_mutex_;

    // Background evictor: every evict_interval_s_ it makes worker_ctx_ current
    // (cuMem* are context-bound) and runs evictIdleSegments. Off when ttl <= 0.
    void evictLoop();
    // CUDA context the imports were created in, captured on first import; the
    // evictor/dtor must make it current before cuMem* (a std::thread has none).
    std::atomic<CUcontext> worker_ctx_{nullptr};
    std::thread evict_thread_;
    std::condition_variable evict_cv_;
    std::mutex evict_cv_mutex_;
    double import_ttl_s_ = 0.0;       // <=0 disables eviction
    double evict_interval_s_ = 30.0;  // scan cadence
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
