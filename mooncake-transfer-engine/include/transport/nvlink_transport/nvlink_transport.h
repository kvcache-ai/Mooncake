// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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
                                    uint64_t target_id,
                                    uint64_t& mapping_base_addr);

    const char* getName() const override { return "nvlink"; }

   private:
    std::atomic_bool running_{false};

    struct ReleaseState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        bool failed = false;
    };

    struct OpenedShmEntry {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        uint64_t target_id = 0;
        uint64_t mapping_base_addr = 0;
        CUmemGenericAllocationHandle import_handle{};
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        CUcontext import_context = nullptr;
#endif
        int device_id = -1;
        bool mapped = false;
        bool releasing = false;
        std::shared_ptr<ReleaseState> release_state;
        bool handle_valid = false;
        bool address_reserved = false;
        std::atomic<int64_t> last_active_ns{0};
        std::atomic<uint64_t> in_flight{0};

        OpenedShmEntry() = default;
        OpenedShmEntry(const OpenedShmEntry& other)
            : shm_addr(other.shm_addr),
              length(other.length),
              target_id(other.target_id),
              mapping_base_addr(other.mapping_base_addr),
              import_handle(other.import_handle),
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HYGON) || \
    defined(USE_COREX)
              import_context(other.import_context),
#endif
              device_id(other.device_id),
              mapped(other.mapped),
              releasing(other.releasing),
              release_state(other.release_state),
              handle_valid(other.handle_valid),
              address_reserved(other.address_reserved),
              last_active_ns(
                  other.last_active_ns.load(std::memory_order_relaxed)),
              in_flight(other.in_flight.load(std::memory_order_relaxed)) {
        }
        OpenedShmEntry& operator=(const OpenedShmEntry& other) {
            shm_addr = other.shm_addr;
            length = other.length;
            target_id = other.target_id;
            mapping_base_addr = other.mapping_base_addr;
            import_handle = other.import_handle;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HYGON) || \
    defined(USE_COREX)
            import_context = other.import_context;
#endif
            device_id = other.device_id;
            mapped = other.mapped;
            releasing = other.releasing;
            release_state = other.release_state;
            handle_valid = other.handle_valid;
            address_reserved = other.address_reserved;
            last_active_ns.store(
                other.last_active_ns.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
            in_flight.store(other.in_flight.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
            return *this;
        }
    };

    bool releaseImportedEntry(OpenedShmEntry& entry);
    void releaseMappingRef(uint64_t target_id, uint64_t mapping_base_addr);
    void releaseSliceMappingRef(Slice* slice);
    int retryPendingReleases();
    int evictIdleSegments(double ttl_s);
    void evictLoop();

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    std::vector<OpenedShmEntry> pending_releases_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;

    std::mutex register_mutex_;
    std::thread evict_thread_;
    std::condition_variable evict_cv_;
    std::mutex evict_cv_mutex_;
    double import_ttl_s_ = 0.0;
    double evict_interval_s_ = 30.0;
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
