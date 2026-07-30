// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/nvlink_transport/nvlink_host_numa_allocation.h"
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
                                    uint64_t target_id);

    const char* getName() const override { return "nvlink"; }

   private:
    friend class NvlinkTransportTestPeer;

    std::atomic_bool running_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
    };

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;

    std::mutex register_mutex_;
#if defined(USE_MNNVL) && defined(USE_CUDA) && CUDA_VERSION >= 12030
    using AddLocalMemoryBufferOp = std::function<int(const BufferDesc&, bool)>;
    using RemoveLocalMemoryBufferOp = std::function<int(void*, bool)>;
    using UpdateLocalSegmentDescOp = std::function<int()>;

    struct HostNumaRegistration {
        CUmemGenericAllocationHandle handle;
        NvlinkHostNumaAllocation::DriverApi api;
        bool metadata_removed_locally = false;
        bool metadata_cleanup_complete = false;
    };
    std::unordered_map<void*, HostNumaRegistration>
        host_numa_registration_handles_;

    int registerHostNumaMemoryLocked(
        void* addr, size_t length, const std::string& location,
        bool update_metadata,
        const NvlinkHostNumaAllocation::DriverApi& driver_api,
        const AddLocalMemoryBufferOp& add_buffer,
        const RemoveLocalMemoryBufferOp& remove_buffer,
        const UpdateLocalSegmentDescOp& update_segment);
    int unregisterHostNumaMemoryLocked(
        void* addr, bool update_metadata,
        const RemoveLocalMemoryBufferOp& remove_buffer,
        const UpdateLocalSegmentDescOp& update_segment);
#endif
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
