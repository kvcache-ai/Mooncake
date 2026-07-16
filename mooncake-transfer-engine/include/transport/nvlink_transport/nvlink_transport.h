// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/nvlink_transport/nvlink_vmm_allocation.h"
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

    [[nodiscard]] bool supportsFabricMemoryTransport() const {
        return use_fabric_mem_;
    }

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

    enum class OpenedMappingKind { IPC, FABRIC };

    struct OpenedShmEntry {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        OpenedMappingKind kind = OpenedMappingKind::IPC;
    };

    struct LocalRegistration {
        void* requested_addr = nullptr;
        uint64_t requested_length = 0;
        void* mapped_base = nullptr;
        uint64_t mapped_length = 0;
        bool remote_accessible = false;
        bool published = false;
        bool retained_handle_owned = false;
        uint64_t retained_handle = 0;
        BufferDesc descriptor;
    };

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;

    std::mutex register_mutex_;
    std::unordered_map<void*, LocalRegistration> local_registrations_;
    int publishLocalRegistration(void* registration_addr,
                                 const BufferDesc& descriptor,
                                 bool update_metadata);
    int ensureLocalDescriptorPresent(const BufferDesc& descriptor);
    static void markSubmissionFailed(TransferTask& task);
    static bool supportsFabricMemory();
#if defined(USE_MNNVL) && defined(USE_CUDA)
    NvlinkVmmAllocation::DriverApi fabric_driver_api_;
    struct FabricMappingCleanup {
        CUmemGenericAllocationHandle handle = 0;
        CUdeviceptr address = 0;
        size_t length = 0;
        bool handle_owned = false;
        bool address_reserved = false;
        bool mapped = false;
    };
    class FabricMappingAttempt;
    bool cleanupFabricMapping(FabricMappingCleanup& cleanup,
                              const char* failure_stage) noexcept;
    bool retryQuarantinedFabricMappings() noexcept;
    void releaseOrQuarantineFabricMapping(FabricMappingCleanup cleanup,
                                          const char* failure_stage) noexcept;
    static void preserveProcessLifetimeFabricCleanup(
        FabricMappingCleanup cleanup) noexcept;
    std::vector<FabricMappingCleanup> quarantined_fabric_mappings_;
    class RetainedHandleGuard;
    bool retryQuarantinedRetainedHandles();
    void releaseOrQuarantineRetainedHandle(CUmemGenericAllocationHandle handle,
                                           const char* failure_stage) noexcept;
    std::vector<uint64_t> quarantined_retained_handles_;
    static bool trackPinnedVmmAllocation(
        std::unique_ptr<NvlinkVmmAllocation> owner);
    static bool releasePinnedVmmAllocation(void* ptr);
#endif
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
