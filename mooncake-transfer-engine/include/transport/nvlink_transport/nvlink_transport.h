// Copyright 2024 KVCache.AI

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include "cuda_alike.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
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

    void finalizeTransferResult(TransferTask& task, bool success) override;

    void appendMetrics(std::string& output) override;

    static void* allocatePinnedLocalMemory(size_t length);

    static void freePinnedLocalMemory(void* addr);

    [[nodiscard]] bool isFabricMemoryEnabled() const { return use_fabric_mem_; }

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

    enum class ConsumerFailureStage {
        IMPORT,
        RESERVE,
        MAP,
        SET_ACCESS,
        COPY,
    };

    struct ConsumerMetrics;

    void observeCacheLookup(bool hit);
    void observeLazyImportLatency(uint64_t duration_us);
    void observeConsumerFailure(ConsumerFailureStage stage);
    void observeTransferResult(TransferRequest::OpCode operation, bool success);
    bool observeTransferResultOnce(TransferTask& task, bool success);
    void finalizeSubmissionFailure(TransferTask& task, bool copy_failure);

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
    bool CleanupFabricMapping(FabricMappingCleanup& cleanup,
                              const char* failure_stage) noexcept;
    bool RetryQuarantinedFabricMappings() noexcept;
    void ReleaseOrQuarantineFabricMapping(FabricMappingCleanup cleanup,
                                          const char* failure_stage) noexcept;
    static void PreserveProcessLifetimeFabricCleanup(
        FabricMappingCleanup cleanup) noexcept;
    std::vector<FabricMappingCleanup> quarantined_fabric_mappings_;
    class RetainedHandleGuard;
    bool RetryQuarantinedRetainedHandles();
    void ReleaseOrQuarantineRetainedHandle(CUmemGenericAllocationHandle handle,
                                           const char* failure_stage) noexcept;
    std::vector<uint64_t> quarantined_retained_handles_;
    static bool TrackPinnedVmmAllocation(
        std::unique_ptr<NvlinkVmmAllocation> owner);
    static bool ReleasePinnedVmmAllocation(void* ptr);
#endif

    std::function<int(const BufferDesc&, bool)> add_buffer_for_testing_;
    std::function<int(void*, bool)> remove_buffer_for_testing_;
    std::function<std::shared_ptr<SegmentDesc>(uint64_t)>
        get_segment_for_testing_;
    std::unique_ptr<ConsumerMetrics> consumer_metrics_;
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
