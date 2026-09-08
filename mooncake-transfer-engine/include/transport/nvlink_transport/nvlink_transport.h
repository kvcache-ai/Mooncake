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
#include <vector>
#include <utility>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

// Backend-specific hooks for the CUDA-compatible IPC transport core.  The
// transfer bookkeeping and batched-copy algorithm stay in NvlinkTransport;
// vendors only decide which device owns an imported IPC mapping and which
// device stream submits a copy.
class GpuIpcTransportPolicy {
   public:
    virtual ~GpuIpcTransportPolicy() = default;

    virtual const char* protocol() const = 0;
    virtual const char* displayName() const = 0;

    virtual int selectStreamDevice(const void* local_buffer, const void* source,
                                   const void* destination) const = 0;

    // The legacy NVLink path submits one homogeneous batch on the stream
    // selected from its first local buffer.  MUSA can opt into per-device
    // grouping because its destination-owned stream rule may select a
    // different device for each entry.
    virtual bool groupTransfersByDevice() const { return false; }

    // CUDA's legacy path already runs with the caller's active device.  MUSA
    // needs an explicit guard around submit/query because the destination
    // stream can belong to another logical device.
    virtual bool requiresStreamDeviceGuard() const { return false; }

    // MUSA participates in the multi-protocol metadata format used alongside
    // TCP/RDMA.  Keep NVLink's legacy segment replacement behavior unchanged.
    virtual bool preserveExistingMetadata() const { return false; }

    // Resolve wildcard registrations to a vendor-specific location when the
    // runtime can identify the owning device. Legacy policies keep the
    // caller-provided location unchanged.
    virtual std::string normalizeMemoryLocation(
        const void* addr, const std::string& location) const {
        (void)addr;
        return location;
    }

    virtual cudaError_t openIpcMemHandle(void** address,
                                         cudaIpcMemHandle_t handle,
                                         const std::string& location,
                                         int& opened_device) const = 0;

    virtual cudaError_t closeIpcMemHandle(void* address,
                                          int opened_device) const = 0;

    // Return true when the policy submitted the whole copy group itself.  A
    // false return asks the shared core to use its CUDA-compatible fallback.
    // On a handled call, fail_index is count on success, an index when the
    // backend reports a partial parse failure, or SIZE_MAX for an unknown
    // failure.  Keeping this hook here lets MUSA use its low-CPU driver batch
    // API without duplicating the transfer bookkeeping in a second transport.
    virtual bool submitBatchCopies(const std::vector<void*>& srcs,
                                   const std::vector<void*>& dsts,
                                   const std::vector<size_t>& sizes,
                                   cudaStream_t stream,
                                   size_t& fail_index) const {
        (void)srcs;
        (void)dsts;
        (void)sizes;
        (void)stream;
        (void)fail_index;
        return false;
    }
};

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
    NvlinkTransport(std::shared_ptr<GpuIpcTransportPolicy> policy,
                    bool use_fabric_mem);

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

    const char* getName() const override { return policy_->protocol(); }

   private:
    std::atomic_bool running_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
        int device_id{-1};
    };

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;
    std::shared_ptr<GpuIpcTransportPolicy> policy_;

    std::mutex register_mutex_;
};

}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
