// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.

#ifndef HIP_TRANSPORT_H_
#define HIP_TRANSPORT_H_

#include <hip/hip_runtime.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <utility>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include "transport/hip_transport/event_pool.h"
#include "transport/hip_transport/stream_pool.h"

namespace mooncake {

class TransferMetadata;

// GPU-to-GPU transport over mapped device memory. Each registered buffer is
// shared as one of (see hip_share_handle.h):
//   - HIP IPC handle (hipMalloc memory, same host),
//   - POSIX fd (VMM memory with MC_USE_HIP_IPC=0, same host),
//   - fabric handle (VMM memory exportable as hipMemHandleTypeFabric),
//     reachable from any host in the same UALink vPOD.
class HipTransport : public Transport {
   public:
    HipTransport();

    ~HipTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    int bindSegment(SegmentID segment_id, int local_device) override;

    void invalidateSegment(SegmentID segment_id) override;

    static void* allocatePinnedLocalMemory(size_t length);

    static void freePinnedLocalMemory(void* addr);

    // True when shm_name is a fabric export from the vPOD of a local device,
    // i.e. the buffer is reachable even though it lives on another host.
    static bool isFabricReachable(const std::string& shm_name);

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

    const char* getName() const override { return "hip"; }

   private:
    friend class HipTransportTestPeer;

    struct RemoteMapping;
    using MappingPtr = std::shared_ptr<RemoteMapping>;

    struct Prepared {
        Slice* slice = nullptr;
        int device_id = -1;
        void* dst = nullptr;
        const void* src = nullptr;
        bool device_to_device = false;
        MappingPtr mapping;
    };

    // Enqueue the copies without waiting for them; getTransferStatus() polls
    // the recorded events.
    Status submitRequests(
        const std::vector<std::pair<const TransferRequest*, TransferTask*>>&
            work);

    // Enqueue one device's copies on one stream (hipMemcpyBatchAsync for the
    // device-to-device ones, hipMemcpyAsync for the rest), followed by one
    // event.
    // Slices that could not be enqueued are failed and reported in the
    // returned status; the others stay tracked by the event.
    Status submitDeviceCopies(int device_id,
                              const std::vector<Prepared*>& items);

    // Lookups reused across the requests of one submit call, which usually
    // share the same local and remote buffers.
    struct PrepareCache {
        uint64_t src_begin = 0;
        uint64_t src_end = 0;
        int src_device = -1;
        int current_device = -1;
        uint64_t target_id = UINT64_MAX;
        std::shared_ptr<SegmentDesc> desc;
        uint64_t buf_addr = 0;
        uint64_t buf_length = 0;
        int buf_device = -1;
        MappingPtr mapping;
    };

    // Resolves the device and the mapped addresses of one request without
    // enqueuing anything.
    Status prepareRequest(const TransferRequest& request, Prepared& prepared,
                          PrepareCache& cache);

    int sourceDevice(const void* source, uint64_t length, PrepareCache& cache);

    int relocate(uint64_t& dest_addr, uint64_t length, uint64_t target_id,
                 int local_device, MappingPtr& mapping);

    int mapBuffer(uint64_t target_id, const BufferDesc& entry, int local_device,
                  MappingPtr& mapping);

    int openMapping(const BufferDesc& entry, int local_device,
                    MappingPtr& mapping);

    int grantAccess(RemoteMapping& mapping, int local_device);

    int exportBuffer(void* addr, size_t length, int device, BufferDesc& desc);

    // Callers hold register_mutex_.
    void addAlias(void* addr, size_t length, int device, uint64_t base);
    void removeAlias(void* addr);

    std::unordered_map<std::pair<uint64_t, uint64_t>, MappingPtr, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;
    bool fabric_export_;
    bool access_all_devices_;
    int num_devices_ = 0;
    bool memcpy_batch_;

    std::mutex register_mutex_;
    struct PublishedBuffer {
        size_t refs;
        uint64_t length;
    };
    // Registered address -> base of the buffer published for it; base ->
    // live alias count and published length. Sub-ranges of one allocation
    // share one published buffer, removed by the last unregister.
    std::unordered_map<uint64_t, uint64_t> registered_addr_to_base_;
    std::unordered_map<uint64_t, PublishedBuffer> registered_base_refs_;
    // Addresses registerLocalMemory deliberately skipped (e.g. host memory
    // left to other transports), distinguishing them from never-registered
    // ones at unregister time.
    std::unordered_set<uint64_t> skipped_addrs_;
    // Registered device memory: begin -> (end, device).
    RWSpinlock local_regions_lock_;
    std::map<uint64_t, std::pair<uint64_t, int>> local_regions_;

    // Stream and event pools for async operations
    StreamPool stream_pool_;
    EventPool event_pool_;
};

}  // namespace mooncake

#endif  // HIP_TRANSPORT_H_
