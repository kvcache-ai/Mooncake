// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.

#ifndef HIP_TRANSPORT_H_
#define HIP_TRANSPORT_H_

#include <hip/hip_runtime.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <string>
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

    const char* getName() const override { return "hip"; }

   private:
    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
    };

    // Enqueue the copies without waiting for them; getTransferStatus() polls
    // the recorded events.
    Status submitTasks(
        const std::vector<std::pair<TransferTask*, const TransferRequest*>>&
            tasks);

    // Enqueue one device's copies on one stream, followed by one event.
    void submitDeviceCopies(int device_id, const std::vector<Slice*>& slices);

    // With MC_HIP_WAIT_CALLER_STREAM: make stream wait for the null stream
    // of each device in this process that holds data of slices.
    bool waitForCallerStreams(int device_id, hipStream_t stream,
                              hipEvent_t event,
                              const std::vector<Slice*>& slices);

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_;
    // MC_HIP_WAIT_CALLER_STREAM: order copies after the caller's null stream.
    bool wait_caller_stream_;

    std::mutex register_mutex_;

    // Stream and event pools for async operations
    StreamPool stream_pool_;
    EventPool event_pool_;
};

}  // namespace mooncake

#endif  // HIP_TRANSPORT_H_
