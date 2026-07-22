// Copyright 2024 KVCache.AI

#ifndef MUSA_TRANSPORT_H_
#define MUSA_TRANSPORT_H_

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
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

class MusaTransport : public Transport {
   public:
    MusaTransport();

    ~MusaTransport();

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

    const char* getName() const override { return "musa"; }

   private:
    void rememberStreamDevices(
        const std::vector<std::pair<cudaStream_t, int>>& streams);
    int streamDevice(cudaStream_t stream, const void* destination);

    std::atomic_bool running_;

    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
        int device_id{-1};
    };

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;

    std::mutex register_mutex_;
    std::mutex stream_device_mutex_;
    std::unordered_map<cudaStream_t, int> stream_devices_;
};

}  // namespace mooncake

#endif  // MUSA_TRANSPORT_H_
