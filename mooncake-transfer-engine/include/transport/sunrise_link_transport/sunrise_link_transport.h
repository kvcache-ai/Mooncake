// Copyright 2024 KVCache.AI

#ifndef SUNRISE_LINK_TRANSPORT_H_
#define SUNRISE_LINK_TRANSPORT_H_

#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/hash_utils.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

class SunriseLinkTransport : public Transport {
   public:
    SunriseLinkTransport();

    ~SunriseLinkTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    static int parseDeviceIdForTest(const std::string& location);
    static bool requestRangeFitsInBufferForTest(uint64_t dest_addr,
                                                uint64_t length,
                                                uint64_t buffer_addr,
                                                uint64_t buffer_length);

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

    const char* getName() const override { return "sunrise_link"; }

   private:
    struct OpenedShmEntry {
        void* shm_addr;
        uint64_t length;
        int gpu_id;
        bool is_raw_addr = false;
    };

    int relocateSharedMemoryAddress(uint64_t& dest_addr, uint64_t length,
                                    uint64_t target_id, int* target_gpu_id);
    int submitRequest(TransferTask& task, const TransferRequest& request,
                      bool keep_slice_ref);
    int startCopy(void* src, void* dst, size_t length, int remote_dev,
                  int local_dev);
    int inferRegisteredDevice(void* ptr) const;
    int inferPointerDeviceBestEffort(void* ptr, int preferred_dev) const;
    bool loadRuntime();

    mutable std::mutex register_mutex_;
    struct RegisteredRegion {
        size_t length;
        int gpu_id;
    };
    std::unordered_map<void*, RegisteredRegion> registered_regions_;

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
};

}  // namespace mooncake

#endif  // SUNRISE_LINK_TRANSPORT_H_
