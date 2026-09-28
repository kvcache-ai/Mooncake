// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_HYLINK_TRANSPORT_H_
#define MOONCAKE_HYLINK_TRANSPORT_H_

#include <hip/hip_runtime.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class TransferMetadata;

// Hylink transport for Hygon DCU memory over HSL (Hygon Scale-up Link).
// HSL connects DCUs directly, including across machines, and does not use
// an IB/RoCE NIC. Peers exchange a DTK VMM fabric handle (allocation type
// 0x8, 512 bytes). An imported fabric address can be granted access only
// once and only to one device, so each GPU maps its own address.
class HylinkTransport : public Transport {
   public:
    HylinkTransport();

    ~HylinkTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    // hipMemCreate memory exportable through the DTK fabric handle. Returns
    // nullptr when fabric VMM is unavailable. Free with freeFabricMemory().
    static void* allocateFabricMemory(size_t size);
    static void freeFabricMemory(void* addr);

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

    const char* getName() const override { return "hylink"; }

   private:
    struct OpenedMapping {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        hipMemGenericAllocationHandle_t fabric_handle = nullptr;
    };

    struct PendingTransfer {
        hipEvent_t event;
        int device_id;
        Slice* slice;
    };

    Status startAsyncTransfer(const TransferRequest& request,
                              TransferTask& task, PendingTransfer& pending);
    void synchronizePendingTransfers(
        std::vector<PendingTransfer>& pending_transfers);

    hipStream_t acquireStream(int device_id);
    void closeMapping(OpenedMapping& mapping);

    // target -> remote buffer -> local device. An imported fabric VA accepts
    // hipMemSetAccess only once and only for one device, so each GPU maps
    // its own address.
    using RelocateMap = std::unordered_map<
        uint64_t,
        std::unordered_map<uint64_t, std::unordered_map<int, OpenedMapping>>>;

    RWSpinlock remap_lock_;
    RelocateMap remap_;

    std::mutex register_mutex_;
    std::unordered_map<uint64_t, std::string> registered_exports_;

    static constexpr int kStreamsPerDevice = 8;
    std::mutex stream_pool_mutex_;
    std::unordered_map<int, std::vector<hipStream_t>> stream_pools_;
    std::unordered_map<int, size_t> stream_cursors_;

    static std::mutex fabric_alloc_mutex_;
    static std::unordered_set<void*> fabric_allocs_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HYLINK_TRANSPORT_H_
