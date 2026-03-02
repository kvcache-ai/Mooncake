// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
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

#ifndef UBSHMEM_TRANSPORT_H_
#define UBSHMEM_TRANSPORT_H_

#include <acl/acl.h>

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
#include "transport/ascend_transport/ubshmem_transport/stream_pool.h"

namespace mooncake {

class TransferMetadata;

class UBShmemTransport : public Transport {
   public:
    UBShmemTransport();

    ~UBShmemTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    static void *allocatePinnedLocalMemory(size_t length);

    static void freePinnedLocalMemory(void *addr);

   private:
    void submitSlices(std::vector<Slice *> &slice_list);

   protected:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata = true) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    int relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length,
                                    uint64_t target_id);

    const char *getName() const override { return "ubshmem"; }

   private:
    struct OpenedShmEntry {
        void *shm_addr;
        uint64_t length;
        char *key;
    };

    void workerThread();

    void initializeThreadPool();

    void stopThreadPool();

    std::unordered_map<std::pair<uint64_t, uint64_t>, OpenedShmEntry, PairHash>
        remap_entries_;
    RWSpinlock remap_lock_;
    bool use_fabric_mem_{true};

    std::mutex register_mutex_;
    std::unique_ptr<StreamPool> stream_pool_;

    bool running_{false};
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_queue_cv_;

    int num_streams_per_transfer_;  // Number of streams per transfer (default:
                                    // 4)
    size_t thread_pool_size_;       // Thread pool size (default: 8)
    uint64_t stream_pool_timeout_ms_;  // Get stream timeout in milliseconds
                                       // (default: 3000)

    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
};

}  // namespace mooncake

#endif  // UBSHMEM_TRANSPORT_H_
