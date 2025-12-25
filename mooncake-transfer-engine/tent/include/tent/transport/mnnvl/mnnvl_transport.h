// Copyright 2025 KVCache.AI
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

#ifndef MNNVL_TRANSPORT_H_
#define MNNVL_TRANSPORT_H_

#include <cuda.h>
#include <cuda_runtime.h>

#include <functional>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct MnnvlTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    int cuda_id = 0;
};

struct MnnvlSubBatch : public Transport::SubBatch {
    std::vector<MnnvlTask> task_list;
    size_t max_size;
    cudaStream_t stream;
    virtual size_t size() const { return task_list.size(); }
};

class MnnvlTransport : public Transport {
   public:
    MnnvlTransport();

    ~MnnvlTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "mnnvl"; }

    virtual Status allocateLocalMemory(void **addr, size_t size,
                                       MemoryOptions &options);

    virtual Status freeLocalMemory(void *addr, size_t size);

   private:
    void startTransfer(MnnvlTask *task, MnnvlSubBatch *batch);

    void *createSharedMemory(const std::string &path, size_t size);

    Status relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length,
                                       uint64_t target_id);

    Status setPeerAccess();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;

    struct OpenedMnnvlEntry {
        void *mnnvl_addr;
        uint64_t length;
        int cuda_id;
    };

    using HashMap =
        std::unordered_map<SegmentID,
                           std::unordered_map<uint64_t, OpenedMnnvlEntry>>;

    RWSpinlock relocate_lock_;
    HashMap relocate_map_;
    std::shared_ptr<Config> conf_;

    std::string machine_id_;

    std::mutex allocate_mutex_;
    std::unordered_set<void *> allocate_set_;
    uint64_t async_memcpy_threshold_;
    bool supported_;
    CUmemAllocationHandleType handle_type_;

    bool host_register_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // MNNVL_TRANSPORT_H_