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

#ifndef NVLINK_TRANSPORT_H_
#define NVLINK_TRANSPORT_H_

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/concurrent/ticket_lock.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {

struct NVLinkTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    bool is_cuda_ipc;
    cudaEvent_t completion_event = nullptr;
};

struct NVLinkSubBatch : public Transport::SubBatch {
    std::vector<NVLinkTask> task_list;
    size_t max_size;
    CUDAStreamHandle sync_stream;
    CUDAStreamHandle async_stream;
    int stream_device_id = -1;
    // Completion events created in startTransfer (one per submit). Destroyed by
    // the destructor (RAII); Slab<T>::deallocate() invokes ~NVLinkSubBatch()
    // before reusing the storage, so this runs on every free.
    std::vector<cudaEvent_t> completion_events;
    ~NVLinkSubBatch() {
        for (auto event : completion_events) cudaEventDestroy(event);
    }
    virtual size_t size() const { return task_list.size(); }
};

class NVLinkTransport : public Transport {
   public:
    NVLinkTransport();

    ~NVLinkTransport();

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

    const char *getName() const override { return "nvlink"; }
    bool supportsGlobalAddressing() const override { return true; }
    Status allocateLocalMemory(void **addr, size_t size,
                               MemoryOptions &options) override;
    Status freeLocalMemory(void *addr, size_t size) override;
    Status mapRemoteMemory(SegmentID peer, void *address, size_t size) override;
    Status unmapRemoteMemory(SegmentID peer, void *address) override;

   private:
    void startTransfer(std::vector<NVLinkTask *> &tasks, NVLinkSubBatch *batch);

    Status relocateSharedMemoryAddress(uint64_t &address, uint64_t length,
                                       uint64_t target_id);
    Status getOrMapRemoteMemory(uint64_t &address, uint64_t length,
                                SegmentID peer, bool fixed_address);

    Status setPeerAccess();

    friend class NVLinkTransportTestPeer;

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    CudaPlatform *platform_;

    using Memory = std::shared_ptr<CudaSharedMemory>;
    using HashMap =
        std::unordered_map<SegmentID, std::unordered_map<uint64_t, Memory>>;

    RWSpinlock relocate_lock_;
    HashMap relocate_map_;
    std::shared_ptr<Config> conf_;

    std::string machine_id_;
    uint64_t async_memcpy_threshold_;
    bool host_register_;

    std::mutex allocate_mutex_;
    std::unordered_map<void *, Memory> allocate_set_;
    std::unordered_map<uint64_t, Memory> registered_memory_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // NVLINK_TRANSPORT_H_
