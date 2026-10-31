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
#include "tent/common/cuda_fabric_compat.h"

#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {

class MnnvlCompletionEventPool {
    friend class MnnvlTransportTestPeer;

   public:
    explicit MnnvlCompletionEventPool(size_t capacity) : capacity_(capacity) {}
    ~MnnvlCompletionEventPool();
    Status acquire(int device, cudaEvent_t &event);
    void release(int device, cudaEvent_t event, bool completed);

   private:
    std::mutex mutex_;
    size_t capacity_;
    std::unordered_map<int, std::vector<cudaEvent_t>> idle_;
};

struct MnnvlCompletion {
    MnnvlCompletion() = default;
    MnnvlCompletion(const MnnvlCompletion &) = delete;
    MnnvlCompletion &operator=(const MnnvlCompletion &) = delete;
    std::shared_ptr<MnnvlCompletionEventPool> pool;
    cudaEvent_t event{nullptr};
    int device{-1};
    cudaError_t result{cudaErrorNotReady};
    bool recorded{false};
    int last_polled_task{-1};
    ~MnnvlCompletion();
};

struct MnnvlTask {
    Request request;
    TransferStatusEnum status_word{PENDING};
    size_t transferred_bytes{0};
    uint64_t target_addr = 0;
    size_t completion_group{0};
};

struct MnnvlSubBatch : public Transport::SubBatch {
    std::vector<MnnvlTask> task_list;
    size_t max_size{0};
    CUDAStreamHandle sync_stream;
    CUDAStreamHandle async_stream;
    int stream_device_id = -1;
    mutable std::mutex mutex;
    bool shared_polling{false};
    // One independently owned event per submission, even for appended work.
    std::vector<std::unique_ptr<MnnvlCompletion>> completion_groups;
    ~MnnvlSubBatch();
    size_t size() const override {
        std::lock_guard<std::mutex> lock(mutex);
        return task_list.size();
    }
};

class MnnvlTransport : public Transport {
    friend class MnnvlTransportTestPeer;

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
    bool tracksLocalBuffer(const BufferDesc &desc) const override;

    virtual const char *getName() const { return "mnnvl"; }

    virtual Status allocateLocalMemory(void **addr, size_t size,
                                       MemoryOptions &options);

    virtual Status freeLocalMemory(void *addr, size_t size);

   private:
    void startTransfer(std::vector<MnnvlTask *> &tasks,
                       std::vector<void *> &srcs, std::vector<void *> &dsts,
                       std::vector<size_t> &sizes, MnnvlSubBatch *batch,
                       MnnvlCompletion &completion);

    void *createSharedMemory(const std::string &path, size_t size);

    Status relocateSharedMemoryAddress(uint64_t &dest_addr, uint64_t length,
                                       uint64_t target_id);

    Status setPeerAccess();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    CudaPlatform *platform_{nullptr};

    struct OpenedMnnvlEntry {
        void *mnnvl_addr;
        uint64_t length;  // Logical range used by cache lookups.
        uint64_t mapped_length;
        uint64_t mapping_offset;
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
    // VMM allocations owned by this transport: base address -> mapped extent
    // (rounded to the allocation granularity), needed to unmap on free.
    std::unordered_map<void *, size_t> allocate_set_;
    uint64_t async_memcpy_threshold_;
    bool supported_;
    CUmemAllocationHandleType handle_type_;

    bool host_register_{true};
    mutable std::mutex registration_mutex_;
    struct HostPin {
        size_t length;
        std::shared_ptr<const uint8_t> owner;
    };
    std::unordered_map<uint64_t, HostPin> pinned_host_buffers_;
    // transports/mnnvl/egm: export HOST_NUMA (EGM) allocations so peers can
    // read/write host memory over NVLink; adds dram_to_dram / gpu_to_dram.
    bool egm_enabled_ = false;
    MnnvlConfig options_;
    std::shared_ptr<MnnvlCompletionEventPool> completion_pool_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // MNNVL_TRANSPORT_H_
