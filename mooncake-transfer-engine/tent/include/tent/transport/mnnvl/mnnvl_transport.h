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

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {

struct MnnvlCompletion {
    MnnvlCompletion() = default;
    MnnvlCompletion(const MnnvlCompletion &) = delete;
    MnnvlCompletion &operator=(const MnnvlCompletion &) = delete;
    cudaEvent_t event{nullptr};
    int device{-1};
    cudaError_t result{cudaErrorNotReady};
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

    virtual Status quiesce();

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

    bool strictFabricEgm() const {
        return installed_ && egm_enabled_ && egm_strict_ &&
               handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC;
    }
    Status getEgmNumaNodes(std::vector<int> &nodes, bool nearest);
    Status getEgmGranularity(int node, size_t &granularity);

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

    static Status ensureCudaContext();

    Status getLocalAllocationRange(uint64_t address, size_t length,
                                   uint64_t &base, size_t &size);

    Status setPeerAccess();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    CudaPlatform *platform_{nullptr};

    struct OpenedMnnvlEntry {
        void *mnnvl_addr = nullptr;
        uint64_t length = 0;  // Logical range used by cache lookups.
        uint64_t mapped_length = 0;
        uint64_t mapping_offset = 0;
        int cuda_id = -1;
        // Handle the mapping was imported from; a peer that re-registers the
        // same address publishes a different one.
        std::string mnnvl_handle;
    };

    Status importRemoteBuffer(const BufferDesc &buffer,
                              OpenedMnnvlEntry &entry);

    static void releaseImportedEntry(const OpenedMnnvlEntry &entry);

    using HashMap =
        std::unordered_map<SegmentID,
                           std::unordered_map<uint64_t, OpenedMnnvlEntry>>;

    RWSpinlock relocate_lock_;
    HashMap relocate_map_;
    // Tags thread-local relocation caches; renewed whenever relocate_map_
    // changes and on uninstall.
    std::atomic<uint64_t> relocate_epoch_;
    // Mappings replaced by a re-imported peer buffer. In-flight copies may
    // still target them, so they are only unmapped by uninstall().
    std::vector<OpenedMnnvlEntry> retired_entries_;
    std::shared_ptr<Config> conf_;

    std::string machine_id_;

    std::mutex batch_mutex_;
    std::unordered_set<MnnvlSubBatch *> active_batches_;
    // Each POSIX export has its own published pid-fd handle. Different
    // registrations can share a VMM allocation without sharing an FD.
    // Guarded by registration_mutex_.
    std::unordered_map<std::string, int> exported_fds_;

    std::mutex allocate_mutex_;
    struct LocalAllocation {
        void *addr = nullptr;
        size_t size = 0;
        CUmemGenericAllocationHandle handle = 0;
        bool mapped = false;
    };
    // Retain every unfinished VMM cleanup stage, including early failures.
    std::unordered_map<void *, LocalAllocation> allocate_set_;
    std::vector<LocalAllocation> failed_allocations_;
    static Status releaseLocalAllocation(LocalAllocation &allocation);
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
    // transports/mnnvl/egm_strict: host memory must really be EGM. Host
    // allocation fails instead of falling back to plain memory, and an
    // explicit MNNVL registration (MemoryOptions::type == MNNVL) of a host
    // buffer that cannot be exported fails instead of staying local-only.
    bool egm_strict_ = false;
};
}  // namespace tent
}  // namespace mooncake

#endif  // MNNVL_TRANSPORT_H_
