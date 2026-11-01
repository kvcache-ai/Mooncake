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

#ifndef HYLINK_TRANSPORT_H_
#define HYLINK_TRANSPORT_H_

#include <hip/hip_runtime.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/concurrent/ticket_lock.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

// TENT hylink transport for Hygon DCU memory over HSL (Hygon Scale-up
// Link). HSL connects DCUs directly, including across machines, and does
// not use an IB/RoCE NIC. By default peers exchange a DTK VMM fabric
// handle (allocation type 0x8, 512-byte blob). Set MC_HYLINK_USE_VMM=0 on
// both sides to use HIP IPC instead. IPC cannot cross machines.
constexpr auto kDtkFabricHandleType =
    static_cast<hipMemAllocationHandleType>(0x8);
constexpr size_t kDtkFabricHandleBytes = 512;
using DtkFabricHandle = std::array<unsigned char, kDtkFabricHandleBytes>;

enum class HylinkHandleKind { kNone, kIpc, kFabric };

struct HylinkTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    // Device of the source buffer. The remote handle import, the copy and
    // the completion event all run under this device's context.
    int device_id = -1;
    hipEvent_t completion_event = nullptr;
};

struct HylinkSubBatch : public Transport::SubBatch {
    std::vector<HylinkTask> task_list;
    size_t max_size = 0;
    ~HylinkSubBatch() {
        for (auto& task : task_list) {
            if (task.completion_event)
                (void)hipEventDestroy(task.completion_event);
        }
    }
    size_t size() const override { return task_list.size(); }
};

class HylinkTransport : public Transport {
   public:
    HylinkTransport();
    ~HylinkTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "hylink"; }

    // VMM allocation exportable through the DTK fabric handle type, for
    // buffers that must be reachable from other machines. Returns nullptr
    // when fabric VMM is unavailable. With MC_HYLINK_USE_VMM=0, allocates
    // via hipMalloc for the IPC path. Free with freeFabricMemory().
    static void* allocateFabricMemory(size_t size);
    static void freeFabricMemory(void* addr);

   private:
    struct ExportedHandle {
        bool has_ipc = false;
        bool has_fabric = false;
        hipIpcMemHandle_t ipc{};
        DtkFabricHandle fabric{};
    };

    struct OpenedMapping {
        void* shm_addr = nullptr;
        uint64_t length = 0;
        HylinkHandleKind kind = HylinkHandleKind::kNone;
        hipMemGenericAllocationHandle_t fabric_handle = nullptr;
    };

    // Per-device copy streams, created lazily on first use (mirrors the
    // classic hip transport's stream pool: every task runs on the device
    // of its source buffer).
    hipStream_t acquireStream(int device_id);
    // Issues the task's copy and records its completion event. Must be
    // called with task.device_id as the current device.
    Status submitTaskCopy(HylinkTask& task);
    // Handles one request end to end under the source buffer's device
    // context: validate, open the remote handle, submit the copy. Mirrors
    // the classic hip transport's startAsyncTransfer.
    Status startAsyncTransfer(const Request& request,
                              const SegmentDescRef& local_segment,
                              HylinkTask& task);

    Status relocateSharedMemoryAddress(uint64_t& dest_addr, uint64_t length,
                                       uint64_t target_id, int device_id,
                                       HylinkHandleKind* kind = nullptr);

    Status setPeerAccess();

    Status exportDeviceBuffer(BufferDesc& desc, const LocationParser& location);

    static std::string encodeExport(const ExportedHandle& exported);
    static bool decodeExport(const std::string& encoded,
                             ExportedHandle& exported);

    void closeMapping(OpenedMapping& mapping);

    std::string machine_id_;
    bool installed_ = false;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;

    // target -> remote buffer -> local device. Fabric uses device_id so
    // each GPU imports and maps its own VA. IPC uses key -1.
    using RelocateMap = std::unordered_map<
        SegmentID,
        std::unordered_map<uint64_t, std::unordered_map<int, OpenedMapping>>>;

    RWSpinlock relocate_lock_;
    RelocateMap relocate_map_;
    // Bumped on uninstall so per-thread copies of relocate_map_ drop mappings
    // whose HIP handles have already been closed.
    std::atomic<uint64_t> relocate_epoch_{1};

    static constexpr int kStreamsPerDevice = 8;
    std::mutex stream_pool_mutex_;
    std::unordered_map<int, std::vector<hipStream_t>> stream_pools_;
    std::unordered_map<int, size_t> stream_cursors_;

    mutable std::mutex register_mutex_;
    // hipMalloc / VMM base -> serialized export blob. Sub-allocations inside
    // one physical allocation reuse the same handle.
    std::unordered_map<uint64_t, std::string> registered_base_addrs_;

    // Pointers handed out by allocateFabricMemory() that are VMM fabric
    // allocations (as opposed to hipMalloc fallbacks). Static because the
    // allocator entry points are static.
    static std::mutex fabric_alloc_mutex_;
    static std::unordered_set<void*> fabric_allocs_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // HYLINK_TRANSPORT_H_
