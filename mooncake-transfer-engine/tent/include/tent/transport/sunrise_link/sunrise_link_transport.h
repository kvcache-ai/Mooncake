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

#pragma once

#include <map>
#include <mutex>
#include <utility>
#include <vector>
#include <unordered_map>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct SunriseLinkTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
    bool is_tang_ipc = false;
    int tang_gpu_id = -1;
};

struct SunriseLinkSubBatch : public Transport::SubBatch {
    std::vector<SunriseLinkTask> task_list;
    size_t max_size = 0;
    /// Opaque `tangStream_t` when built with Tang; mirrors
    /// NVLinkSubBatch::stream.
    void* stream = nullptr;
    virtual size_t size() const { return task_list.size(); }
};

struct SunriseLinkDeviceInfo {
    int device_id;
    std::vector<int> active_ports;
};

struct SunriseLinkPeerInfo {
    int peer_device_id;
    int peer_port;
    bool is_active;
};

class SunriseLinkTransport : public Transport {
   public:
    SunriseLinkTransport();
    ~SunriseLinkTransport() override;

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

    const char* getName() const override { return "sunrise_link"; }

    bool supportNotification() const override { return true; }

   private:
    Status startTransfer(SunriseLinkTask* task, SunriseLinkSubBatch* batch);

    // SunriseLink initialization
    Status initSunriseLink();

    // Memory registration
    Status registerMemory(void* addr, size_t size);

    // Memory deregistration
    Status deregisterMemory(void* addr);

    // Topology detection
    Status detectTopology();
    Status relocateRemoteAddress(uint64_t& dest_addr, uint64_t length,
                                 uint64_t target_id,
                                 int* target_gpu_id = nullptr);

    // Runtime library handle
    void* runtime_lib_handle_ = nullptr;

    void* ptml_handle_ = nullptr;

    // Local device information
    std::vector<SunriseLinkDeviceInfo> local_devices_;

    // Topology map: device_id -> peer info
    std::map<int, std::vector<SunriseLinkPeerInfo>> topology_map_;

    // (local_chipid, remote_chipid) -> local PTLink port (S2 C2C)
    std::map<std::pair<int, int>, int> c2c_port_by_pair_;

    // Registered memory regions
    std::map<void*, size_t> registered_memory_;
    std::map<void*, int> registered_memory_gpu_id_;
    mutable std::mutex registered_memory_mutex_;

    struct OpenedIpcEntry {
        void* dev_ptr{nullptr};
        uint64_t length{0};
        int gpu_id{-1};
    };
    using IpcRelocateMap =
        std::unordered_map<SegmentID,
                           std::unordered_map<uint64_t, OpenedIpcEntry>>;
    RWSpinlock relocate_lock_;
    IpcRelocateMap relocate_map_;

    // Installed flag
    bool installed_ = false;

    // Config
    std::shared_ptr<Config> conf_;
    std::shared_ptr<ControlService> metadata_;

    uint64_t async_memcpy_threshold_ = 0;
};

}  // namespace tent
}  // namespace mooncake
