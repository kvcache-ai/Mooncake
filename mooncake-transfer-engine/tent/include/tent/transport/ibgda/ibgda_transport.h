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

#ifndef TENT_IBGDA_TRANSPORT_H_
#define TENT_IBGDA_TRANSPORT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/device_transport.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

class Config;
class ControlService;
class Topology;

class IbGdaTransport : public Transport, public DeviceTransport {
   public:
    IbGdaTransport();
    ~IbGdaTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    const char* getName() const override { return "ibgda"; }

    DeviceTransport* asDeviceTransport() override { return this; }

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    Status symAlloc(void** ptr, size_t size) override;

    Status symFree(void* ptr) override;

    void* getRemotePtr(void* local_ptr, int dst_rank) override;

    Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                          uint32_t& rkey) override;

    Status unregisterMemory(void* ptr) override;

    Status getChannelResources(int channel_id,
                               DeviceChannelResources& resources) override;

    Status getPeerInfo(int rank, DevicePeerInfo& peer) override;

    Status connect(int rank) override;

    Status barrier() override;

    const DeviceCommCapabilities deviceCapabilities() const override;

    // Stage-C bridge: lets an existing IBGDA host setup hand a GPU-visible
    // backend context to the TENT DeviceTransport interface. The full Stage-D
    // transport will allocate and populate this context internally.
    Status adoptDeviceContext(void* network_ctx, int num_channels);

    Status adoptDeviceContext(IbGdaDeviceContext* network_ctx,
                              int num_channels);

    Status setPeerInfo(int rank, const DevicePeerInfo& peer);

   private:
    bool installed_ = false;
    std::string local_segment_name_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<Config> conf_;

    void* network_ctx_ = nullptr;
    int num_channels_ = 0;
    std::unordered_map<int, DevicePeerInfo> peer_info_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_IBGDA_TRANSPORT_H_
