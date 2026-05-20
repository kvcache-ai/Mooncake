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

#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>

#include "tent/transport/ibgda/detail/memheap.h"
#include "tent/transport/ibgda/detail/mlx5gda.h"
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

    // Initialize the verbs resources that are shared by host-side QP setup and
    // device-side IBGDA resources.  This is the first ownership slice moved out
    // of mooncake-ep: NIC selection, GID discovery, context open, and PD setup.
    Status initializeDevice(const std::string& device_name,
                            uint8_t port_num = 1);

    ibv_context* verbsContext() const { return ctx_; }
    ibv_pd* protectionDomain() const { return pd_; }
    const mlx5dv_pd& mlx5ProtectionDomain() const { return mpd_; }
    ibv_mr* memoryRegion() const { return mr_; }
    const ibv_gid& gid() const { return gid_; }
    int gidIndex() const { return gid_index_; }
    bool isRoce() const { return is_roce_; }
    uint8_t portNum() const { return port_num_; }

    // Allocate the GPU-visible control buffer used by IBGDA DevX queues and
    // register it as a DevX umem.  The allocation currently uses CUDA, but the
    // ownership boundary intentionally lives in TENT so this can later dispatch
    // to other platform backends without changing mooncake-ep.
    Status allocateControlBuffer(size_t size);

    Status releaseControlBuffer();

    void* controlBuffer() const { return ctrl_buf_; }
    size_t controlBufferSize() const { return ctrl_buf_size_; }
    mlx5dv_devx_umem* controlBufferUmem() const { return ctrl_buf_umem_; }

    // Create/destroy the GPU-initiated RC QPs backed by the TENT-owned control
    // buffer.  This is still mlx5/CUDA today, but EP only consumes the stable
    // handles and device-context bytes so future TENT backends can replace the
    // queue builder behind this boundary.
    Status createQueuePairs(int num_qps, int wqe, cudaStream_t stream,
                            void* qp_devctxs);

    Status recreateQueuePairs(int num_qps, int wqe, cudaStream_t stream,
                              void* qp_devctxs);

    Status destroyQueuePairs();

    Status connectQueuePair(int qp_index, const ibv_ah_attr& ah_attr,
                            uint32_t remote_qpn, ibv_mtu mtu);

    // Consume exchanged per-rank IBGDA metadata, connect QPs in deterministic
    // QP-index order, and publish remote addresses/keys into the GPU-visible
    // device context arrays.  The actual metadata exchange can still be driven
    // by the caller during the migration window, but the interpretation of that
    // metadata and the queue bring-up state machine lives in TENT.
    Status connectPeers(const std::vector<int64_t>& remote_addrs,
                        const std::vector<int32_t>& remote_keys,
                        const std::vector<std::vector<int32_t>>& peer_qpns,
                        const std::vector<std::vector<int32_t>>& peer_lids,
                        const std::vector<int64_t>& subnet_prefixes,
                        const std::vector<int64_t>& interface_ids,
                        const std::vector<int>& active_ranks_mask,
                        int rank, int num_ranks, void* raddrs,
                        void* rkeys);

    int queuePairCount() const { return static_cast<int>(qps_.size()); }
    uint32_t queuePairQpn(int qp_index) const;
    uint16_t queuePairLid(int qp_index) const;

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

    std::string device_name_;
    uint8_t port_num_ = 1;
    ibv_context* ctx_ = nullptr;
    ibv_pd* pd_ = nullptr;
    mlx5dv_pd mpd_ = {};
    ibv_mr* mr_ = nullptr;
    ibv_gid gid_ = {};
    int gid_index_ = -1;
    bool is_roce_ = false;
    std::unordered_map<void*, ibv_mr*> registered_mrs_;

    void* ctrl_buf_ = nullptr;
    size_t ctrl_buf_size_ = 0;
    mlx5dv_devx_umem* ctrl_buf_umem_ = nullptr;

    memheap* ctrl_buf_heap_ = nullptr;
    std::vector<mlx5gda_qp*> qps_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_IBGDA_TRANSPORT_H_
