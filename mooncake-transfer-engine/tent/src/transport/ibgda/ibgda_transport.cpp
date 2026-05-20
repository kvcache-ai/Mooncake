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

#include "tent/transport/ibgda/ibgda_transport.h"

#include <arpa/inet.h>
#include <cuda_runtime.h>

#include <sstream>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

namespace {
bool ipv6AddrV4Mapped(const struct in6_addr* addr) {
    return ((addr->s6_addr32[0] | addr->s6_addr32[1]) == 0 &&
            addr->s6_addr32[2] == htonl(0x0000ffff));
}

int findBestGidIndex(ibv_context* ctx, uint8_t port,
                     ibv_port_attr& port_attr) {
    for (int i = 0; i < port_attr.gid_tbl_len; i++) {
        ibv_gid_entry gid_entry;
        int ret = ibv_query_gid_ex(ctx, port, i, &gid_entry, 0);
        if (ret) continue;

        bool is_v4mapped = ipv6AddrV4Mapped(
            reinterpret_cast<const struct in6_addr*>(gid_entry.gid.raw));
        if ((is_v4mapped && gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            gid_entry.gid_type == IBV_GID_TYPE_IB) {
            return i;
        }
    }
    return -1;
}

Status notImplemented(const char* method) {
    std::stringstream ss;
    ss << "IbGdaTransport::" << method
       << " is not implemented in Stage C scaffold" << LOC_MARK;
    return Status::NotImplemented(ss.str());
}
}  // namespace

IbGdaTransport::IbGdaTransport() {
    caps.gpu_to_gpu = true;
}

IbGdaTransport::~IbGdaTransport() { (void)uninstall(); }

Status IbGdaTransport::install(std::string& local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    local_segment_name_ = local_segment_name;
    metadata_ = std::move(metadata);
    local_topology_ = std::move(local_topology);
    conf_ = std::move(conf);
    installed_ = true;
    return Status::OK();
}

Status IbGdaTransport::uninstall() {
    (void)releaseControlBuffer();
    for (auto& entry : registered_mrs_) {
        if (entry.second) ibv_dereg_mr(entry.second);
    }
    registered_mrs_.clear();
    mr_ = nullptr;
    if (pd_) {
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
    }
    if (ctx_) {
        ibv_close_device(ctx_);
        ctx_ = nullptr;
    }
    installed_ = false;
    network_ctx_ = nullptr;
    num_channels_ = 0;
    peer_info_.clear();
    return Status::OK();
}

Status IbGdaTransport::allocateSubBatch(SubBatchRef&, size_t) {
    return notImplemented("allocateSubBatch");
}

Status IbGdaTransport::freeSubBatch(SubBatchRef&) {
    return notImplemented("freeSubBatch");
}

Status IbGdaTransport::submitTransferTasks(
    SubBatchRef, const std::vector<Request>&) {
    return notImplemented("submitTransferTasks");
}

Status IbGdaTransport::getTransferStatus(SubBatchRef, int,
                                         TransferStatus& status) {
    status = {TransferStatusEnum::FAILED, 0};
    return notImplemented("getTransferStatus");
}

Status IbGdaTransport::addMemoryBuffer(BufferDesc&, const MemoryOptions&) {
    return notImplemented("addMemoryBuffer");
}

Status IbGdaTransport::removeMemoryBuffer(BufferDesc&) {
    return notImplemented("removeMemoryBuffer");
}

Status IbGdaTransport::symAlloc(void** ptr, size_t size) {
    if (!ptr) return Status::InvalidArgument("ptr is null" LOC_MARK);
    auto err = cudaMalloc(ptr, size);
    if (err != cudaSuccess) {
        return Status::CudaError(std::string("cudaMalloc failed: ") +
                                 cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
}

Status IbGdaTransport::symFree(void* ptr) {
    if (!ptr) return Status::OK();
    auto err = cudaFree(ptr);
    if (err != cudaSuccess) {
        return Status::CudaError(std::string("cudaFree failed: ") +
                                 cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
}

void* IbGdaTransport::getRemotePtr(void* local_ptr, int) { return local_ptr; }

Status IbGdaTransport::registerMemory(void* ptr, size_t size, uint32_t& lkey,
                                      uint32_t& rkey) {
    if (!ptr) return Status::InvalidArgument("ptr is null" LOC_MARK);
    if (size == 0) return Status::InvalidArgument("size is zero" LOC_MARK);
    if (!pd_) {
        return Status::InvalidArgument(
            "IBGDA device has not been initialized" LOC_MARK);
    }
    auto it = registered_mrs_.find(ptr);
    if (it != registered_mrs_.end()) {
        lkey = it->second->lkey;
        rkey = it->second->rkey;
        mr_ = it->second;
        return Status::OK();
    }
    ibv_mr* mr = ibv_reg_mr(pd_, ptr, size,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                IBV_ACCESS_REMOTE_WRITE |
                                IBV_ACCESS_REMOTE_ATOMIC);
    if (!mr) return Status::RdmaError("failed to register IBGDA memory" LOC_MARK);
    registered_mrs_[ptr] = mr;
    mr_ = mr;
    lkey = mr->lkey;
    rkey = mr->rkey;
    return Status::OK();
}

Status IbGdaTransport::unregisterMemory(void* ptr) {
    auto it = registered_mrs_.find(ptr);
    if (it == registered_mrs_.end()) return Status::OK();
    ibv_mr* mr = it->second;
    registered_mrs_.erase(it);
    if (mr == mr_) mr_ = nullptr;
    if (ibv_dereg_mr(mr)) {
        return Status::RdmaError("failed to unregister IBGDA memory" LOC_MARK);
    }
    return Status::OK();
}

Status IbGdaTransport::allocateControlBuffer(size_t size) {
    if (size == 0) {
        return Status::InvalidArgument("control buffer size is zero" LOC_MARK);
    }
    if (!ctx_) {
        return Status::InvalidArgument(
            "IBGDA device has not been initialized" LOC_MARK);
    }
    if (ctrl_buf_) {
        if (ctrl_buf_size_ == size && ctrl_buf_umem_) return Status::OK();
        return Status::InvalidArgument(
            "IBGDA control buffer already allocated with a different size"
            LOC_MARK);
    }

    auto err = cudaMalloc(&ctrl_buf_, size);
    if (err != cudaSuccess) {
        ctrl_buf_ = nullptr;
        return Status::CudaError(std::string("cudaMalloc control buffer failed: ") +
                                 cudaGetErrorString(err) + LOC_MARK);
    }
    ctrl_buf_size_ = size;

    ctrl_buf_umem_ = mlx5dv_devx_umem_reg(ctx_, ctrl_buf_, ctrl_buf_size_,
                                          IBV_ACCESS_LOCAL_WRITE);
    if (!ctrl_buf_umem_) {
        err = cudaFree(ctrl_buf_);
        ctrl_buf_ = nullptr;
        ctrl_buf_size_ = 0;
        if (err != cudaSuccess) {
            return Status::CudaError(
                std::string("failed to register control buffer as umem; ") +
                "cudaFree cleanup also failed: " + cudaGetErrorString(err) +
                LOC_MARK);
        }
        return Status::RdmaError(
            "failed to register IBGDA control buffer as umem" LOC_MARK);
    }

    return Status::OK();
}

Status IbGdaTransport::releaseControlBuffer() {
    if (ctrl_buf_umem_) {
        if (mlx5dv_devx_umem_dereg(ctrl_buf_umem_)) {
            return Status::RdmaError(
                "failed to deregister IBGDA control buffer umem" LOC_MARK);
        }
        ctrl_buf_umem_ = nullptr;
    }
    if (ctrl_buf_) {
        auto err = cudaFree(ctrl_buf_);
        ctrl_buf_ = nullptr;
        ctrl_buf_size_ = 0;
        if (err != cudaSuccess) {
            return Status::CudaError(std::string("cudaFree control buffer failed: ") +
                                     cudaGetErrorString(err) + LOC_MARK);
        }
    }
    ctrl_buf_size_ = 0;
    return Status::OK();
}

Status IbGdaTransport::getChannelResources(
    int channel_id, DeviceChannelResources& resources) {
    if (channel_id < 0 || channel_id >= num_channels_) {
        return Status::InvalidArgument("invalid IBGDA channel id" LOC_MARK);
    }
    if (!network_ctx_) {
        return Status::InvalidArgument(
            "IBGDA device context has not been adopted" LOC_MARK);
    }
    resources.channel_id = channel_id;
    resources.num_channels = num_channels_;
    resources.network_ctx = network_ctx_;
    return Status::OK();
}

Status IbGdaTransport::getPeerInfo(int rank, DevicePeerInfo& peer) {
    auto it = peer_info_.find(rank);
    if (it == peer_info_.end()) {
        return Status::InvalidArgument("IBGDA peer info not found" LOC_MARK);
    }
    peer = it->second;
    return Status::OK();
}

Status IbGdaTransport::connect(int) { return notImplemented("connect"); }

Status IbGdaTransport::barrier() { return notImplemented("barrier"); }

const DeviceCommCapabilities IbGdaTransport::deviceCapabilities() const {
    DeviceCommCapabilities caps;
    caps.gpu_initiated_rdma = true;
    caps.symmetric_memory = true;
    caps.signal = true;
    caps.atomic = true;
    caps.latency_tier_ns = 1000;
    return caps;
}

Status IbGdaTransport::initializeDevice(const std::string& device_name,
                                        uint8_t port_num) {
    if (ctx_ || pd_) return Status::OK();
    device_name_ = device_name;
    port_num_ = port_num;

    int num_devices = 0;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        return Status::RdmaError("failed to get IB device list" LOC_MARK);
    }

    ibv_device* selected = nullptr;
    for (int i = 0; i < num_devices; ++i) {
        const char* name = ibv_get_device_name(dev_list[i]);
        if (name && device_name == name) {
            selected = dev_list[i];
            break;
        }
    }
    if (!selected) {
        ibv_free_device_list(dev_list);
        return Status::DeviceNotFound("IBGDA device not found: " +
                                      device_name + LOC_MARK);
    }

    ctx_ = ibv_open_device(selected);
    ibv_free_device_list(dev_list);
    if (!ctx_) return Status::RdmaError("failed to open IBGDA device" LOC_MARK);

    ibv_port_attr port_attr;
    if (ibv_query_port(ctx_, port_num_, &port_attr)) {
        (void)uninstall();
        return Status::RdmaError("failed to query IBGDA port" LOC_MARK);
    }
    is_roce_ = port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;

    gid_index_ = findBestGidIndex(ctx_, port_num_, port_attr);
    if (gid_index_ < 0) {
        (void)uninstall();
        return Status::RdmaError("failed to find IBGDA GID index" LOC_MARK);
    }
    if (ibv_query_gid(ctx_, port_num_, gid_index_, &gid_)) {
        (void)uninstall();
        return Status::RdmaError("failed to query IBGDA GID" LOC_MARK);
    }

    pd_ = ibv_alloc_pd(ctx_);
    if (!pd_) {
        (void)uninstall();
        return Status::RdmaError("failed to allocate IBGDA PD" LOC_MARK);
    }

    mlx5dv_obj dv_obj = {};
    dv_obj.pd.in = pd_;
    dv_obj.pd.out = &mpd_;
    if (mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD)) {
        (void)uninstall();
        return Status::RdmaError("failed to initialize mlx5dv PD" LOC_MARK);
    }
    return Status::OK();
}

Status IbGdaTransport::adoptDeviceContext(void* network_ctx,
                                          int num_channels) {
    if (!network_ctx) {
        return Status::InvalidArgument("network_ctx is null" LOC_MARK);
    }
    if (num_channels <= 0) {
        return Status::InvalidArgument("num_channels must be positive" LOC_MARK);
    }
    network_ctx_ = network_ctx;
    num_channels_ = num_channels;
    return Status::OK();
}

Status IbGdaTransport::adoptDeviceContext(IbGdaDeviceContext* network_ctx,
                                          int num_channels) {
    if (!network_ctx) {
        return Status::InvalidArgument("network_ctx is null" LOC_MARK);
    }
    if (network_ctx->abi_version != kIbGdaDeviceContextAbiVersion) {
        return Status::InvalidArgument("unsupported IBGDA device context ABI" LOC_MARK);
    }
    if (!network_ctx->raddrs || !network_ctx->rkeys || !network_ctx->qp_devctxs) {
        return Status::InvalidArgument("incomplete IBGDA device context" LOC_MARK);
    }
    if (network_ctx->num_qps <= 0 || network_ctx->num_ranks <= 0) {
        return Status::InvalidArgument("invalid IBGDA device context shape" LOC_MARK);
    }
    return adoptDeviceContext(static_cast<void*>(network_ctx), num_channels);
}

Status IbGdaTransport::setPeerInfo(int rank, const DevicePeerInfo& peer) {
    if (rank < 0) return Status::InvalidArgument("rank must be non-negative" LOC_MARK);
    peer_info_[rank] = peer;
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
