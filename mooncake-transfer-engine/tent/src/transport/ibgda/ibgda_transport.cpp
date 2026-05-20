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
    (void)destroyQueuePairs();
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
    auto destroy_status = destroyQueuePairs();
    if (!destroy_status.ok()) return destroy_status;
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

Status IbGdaTransport::createQueuePairs(int num_qps, int wqe,
                                        cudaStream_t stream,
                                        void* qp_devctxs) {
    if (num_qps <= 0) {
        return Status::InvalidArgument("num_qps must be positive" LOC_MARK);
    }
    if (wqe <= 0) {
        return Status::InvalidArgument("wqe must be positive" LOC_MARK);
    }
    if (!qp_devctxs) {
        return Status::InvalidArgument("qp_devctxs is null" LOC_MARK);
    }
    if (!pd_ || !ctrl_buf_ || !ctrl_buf_umem_) {
        return Status::InvalidArgument(
            "IBGDA control resources are not initialized" LOC_MARK);
    }
    if (!qps_.empty() || ctrl_buf_heap_) {
        return Status::InvalidArgument(
            "IBGDA queue pairs have already been created" LOC_MARK);
    }

    ctrl_buf_heap_ = memheap_create(ctrl_buf_size_);
    if (!ctrl_buf_heap_) {
        return Status::RdmaError("failed to create IBGDA control heap" LOC_MARK);
    }

    qps_.reserve(num_qps);
    for (int i = 0; i < num_qps; ++i) {
        mlx5gda_qp* qp = mlx5gda_create_rc_qp(mpd_, ctrl_buf_, ctrl_buf_umem_,
                                              ctrl_buf_heap_, pd_, wqe,
                                              port_num_, stream);
        if (!qp) {
            (void)destroyQueuePairs();
            return Status::RdmaError("failed to create IBGDA QP" LOC_MARK);
        }
        is_roce_ = qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
        if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
            (void)mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
            (void)destroyQueuePairs();
            return Status::RdmaError(
                "failed to modify IBGDA QP RST->INIT" LOC_MARK);
        }

        auto err = cudaStreamSynchronize(stream);
        if (err != cudaSuccess) {
            (void)mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
            (void)destroyQueuePairs();
            return Status::CudaError(
                std::string("cudaStreamSynchronize before QP devctx copy failed: ") +
                cudaGetErrorString(err) + LOC_MARK);
        }

        mlx5gda_qp_devctx qp_devctx = {
            .qpn = qp->qpn,
            .wqeid_mask = qp->num_wqebb - 1,
            .wq = reinterpret_cast<mlx5gda_wqebb*>(
                static_cast<uint8_t*>(ctrl_buf_) + qp->wq_offset),
            .cq = reinterpret_cast<mlx5_cqe64*>(
                static_cast<uint8_t*>(ctrl_buf_) + qp->send_cq->cq_offset),
            .dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
                static_cast<uint8_t*>(ctrl_buf_) + qp->dbr_offset),
            .bf = static_cast<char*>(qp->uar->reg_addr),
        };
        err = cudaMemcpy(static_cast<uint8_t*>(qp_devctxs) +
                             i * sizeof(mlx5gda_qp_devctx),
                         &qp_devctx, sizeof(mlx5gda_qp_devctx),
                         cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            (void)mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
            (void)destroyQueuePairs();
            return Status::CudaError(
                std::string("cudaMemcpy QP devctx failed: ") +
                cudaGetErrorString(err) + LOC_MARK);
        }
        qps_.push_back(qp);
    }
    return Status::OK();
}

Status IbGdaTransport::recreateQueuePairs(int num_qps, int wqe,
                                          cudaStream_t stream,
                                          void* qp_devctxs) {
    auto status = destroyQueuePairs();
    if (!status.ok()) return status;
    return createQueuePairs(num_qps, wqe, stream, qp_devctxs);
}

Status IbGdaTransport::destroyQueuePairs() {
    for (auto* qp : qps_) {
        if (qp && ctrl_buf_heap_) mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
    }
    qps_.clear();
    if (ctrl_buf_heap_) {
        memheap_destroy(ctrl_buf_heap_);
        ctrl_buf_heap_ = nullptr;
    }
    return Status::OK();
}

Status IbGdaTransport::connectQueuePair(int qp_index,
                                        const ibv_ah_attr& ah_attr,
                                        uint32_t remote_qpn, ibv_mtu mtu) {
    if (qp_index < 0 || qp_index >= static_cast<int>(qps_.size())) {
        return Status::InvalidArgument("invalid IBGDA QP index" LOC_MARK);
    }
    auto* qp = qps_[qp_index];
    if (!qp) return Status::InvalidArgument("IBGDA QP is null" LOC_MARK);
    if (mlx5gda_modify_rc_qp_init2rtr(qp, ah_attr, remote_qpn, mtu)) {
        return Status::RdmaError("failed to modify IBGDA QP INIT->RTR" LOC_MARK);
    }
    if (mlx5gda_modify_rc_qp_rtr2rts(qp)) {
        return Status::RdmaError("failed to modify IBGDA QP RTR->RTS" LOC_MARK);
    }
    return Status::OK();
}

Status IbGdaTransport::connectPeers(
    const std::vector<int64_t>& remote_addrs,
    const std::vector<int32_t>& remote_keys,
    const std::vector<std::vector<int32_t>>& peer_qpns,
    const std::vector<std::vector<int32_t>>& peer_lids,
    const std::vector<int64_t>& subnet_prefixes,
    const std::vector<int64_t>& interface_ids,
    const std::vector<int>& active_ranks_mask, int rank, int num_ranks,
    void* raddrs, void* rkeys) {
    if (rank < 0 || num_ranks <= 0 || rank >= num_ranks) {
        return Status::InvalidArgument("invalid IBGDA rank shape" LOC_MARK);
    }
    if (!mr_) {
        return Status::InvalidArgument(
            "IBGDA memory has not been registered" LOC_MARK);
    }
    if (!raddrs || !rkeys) {
        return Status::InvalidArgument(
            "IBGDA device metadata arrays are null" LOC_MARK);
    }
    if (qps_.empty() || static_cast<int>(qps_.size()) % num_ranks != 0) {
        return Status::InvalidArgument(
            "IBGDA QP count must be positive and divisible by num_ranks"
            LOC_MARK);
    }
    if (static_cast<int>(remote_addrs.size()) != num_ranks ||
        static_cast<int>(remote_keys.size()) != num_ranks ||
        static_cast<int>(peer_qpns.size()) != num_ranks ||
        static_cast<int>(active_ranks_mask.size()) != num_ranks) {
        return Status::InvalidArgument(
            "incomplete IBGDA peer metadata" LOC_MARK);
    }
    if (is_roce_) {
        if (static_cast<int>(subnet_prefixes.size()) != num_ranks ||
            static_cast<int>(interface_ids.size()) != num_ranks) {
            return Status::InvalidArgument(
                "incomplete RoCE GID metadata" LOC_MARK);
        }
    } else if (static_cast<int>(peer_lids.size()) != num_ranks) {
        return Status::InvalidArgument("incomplete IB LID metadata" LOC_MARK);
    }

    const int qp_count = static_cast<int>(qps_.size());
    const int qps_per_rank = qp_count / num_ranks;
    const int local_qp_base = rank * qps_per_rank;

    for (int peer_rank = 0; peer_rank < num_ranks; ++peer_rank) {
        if (static_cast<int>(peer_qpns[peer_rank].size()) <
            local_qp_base + qps_per_rank) {
            return Status::InvalidArgument(
                "insufficient peer QPN metadata" LOC_MARK);
        }
        if (!is_roce_ &&
            static_cast<int>(peer_lids[peer_rank].size()) <
                local_qp_base + qps_per_rank) {
            return Status::InvalidArgument(
                "insufficient peer LID metadata" LOC_MARK);
        }
    }

    // QP ordering mirrors EP's original flattened layout: each contiguous block
    // of qps_per_rank local QPs targets one peer rank, while each remote peer's
    // QPN array is indexed by this rank's QP block.
    for (int qp_index = 0; qp_index < qp_count; ++qp_index) {
        int peer_rank = qp_index * num_ranks / qp_count;
        if (active_ranks_mask[peer_rank] == 0) continue;
        int local_index = qp_index - peer_rank * qps_per_rank;
        int remote_qp_index = local_qp_base + local_index;
        auto remote_qpn = static_cast<uint32_t>(
            peer_qpns[peer_rank][remote_qp_index]);

        ibv_ah_attr ah_attr = {};
        if (is_roce_) {
            ibv_gid remote_gid{};
            remote_gid.global.subnet_prefix = subnet_prefixes[peer_rank];
            remote_gid.global.interface_id = interface_ids[peer_rank];
            ah_attr.is_global = 1;
            ah_attr.grh.dgid = remote_gid;
            ah_attr.grh.sgid_index = gid_index_;
            ah_attr.grh.hop_limit = 1;
            ah_attr.port_num = port_num_;
            ah_attr.dlid = queuePairLid(qp_index) | 0xC000;
        } else {
            ah_attr.dlid = static_cast<uint16_t>(
                peer_lids[peer_rank][remote_qp_index]);
            ah_attr.port_num = 0;
        }

        auto status = connectQueuePair(qp_index, ah_attr, remote_qpn,
                                       IBV_MTU_4096);
        if (!status.ok()) return status;
    }

    for (int peer_rank = 0; peer_rank < num_ranks; ++peer_rank) {
        if (active_ranks_mask[peer_rank] == 0) continue;
        uint64_t raddr = peer_rank == rank
                             ? reinterpret_cast<uint64_t>(mr_->addr)
                             : static_cast<uint64_t>(remote_addrs[peer_rank]);
        uint32_t rkey = peer_rank == rank
                            ? mr_->lkey
                            : static_cast<uint32_t>(remote_keys[peer_rank]);

        auto err = cudaMemcpy(static_cast<uint8_t*>(raddrs) +
                                  peer_rank * sizeof(uint64_t),
                              &raddr, sizeof(uint64_t),
                              cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            return Status::CudaError(
                std::string("cudaMemcpy IBGDA remote address failed: ") +
                cudaGetErrorString(err) + LOC_MARK);
        }
        err = cudaMemcpy(static_cast<uint8_t*>(rkeys) +
                             peer_rank * sizeof(uint32_t),
                         &rkey, sizeof(uint32_t), cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            return Status::CudaError(
                std::string("cudaMemcpy IBGDA remote key failed: ") +
                cudaGetErrorString(err) + LOC_MARK);
        }
    }

    return Status::OK();
}

uint32_t IbGdaTransport::queuePairQpn(int qp_index) const {
    if (qp_index < 0 || qp_index >= static_cast<int>(qps_.size()) ||
        !qps_[qp_index]) {
        return 0;
    }
    return qps_[qp_index]->qpn;
}

uint16_t IbGdaTransport::queuePairLid(int qp_index) const {
    if (qp_index < 0 || qp_index >= static_cast<int>(qps_.size()) ||
        !qps_[qp_index]) {
        return 0;
    }
    return qps_[qp_index]->port_attr.lid;
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
