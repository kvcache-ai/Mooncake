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

// IBGDA device transport — wraps mlx5gda QP lifecycle behind RdmaTransport.
//
// Uses TE's Topology for NIC auto-detection when device_name is empty.
// Uses ibv_open_device / ibv_alloc_pd directly (same as the original
// MooncakeEpBuffer::init_ibgda()), but encapsulated in a reusable class.

#include "transport/device/device_transport.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <stdexcept>

#include "cuda_alike.h"
#include "mooncake_ibgda/memheap.h"
#include "mooncake_ibgda/mlx5gda.h"
#include "topology.h"

namespace mooncake {
namespace device {

static constexpr size_t kCtrlBufSize = 1024ULL * 1024 * 1024;  // 1 GiB

// Check if IPv6 address is IPv4-mapped (::ffff:x.x.x.x)
static bool isIpv4Mapped(const struct in6_addr* a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) == 0 &&
            a->s6_addr32[2] == htonl(0x0000ffff));
}

// Find the best GID index: RoCE v2 + IPv4-mapped, or IB.
static int findBestGidIndex(ibv_context* ctx, uint8_t port,
                            const ibv_port_attr& port_attr) {
    for (int i = 0; i < port_attr.gid_tbl_len; ++i) {
        ibv_gid_entry entry;
        if (ibv_query_gid_ex(ctx, port, i, &entry, 0)) continue;
        bool v4mapped = isIpv4Mapped(
            reinterpret_cast<const struct in6_addr*>(entry.gid.raw));
        if ((v4mapped && entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            entry.gid_type == IBV_GID_TYPE_IB) {
            return i;
        }
    }
    return -1;
}

// Auto-detect the best NIC for the current GPU using TE's Topology.
// filter: if non-empty, only consider NICs in this list.
static std::string autoDetectNic(const std::vector<std::string>& filter) {
    Topology topo;
    if (topo.discover(filter) != 0) return "";
    const auto& hca_list = topo.getHcaList();
    if (hca_list.empty()) return "";

    // Build a location string for the current GPU so Topology picks the
    // topologically closest NIC.  Fall back to wildcard if cudaGetDevice fails.
    int device_id = 0;
    cudaGetDevice(&device_id);
    std::string location = "cuda:" + std::to_string(device_id);

    int idx = topo.selectDevice(location);
    if (idx < 0) idx = topo.selectDevice("*");  // wildcard fallback
    if (idx < 0 || idx >= static_cast<int>(hca_list.size())) return "";
    return hca_list[idx];
}

class IbgdaDeviceTransportImpl : public RdmaTransport {
   public:
    explicit IbgdaDeviceTransportImpl(std::vector<std::string> filter)
        : device_filter_(std::move(filter)) {}

    ~IbgdaDeviceTransportImpl() override { teardown(); }

    int initialize(const std::string& device_name, int num_ranks,
                   int num_qps) override {
        num_ranks_ = num_ranks;
        num_qps_ = num_qps;

        std::string nic = device_name.empty()
                              ? autoDetectNic(device_filter_)
                              : device_name;
        if (nic.empty()) {
            LOG(WARNING) << "[EP IBGDA] No RDMA NIC found";
            return -1;
        }

        int num_devices = 0;
        ibv_device** dev_list = ibv_get_device_list(&num_devices);
        if (!dev_list) {
            LOG(ERROR) << "[EP IBGDA] ibv_get_device_list failed";
            return -1;
        }

        ibv_device* dev = nullptr;
        for (int i = 0; i < num_devices; ++i) {
            if (nic == ibv_get_device_name(dev_list[i])) {
                dev = dev_list[i];
                break;
            }
        }
        if (!dev) {
            LOG(ERROR) << "[EP IBGDA] NIC '" << nic << "' not found";
            ibv_free_device_list(dev_list);
            return -1;
        }

        ctx_ = ibv_open_device(dev);
        ibv_free_device_list(dev_list);
        if (!ctx_) {
            LOG(ERROR) << "[EP IBGDA] ibv_open_device failed for " << nic;
            return -1;
        }

        const uint8_t port = 1;
        ibv_port_attr port_attr{};
        if (ibv_query_port(ctx_, port, &port_attr)) {
            LOG(ERROR) << "[EP IBGDA] ibv_query_port failed";
            return -1;
        }

        gid_index_ = findBestGidIndex(ctx_, port, port_attr);
        if (gid_index_ < 0) {
            LOG(ERROR) << "[EP IBGDA] No suitable GID on " << nic;
            return -1;
        }

        if (ibv_query_gid(ctx_, port, gid_index_, &gid_)) {
            LOG(ERROR) << "[EP IBGDA] ibv_query_gid failed";
            return -1;
        }

        is_roce_ = (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET);
        lid_ = port_attr.lid;
        device_name_ = nic;

        pd_ = ibv_alloc_pd(ctx_);
        if (!pd_) {
            LOG(ERROR) << "[EP IBGDA] ibv_alloc_pd failed";
            return -1;
        }

        mlx5dv_obj dv_obj{};
        dv_obj.pd.in = pd_;
        dv_obj.pd.out = &mpd_;
        if (mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD)) {
            LOG(ERROR) << "[EP IBGDA] mlx5dv_init_obj failed";
            return -1;
        }

        // Allocate device-visible tables
        cudaMalloc(&raddrs_, num_ranks_ * sizeof(uint64_t));
        cudaMalloc(&rkeys_, num_ranks_ * sizeof(uint32_t));
        cudaMalloc(&qp_devctxs_, num_qps_ * sizeof(mlx5gda_qp_devctx));

        LOG(INFO) << "[EP IBGDA] Initialized on " << nic
                  << " (gid_index=" << gid_index_
                  << ", roce=" << is_roce_ << ")";
        return 0;
    }

    int registerMemory(void* ptr, size_t bytes) override {
        mr_ = ibv_reg_mr(pd_, ptr, bytes,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        if (!mr_) {
            LOG(ERROR) << "[EP IBGDA] ibv_reg_mr failed";
            return -1;
        }
        mr_ptr_ = ptr;
        return 0;
    }

    int allocateControlBuffer() override {
        cudaError_t err = cudaMalloc(&ctrl_buf_, kCtrlBufSize);
        if (err != cudaSuccess) {
            LOG(ERROR) << "[EP IBGDA] cudaMalloc ctrl_buf failed: "
                       << cudaGetErrorString(err);
            return -1;
        }
        ctrl_buf_umem_ = mlx5dv_devx_umem_reg(ctx_, ctrl_buf_, kCtrlBufSize,
                                               IBV_ACCESS_LOCAL_WRITE);
        if (!ctrl_buf_umem_) {
            LOG(ERROR) << "[EP IBGDA] mlx5dv_devx_umem_reg failed. "
                       << "GPU may not support GPUDirect RDMA.";
            cudaFree(ctrl_buf_);
            ctrl_buf_ = nullptr;
            return -1;
        }
        ctrl_buf_heap_ = memheap_create(kCtrlBufSize);
        if (!ctrl_buf_heap_) {
            LOG(ERROR) << "[EP IBGDA] memheap_create failed";
            return -1;
        }
        return 0;
    }

    int createQueuePairs(void* stream_ptr) override {
        auto stream = static_cast<cudaStream_t>(stream_ptr);
        for (int i = 0; i < num_qps_; ++i) {
            mlx5gda_qp* qp = mlx5gda_create_rc_qp(
                mpd_, ctrl_buf_, ctrl_buf_umem_, ctrl_buf_heap_, pd_, 16384, 1,
                stream);
            if (!qp) {
                LOG(ERROR) << "[EP IBGDA] mlx5gda_create_rc_qp failed at " << i;
                return -1;
            }
            if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
                LOG(ERROR) << "[EP IBGDA] rst2init failed at " << i;
                return -1;
            }
            cudaStreamSynchronize(stream);
            mlx5gda_qp_devctx devctx{
                .qpn = qp->qpn,
                .wqeid_mask = qp->num_wqebb - 1,
                .wq = reinterpret_cast<mlx5gda_wqebb*>(
                    static_cast<char*>(ctrl_buf_) + qp->wq_offset),
                .cq = reinterpret_cast<mlx5_cqe64*>(
                    static_cast<char*>(ctrl_buf_) + qp->send_cq->cq_offset),
                .dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
                    static_cast<char*>(ctrl_buf_) + qp->dbr_offset),
                .bf = static_cast<char*>(qp->uar->reg_addr),
            };
            cudaMemcpy(static_cast<char*>(qp_devctxs_) +
                           i * sizeof(mlx5gda_qp_devctx),
                       &devctx, sizeof(mlx5gda_qp_devctx),
                       cudaMemcpyHostToDevice);
            qps_.push_back(qp);
        }
        return 0;
    }

    int recreateQueuePairs(void* stream_ptr) override {
        auto stream = static_cast<cudaStream_t>(stream_ptr);
        for (auto* qp : qps_) {
            if (qp) mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
        }
        qps_.clear();
        return createQueuePairs(stream_ptr);
    }

    int connectPeers(bool is_roce,
                     const std::vector<int64_t>& remote_addrs,
                     const std::vector<int32_t>& remote_keys,
                     const std::vector<int32_t>& remote_qpns,
                     const std::vector<int32_t>& remote_lids,
                     const std::vector<int64_t>& subnet_prefixes,
                     const std::vector<int64_t>& interface_ids,
                     const std::vector<int>& active_ranks_mask) override {
        for (int i = 0; i < num_qps_; ++i) {
            int peer_rank = i * num_ranks_ / num_qps_;
            if (active_ranks_mask[peer_rank] == 0) continue;

            ibv_ah_attr ah_attr{};
            if (is_roce) {
                ibv_gid remote_gid{};
                remote_gid.global.subnet_prefix = subnet_prefixes[peer_rank];
                remote_gid.global.interface_id = interface_ids[peer_rank];
                ah_attr.is_global = 1;
                ah_attr.grh.dgid = remote_gid;
                ah_attr.grh.sgid_index = gid_index_;
                ah_attr.grh.hop_limit = 1;
                ah_attr.port_num = 1;
                ah_attr.dlid = qps_[i]->port_attr.lid | 0xC000;
            } else {
                ah_attr.dlid = static_cast<uint16_t>(remote_lids[i]);
                ah_attr.port_num = 0;
            }

            if (mlx5gda_modify_rc_qp_init2rtr(qps_[i], ah_attr,
                                               remote_qpns[i], IBV_MTU_4096)) {
                LOG(ERROR) << "[EP IBGDA] init2rtr failed for QP " << i;
                return -1;
            }
            if (mlx5gda_modify_rc_qp_rtr2rts(qps_[i])) {
                LOG(ERROR) << "[EP IBGDA] rtr2rts failed for QP " << i;
                return -1;
            }
        }

        // Populate device-visible raddrs/rkeys tables
        for (int i = 0; i < num_ranks_; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            uint64_t raddr = static_cast<uint64_t>(remote_addrs[i]);
            uint32_t rkey = static_cast<uint32_t>(remote_keys[i]);
            cudaMemcpy(static_cast<char*>(raddrs_) + i * sizeof(uint64_t),
                       &raddr, sizeof(uint64_t), cudaMemcpyHostToDevice);
            cudaMemcpy(static_cast<char*>(rkeys_) + i * sizeof(uint32_t),
                       &rkey, sizeof(uint32_t), cudaMemcpyHostToDevice);
        }
        return 0;
    }

    RdmaLocalMetadata localMetadata() const override {
        RdmaLocalMetadata meta;
        meta.raddr = mr_ ? reinterpret_cast<int64_t>(mr_->addr) : 0;
        meta.rkey = mr_ ? static_cast<int32_t>(mr_->lkey) : 0;
        meta.subnet_prefix =
            static_cast<int64_t>(gid_.global.subnet_prefix);
        meta.interface_id =
            static_cast<int64_t>(gid_.global.interface_id);
        for (auto* qp : qps_) {
            meta.qpns.push_back(static_cast<int32_t>(qp->qpn));
            meta.lids.push_back(static_cast<int32_t>(lid_));
        }
        return meta;
    }

    void* raddrsPtr() override { return raddrs_; }
    void* rkeysPtr() override { return rkeys_; }
    void* qpDevCtxsPtr() override { return qp_devctxs_; }
    bool isRoce() const override { return is_roce_; }
    int gidIndex() const override { return gid_index_; }

   private:
    void teardown() {
        for (auto* qp : qps_) {
            if (qp) mlx5gda_destroy_qp(ctrl_buf_heap_, qp);
        }
        qps_.clear();
        if (ctrl_buf_heap_) {
            memheap_destroy(ctrl_buf_heap_);
            ctrl_buf_heap_ = nullptr;
        }
        if (ctrl_buf_umem_) {
            mlx5dv_devx_umem_dereg(ctrl_buf_umem_);
            ctrl_buf_umem_ = nullptr;
        }
        if (ctrl_buf_) {
            cudaFree(ctrl_buf_);
            ctrl_buf_ = nullptr;
        }
        if (mr_) {
            ibv_dereg_mr(mr_);
            mr_ = nullptr;
        }
        if (raddrs_) { cudaFree(raddrs_); raddrs_ = nullptr; }
        if (rkeys_) { cudaFree(rkeys_); rkeys_ = nullptr; }
        if (qp_devctxs_) { cudaFree(qp_devctxs_); qp_devctxs_ = nullptr; }
        if (pd_) { ibv_dealloc_pd(pd_); pd_ = nullptr; }
        if (ctx_) { ibv_close_device(ctx_); ctx_ = nullptr; }
    }

    // IB resources
    ibv_context* ctx_ = nullptr;
    ibv_pd* pd_ = nullptr;
    mlx5dv_pd mpd_{};
    ibv_mr* mr_ = nullptr;
    void* mr_ptr_ = nullptr;
    ibv_gid gid_{};
    int gid_index_ = -1;
    uint16_t lid_ = 0;
    bool is_roce_ = false;
    std::string device_name_;
    std::vector<std::string> device_filter_;

    // Control buffer
    void* ctrl_buf_ = nullptr;
    mlx5dv_devx_umem* ctrl_buf_umem_ = nullptr;
    memheap* ctrl_buf_heap_ = nullptr;

    // QPs
    std::vector<mlx5gda_qp*> qps_;
    int num_ranks_ = 0;
    int num_qps_ = 0;

    // Device-visible tables
    void* raddrs_ = nullptr;
    void* rkeys_ = nullptr;
    void* qp_devctxs_ = nullptr;
};

std::unique_ptr<RdmaTransport> createIbgdaDeviceTransport(
    const std::vector<std::string>& device_filter) {
    return std::make_unique<IbgdaDeviceTransportImpl>(device_filter);
}

}  // namespace device
}  // namespace mooncake
