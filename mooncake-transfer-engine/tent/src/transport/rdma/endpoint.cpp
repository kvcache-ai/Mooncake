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

#include "tent/transport/rdma/endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <sstream>
#include <iomanip>
#include <queue>
#include <mutex>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/transport/rdma/context.h"
#include "tent/common/utils/os.h"
#include "tent/common/utils/string_builder.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
static inline const std::string statusToString(
    RdmaEndPoint::EndPointStatus status) {
    switch (status) {
        case RdmaEndPoint::EP_UNINIT:
            return "EP_UNINIT";
        case RdmaEndPoint::EP_HANDSHAKING:
            return "EP_HANDSHAKING";
        case RdmaEndPoint::EP_READY:
            return "EP_READY";
        case RdmaEndPoint::EP_RESET:
            return "EP_RESET";
    }
    return "UNKNOWN";
}

// Forward declaration for notification QP setup
static int setupNotifyQpConnection(ibv_qp* qp, RdmaContext* ctx,
                                   const std::string& peer_gid_str,
                                   uint16_t peer_lid, uint32_t peer_qp_num);

RdmaEndPoint::RdmaEndPoint() : status_(EP_UNINIT) {}

RdmaEndPoint::~RdmaEndPoint() {
    if (status_ != EP_UNINIT) deconstruct();
    if (endpoints_count_)
        endpoints_count_->fetch_sub(1, std::memory_order_relaxed);
}

int RdmaEndPoint::construct(RdmaContext* context, EndPointParams* params,
                            const std::string& endpoint_name,
                            std::atomic<int>* endpoints_count) {
    context_ = context;
    params_ = params;
    endpoint_name_ = endpoint_name;
    inflight_slices_ = 0;
    endpoints_count_ = endpoints_count;
    qp_list_.resize(params_->qp_mul_factor);
    wr_depth_list_ = new WrDepthBlock[params_->qp_mul_factor];

    for (int i = 0; i < params_->qp_mul_factor; ++i) {
        wr_depth_list_[i].value = 0;
        ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        auto cq = context_->cq(i % context_->cqCount())->cq();
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_RC;
        attr.qp_context = this;
        attr.cap.max_send_wr = attr.cap.max_recv_wr = params_->max_qp_wr;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = params_->max_sge;
        attr.cap.max_inline_data = params_->max_inline_bytes;
        qp_list_[i] =
            context_->verbs_.ibv_create_qp(context_->nativePD(), &attr);
        if (!qp_list_[i]) {
            PLOG(ERROR) << "ibv_create_qp";
            deconstruct();
            return -1;
        }
        slice_queue_.emplace_back(params_->max_qp_wr);
    }

    auto notify_cq = context_->notifyCq()->cq();
    ibv_qp_init_attr notify_attr;
    memset(&notify_attr, 0, sizeof(notify_attr));
    notify_attr.send_cq = notify_cq;
    notify_attr.recv_cq = notify_cq;
    notify_attr.sq_sig_all = true;  // Always signal for notifications
    notify_attr.qp_type = IBV_QPT_RC;
    notify_attr.cap.max_send_wr = kNotifyMaxPendingSends;
    notify_attr.cap.max_recv_wr = kNotifyMaxPendingSends;
    notify_attr.cap.max_send_sge = 1;
    notify_attr.cap.max_recv_sge = 1;

    notify_qp_ =
        context_->verbs_.ibv_create_qp(context_->nativePD(), &notify_attr);
    if (!notify_qp_) {
        PLOG(ERROR) << "Failed to create notification QP";
        deconstruct();
        return -1;
    }

    // Modify notification QP to INIT
    ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = context_->portNum();
    qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

    if (context_->verbs_.ibv_modify_qp(notify_qp_, &qp_attr,
                                       IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                           IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        PLOG(ERROR) << "Failed to modify notification QP to INIT";
        deconstruct();
        return -1;
    }

    // Pre-post recv buffers for notification
    notify_recv_buffers_.resize(kNotifyMaxPendingSends);
    notify_recv_mrs_.resize(kNotifyMaxPendingSends);

    // Allocate and register send buffer
    notify_send_buffer_.resize(kNotifyBufferSize);
    notify_send_mr_ = context_->verbs_.ibv_reg_mr_default(
        context_->nativePD(), notify_send_buffer_.data(), kNotifyBufferSize,
        IBV_ACCESS_LOCAL_WRITE);
    if (!notify_send_mr_) {
        PLOG(ERROR) << "Failed to register notification send buffer";
        deconstruct();
        return -1;
    }

    for (size_t i = 0; i < kNotifyMaxPendingSends; ++i) {
        notify_recv_buffers_[i].resize(kNotifyBufferSize);

        // Register memory for recv buffer
        notify_recv_mrs_[i] = context_->verbs_.ibv_reg_mr_default(
            context_->nativePD(), notify_recv_buffers_[i].data(),
            kNotifyBufferSize, IBV_ACCESS_LOCAL_WRITE);
        if (!notify_recv_mrs_[i]) {
            PLOG(ERROR) << "Failed to register notification recv buffer";
            deconstruct();
            return -1;
        }

        postNotifyRecv(i);
    }

    status_ = EP_HANDSHAKING;
    return 0;
}

int RdmaEndPoint::deconstruct() {
    if (status_ == EP_UNINIT) return 0;
    status_ = EP_RESET;
    resetInflightSlices();

    // Destroy notification QP
    if (notify_qp_) {
        // Unregister from transport before destroying
        context_->transport_.unregisterNotifyQp(notify_qp_->qp_num);
        if (context_->verbs_.ibv_destroy_qp(notify_qp_))
            PLOG(ERROR) << "Failed to destroy notification QP";
        notify_qp_ = nullptr;
        notify_connected_ = false;
    }

    // Deregister and free notification memory
    for (auto& mr : notify_recv_mrs_) {
        if (mr) {
            if (context_->verbs_.ibv_dereg_mr(mr))
                PLOG(ERROR) << "Failed to deregister notification recv MR";
            mr = nullptr;
        }
    }
    notify_recv_mrs_.clear();
    if (notify_send_mr_) {
        if (context_->verbs_.ibv_dereg_mr(notify_send_mr_))
            PLOG(ERROR) << "Failed to deregister notification send MR";
        notify_send_mr_ = nullptr;
    }

    notify_recv_buffers_.clear();

    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (context_->verbs_.ibv_destroy_qp(qp_list_[i]))
            PLOG(ERROR) << "ibv_destroy_qp";
        cancelQuota(i, wr_depth_list_[i].value);
    }
    qp_list_.clear();
    slice_queue_.clear();
    delete[] wr_depth_list_;
    wr_depth_list_ = nullptr;
    status_ = EP_UNINIT;
    return 0;
}

Status RdmaEndPoint::connect(const std::string& peer_server_name,
                             const std::string& peer_nic_name) {
    RWSpinlock::WriteGuard guard(lock_);
    if (peer_server_name.empty() || peer_nic_name.empty())
        return Status::InvalidArgument("Invalid peer path" LOC_MARK);
    if (status_ == EP_READY) return Status::OK();
    if (status_ != EP_HANDSHAKING)
        return Status::InvalidArgument(
            "Endpoint not in handshaking state" LOC_MARK);
    auto& transport = context_->transport_;
    auto& manager = transport.metadata_->segmentManager();
    auto qp_num = qpNum();
    BootstrapDesc local_desc, peer_desc;
    local_desc.local_nic_path =
        MakeNicPath(transport.local_segment_name_, context_->name());
    local_desc.peer_nic_path = MakeNicPath(peer_server_name, peer_nic_name);
    local_desc.qp_num = qp_num;
    local_desc.notify_qp_num = notifyQpNum();  // Pass notification QP number
    std::shared_ptr<SegmentDesc> segment_desc;
    if (local_desc.local_nic_path == local_desc.peer_nic_path) {
        segment_desc = manager.getLocal();
    } else {
        CHECK_STATUS(manager.getRemote(segment_desc, peer_server_name));
        auto rpc_server_addr = segment_desc->getMemory().rpc_server_addr;
        assert(!rpc_server_addr.empty());
        CHECK_STATUS(
            ControlClient::bootstrap(rpc_server_addr, local_desc, peer_desc));
        qp_num = peer_desc.qp_num;
    }
    assert(qp_num.size() && segment_desc);
    auto dev_desc = segment_desc->findDevice(peer_nic_name);
    if (!dev_desc) {
        LOG(ERROR) << "Unable to find RDMA device: " << peer_nic_name
                   << " in segment " << segment_desc->name;
        return Status::DeviceNotFound("Unable to find RDMA device" LOC_MARK);
    }
    peer_server_name_ = peer_server_name;
    peer_nic_name_ = peer_nic_name;
    int rc = setupAllQPs(dev_desc->gid, dev_desc->lid, qp_num);
    if (rc) {
        return Status::InternalError(
            "Failed to configure RDMA endpoint" LOC_MARK);
    }

    // Setup notification QP connection if peer supports it
    if (peer_desc.notify_qp_num != 0 && notify_qp_) {
        rc = setupNotifyQpConnection(notify_qp_, context_, dev_desc->gid,
                                     dev_desc->lid, peer_desc.notify_qp_num);
        if (rc) {
            LOG(WARNING)
                << "Failed to setup notification QP, notification disabled";
            notify_connected_ = false;
        } else {
            notify_connected_ = true;
            context_->transport_.registerNotifyQp(notify_qp_->qp_num, this);
        }
    }

    return Status::OK();
}

Status RdmaEndPoint::accept(const BootstrapDesc& peer_desc,
                            BootstrapDesc& local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (status_ == EP_READY) {
        LOG(WARNING) << "Endpoint is established with " << peer_nic_name_
                     << " of " << peer_server_name_ << ", resetting first";
        resetUnlocked();
        status_ = EP_HANDSHAKING;
    } else if (status_ == EP_RESET) {
        resetUnlocked();
        status_ = EP_HANDSHAKING;
    } else if (status_ != EP_HANDSHAKING) {
        LOG(ERROR) << "Endpoint not in handshaking state: "
                   << statusToString(status_);
        return Status::InvalidArgument(
            "Endpoint not in handshaking state" LOC_MARK);
    }
    auto& transport = context_->transport_;
    auto& manager = transport.metadata_->segmentManager();
    auto peer_nic_path = peer_desc.local_nic_path;
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path);
    if (peer_server_name.empty() || peer_nic_name.empty())
        return Status::InvalidArgument("Invalid peer path" LOC_MARK);
    local_desc.local_nic_path =
        MakeNicPath(transport.local_segment_name_, context_->name());
    local_desc.peer_nic_path = peer_nic_path;
    local_desc.qp_num = qpNum();
    local_desc.notify_qp_num = notifyQpNum();  // Pass notification QP number
    SegmentDescRef segment_desc;
    CHECK_STATUS(manager.getRemote(segment_desc, peer_server_name));
    auto dev_desc = segment_desc->findDevice(peer_nic_name);
    if (!dev_desc) {
        LOG(ERROR) << "Unable to find RDMA device: " << peer_nic_name
                   << " in segment " << segment_desc->name;
        return Status::DeviceNotFound("Unable to find RDMA device" LOC_MARK);
    }
    peer_server_name_ = peer_server_name;
    peer_nic_name_ = peer_nic_name;
    int rc = setupAllQPs(dev_desc->gid, dev_desc->lid, peer_desc.qp_num);
    if (rc) {
        return Status::InternalError(
            "Failed to configure RDMA endpoint" LOC_MARK);
    }

    // Setup notification QP connection if peer supports it
    if (peer_desc.notify_qp_num != 0 && notify_qp_) {
        rc = setupNotifyQpConnection(notify_qp_, context_, dev_desc->gid,
                                     dev_desc->lid, peer_desc.notify_qp_num);
        if (rc) {
            notify_connected_ = false;
        } else {
            notify_connected_ = true;
            context_->transport_.registerNotifyQp(notify_qp_->qp_num, this);
        }
    }

    return Status::OK();
}

int RdmaEndPoint::reset() {
    RWSpinlock::WriteGuard guard(lock_);
    return resetUnlocked();
}

int RdmaEndPoint::resetUnlocked() {
    if (status_ != EP_READY) return 0;
    status_ = EP_RESET;
    resetInflightSlices();
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int ret =
            context_->verbs_.ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (ret) {
            PLOG(ERROR) << "ibv_modify_qp(RESET)";
            deconstruct();
            return -1;
        }
        cancelQuota(i, wr_depth_list_[i].value);
    }
    return 0;
}

int RdmaEndPoint::setupAllQPs(const std::string& peer_gid, uint16_t peer_lid,
                              std::vector<uint32_t> peer_qp_num_list,
                              std::string* reply_msg) {
    if (status_ == EP_READY) {
        status_ = EP_RESET;
        return -1;
    }

    if (qp_list_.size() != peer_qp_num_list.size()) {
        std::stringstream ss;
        ss << "Inconsistent qp_mul_factor: local " << qp_list_.size()
           << " peer " << peer_qp_num_list.size() << " for endpoint "
           << peer_nic_name_ << " of " << peer_server_name_;
        LOG(ERROR) << ss.str();
        if (reply_msg) *reply_msg = ss.str();
        status_ = EP_RESET;
        return -1;
    }

    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        int ret = setupOneQP(qp_index, peer_gid, peer_lid,
                             peer_qp_num_list[qp_index], reply_msg);
        if (ret) {
            status_ = EP_RESET;
            return ret;
        }
    }

    status_ = EP_READY;
    return 0;
}

static ibv_wr_opcode getOpCode(RdmaSlice* slice) {
    switch (slice->task->request.opcode) {
        case Request::READ:
            return IBV_WR_RDMA_READ;
        case Request::WRITE:
            return IBV_WR_RDMA_WRITE;
        default:
            return IBV_WR_RDMA_READ;
    }
}

int RdmaEndPoint::submitSlices(std::vector<RdmaSlice*>& slice_list,
                               int qp_index) {
    // RWSpinlock::ReadGuard guard(lock_);  // TODO performance issue
    const static int kSgeEntries = 1;
    if (status_ != EP_READY) return 0;
    if (qp_index < 0) qp_index = 0;
    qp_index %= qp_list_.size();
    auto cq = context_->cq(qp_index % context_->cqCount());
    int wr_count =
        std::min(cq->maxCqe() - cq->getQuota(),
                 std::min(params_->max_qp_wr - wr_depth_list_[qp_index].value,
                          (int)slice_list.size()));
    int sge_count = wr_count * kSgeEntries;

    if (wr_count <= 0 || !reserveQuota(qp_index, wr_count)) return 0;

    ibv_send_wr wr_list[wr_count], *bad_wr = nullptr;
    ibv_sge sge_list[sge_count];
    memset(wr_list, 0, sizeof(ibv_send_wr) * wr_count);
    int sge_idx = 0;

    for (int wr_idx = 0; wr_idx < wr_count; ++wr_idx) {
        auto current = slice_list[wr_idx];
        auto& wr = wr_list[wr_idx];
        for (int sge_off = 0; sge_off < kSgeEntries; ++sge_off) {
            auto& sge = sge_list[sge_idx + sge_off];
            sge.addr = (uint64_t)current->source_addr;
            sge.length = current->length;
            sge.lkey = current->source_lkey;
        }
        current->ep_weak_ptr = this;
        current->qp_index = qp_index;
        current->failed = false;
        wr.wr_id = (uint64_t)current;
        wr.opcode = getOpCode(current);
        wr.num_sge = kSgeEntries;
        wr.sg_list = &sge_list[sge_idx];
        wr.send_flags = IBV_SEND_SIGNALED;
        // TODO the quota algorithm should be changed as well to keep optimal
        // allocation
        // wr.send_flags = (wr_idx + 1 == wr_count) ? IBV_SEND_SIGNALED : 0;
        wr.next = (wr_idx + 1 == wr_count) ? nullptr : &wr_list[wr_idx + 1];
        wr.imm_data = 0;
        wr.wr.rdma.remote_addr = current->target_addr;
        wr.wr.rdma.rkey = current->target_rkey;
        sge_idx += wr.num_sge;
    }

    int rc = ibv_post_send(qp_list_[qp_index], wr_list, &bad_wr);
    if (rc) {
        PLOG(ERROR) << "ibv_post_send";
        while (bad_wr) {
            slice_list[bad_wr - wr_list]->failed = true;
            cancelQuota(qp_index, 1);
            bad_wr = bad_wr->next;
        }
    }

    auto& queue = slice_queue_[qp_index];
    for (int wr_idx = 0; wr_idx < wr_count; ++wr_idx) {
        auto current = slice_list[wr_idx];
        if (!current->failed) {
            queue.push(current);
        }
    }
    return wr_count;
}

int RdmaEndPoint::submitRecvImmDataRequest(int qp_index, uint64_t id) {
    RWSpinlock::ReadGuard guard(lock_);
    if (status_ != EP_READY) return 0;
    if (qp_index < 0 || qp_index >= (int)qp_list_.size()) return 0;
    ibv_recv_wr wr, *bad_wr;
    memset(&wr, 0, sizeof(ibv_recv_wr));
    wr.wr_id = id;
    if (!reserveQuota(qp_index, 1)) return 0;
    int rc = ibv_post_recv(qp_list_[qp_index], &wr, &bad_wr);
    if (rc) {
        cancelQuota(qp_index, 1);
        PLOG(ERROR) << "ibv_post_recv";
        return -1;
    }
    return 1;
}

void RdmaEndPoint::resetInflightSlices() {
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        auto& queue = slice_queue_[qp_index];
        while (!queue.empty()) {
            auto current = queue.pop();
            updateSliceStatus(current, TransferStatusEnum::CANCELED);
        }
    }
}

size_t RdmaEndPoint::acknowledge(RdmaSlice* slice, TransferStatusEnum status) {
    auto qp_index = slice->qp_index;
    auto& queue = slice_queue_[qp_index];
    if (!queue.contains(slice)) return 0;
    int num_entries = 0;
    RdmaSlice* current = nullptr;
    do {
        current = queue.pop();
        if (!current) break;
        num_entries++;
        updateSliceStatus(current, status);
    } while (current != slice);
    cancelQuota(qp_index, num_entries);
    return num_entries;
}

std::vector<uint32_t> RdmaEndPoint::qpNum() {
    std::vector<uint32_t> ret;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        ret.push_back(qp_list_[qp_index]->qp_num);
    return ret;
}

int RdmaEndPoint::getInflightSlices() const { return inflight_slices_; }

bool RdmaEndPoint::reserveQuota(int qp_index, int num_entries) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    auto cq = context_->cq(qp_index % context_->cqCount());
    if (!cq->reserveQuota(num_entries)) return false;
    auto prev_depth_list =
        __sync_fetch_and_add(&wr_depth_list_[qp_index].value, num_entries);
    __sync_fetch_and_add(&inflight_slices_, num_entries);
    if (prev_depth_list + num_entries > params_->max_qp_wr) {
        cancelQuota(qp_index, num_entries);
        return false;
    }
    return true;
}

void RdmaEndPoint::cancelQuota(int qp_index, int num_entries) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    __sync_fetch_and_sub(&wr_depth_list_[qp_index].value, num_entries);
    __sync_fetch_and_sub(&inflight_slices_, num_entries);
    auto cq = context_->cq(qp_index % context_->cqCount());
    cq->cancelQuota(num_entries);
}

int RdmaEndPoint::setupOneQP(int qp_index, const std::string& peer_gid,
                             uint16_t peer_lid, uint32_t peer_qp_num,
                             std::string* reply_msg) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    auto& qp = qp_list_[qp_index];

    // RESET -> INIT
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context().portNum();
    attr.pkey_index = params_->pkey_index;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    int ret = context_->verbs_.ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret) {
        std::stringstream ss;
        ss << "Failed to modify QP's state to INIT in endpoint "
           << peer_nic_name_ << " of " << peer_server_name_
           << ", check local context port num " << context().portNum()
           << ", error code " << errno << ":" << strerror(errno);
        LOG(ERROR) << ss.str();
        if (reply_msg) *reply_msg = ss.str();
        return -1;
    }

    // INIT -> RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = params_->path_mtu;
    ibv_gid peer_gid_raw;
    std::istringstream iss(peer_gid);
    for (int i = 0; i < 16; ++i) {
        int value;
        iss >> std::hex >> value;
        peer_gid_raw.raw[i] = static_cast<uint8_t>(value);
        if (i < 15) iss.ignore(1, ':');
    }

    attr.ah_attr.grh.dgid = peer_gid_raw;
    attr.ah_attr.grh.sgid_index = context().gidIndex();
    attr.ah_attr.grh.hop_limit = params_->hop_limit;
    attr.ah_attr.grh.flow_label = params_->flow_label;
    attr.ah_attr.grh.traffic_class = params_->traffic_class;
    attr.ah_attr.dlid = peer_lid;
    attr.ah_attr.sl = params_->service_level;
    attr.ah_attr.src_path_bits = params_->src_path_bits;
    attr.ah_attr.static_rate = params_->static_rate;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = context().portNum();
    attr.dest_qp_num = peer_qp_num;
    attr.rq_psn = params_->rq_psn;
    attr.max_dest_rd_atomic = params_->max_dest_rd_atomic;
    attr.min_rnr_timer = params_->min_rnr_timer;
    ret = context_->verbs_.ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV |
            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN);
    if (ret) {
        std::stringstream ss;
        ss << "Failed to modify QP's state to RTR in endpoint "
           << peer_nic_name_ << " of " << peer_server_name_
           << ", check peer endpoint reachability, MTU " << params_->path_mtu
           << ", GID " << peer_gid << " and LID " << peer_lid << ", error code "
           << errno << ":" << strerror(errno);
        LOG(ERROR) << ss.str();
        if (reply_msg) *reply_msg = ss.str();
        return -1;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = params_->send_timeout;
    attr.retry_cnt = params_->send_retry_count;
    attr.rnr_retry = params_->send_rnr_count;
    attr.sq_psn = params_->sq_psn;
    attr.max_rd_atomic = params_->max_rd_atomic;
    ret = context_->verbs_.ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
            IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        std::stringstream ss;
        ss << "Failed to modify QP's state to RTS in endpoint "
           << peer_nic_name_ << " of " << peer_server_name_ << ", error code "
           << errno << ":" << strerror(errno);
        LOG(ERROR) << ss.str();
        if (reply_msg) *reply_msg = ss.str();
        return -1;
    }

    return 0;
}

void RdmaEndPoint::postNotifyRecv(size_t idx) {
    if (idx >= notify_recv_buffers_.size() || idx >= notify_recv_mrs_.size())
        return;

    ibv_sge sge = {};
    sge.addr = reinterpret_cast<uint64_t>(notify_recv_buffers_[idx].data());
    sge.length = notify_recv_buffers_[idx].size();
    sge.lkey = notify_recv_mrs_[idx]->lkey;

    ibv_recv_wr wr = {};
    wr.wr_id = idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    ibv_recv_wr* bad_wr = nullptr;
    if (ibv_post_recv(notify_qp_, &wr, &bad_wr)) {
        PLOG(ERROR) << "Failed to post notification recv";
    }
}

static int setupNotifyQpConnection(ibv_qp* qp, RdmaContext* ctx,
                                   const std::string& peer_gid_str,
                                   uint16_t peer_lid, uint32_t peer_qp_num) {
    // Parse GID string to raw bytes
    ibv_gid peer_gid = {};
    std::istringstream iss(peer_gid_str);
    for (int i = 0; i < 16; ++i) {
        int value;
        iss >> std::hex >> value;
        peer_gid.raw[i] = static_cast<uint8_t>(value);
        if (i < 15) iss.ignore(1, ':');
    }

    // Modify to RTR
    ibv_qp_attr qp_attr = {};
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = IBV_MTU_4096;
    qp_attr.dest_qp_num = peer_qp_num;
    qp_attr.rq_psn = 0;
    qp_attr.max_dest_rd_atomic = 1;
    qp_attr.min_rnr_timer = 0x12;
    qp_attr.ah_attr.is_global = 1;
    qp_attr.ah_attr.dlid = peer_lid;
    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = ctx->portNum();
    memcpy(&qp_attr.ah_attr.grh.dgid, &peer_gid, 16);
    qp_attr.ah_attr.grh.flow_label = 0;
    qp_attr.ah_attr.grh.sgid_index = ctx->gidIndex();
    qp_attr.ah_attr.grh.hop_limit = 255;
    qp_attr.ah_attr.grh.traffic_class = 0;

    int ret = ibv_modify_qp(qp, &qp_attr,
                            IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                                IBV_QP_MIN_RNR_TIMER | IBV_QP_AV);
    if (ret) {
        PLOG(ERROR) << "Failed to modify notification QP to RTR";
        return -1;
    }

    // Modify to RTS
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = 0;
    qp_attr.timeout = 0x12;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.max_rd_atomic = 1;

    ret = ibv_modify_qp(qp, &qp_attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        PLOG(ERROR) << "Failed to modify notification QP to RTS";
        return -1;
    }

    return 0;
}

bool RdmaEndPoint::sendNotification(const std::string& name,
                                    const std::string& msg) {
    if (!notify_qp_ || !notify_connected_) {
        LOG(ERROR) << "Notification QP not connected";
        return false;
    }

    // Flow control: wait for pending sends to complete
    std::unique_lock<std::mutex> lock(notify_send_mutex_);
    notify_send_cv_.wait(lock, [this] {
        return notify_pending_count_ < kNotifyMaxPendingSends;
    });

    // Serialize: [name_len(4)][name][msg_len(4)][msg]
    size_t total_size = 4 + name.size() + 4 + msg.size();
    if (total_size > notify_send_buffer_.size()) {
        LOG(ERROR) << "Notification message too large: " << total_size;
        return false;
    }

    uint32_t name_len = name.size();
    uint32_t msg_len = msg.size();
    std::memcpy(notify_send_buffer_.data(), &name_len, 4);
    std::memcpy(notify_send_buffer_.data() + 4, name.data(), name.size());
    std::memcpy(notify_send_buffer_.data() + 4 + name.size(), &msg_len, 4);
    std::memcpy(notify_send_buffer_.data() + 4 + name.size() + 4, msg.data(),
                msg.size());

    // Post send
    ibv_sge sge = {};
    sge.addr = reinterpret_cast<uint64_t>(notify_send_buffer_.data());
    sge.length = total_size;
    sge.lkey = notify_send_mr_->lkey;

    ibv_send_wr wr = {};
    wr.wr_id = notify_send_wr_id_++;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    ibv_send_wr* bad_wr = nullptr;
    int ret = ibv_post_send(notify_qp_, &wr, &bad_wr);
    if (ret) {
        PLOG(ERROR) << "Failed to post notification send, "
                    << "bad_wr id: " << (bad_wr ? bad_wr->wr_id : -1)
                    << ", endpoint: " << peer_nic_name_ << " of "
                    << peer_server_name_ << ", error code " << ret;
        return false;
    }

    notify_pending_count_++;
    return true;
}

bool RdmaEndPoint::handleNotifyRecv(size_t buffer_idx, size_t byte_len) {
    if (buffer_idx >= notify_recv_buffers_.size()) {
        LOG(ERROR) << "Invalid recv buffer index: " << buffer_idx;
        return false;
    }

    // Silent retry for byte_len == 0
    if (byte_len == 0) {
        postNotifyRecv(buffer_idx);
        return false;
    }

    char* data = notify_recv_buffers_[buffer_idx].data();
    size_t len = byte_len;

    // Deserialize: [name_len(4)][name][msg_len(4)][msg]
    if (len < 8) {
        LOG(ERROR) << "Invalid notification message size: " << len;
        postNotifyRecv(buffer_idx);
        return false;
    }

    uint32_t name_len = *reinterpret_cast<uint32_t*>(data);
    if (name_len > len - 8) {
        LOG(ERROR) << "Invalid notification message format (name too long)";
        postNotifyRecv(buffer_idx);
        return false;
    }

    std::string name(data + 4, name_len);
    uint32_t msg_len = *reinterpret_cast<uint32_t*>(data + 4 + name_len);
    if (msg_len > len - 8 - name_len) {
        LOG(ERROR) << "Invalid notification message format (msg too long)";
        postNotifyRecv(buffer_idx);
        return false;
    }

    std::string msg(data + 4 + name_len + 4, msg_len);

    // Add directly to transport queue (skip endpoint queue for lower latency)
    context_->transport_.addNotificationToQueue(name, msg);

    // Repost recv buffer
    postNotifyRecv(buffer_idx);
    return true;
}

void RdmaEndPoint::handleNotifySendComplete(uint64_t wr_id) {
    std::lock_guard<std::mutex> lock(notify_send_mutex_);
    if (notify_pending_count_ > 0) {
        notify_pending_count_--;
        notify_send_cv_.notify_one();
    }
}
}  // namespace tent
}  // namespace mooncake