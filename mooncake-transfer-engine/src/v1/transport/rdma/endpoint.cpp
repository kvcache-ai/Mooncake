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

#include "v1/transport/rdma/endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>

#include "v1/common/status.h"
#include "v1/transport/rdma/context.h"
#include "v1/utility/system.h"
#include "v1/utility/string_builder.h"

namespace mooncake {
namespace v1 {
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

RdmaEndPoint::RdmaEndPoint() : status_(EP_UNINIT) {}

RdmaEndPoint::~RdmaEndPoint() {
    if (status_ != EP_UNINIT) deconstruct();
}

int RdmaEndPoint::construct(RdmaContext *context, EndPointParams *params,
                            const std::string &endpoint_name) {
    context_ = context;
    params_ = params;
    endpoint_name_ = endpoint_name;
    inflight_slices_ = 0;
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
        attr.cap.max_send_wr = attr.cap.max_recv_wr = params_->max_qp_wr;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = params_->max_sge;
        attr.cap.max_inline_data = params_->max_inline_bytes;
        qp_list_[i] = ibv_create_qp(context_->nativePD(), &attr);
        if (!qp_list_[i]) {
            PLOG(ERROR) << "RdmaEndPoint Failure: ibv_create_qp";
            deconstruct();
            return ERR_ENDPOINT;
        }
        slice_queue_.emplace_back(
            new BoundedSliceQueue(params_->max_qp_wr + 16));
    }
    status_ = EP_HANDSHAKING;
    return 0;
}

int RdmaEndPoint::deconstruct() {
    if (status_ == EP_UNINIT) return 0;
    status_ = EP_RESET;
    resetInflightSlices();
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (ibv_destroy_qp(qp_list_[i]))
            PLOG(ERROR) << "RdmaEndPoint Failure: ibv_destroy_qp";
        delete slice_queue_[i];
    }
    qp_list_.clear();
    slice_queue_.clear();
    delete[] wr_depth_list_;
    wr_depth_list_ = nullptr;
    status_ = EP_UNINIT;
    return 0;
}

Status RdmaEndPoint::connect(const std::string &peer_server_name,
                             const std::string &peer_nic_name) {
    RWSpinlock::WriteGuard guard(lock_);
    if (status_ == EP_READY) return Status::OK();
    if (status_ != EP_HANDSHAKING)
        return Status::InvalidArgument("Endpoint not in HANDSHAKING state");
    auto &transport = context_->transport_;
    auto &manager = transport.metadata_->segmentManager();
    auto qp_num = qpNum();
    BootstrapDesc local_desc, peer_desc;
    local_desc.local_nic_path =
        MakeNicPath(transport.local_segment_name_, context_->name());
    local_desc.peer_nic_path = MakeNicPath(peer_server_name, peer_nic_name);
    local_desc.qp_num = qp_num;
    std::shared_ptr<SegmentDesc> segment_desc;
    if (local_desc.local_nic_path == local_desc.peer_nic_path) {
        segment_desc = manager.getLocal();
    } else {
        CHECK_STATUS(manager.getRemote(segment_desc, peer_server_name));
        auto rpc_server_addr = getRpcServerAddr(segment_desc.get());
        assert(!rpc_server_addr.empty());
        CHECK_STATUS(
            RpcClient::bootstrap(rpc_server_addr, local_desc, peer_desc));
        qp_num = peer_desc.qp_num;
    }
    assert(qp_num.size() && segment_desc);
    auto dev_desc = getDeviceDesc(segment_desc.get(), peer_nic_name);
    if (!dev_desc)
        return Status::InvalidArgument("Unable to find RDMA device" LOC_MARK);
    int rc = setupAllQPs(dev_desc->gid, dev_desc->lid, qp_num);
    if (rc) {
        return Status::InternalError(
            "Failed to configure RDMA endpoint" LOC_MARK);
    }
    return Status::OK();
}

Status RdmaEndPoint::accept(const BootstrapDesc &peer_desc,
                            BootstrapDesc &local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (status_ == EP_READY) {
        LOG(WARNING) << "EndPoint in READY state, convert to RESET state";
        resetUnlocked();
    } else if (status_ != EP_HANDSHAKING)
        return Status::InvalidArgument("Endpoint not in HANDSHAKING state");
    auto &transport = context_->transport_;
    auto &manager = transport.metadata_->segmentManager();
    auto peer_nic_path = peer_desc.local_nic_path;
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path);
    if (peer_server_name.empty() || peer_nic_name.empty())
        return Status::InvalidArgument("Invalid peer_nic_path in request");
    local_desc.local_nic_path =
        MakeNicPath(transport.local_segment_name_, context_->name());
    local_desc.peer_nic_path = peer_nic_path;
    local_desc.qp_num = qpNum();
    SegmentDescRef segment_desc;
    CHECK_STATUS(manager.getRemote(segment_desc, peer_server_name));
    auto dev_desc = getDeviceDesc(segment_desc.get(), peer_nic_name);
    if (!dev_desc)
        return Status::InvalidArgument("Unable to find RDMA device" LOC_MARK);
    int rc = setupAllQPs(dev_desc->gid, dev_desc->lid, peer_desc.qp_num);
    if (rc) {
        return Status::InternalError(
            "Failed to configure RDMA endpoint" LOC_MARK);
    }
    return Status::OK();
}

int RdmaEndPoint::reset() {
    RWSpinlock::WriteGuard guard(lock_);
    return resetUnlocked();
}

int RdmaEndPoint::resetUnlocked() {
    if (status_ == EP_UNINIT) return 0;
    status_ = EP_RESET;
    resetInflightSlices();
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int ret = ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (ret) {
            PLOG(ERROR) << "Failed to modify QP to RESET";
            deconstruct();
            return ERR_ENDPOINT;
        }
    }
    for (size_t i = 0; i < qp_list_.size(); ++i) wr_depth_list_[i].value = 0;
    status_ = EP_HANDSHAKING;
    return 0;
}

int RdmaEndPoint::setupAllQPs(const std::string &peer_gid, uint16_t peer_lid,
                              std::vector<uint32_t> peer_qp_num_list,
                              std::string *reply_msg) {
    if (status_ == EP_READY) {
        LOG(WARNING) << "Reconfigure an already READY endpoint ["
                     << endpoint_name_ << "]";
        if (reset()) return ERR_ENDPOINT;
    }

    if (qp_list_.size() != peer_qp_num_list.size()) {
        std::string message =
            "[ERR-01] Inconsistent qp_mul_factor: local (expected) " +
            std::to_string(qp_list_.size()) + ", peer " +
            std::to_string(peer_qp_num_list.size());
        LOG(ERROR) << "[TE ConnEst] Handshake error detected: " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        int ret = setupOneQP(qp_index, peer_gid, peer_lid,
                             peer_qp_num_list[qp_index], reply_msg);
        if (ret) return ret;
    }

    status_ = EP_READY;
    return 0;
}

static ibv_wr_opcode getOpCode(RdmaSlice *slice) {
    switch (slice->task->request.opcode) {
        case Request::READ:
            return IBV_WR_RDMA_READ;
        case Request::WRITE:
            return IBV_WR_RDMA_WRITE;
        default:
            return IBV_WR_RDMA_READ;
    }
}

int RdmaEndPoint::submitSlices(std::vector<RdmaSlice *> &slice_list,
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
        auto &wr = wr_list[wr_idx];
        for (int sge_off = 0; sge_off < kSgeEntries; ++sge_off) {
            auto &sge = sge_list[sge_idx + sge_off];
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

    auto &queue = slice_queue_[qp_index];
    for (int wr_idx = 0; wr_idx < wr_count; ++wr_idx) {
        auto current = slice_list[wr_idx];
        if (!current->failed) queue->push(current);
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
        return ERR_ENDPOINT;
    }
    return 1;
}

void RdmaEndPoint::resetInflightSlices() {
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        auto &queue = slice_queue_[qp_index];
        while (!queue->empty()) {
            auto current = queue->pop();
            markSliceFailed(current);
        }
    }
}

void RdmaEndPoint::acknowledge(RdmaSlice *slice, SliceCallbackType status) {
    auto qp_index = slice->qp_index;
    auto &queue = slice_queue_[qp_index];
    if (!queue->contains(slice)) return;
    int num_entries = 0;
    RdmaSlice *current = nullptr;
    do {
        current = queue->pop();
        num_entries++;
        if (status == SliceCallbackType::SUCCESS)
            markSliceSuccess(current);
        else if (status == SliceCallbackType::FAILED)
            markSliceFailed(current);
    } while (current != slice);
    __sync_fetch_and_sub(&wr_depth_list_[qp_index].value, num_entries);
    __sync_fetch_and_sub(&inflight_slices_, num_entries);
}

void RdmaEndPoint::evictTimeoutSlices() {
    auto current_ts = getCurrentTimeInNano();
    const static uint64_t kTimeoutInNano = 15000000000ull;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        auto &queue = slice_queue_[qp_index];
        while (!queue->empty()) {
            auto current = queue->peek();
            if (current_ts - current->enqueue_ts > kTimeoutInNano) {
                markSliceFailed(current);
                queue->pop();
            }
        }
    }
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
    if (prev_depth_list + num_entries > params_->max_qp_wr) {
        cancelQuota(qp_index, num_entries);
        return false;
    }
    __sync_fetch_and_add(&inflight_slices_, num_entries);
    return true;
}

void RdmaEndPoint::cancelQuota(int qp_index, int num_entries) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    __sync_fetch_and_sub(&wr_depth_list_[qp_index].value, num_entries);
    auto cq = context_->cq(qp_index % context_->cqCount());
    cq->cancelQuota(num_entries);
}

int RdmaEndPoint::setupOneQP(int qp_index, const std::string &peer_gid,
                             uint16_t peer_lid, uint32_t peer_qp_num,
                             std::string *reply_msg) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    auto &qp = qp_list_[qp_index];

    // RESET -> INIT
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context().portNum();
    attr.pkey_index = params_->pkey_index;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    int ret = ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret) {
        std::string message =
            "[ERR-02] Failed to modify QP's state to INIT, check local context "
            "port num. Error code: " +
            std::to_string(ret);
        LOG(ERROR) << "[TE ConnEst] Connection establish error detected: "
                   << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
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
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER |
                            IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN);
    if (ret) {
        std::string message =
            "[ERR-03] Failed to modify QP to RTR, check the connectivity "
            "between two NICs and detected value of MTU, GID, LID. "
            "Error code: " +
            std::to_string(ret);
        LOG(ERROR) << "[TE ConnEst] Connection establish error detected: "
                   << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = params_->send_timeout;
    attr.retry_cnt = params_->send_retry_count;
    attr.rnr_retry = params_->send_rnr_count;
    attr.sq_psn = params_->sq_psn;
    attr.max_rd_atomic = params_->max_rd_atomic;
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        std::string message =
            "[ERR-04] Failed to modify QP to RTS. Error code: " +
            std::to_string(ret);
        LOG(ERROR) << "[TE ConnEst] Connection establish error detected: "
                   << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    return 0;
}
}  // namespace v1
}  // namespace mooncake