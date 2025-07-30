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

#define ASSERT_STATUS(status)                                                \
    do {                                                                     \
        if (status_ != (status)) {                                           \
            LOG(FATAL) << "Detected incorrect status in endpoint ["          \
                       << endpoint_name_ << "], expect " #status ", actual " \
                       << statusToString(status_);                           \
            exit(EXIT_FAILURE);                                              \
        }                                                                    \
    } while (0)

#define ASSERT_STATUS_NOT(status)                                   \
    do {                                                            \
        if (status_ == (status)) {                                  \
            LOG(FATAL) << "Detected incorrect status in endpoint [" \
                       << endpoint_name_                            \
                       << "], expect NOT " #status ", actual "      \
                       << statusToString(status_);                  \
            exit(EXIT_FAILURE);                                     \
        }                                                           \
    } while (0)

namespace mooncake {
namespace v1 {
static inline const std::string statusToString(
    RdmaEndPoint::EndPointStatus status) {
    switch (status) {
        case RdmaEndPoint::EP_UNINIT:
            return "EP_UNINIT";
        case RdmaEndPoint::EP_DISABLED:
            return "EP_DISABLED";
        case RdmaEndPoint::EP_INPROGRESS:
            return "EP_INPROGRESS";
        case RdmaEndPoint::EP_READY:
            return "EP_READY";
    }
    return "UNKNOWN";
}

RdmaEndPoint::RdmaEndPoint() : status_(EP_UNINIT) {}

RdmaEndPoint::~RdmaEndPoint() {
    if (status_ != EP_UNINIT) disable();
}

int RdmaEndPoint::construct(RdmaContext *context, EndPointParams *params,
                            const std::string &endpoint_name) {
    ASSERT_STATUS(EP_UNINIT);
    context_ = context;
    params_ = params;
    endpoint_name_ = endpoint_name;
    status_ = EP_DISABLED;
    return enable();
}

int RdmaEndPoint::enable() {
    RWSpinlock::WriteGuard guard(ep_lock_);
    ASSERT_STATUS_NOT(EP_UNINIT);
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
        qp_list_[i] = ibv_create_qp(context().nativePD(), &attr);
        if (!qp_list_[i]) {
            PLOG(ERROR) << "RdmaEndPoint Failure: ibv_create_qp";
            disable();
            return ERR_ENDPOINT;
        }
    }
    status_ = EP_INPROGRESS;
    return 0;
}

int RdmaEndPoint::disable() {
    RWSpinlock::WriteGuard guard(ep_lock_);
    ASSERT_STATUS_NOT(EP_UNINIT);
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        // TODO force cancel these work requests
        if (wr_depth_list_[i].value != 0)
            LOG(WARNING)
                << "Outstanding work requests found, CQ will not be generated";
        if (ibv_destroy_qp(qp_list_[i])) {
            PLOG(ERROR) << "RdmaEndPoint Failure: ibv_destroy_qp";
        }
    }

    qp_list_.clear();
    delete[] wr_depth_list_;
    wr_depth_list_ = nullptr;
    status_ = EP_DISABLED;
    return 0;
}

int RdmaEndPoint::reset() {
    RWSpinlock::WriteGuard guard(ep_lock_);
    ASSERT_STATUS_NOT(EP_UNINIT);
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        // TODO force cancel these work requests
        if (wr_depth_list_[i].value != 0)
            LOG(WARNING)
                << "Outstanding work requests found, CQ will not be generated";
    }
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int ret = ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (ret) {
            PLOG(ERROR) << "Failed to modify QP to RESET";
            disable();
            return ERR_ENDPOINT;
        }
    }
    for (size_t i = 0; i < qp_list_.size(); ++i) wr_depth_list_[i].value = 0;
    status_ = EP_INPROGRESS;
    return 0;
}

int RdmaEndPoint::configurePeer(const std::string &peer_gid, uint16_t peer_lid,
                                std::vector<uint32_t> peer_qp_num_list,
                                std::string *reply_msg) {
    if (status_ == EP_READY) {
        LOG(WARNING) << "Received a new handshake request, reset endpoint "
                     << endpoint_name_;
        if (reset()) return ERR_ENDPOINT;
    }
    RWSpinlock::WriteGuard guard(ep_lock_);
    if (qp_list_.size() != peer_qp_num_list.size()) {
        std::string message =
            "[ERR-01] Inconsistent qp_mul_factor: local (expected) " +
            std::to_string(qp_list_.size()) + ", peer " +
            std::to_string(peer_qp_num_list.size());
        LOG(ERROR) << "[TE ConnEst] Connection establish error detected: "
                   << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        int ret = setupSingleQueuePair(qp_index, peer_gid, peer_lid,
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
    // RWSpinlock::ReadGuard guard(ep_lock_);
    const static int kSgeEntries = 1;

    ASSERT_STATUS(EP_READY);
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
        current->endpoint_quota = &wr_depth_list_[qp_index].value;
        current->failed = false;
        wr.wr_id = (uint64_t)current;
        wr.opcode = getOpCode(current);
        wr.num_sge = kSgeEntries;
        wr.sg_list = &sge_list[sge_idx];
        wr.send_flags = IBV_SEND_SIGNALED;  // TODO remove it for performance
        wr.next = (wr_idx + 1 == wr_count) ? nullptr : &wr_list[wr_idx + 1];
        wr.imm_data = 0;  // TODO notification signal for remote
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

    return wr_count;
}

int RdmaEndPoint::submitRecvImmDataRequest(int qp_index, uint64_t id) {
    RWSpinlock::ReadGuard guard(ep_lock_);
    ASSERT_STATUS(EP_READY);
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

std::vector<uint32_t> RdmaEndPoint::qpNum() {
    RWSpinlock::ReadGuard guard(ep_lock_);
    std::vector<uint32_t> ret;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        ret.push_back(qp_list_[qp_index]->qp_num);
    return ret;
}

int RdmaEndPoint::outstandingSlices() const {
    int sum = 0;
    for (int i = 0; i < (int)qp_list_.size(); ++i)
        sum += wr_depth_list_[i].value;
    return sum;
}

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
    return true;
}

void RdmaEndPoint::cancelQuota(int qp_index, int num_entries) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());
    __sync_fetch_and_sub(&wr_depth_list_[qp_index].value, num_entries);
    auto cq = context_->cq(qp_index % context_->cqCount());
    cq->cancelQuota(num_entries);
}

int RdmaEndPoint::setupSingleQueuePair(int qp_index,
                                       const std::string &peer_gid,
                                       uint16_t peer_lid, uint32_t peer_qp_num,
                                       std::string *reply_msg) {
    assert(qp_index >= 0 && qp_index < (int)qp_list_.size());

    auto &qp = qp_list_[qp_index];
    // Any state -> RESET
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    int ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    if (ret) {
        std::string message =
            "Failed to modify QP to RESET, error code " + std::to_string(ret);
        LOG(ERROR) << "RdmaEndPoint Failure: Handshake: " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context().portNum();
    attr.pkey_index = params_->pkey_index;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    ret = ibv_modify_qp(
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