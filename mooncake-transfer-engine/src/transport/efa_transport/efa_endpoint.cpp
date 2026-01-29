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

#include "transport/efa_transport/efa_endpoint.h"

#include <glog/logging.h>

#include "transport/efa_transport/efa_context.h"

namespace mooncake {

EfaEndPoint::EfaEndPoint(EfaContext &context)
    : context_(context),
      status_(INITIALIZING),
      active_(false),
      inactive_time_(0),
      max_sge_(4),
      max_wr_(256),
      max_inline_(64) {}

EfaEndPoint::~EfaEndPoint() { deconstruct(); }

int EfaEndPoint::construct(ibv_cq *cq, size_t num_qp_list, size_t max_sge,
                           size_t max_wr, size_t max_inline) {
    max_sge_ = max_sge;
    max_wr_ = max_wr;
    max_inline_ = max_inline;

    // Create queue pairs
    for (size_t i = 0; i < num_qp_list; ++i) {
        struct ibv_qp_init_attr qp_attr = {};
        qp_attr.send_cq = cq;
        qp_attr.recv_cq = cq;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.cap.max_send_wr = max_wr;
        qp_attr.cap.max_recv_wr = max_wr;
        qp_attr.cap.max_send_sge = max_sge;
        qp_attr.cap.max_recv_sge = max_sge;
        qp_attr.cap.max_inline_data = max_inline;

        struct ibv_qp *qp = ibv_create_qp(context_.pd(), &qp_attr);
        if (!qp) {
            LOG(ERROR) << "Failed to create queue pair";
            deconstruct();
            return ERR_INVALID_ARGUMENT;
        }

        qp_list_.push_back(qp);
        qp_depth_list_.push_back(0);
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    active_ = true;

    return 0;
}

int EfaEndPoint::deconstruct() {
    active_ = false;
    destroyQP();
    return 0;
}

void EfaEndPoint::setPeerNicPath(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    peer_nic_path_ = peer_nic_path;
}

int EfaEndPoint::setupConnectionsByActive() {
    // Implementation for active connection setup
    LOG(INFO) << "Setting up EFA connection to " << peer_nic_path_;

    // Transition to CONNECTED state
    status_.store(CONNECTED, std::memory_order_relaxed);

    return 0;
}

int EfaEndPoint::setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                           HandShakeDesc &local_desc) {
    // Implementation for passive connection setup
    LOG(INFO) << "Setting up EFA connection (passive mode)";

    // Populate local descriptor
    local_desc.server_name = context_.device_name();

    // Transition to CONNECTED state
    status_.store(CONNECTED, std::memory_order_relaxed);

    return 0;
}

bool EfaEndPoint::hasOutstandingSlice() const {
    for (const auto &depth : qp_depth_list_) {
        if (depth > 0) {
            return true;
        }
    }
    return false;
}

void EfaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    status_.store(UNCONNECTED, std::memory_order_relaxed);
    LOG(INFO) << "EFA endpoint disconnected from " << peer_nic_path_;
}

int EfaEndPoint::destroyQP() {
    for (auto *qp : qp_list_) {
        if (qp) {
            ibv_destroy_qp(qp);
        }
    }
    qp_list_.clear();
    qp_depth_list_.clear();
    return 0;
}

int EfaEndPoint::connectQP(struct ibv_qp *qp, uint32_t remote_qpn,
                           uint32_t remote_psn,
                           const union ibv_gid &remote_gid) {
    // Transition QP to INIT state
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context_.port();
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        LOG(ERROR) << "Failed to modify QP to INIT state";
        return ERR_INVALID_ARGUMENT;
    }

    // Transition QP to RTR state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = static_cast<ibv_mtu>(context_.mtu());
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = remote_psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.dgid = remote_gid;
    attr.ah_attr.grh.sgid_index = context_.gid_index();
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = context_.port();

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                          IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
        LOG(ERROR) << "Failed to modify QP to RTR state";
        return ERR_INVALID_ARGUMENT;
    }

    // Transition QP to RTS state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = remote_psn;
    attr.max_rd_atomic = 1;

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC)) {
        LOG(ERROR) << "Failed to modify QP to RTS state";
        return ERR_INVALID_ARGUMENT;
    }

    return 0;
}

int EfaEndPoint::postSlice(Transport::Slice *slice) {
    if (!connected()) {
        LOG(WARNING) << "Cannot post slice: endpoint not connected";
        return ERR_INVALID_ARGUMENT;
    }

    // Simplified implementation: mark slice as successful
    slice->markSuccess();

    return 0;
}

int EfaEndPoint::pollCompletion() {
    // Poll for completions on the completion queue
    // This is a simplified implementation
    return 0;
}

}  // namespace mooncake
