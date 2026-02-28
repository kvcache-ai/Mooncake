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

#include "transport/rdma_transport/rdma_endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>

#include "config.h"

namespace mooncake {
const static uint8_t MAX_HOP_LIMIT = 16;
const static uint8_t TIMEOUT = 14;
const static uint8_t RETRY_CNT = 7;

RdmaEndPoint::RdmaEndPoint(RdmaContext &context)
    : context_(context),
      status_(INITIALIZING),
      active_(true),
      cq_outstanding_(nullptr) {}

RdmaEndPoint::~RdmaEndPoint() {
    if (!qp_list_.empty()) deconstruct();
}

int RdmaEndPoint::construct(ibv_cq *cq, size_t num_qp_list,
                            size_t max_sge_per_wr, size_t max_wr_depth,
                            size_t max_inline_bytes) {
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    qp_list_.resize(num_qp_list);
    cq_outstanding_ = (volatile int *)cq->cq_context;

    max_wr_depth_ = (int)max_wr_depth;
    wr_depth_list_ = new volatile int[num_qp_list];
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }
    for (size_t i = 0; i < num_qp_list; ++i) {
        wr_depth_list_[i] = 0;
        ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_RC;
        attr.qp_context = this;
        attr.cap.max_send_wr = attr.cap.max_recv_wr = max_wr_depth;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = max_sge_per_wr;
        attr.cap.max_inline_data = max_inline_bytes;
        qp_list_[i] = ibv_create_qp(context_.pd(), &attr);
        if (!qp_list_[i]) {
            PLOG(ERROR) << "Failed to create QP";
            return ERR_ENDPOINT;
        }
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int RdmaEndPoint::deconstruct() {
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (ibv_destroy_qp(qp_list_[i])) {
            PLOG(ERROR) << "Failed to destroy QP";
            return ERR_ENDPOINT;
        }
        // After destroying QP, the wr_depth_list_ won't change
        bool displayed = false;
        if (wr_depth_list_[i] != 0) {
            if (!displayed) {
                LOG(WARNING) << "Outstanding work requests found, CQ will not "
                                "be generated";
                displayed = true;
            }
            __sync_fetch_and_sub(cq_outstanding_, wr_depth_list_[i]);
            wr_depth_list_[i] = 0;
        }
    }
    qp_list_.clear();
    delete[] wr_depth_list_;
    return 0;
}

int RdmaEndPoint::destroyQP() { return deconstruct(); }

void RdmaEndPoint::setPeerNicPath(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Previous connection will be discarded";
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

int RdmaEndPoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(INFO) << "Connection has been established";
        return 0;
    }

    // loopback mode
    if (context_.nicPath() == peer_nic_path_) {
        auto segment_desc =
            context_.engine().meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (segment_desc) {
            for (auto &nic : segment_desc->devices)
                if (nic.name == context_.deviceName())
                    return doSetupConnection(nic.gid, nic.lid, qpNum());
        }
        LOG(ERROR) << "Peer NIC " << context_.deviceName()
                   << " not found in localhost";
        return ERR_DEVICE_NOT_FOUND;
    }

    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                             peer_desc);
    if (rc) return rc;
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Reject the handshake request by peer "
                   << local_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.local_nic_path != peer_nic_path_ ||
        peer_desc.peer_nic_path != local_desc.local_nic_path) {
        LOG(ERROR) << "Invalid argument: received packet mismatch, "
                      "local.local_nic_path: "
                   << local_desc.local_nic_path
                   << ", local.peer_nic_path: " << local_desc.peer_nic_path
                   << ", peer.local_nic_path: " << peer_desc.local_nic_path
                   << ", peer.peer_nic_path: " << peer_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto &nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num);
    }
    LOG(ERROR) << "Peer NIC " << peer_nic_name << " not found in "
               << peer_server_name;
    return ERR_DEVICE_NOT_FOUND;
}

int RdmaEndPoint::setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                            HandShakeDesc &local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Re-establish connection: " << toString();
        disconnectUnlocked();
    }

    if (peer_desc.peer_nic_path != context_.nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg =
            "Invalid argument: peer nic path inconsistency, expect " +
            context_.nicPath() + " + " + peer_nic_path_ + ", while got " +
            peer_desc.peer_nic_path + " + " + peer_desc.local_nic_path;

        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();

    auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto &nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num,
                                         &local_desc.reply_msg);
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

void RdmaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    disconnectUnlocked();
}

void RdmaEndPoint::disconnectUnlocked() {
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int ret = ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (ret) PLOG(ERROR) << "Failed to modify QP to RESET";
        // After resetting QP, the wr_depth_list_ won't change
        bool displayed = false;
        if (wr_depth_list_[i] != 0) {
            if (!displayed) {
                LOG(WARNING) << "Outstanding work requests found, CQ will not "
                                "be generated";
                displayed = true;
            }
            __sync_fetch_and_sub(cq_outstanding_, wr_depth_list_[i]);
            wr_depth_list_[i] = 0;
        }
    }
    status_.store(UNCONNECTED, std::memory_order_release);
}

const std::string RdmaEndPoint::toString() const {
    auto status = status_.load(std::memory_order_relaxed);
    if (status == CONNECTED)
        return "EndPoint: local " + context_.nicPath() + ", peer " +
               peer_nic_path_;
    else
        return "EndPoint: local " + context_.nicPath() + " (unconnected)";
}

bool RdmaEndPoint::hasOutstandingSlice() const {
    if (active_) return true;
    for (size_t i = 0; i < qp_list_.size(); i++)
        if (wr_depth_list_[i] != 0) return true;
    return false;
}

int RdmaEndPoint::submitPostSend(
    std::vector<Transport::Slice *> &slice_list,
    std::vector<Transport::Slice *> &failed_slice_list) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!active_) return 0;
    int qp_index = SimpleRandom::Get().next(qp_list_.size());
    int wr_count = std::min(max_wr_depth_ - wr_depth_list_[qp_index],
                            (int)slice_list.size());
    wr_count =
        std::min(int(globalConfig().max_cqe) - *cq_outstanding_, wr_count);
    if (wr_count <= 0) return 0;

    std::vector<ibv_send_wr> wr_list(wr_count);
    std::vector<ibv_sge> sge_list(wr_count);
    ibv_send_wr *bad_wr = nullptr;
    memset(wr_list.data(), 0, sizeof(ibv_send_wr) * wr_count);
    for (int i = 0; i < wr_count; ++i) {
        auto slice = slice_list[i];
        auto &sge = sge_list[i];
        sge.addr = (uint64_t)slice->source_addr;
        sge.length = slice->length;
        sge.lkey = slice->rdma.source_lkey;

        auto &wr = wr_list[i];
        wr.wr_id = (uint64_t)slice;
        wr.opcode = slice->opcode == Transport::TransferRequest::READ
                        ? IBV_WR_RDMA_READ
                        : IBV_WR_RDMA_WRITE;
        wr.num_sge = 1;
        wr.sg_list = &sge;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
        wr.imm_data = 0;
        wr.wr.rdma.remote_addr = slice->rdma.dest_addr;
        wr.wr.rdma.rkey = slice->rdma.dest_rkey;
        slice->ts = getCurrentTimeInNano();
        slice->status = Transport::Slice::POSTED;
        slice->rdma.qp_depth = &wr_depth_list_[qp_index];
    }
    __sync_fetch_and_add(&wr_depth_list_[qp_index], wr_count);
    __sync_fetch_and_add(cq_outstanding_, wr_count);
    int rc = ibv_post_send(qp_list_[qp_index], wr_list.data(), &bad_wr);
    if (rc) {
        PLOG(ERROR) << "Failed to ibv_post_send";
        while (bad_wr) {
            int i = bad_wr - wr_list.data();
            failed_slice_list.push_back(slice_list[i]);
            __sync_fetch_and_sub(&wr_depth_list_[qp_index], 1);
            __sync_fetch_and_sub(cq_outstanding_, 1);
            bad_wr = bad_wr->next;
        }
    }
    slice_list.erase(slice_list.begin(), slice_list.begin() + wr_count);
    return 0;
}

size_t RdmaEndPoint::getQPNumber() const { return qp_list_.size(); }

std::vector<uint32_t> RdmaEndPoint::qpNum() const {
    std::vector<uint32_t> ret;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        ret.push_back(qp_list_[qp_index]->qp_num);
    return ret;
}

int RdmaEndPoint::doSetupConnection(const std::string &peer_gid,
                                    uint16_t peer_lid,
                                    std::vector<uint32_t> peer_qp_num_list,
                                    std::string *reply_msg) {
    if (qp_list_.size() != peer_qp_num_list.size()) {
        std::string message =
            "QP count mismatch in peer and local endpoints, check "
            "MC_MAX_EP_PER_CTX";
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        int ret = doSetupConnection(qp_index, peer_gid, peer_lid,
                                    peer_qp_num_list[qp_index], reply_msg);
        if (ret) return ret;
    }

    status_.store(CONNECTED, std::memory_order_relaxed);
    return 0;
}

int RdmaEndPoint::doSetupConnection(int qp_index, const std::string &peer_gid,
                                    uint16_t peer_lid, uint32_t peer_qp_num,
                                    std::string *reply_msg) {
    if (qp_index < 0 || qp_index > (int)qp_list_.size())
        return ERR_INVALID_ARGUMENT;
    auto &qp = qp_list_[qp_index];

    // Any state -> RESET
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    int ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    if (ret) {
        std::string message = "Failed to modify QP to RESET";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context_.portNum();
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    ret = ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret) {
        std::string message =
            "Failed to modify QP to INIT, check local context port num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // INIT -> RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = context_.activeMTU();
    if (globalConfig().mtu_length < attr.path_mtu)
        attr.path_mtu = globalConfig().mtu_length;
    ibv_gid peer_gid_raw;
    std::istringstream iss(peer_gid);
    for (int i = 0; i < 16; ++i) {
        int value;
        iss >> std::hex >> value;
        peer_gid_raw.raw[i] = static_cast<uint8_t>(value);
        if (i < 15) iss.ignore(1, ':');
    }
    attr.ah_attr.grh.dgid = peer_gid_raw;
    // TODO gidIndex and portNum must fetch from REMOTE
    attr.ah_attr.grh.sgid_index = context_.gidIndex();
    attr.ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
    // Set traffic class if configured (-1 means use default)
    if (globalConfig().ib_traffic_class >= 0) {
        attr.ah_attr.grh.traffic_class =
            static_cast<uint8_t>(globalConfig().ib_traffic_class);
    }
    attr.ah_attr.dlid = peer_lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.static_rate = 0;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = context_.portNum();
    attr.dest_qp_num = peer_qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;  // 12 in previous implementation
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER |
                            IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN);
    if (ret) {
        std::string message =
            "Failed to modify QP to RTR, check mtu, gid, peer lid, peer qp num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = TIMEOUT;
    attr.retry_cnt = RETRY_CNT;
    attr.rnr_retry = 7;  // or 7,RNR error
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        std::string message = "Failed to modify QP to RTS";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    return 0;
}
}  // namespace mooncake
