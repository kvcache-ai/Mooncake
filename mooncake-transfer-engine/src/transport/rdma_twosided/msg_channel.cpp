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

#include "transport/rdma_twosided/msg_channel.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_twosided/rdma_twosided_transport.h"

namespace mooncake {

namespace {

constexpr int kMsgHopLimit = 16;
constexpr int kMsgTimeout = 14;
constexpr int kMsgRetryCount = 7;

int parseGidString(const std::string &gid_str, ibv_gid &gid_out) {
    if (gid_str.empty()) return ERR_INVALID_ARGUMENT;
    std::istringstream iss(":" + gid_str);
    for (size_t i = 0; i < sizeof(gid_out.raw); i++) {
        if (iss.get() != ':') return ERR_INVALID_ARGUMENT;
        uint32_t byte = 0;
        iss >> std::hex >> byte;
        if (iss.fail() || byte > 0xFF) return ERR_INVALID_ARGUMENT;
        gid_out.raw[i] = static_cast<uint8_t>(byte);
    }
    char extra;
    if (iss.get(extra)) return ERR_INVALID_ARGUMENT;
    return 0;
}

}  // namespace

MsgChannel::MsgChannel(RdmaTwoSidedTransport &transport, RdmaContext &context,
                       std::string peer_server_name)
    : transport_(transport),
      context_(context),
      peer_server_name_(std::move(peer_server_name)) {
    pool_base_ = globalConfig().rdma_msg_pool_base;
    pool_max_ = globalConfig().rdma_msg_pool_max;
    if (pool_max_ < pool_base_) pool_max_ = pool_base_;
    max_pending_sends_ = pool_max_;
}

MsgChannel::~MsgChannel() { destroyResources(); }

int MsgChannel::construct() {
    std::lock_guard<std::mutex> lock(resource_mutex_);
    if (qp_) return 0;
    return createResources();
}

int MsgChannel::createResources() {
    auto &cfg = globalConfig();
    pool_base_ = cfg.rdma_msg_pool_base;
    pool_max_ = cfg.rdma_msg_pool_max;
    if (pool_max_ < pool_base_) pool_max_ = pool_base_;
    size_t slots = pool_base_;
    size_t slot_size = cfg.rdma_msg_slot_size;
    // Soft send cap tracks peer RQ; hard QP/inflight sized to pool_max.
    max_pending_sends_ = pool_max_;
    if (slots == 0 || pool_max_ == 0 || slot_size < kMsgHeaderSize + 64) {
        return ERR_INVALID_ARGUMENT;
    }

    cq_ = ibv_create_cq(context_.context(), static_cast<int>(cfg.max_cqe),
                        nullptr, nullptr, 0);
    if (!cq_) {
        PLOG(ERROR) << "MsgChannel: failed to create CQ for "
                    << peer_server_name_;
        return ERR_ENDPOINT;
    }

    if (pool_.construct(context_.pd(), slot_size, slots)) {
        destroyResources();
        return ERR_MEMORY;
    }

    ibv_qp_init_attr attr{};
    attr.send_cq = cq_;
    attr.recv_cq = cq_;
    attr.qp_type = IBV_QPT_RC;
    // Size QP to pool_max so background expand can post_recv without recreate.
    attr.cap.max_send_wr = static_cast<uint32_t>(pool_max_);
    attr.cap.max_recv_wr = static_cast<uint32_t>(pool_max_);
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = static_cast<uint32_t>(cfg.max_inline);

    qp_ = ibv_create_qp(context_.pd(), &attr);
    if (!qp_) {
        PLOG(ERROR) << "MsgChannel: failed to create QP for "
                    << peer_server_name_;
        destroyResources();
        return ERR_ENDPOINT;
    }

    ibv_qp_attr qp_attr{};
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.port_num = context_.portNum();
    qp_attr.pkey_index = cfg.pkey_index;
    qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
    if (ibv_modify_qp(qp_, &qp_attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        PLOG(ERROR) << "MsgChannel: QP INIT failed";
        destroyResources();
        return ERR_ENDPOINT;
    }

    inflight_slots_.assign(pool_max_, -1);
    if (repostAllRecvs()) {
        destroyResources();
        return ERR_ENDPOINT;
    }
    // construct() only runs from connectActive() / acceptPassive(), i.e. after
    // make_shared has published the owner.
    context_.registerMsgChannel(shared_from_this());
    registered_with_context_ = true;
    return 0;
}

void MsgChannel::destroyResources() {
    if (registered_with_context_) {
        context_.unregisterMsgChannel(this);
        registered_with_context_ = false;
    }
    connected_.store(false, std::memory_order_release);
    if (qp_) {
        ibv_destroy_qp(qp_);
        qp_ = nullptr;
    }
    if (cq_) {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
    pool_.destroy();
    inflight_slots_.clear();
    pending_sends_ = 0;
}

std::string MsgChannel::nicPath() const { return context_.nicPath(); }

int MsgChannel::postRecv(size_t idx) {
    if (!qp_ || idx >= pool_.slotCount()) return ERR_ENDPOINT;
    auto *mr = pool_.recvSlotMr(idx);
    auto *ptr = pool_.recvSlotPtr(idx);
    if (!mr || !ptr) return ERR_ENDPOINT;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(ptr);
    sge.length = static_cast<uint32_t>(pool_.slotSize());
    sge.lkey = mr->lkey;

    ibv_recv_wr wr{};
    wr.wr_id = idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    ibv_recv_wr *bad = nullptr;
    int ret = ibv_post_recv(qp_, &wr, &bad);
    if (ret) {
        LOG(ERROR) << "MsgChannel: ibv_post_recv failed: "
                   << strerror(std::abs(ret)) << " [" << ret << "]";
        return ERR_ENDPOINT;
    }
    pool_.markRecvPosted(idx, true);
    return 0;
}

int MsgChannel::repostAllRecvs() {
    for (size_t i = 0; i < pool_.activeCount(); ++i) {
        if (postRecv(i)) return ERR_ENDPOINT;
    }
    return 0;
}

int MsgChannel::expandPool(size_t extra) {
    if (extra == 0) return 0;
    int rc = 0;
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        if (!qp_) return ERR_ENDPOINT;
        size_t cur = pool_.slotCount();
        if (cur >= pool_max_) return 0;
        if (cur + extra > pool_max_) extra = pool_max_ - cur;
        size_t old = cur;
        if (pool_.expand(extra)) return ERR_MEMORY;
        for (size_t i = old; i < old + extra; ++i) {
            if (postRecv(i)) {
                rc = ERR_ENDPOINT;
                break;
            }
        }
        expand_hint_.store(0, std::memory_order_relaxed);
        LOG(INFO) << "MsgChannel: expanded bounce pool for "
                  << peer_server_name_ << " active=" << pool_.activeCount()
                  << " max=" << pool_max_;
    }
    // New send slots are available now: replay any held-back READ_RESP.
    drainPendingReadResps();
    return rc;
}

size_t MsgChannel::shrinkPoolToward(size_t target_active) {
    std::lock_guard<std::mutex> lock(resource_mutex_);
    if (!qp_) return pool_.activeCount();
    size_t next = pool_.shrinkToward(target_active, pool_base_);
    VLOG(1) << "MsgChannel: shrink toward " << target_active
            << " active=" << next << " allocated=" << pool_.slotCount()
            << " peer=" << peer_server_name_;
    return next;
}

void MsgChannel::fillLocalDesc(HandShakeDesc &local_desc) const {
    local_desc = HandShakeDesc();
    local_desc.local_nic_path = context_.nicPath();
    local_desc.local_lid = context_.lid();
    local_desc.local_gid = context_.gid();
    local_desc.peer_nic_path = "__msg__";
    local_desc.msg_channel = true;
    local_desc.msg_qp_num = msgQpNum();
    local_desc.msg_rq_depth = msgRqDepth();
}

int MsgChannel::connectQp(const std::string &peer_gid, uint16_t peer_lid,
                          uint32_t peer_qp_num) {
    if (!qp_ || peer_qp_num == 0) return ERR_INVALID_ARGUMENT;
    ibv_gid peer_gid_raw{};
    if (parseGidString(peer_gid, peer_gid_raw)) return ERR_INVALID_ARGUMENT;

    ibv_qp_attr query_attr{};
    ibv_qp_init_attr query_init{};
    if (ibv_query_qp(qp_, &query_attr, IBV_QP_STATE, &query_init)) {
        return ERR_ENDPOINT;
    }

    ibv_qp_attr attr{};
    if (query_attr.qp_state != IBV_QPS_INIT) {
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE)) return ERR_ENDPOINT;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = context_.portNum();
        attr.pkey_index = globalConfig().pkey_index;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
        if (ibv_modify_qp(qp_, &attr,
                          IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                              IBV_QP_ACCESS_FLAGS))
            return ERR_ENDPOINT;
        if (repostAllRecvs()) return ERR_ENDPOINT;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = context_.activeMTU();
    if (globalConfig().mtu_length < attr.path_mtu)
        attr.path_mtu = globalConfig().mtu_length;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.dgid = peer_gid_raw;
    attr.ah_attr.grh.sgid_index = context_.gidIndex();
    attr.ah_attr.grh.hop_limit = kMsgHopLimit;
    if (globalConfig().ib_traffic_class >= 0) {
        attr.ah_attr.grh.traffic_class =
            static_cast<uint8_t>(globalConfig().ib_traffic_class);
    }
    attr.ah_attr.dlid = peer_lid;
    attr.ah_attr.sl = 0;
    if (globalConfig().ib_service_level >= 0) {
        attr.ah_attr.sl = static_cast<uint8_t>(globalConfig().ib_service_level);
    }
    attr.ah_attr.port_num = context_.portNum();
    attr.dest_qp_num = peer_qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    if (ibv_modify_qp(qp_, &attr,
                      IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER |
                          IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC |
                          IBV_QP_DEST_QPN | IBV_QP_RQ_PSN)) {
        PLOG(ERROR) << "MsgChannel: QP RTR failed";
        return ERR_ENDPOINT;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = kMsgTimeout;
    attr.retry_cnt = kMsgRetryCount;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(qp_, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC)) {
        PLOG(ERROR) << "MsgChannel: QP RTS failed";
        return ERR_ENDPOINT;
    }
    connected_.store(true, std::memory_order_release);
    return 0;
}

int MsgChannel::connectActive() {
    if (construct()) return ERR_ENDPOINT;
    HandShakeDesc local_desc, peer_desc;
    fillLocalDesc(local_desc);
    int ret =
        transport_.sendHandshake(peer_server_name_, local_desc, peer_desc);
    if (ret) {
        LOG(ERROR) << "MsgChannel: active handshake failed to "
                   << peer_server_name_;
        return ret;
    }
    if (peer_desc.msg_qp_num == 0) {
        LOG(ERROR) << "MsgChannel: peer has no msg_qp_num";
        return ERR_ENDPOINT;
    }
    if (peer_desc.msg_rq_depth) {
        max_pending_sends_ =
            std::min(pool_max_, static_cast<size_t>(peer_desc.msg_rq_depth));
    }
    std::lock_guard<std::mutex> lock(resource_mutex_);
    ret = connectQp(peer_desc.local_gid, peer_desc.local_lid,
                    peer_desc.msg_qp_num);
    if (ret == 0) {
        LOG(INFO) << "MsgChannel: connected to " << peer_server_name_
                  << " local_qp=" << msgQpNum()
                  << " peer_qp=" << peer_desc.msg_qp_num
                  << " max_pending=" << max_pending_sends_;
    }
    return ret;
}

int MsgChannel::acceptPassive(const HandShakeDesc &peer_desc,
                              HandShakeDesc &local_desc) {
    if (construct()) {
        local_desc.reply_msg = "MsgChannel construct failed";
        return ERR_ENDPOINT;
    }
    if (peer_desc.msg_qp_num == 0) {
        local_desc.reply_msg = "Peer msg_qp_num missing";
        return ERR_INVALID_ARGUMENT;
    }
    if (peer_desc.msg_rq_depth) {
        max_pending_sends_ =
            std::min(pool_max_, static_cast<size_t>(peer_desc.msg_rq_depth));
    }
    std::lock_guard<std::mutex> lock(resource_mutex_);
    fillLocalDesc(local_desc);
    int ret = connectQp(peer_desc.local_gid, peer_desc.local_lid,
                        peer_desc.msg_qp_num);
    if (ret) {
        local_desc.reply_msg = "MsgChannel connectQp failed";
        return ret;
    }
    LOG(INFO) << "MsgChannel: accepted from " << peer_server_name_
              << " local_qp=" << msgQpNum()
              << " peer_qp=" << peer_desc.msg_qp_num
              << " max_pending=" << max_pending_sends_;
    return 0;
}

int MsgChannel::postSend(const MsgHeader &hdr, const void *payload,
                         uint32_t length) {
    if (!connected_.load(std::memory_order_acquire)) return ERR_ENDPOINT;
    size_t total = kMsgHeaderSize + length;
    if (total > pool_.slotSize()) return ERR_INVALID_ARGUMENT;

    std::unique_lock<std::mutex> lock(send_mutex_);
    if (pending_sends_ >= max_pending_sends_) {
        requestExpand();
        return ERR_TOO_MANY_REQUESTS;
    }

    // resource_mutex_ before BouncePool locks (same order as expand/shrink).
    std::lock_guard<std::mutex> resource_guard(resource_mutex_);
    if (!qp_) return ERR_ENDPOINT;

    int slot = pool_.acquireSendSlot();
    if (slot < 0) {
        requestExpand();
        return ERR_TOO_MANY_REQUESTS;
    }

    char *buf = pool_.slotPtr(slot);
    if (encodeMsgHeader(hdr, buf, kMsgHeaderSize)) {
        pool_.releaseSendSlot(slot);
        return ERR_INVALID_ARGUMENT;
    }
    if (length && payload) {
        std::memcpy(buf + kMsgHeaderSize, payload, length);
    }

    uint64_t wr_id = send_wr_id_++;
    {
        std::lock_guard<std::mutex> inflight(inflight_mutex_);
        size_t idx = wr_id % inflight_slots_.size();
        inflight_slots_[idx] = slot;
    }

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(buf);
    sge.length = static_cast<uint32_t>(total);
    sge.lkey = pool_.slotLkey(slot);

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    if (total <= globalConfig().max_inline) wr.send_flags |= IBV_SEND_INLINE;

    ibv_send_wr *bad = nullptr;
    int ret = ibv_post_send(qp_, &wr, &bad);
    if (ret) {
        // It is undefined whether the WR entered the queue on failure, so
        // clear the table entry as well as releasing the slot: a SEND
        // completion that does arrive must find -1 and skip the release
        // instead of double-freeing the slot.
        {
            std::lock_guard<std::mutex> inflight(inflight_mutex_);
            size_t idx = wr_id % inflight_slots_.size();
            if (inflight_slots_[idx] == slot) inflight_slots_[idx] = -1;
        }
        pool_.releaseSendSlot(slot);
        LOG(ERROR) << "MsgChannel: ibv_post_send failed: "
                   << strerror(std::abs(ret));
        return ERR_ENDPOINT;
    }
    pending_sends_++;
    return 0;
}

int MsgChannel::sendDataWrite(uint64_t task_id, uint32_t slice_seq,
                              uint64_t dest_addr, const void *src,
                              uint32_t length, uint32_t total_chunks) {
    MsgHeader hdr;
    hdr.type = MsgType::DATA_WRITE;
    hdr.session = transport_.localCtrlSessionId();
    hdr.task_id = task_id;
    hdr.slice_seq = slice_seq;
    hdr.dest_addr = dest_addr;
    hdr.length = length;
    hdr.total_chunks = total_chunks;
    return postSend(hdr, src, length);
}

int MsgChannel::sendReadReq(uint64_t task_id, uint32_t slice_seq,
                            uint64_t src_addr, uint32_t length) {
    MsgHeader hdr;
    hdr.type = MsgType::READ_REQ;
    hdr.session = transport_.localCtrlSessionId();
    hdr.task_id = task_id;
    hdr.slice_seq = slice_seq;
    hdr.dest_addr = src_addr;  // remote source address to read from
    hdr.length = length;
    return postSend(hdr, nullptr, 0);
}

int MsgChannel::sendReadResp(uint64_t task_id, uint32_t slice_seq,
                             uint64_t dest_addr, const void *src,
                             uint32_t length) {
    MsgHeader hdr;
    hdr.type = MsgType::READ_RESP;
    hdr.session = transport_.localCtrlSessionId();
    hdr.task_id = task_id;
    hdr.slice_seq = slice_seq;
    hdr.dest_addr = dest_addr;
    hdr.length = length;
    return postSend(hdr, src, length);
}

int MsgChannel::sendOrQueueReadResp(uint64_t task_id, uint32_t slice_seq,
                                    uint64_t addr, uint32_t length) {
    int rc = sendReadResp(task_id, slice_seq, addr,
                          reinterpret_cast<const void *>(addr), length);
    if (rc != ERR_TOO_MANY_REQUESTS) return rc;
    // Bounce pool is momentarily full (postSend already requested an expand).
    // Hold the response and replay it once a SEND completion or the expansion
    // frees a slot; the requester waits for this exact slice, so dropping it
    // would hang the transfer.
    std::lock_guard<std::mutex> lock(resp_mutex_);
    pending_resps_.push_back({task_id, addr, length, slice_seq});
    return 0;
}

void MsgChannel::drainPendingReadResps() {
    for (;;) {
        PendingReadResp item;
        {
            std::lock_guard<std::mutex> lock(resp_mutex_);
            if (pending_resps_.empty()) return;
            item = pending_resps_.front();
            pending_resps_.pop_front();
        }
        int rc = sendReadResp(item.task_id, item.slice_seq, item.addr,
                              reinterpret_cast<const void *>(item.addr),
                              item.length);
        if (rc == ERR_TOO_MANY_REQUESTS) {
            // Still full: put it back at the front (order is not required for
            // correctness since the requester places by slice_seq, but keeping
            // FIFO avoids starving the oldest response) and wait for the next
            // recycle.
            std::lock_guard<std::mutex> lock(resp_mutex_);
            pending_resps_.push_front(item);
            return;
        }
        if (rc != 0) {
            LOG(ERROR) << "MsgChannel: dropping queued READ_RESP after send "
                          "error rc="
                       << rc << " peer=" << peer_server_name_;
        }
    }
}

void MsgChannel::handleSendComplete(uint64_t wr_id) {
    int slot = -1;
    {
        std::lock_guard<std::mutex> inflight(inflight_mutex_);
        if (!inflight_slots_.empty()) {
            size_t idx = wr_id % inflight_slots_.size();
            slot = inflight_slots_[idx];
            inflight_slots_[idx] = -1;
        }
    }
    if (slot >= 0) pool_.releaseSendSlot(slot);
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        if (pending_sends_ > 0) pending_sends_--;
    }
    // A send slot just freed up: replay any READ_RESP held back by a full pool.
    drainPendingReadResps();
}

void MsgChannel::dispatchRecv(size_t idx, size_t byte_len) {
    if (byte_len < kMsgHeaderSize) return;
    char *buf = pool_.recvSlotPtr(idx);
    if (!buf) return;
    MsgHeader hdr;
    if (decodeMsgHeader(buf, byte_len, hdr)) {
        LOG(ERROR) << "MsgChannel: bad msg header from " << peer_server_name_;
        return;
    }
    // READ_REQ carries length in the header but no payload.
    const bool expects_payload =
        hdr.type == MsgType::DATA_WRITE || hdr.type == MsgType::READ_RESP;
    if (expects_payload && kMsgHeaderSize + hdr.length > byte_len) {
        LOG(ERROR) << "MsgChannel: truncated payload from "
                   << peer_server_name_;
        return;
    }
    const void *payload = nullptr;
    if (expects_payload && hdr.length > 0) {
        payload = static_cast<const void *>(buf + kMsgHeaderSize);
    }
    transport_.onMsgReceived(peer_server_name_, hdr, payload, this);
}

int MsgChannel::pollCompletions(int max_entries) {
    std::vector<ibv_wc> wc(static_cast<size_t>(max_entries));
    int n = 0;
    {
        // disconnect() destroys cq_ under this lock, so the null check and the
        // poll must be atomic with respect to it. The WC loop below stays out:
        // it re-takes the lock and dispatchRecv() re-enters the transport.
        std::lock_guard<std::mutex> lock(resource_mutex_);
        if (!cq_) return 0;
        n = ibv_poll_cq(cq_, max_entries, wc.data());
    }
    if (n < 0) return n;
    for (int i = 0; i < n; ++i) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "MsgChannel: WC error status=" << wc[i].status
                       << " opcode=" << wc[i].opcode
                       << " peer=" << peer_server_name_;
            connected_.store(false, std::memory_order_release);
            // The channel is dead, so every outstanding SEND is now void.
            // Error WCs may not carry a reliable opcode/wr_id, so instead of
            // routing them through handleSendComplete(), return every
            // in-flight slot to the pool and settle the count here. Entries
            // are cleared under the lock before release, so a later success
            // WC for the same wr_id finds -1 and cannot double-release.
            std::vector<int> slots;
            {
                std::lock_guard<std::mutex> inflight(inflight_mutex_);
                for (int &slot : inflight_slots_) {
                    if (slot >= 0) slots.push_back(slot);
                }
                std::fill(inflight_slots_.begin(), inflight_slots_.end(), -1);
            }
            for (int slot : slots) pool_.releaseSendSlot(slot);
            std::lock_guard<std::mutex> lock(send_mutex_);
            pending_sends_ = 0;
            continue;
        }
        if (wc[i].opcode == IBV_WC_RECV) {
            size_t idx = static_cast<size_t>(wc[i].wr_id);
            size_t len = wc[i].byte_len;
            // Keep recv_posted=true through dispatch so shrink cannot dereg.
            dispatchRecv(idx, len);
            std::lock_guard<std::mutex> lock(resource_mutex_);
            pool_.markRecvPosted(idx, false);
            if (idx < pool_.activeCount()) {
                postRecv(idx);
            }
        } else if (wc[i].opcode == IBV_WC_SEND) {
            handleSendComplete(wc[i].wr_id);
        }
    }
    return n;
}

void MsgChannel::disconnect() {
    std::lock_guard<std::mutex> lock(resource_mutex_);
    destroyResources();
}

}  // namespace mooncake
