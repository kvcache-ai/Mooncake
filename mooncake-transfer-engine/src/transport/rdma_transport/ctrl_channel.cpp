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

#include "transport/rdma_transport/ctrl_channel.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_transport/ctrl_frame.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {

namespace {

constexpr int kCtrlHopLimit = 16;
constexpr int kCtrlTimeout = 14;
constexpr int kCtrlRetryCount = 7;

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

CtrlChannel::CtrlChannel(RdmaTransport &transport, RdmaContext &context,
                         std::string peer_server_name)
    : transport_(transport),
      context_(context),
      peer_server_name_(std::move(peer_server_name)) {
    auto &cfg = globalConfig();
    recv_count_ = cfg.rdma_notify_recv_count;
    buffer_size_ = cfg.rdma_notify_buffer_size;
    max_pending_sends_ = cfg.rdma_notify_max_pending_sends;
}

CtrlChannel::~CtrlChannel() { destroyResources(); }

int CtrlChannel::construct() {
    std::lock_guard<std::mutex> lock(resource_mutex_);
    if (qp_) return 0;
    return createResources();
}

int CtrlChannel::createResources() {
    auto &cfg = globalConfig();
    recv_count_ = cfg.rdma_notify_recv_count;
    buffer_size_ = cfg.rdma_notify_buffer_size;
    max_pending_sends_ = cfg.rdma_notify_max_pending_sends;
    if (recv_count_ == 0 || buffer_size_ == 0 || max_pending_sends_ == 0) {
        return ERR_INVALID_ARGUMENT;
    }

    cq_ = ibv_create_cq(context_.context(), static_cast<int>(cfg.max_cqe),
                        nullptr, nullptr, 0);
    if (!cq_) {
        PLOG(ERROR) << "CtrlChannel: failed to create CQ for peer "
                    << peer_server_name_;
        return ERR_ENDPOINT;
    }

    ibv_qp_init_attr attr{};
    attr.send_cq = cq_;
    attr.recv_cq = cq_;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;
    attr.cap.max_send_wr = static_cast<uint32_t>(max_pending_sends_);
    attr.cap.max_recv_wr = static_cast<uint32_t>(recv_count_);
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = static_cast<uint32_t>(cfg.max_inline);

    qp_ = ibv_create_qp(context_.pd(), &attr);
    if (!qp_) {
        PLOG(ERROR) << "CtrlChannel: failed to create QP for peer "
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
        PLOG(ERROR) << "CtrlChannel: failed to modify QP to INIT";
        destroyResources();
        return ERR_ENDPOINT;
    }

    send_buffer_.assign(buffer_size_ * max_pending_sends_, 0);
    send_mr_ =
        ibv_reg_mr(context_.pd(), send_buffer_.data(), send_buffer_.size(),
                   IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr_) {
        PLOG(ERROR) << "CtrlChannel: failed to register send buffer";
        destroyResources();
        return ERR_ENDPOINT;
    }

    recv_buffers_.resize(recv_count_);
    recv_mrs_.assign(recv_count_, nullptr);
    for (size_t i = 0; i < recv_count_; ++i) {
        recv_buffers_[i].assign(buffer_size_, 0);
        recv_mrs_[i] =
            ibv_reg_mr(context_.pd(), recv_buffers_[i].data(), buffer_size_,
                       IBV_ACCESS_LOCAL_WRITE);
        if (!recv_mrs_[i]) {
            PLOG(ERROR) << "CtrlChannel: failed to register recv buffer " << i;
            destroyResources();
            return ERR_ENDPOINT;
        }
    }

    if (repostAllRecvs()) {
        destroyResources();
        return ERR_ENDPOINT;
    }
    return 0;
}

void CtrlChannel::destroyResources() {
    connected_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        pending_sends_ = 0;
        send_cv_.notify_all();
    }
    if (qp_) {
        ibv_destroy_qp(qp_);
        qp_ = nullptr;
    }
    if (cq_) {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
    if (send_mr_) {
        ibv_dereg_mr(send_mr_);
        send_mr_ = nullptr;
    }
    for (auto *mr : recv_mrs_) {
        if (mr) ibv_dereg_mr(mr);
    }
    recv_mrs_.clear();
    recv_buffers_.clear();
    send_buffer_.clear();
}

int CtrlChannel::postRecv(size_t idx) {
    if (!qp_ || idx >= recv_buffers_.size() || !recv_mrs_[idx]) {
        return ERR_ENDPOINT;
    }
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(recv_buffers_[idx].data());
    sge.length = static_cast<uint32_t>(buffer_size_);
    sge.lkey = recv_mrs_[idx]->lkey;

    ibv_recv_wr wr{};
    wr.wr_id = idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    ibv_recv_wr *bad = nullptr;
    int ret = ibv_post_recv(qp_, &wr, &bad);
    if (ret) {
        // ibv_post_recv returns errno value on failure (errno itself may be 0).
        LOG(ERROR) << "CtrlChannel: ibv_post_recv failed idx=" << idx << ": "
                   << strerror(std::abs(ret)) << " [" << ret << "]";
        return ERR_ENDPOINT;
    }
    return 0;
}

int CtrlChannel::repostAllRecvs() {
    for (size_t i = 0; i < recv_count_; ++i) {
        if (postRecv(i)) return ERR_ENDPOINT;
    }
    return 0;
}

void CtrlChannel::fillLocalDesc(HandShakeDesc &local_desc) const {
    local_desc = HandShakeDesc();
    local_desc.local_nic_path = context_.nicPath();
    local_desc.local_lid = context_.lid();
    local_desc.local_gid = context_.gid();
    local_desc.peer_nic_path = "__ctrl__";
    local_desc.ctrl_channel = true;
    local_desc.notify_qp_num = notifyQpNum();
    local_desc.notify_rq_depth = notifyRqDepth();
}

int CtrlChannel::connectQp(const std::string &peer_gid, uint16_t peer_lid,
                           uint32_t peer_qp_num) {
    if (!qp_ || peer_qp_num == 0) return ERR_INVALID_ARGUMENT;

    ibv_gid peer_gid_raw{};
    if (parseGidString(peer_gid, peer_gid_raw)) {
        LOG(ERROR) << "CtrlChannel: invalid peer GID " << peer_gid;
        return ERR_INVALID_ARGUMENT;
    }

    // First connect leaves the QP in INIT with recv WRs already posted by
    // createResources(). Only RESET/repost on reconnect; some providers
    // (e.g. eRDMA) do not clear the RQ on RESET, so re-posting after an
    // unnecessary RESET fails with ENOMEM.
    ibv_qp_attr query_attr{};
    ibv_qp_init_attr query_init_attr{};
    if (ibv_query_qp(qp_, &query_attr, IBV_QP_STATE, &query_init_attr)) {
        PLOG(ERROR) << "CtrlChannel: ibv_query_qp failed";
        return ERR_ENDPOINT;
    }

    ibv_qp_attr attr{};
    if (query_attr.qp_state != IBV_QPS_INIT) {
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE)) {
            PLOG(ERROR) << "CtrlChannel: QP RESET failed";
            return ERR_ENDPOINT;
        }

        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = context_.portNum();
        attr.pkey_index = globalConfig().pkey_index;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
        if (ibv_modify_qp(qp_, &attr,
                          IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                              IBV_QP_ACCESS_FLAGS)) {
            PLOG(ERROR) << "CtrlChannel: QP INIT failed";
            return ERR_ENDPOINT;
        }

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
    attr.ah_attr.grh.hop_limit = kCtrlHopLimit;
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
        PLOG(ERROR) << "CtrlChannel: QP RTR failed peer_qp=" << peer_qp_num;
        return ERR_ENDPOINT;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = kCtrlTimeout;
    attr.retry_cnt = kCtrlRetryCount;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(qp_, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC)) {
        PLOG(ERROR) << "CtrlChannel: QP RTS failed";
        return ERR_ENDPOINT;
    }

    connected_.store(true, std::memory_order_release);
    return 0;
}

int CtrlChannel::connectActive() {
    if (construct()) return ERR_ENDPOINT;

    HandShakeDesc local_desc, peer_desc;
    fillLocalDesc(local_desc);

    int ret = transport_.sendHandshake(peer_server_name_, local_desc, peer_desc);
    if (ret) {
        LOG(ERROR) << "CtrlChannel: active handshake failed to "
                   << peer_server_name_ << " ret=" << ret;
        return ret;
    }
    if (peer_desc.notify_qp_num == 0) {
        LOG(ERROR) << "CtrlChannel: peer " << peer_server_name_
                   << " has no notify_qp_num";
        return ERR_ENDPOINT;
    }

    max_pending_sends_ =
        std::min(max_pending_sends_,
                 peer_desc.notify_rq_depth
                     ? static_cast<size_t>(peer_desc.notify_rq_depth)
                     : max_pending_sends_);

    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        ret = connectQp(peer_desc.local_gid, peer_desc.local_lid,
                        peer_desc.notify_qp_num);
    }
    if (ret == 0) {
        LOG(INFO) << "CtrlChannel: connected to " << peer_server_name_
                  << " local_qp=" << notifyQpNum()
                  << " peer_qp=" << peer_desc.notify_qp_num;
        // SESSION_OPEN only here; CREDIT_GRANT is sent after we receive the
        // peer's SESSION_OPEN (both QPs are RTS by then).
        (void)postSessionOpen();
    }
    return ret;
}

int CtrlChannel::acceptPassive(const HandShakeDesc &peer_desc,
                               HandShakeDesc &local_desc) {
    if (construct()) {
        local_desc.reply_msg = "CtrlChannel construct failed";
        return ERR_ENDPOINT;
    }
    if (peer_desc.notify_qp_num == 0) {
        local_desc.reply_msg = "Peer notify_qp_num missing";
        return ERR_INVALID_ARGUMENT;
    }

    max_pending_sends_ =
        std::min(max_pending_sends_,
                 peer_desc.notify_rq_depth
                     ? static_cast<size_t>(peer_desc.notify_rq_depth)
                     : max_pending_sends_);

    int ret = 0;
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        fillLocalDesc(local_desc);
        ret = connectQp(peer_desc.local_gid, peer_desc.local_lid,
                        peer_desc.notify_qp_num);
    }
    if (ret) {
        local_desc.reply_msg = "CtrlChannel connectQp failed";
        return ret;
    }
    LOG(INFO) << "CtrlChannel: accepted from " << peer_server_name_
              << " local_qp=" << notifyQpNum()
              << " peer_qp=" << peer_desc.notify_qp_num;
    (void)postSessionOpen();
    return 0;
}

int CtrlChannel::postSessionOpen() {
    if (!globalConfig().rdma_credit_enabled && !globalConfig().rdma_msg_enabled)
        return 0;
    CtrlFrame frame;
    frame.type = CtrlFrameType::SESSION_OPEN;
    frame.session = transport_.localCtrlSessionId();
    frame.epoch = 1;
    frame.seq = 0;  // filled by sendCtrlFrame
    uint32_t slots = static_cast<uint32_t>(globalConfig().rdma_msg_pool_base);
    uint32_t slot_size = static_cast<uint32_t>(globalConfig().rdma_msg_slot_size);
    if (encodeSessionOpenPayload(slots, slot_size, frame.payload))
        return ERR_INVALID_ARGUMENT;
    return sendCtrlFrame(frame);
}

int CtrlChannel::sendCtrlFrame(const CtrlFrame &frame_in) {
    if (!connected_.load(std::memory_order_acquire)) return ERR_ENDPOINT;

    CtrlFrame frame = frame_in;
    if (frame.version == 0) frame.version = kCtrlFrameVersion;
    if (frame.session == 0) frame.session = transport_.localCtrlSessionId();

    std::vector<uint8_t> wired;
    {
        std::unique_lock<std::mutex> lock(send_mutex_);
        if (frame.seq == 0) frame.seq = next_frame_seq_++;
        if (encodeCtrlFrame(frame, wired)) return ERR_INVALID_ARGUMENT;
        if (wired.size() > buffer_size_) {
            LOG(ERROR) << "CtrlChannel: frame too large (" << wired.size()
                       << " > " << buffer_size_ << ") type="
                       << static_cast<int>(frame.type);
            return ERR_INVALID_ARGUMENT;
        }

        // Poll SEND completions while waiting so a full SQ cannot deadlock
        // when the caller is (or contends with) the ctrl worker.
        while (connected_.load(std::memory_order_acquire) &&
               pending_sends_ >= max_pending_sends_) {
            lock.unlock();
            pollSendCompletions(16);
            lock.lock();
            if (pending_sends_ < max_pending_sends_) break;
            send_cv_.wait_for(lock, std::chrono::microseconds(50));
        }
        if (!connected_.load(std::memory_order_acquire)) return ERR_ENDPOINT;

        std::lock_guard<std::mutex> resource_guard(resource_mutex_);
        if (!qp_ || !send_mr_) return ERR_ENDPOINT;

        size_t slot = send_wr_id_ % max_pending_sends_;
        char *slot_ptr = send_buffer_.data() + slot * buffer_size_;
        std::memcpy(slot_ptr, wired.data(), wired.size());

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uint64_t>(slot_ptr);
        sge.length = static_cast<uint32_t>(wired.size());
        sge.lkey = send_mr_->lkey;

        ibv_send_wr wr{};
        wr.wr_id = send_wr_id_++;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED;
        if (wired.size() <= globalConfig().max_inline) {
            wr.send_flags |= IBV_SEND_INLINE;
        }

        ibv_send_wr *bad = nullptr;
        int ret = ibv_post_send(qp_, &wr, &bad);
        if (ret) {
            LOG(ERROR) << "CtrlChannel: ibv_post_send failed to "
                       << peer_server_name_ << ": " << strerror(std::abs(ret))
                       << " [" << ret << "]";
            return ERR_ENDPOINT;
        }
        pending_sends_++;
    }
    return 0;
}

int CtrlChannel::sendNotify(const NotifyDesc &notify) {
    CtrlFrame frame;
    frame.type = CtrlFrameType::NOTIFY_COMPAT;
    if (encodeNotifyCompatPayload(notify, frame.payload))
        return ERR_INVALID_ARGUMENT;
    return sendCtrlFrame(frame);
}

void CtrlChannel::dispatchRecvPayload(const uint8_t *data, size_t byte_len) {
    // Typed CtrlFrame path (Phase 2+).
    if (isCtrlFrameMagic(data, byte_len)) {
        CtrlFrame frame;
        if (decodeCtrlFrame(data, byte_len, frame)) {
            LOG(ERROR) << "CtrlChannel: malformed typed frame from "
                       << peer_server_name_ << " len=" << byte_len;
            return;
        }
        transport_.onCtrlFrameReceived(peer_server_name_, frame);
        return;
    }

    // Legacy Phase-1 raw notify wire: [name_len][name][msg_len][msg].
    if (byte_len < 8) {
        LOG(ERROR) << "CtrlChannel: short notify message len=" << byte_len;
        return;
    }
    NotifyDesc notify;
    if (decodeNotifyCompatPayload(data, byte_len, notify)) {
        LOG(ERROR) << "CtrlChannel: invalid legacy notify from "
                   << peer_server_name_;
        return;
    }
    transport_.onCtrlNotifyReceived(notify);
}

void CtrlChannel::handleSendComplete() {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (pending_sends_ > 0) pending_sends_--;
    send_cv_.notify_one();
}

void CtrlChannel::handleRecvComplete(const ibv_wc &wc) {
    // Copy payload then release resources before dispatching callbacks.
    // Callbacks may sendCtrlFrame (e.g. CREDIT_GRANT) which needs
    // resource_mutex_ / send_mutex_ — must not hold them here.
    const size_t idx = static_cast<size_t>(wc.wr_id);
    const size_t byte_len = wc.byte_len;
    std::vector<uint8_t> payload;
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        if (idx < recv_buffers_.size() && byte_len > 0) {
            auto *data =
                reinterpret_cast<const uint8_t *>(recv_buffers_[idx].data());
            payload.assign(data, data + byte_len);
        }
    }
    if (!payload.empty()) {
        dispatchRecvPayload(payload.data(), payload.size());
    }
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        postRecv(idx);
    }
}

int CtrlChannel::drainCompletions(int max_entries, bool log_poll_error) {
    if (!cq_) return 0;
    std::vector<ibv_wc> wc(static_cast<size_t>(max_entries));
    int n = ibv_poll_cq(cq_, max_entries, wc.data());
    if (n < 0) {
        if (log_poll_error) {
            LOG(ERROR) << "CtrlChannel: ibv_poll_cq failed";
        }
        return n;
    }
    int handled = 0;
    for (int i = 0; i < n; ++i) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "CtrlChannel: WC error status=" << wc[i].status
                       << " opcode=" << wc[i].opcode
                       << " peer=" << peer_server_name_;
            connected_.store(false, std::memory_order_release);
            std::lock_guard<std::mutex> lock(send_mutex_);
            send_cv_.notify_all();
            continue;
        }
        if (wc[i].opcode == IBV_WC_SEND) {
            handleSendComplete();
            handled++;
        } else if (wc[i].opcode == IBV_WC_RECV) {
            // Also required on send-path poll: opportunistic SEND polling must
            // not drop RECV WCs stolen from the ctrl worker.
            handleRecvComplete(wc[i]);
            handled++;
        }
    }
    return handled;
}

int CtrlChannel::pollSendCompletions(int max_entries) {
    return drainCompletions(max_entries, /*log_poll_error=*/false);
}

int CtrlChannel::pollCompletions(int max_entries) {
    return drainCompletions(max_entries, /*log_poll_error=*/true);
}

void CtrlChannel::disconnect() {
    std::lock_guard<std::mutex> lock(resource_mutex_);
    destroyResources();
}

}  // namespace mooncake
