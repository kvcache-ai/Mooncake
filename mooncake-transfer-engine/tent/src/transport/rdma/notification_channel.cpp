// Copyright 2025 KVCache.AI
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

#include "tent/transport/rdma/notification_channel.h"

#include <glog/logging.h>

#include "tent/transport/rdma/cq.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/rdma_transport.h"

namespace mooncake {
namespace tent {

class NotificationChannel::Impl {
   public:
    Impl(RdmaTransport& transport, RdmaContext& context)
        : transport_(transport),
          context_(context),
          qp_(nullptr),
          connected_(false) {
        // Use the first CQ for notifications
        cq_ = context.cq(0);
        if (!cq_) {
            LOG(ERROR) << "Failed to get CQ for notification channel";
        }
    }

    ~Impl() {
        if (cq_) {
            // Note: Don't destroy CQ and comp_channel here as they're managed
            // by RdmaContext
        }
        if (qp_) {
            ibv_destroy_qp(qp_);
        }
    }

    bool createQueuePair() {
        if (!cq_) {
            LOG(ERROR) << "CQ not initialized";
            return false;
        }

        // Create QP for notification
        ibv_qp_init_attr qp_init_attr = {};
        qp_init_attr.send_cq = cq_->cq();
        qp_init_attr.recv_cq = cq_->cq();
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 64;
        qp_init_attr.cap.max_recv_wr = 64;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;

        qp_ = ibv_create_qp(context_.nativePD(), &qp_init_attr);
        if (!qp_) {
            LOG(ERROR) << "Failed to create notification QP";
            return false;
        }

        // Modify QP to INIT
        ibv_qp_attr qp_attr = {};
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = context_.portNum();
        qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

        if (ibv_modify_qp(qp_, &qp_attr,
                          IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                              IBV_QP_ACCESS_FLAGS)) {
            LOG(ERROR) << "Failed to modify QP to INIT";
            return false;
        }

        // Post initial recv buffers
        for (int i = 0; i < 64; ++i) {
            recv_buffers_.push_back(std::vector<char>(4096));
            postRecv(i);
        }

        return true;
    }

    bool connect(uint32_t peer_qp_num, uint16_t peer_lid,
                 const std::vector<uint8_t>& peer_gid) {
        if (!qp_) {
            if (!createQueuePair()) {
                return false;
            }
        }

        // Modify QP to RTR
        ibv_qp_attr qp_attr = {};
        qp_attr.qp_state = IBV_QPS_RTR;
        qp_attr.path_mtu = IBV_MTU_4096;
        qp_attr.dest_qp_num = peer_qp_num;
        qp_attr.rq_psn = 0;
        qp_attr.max_dest_rd_atomic = 1;
        qp_attr.min_rnr_timer = 0x12;
        qp_attr.ah_attr.is_global = 0;
        qp_attr.ah_attr.dlid = peer_lid;
        qp_attr.ah_attr.sl = 0;
        qp_attr.ah_attr.src_path_bits = 0;
        qp_attr.ah_attr.port_num = context_.portNum();

        if (ibv_modify_qp(qp_, &qp_attr,
                          IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                              IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                              IBV_QP_MIN_RNR_TIMER | IBV_QP_AV)) {
            LOG(ERROR) << "Failed to modify QP to RTR";
            return false;
        }

        // Modify QP to RTS
        qp_attr.qp_state = IBV_QPS_RTS;
        qp_attr.sq_psn = 0;
        qp_attr.timeout = 0x12;
        qp_attr.retry_cnt = 7;
        qp_attr.rnr_retry = 7;
        qp_attr.max_rd_atomic = 1;

        if (ibv_modify_qp(qp_, &qp_attr,
                          IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                              IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                              IBV_QP_MAX_QP_RD_ATOMIC)) {
            LOG(ERROR) << "Failed to modify QP to RTS";
            return false;
        }

        connected_ = true;
        LOG(INFO) << "Notification channel connected to QP " << peer_qp_num;
        return true;
    }

    bool send(const std::string& name, const std::string& msg) {
        if (!connected_) {
            LOG(ERROR) << "Notification channel not connected";
            return false;
        }

        // Allocate send buffer
        size_t total_size = 4 + name.size() + 4 + msg.size();
        std::vector<char> send_buf(total_size);

        // Serialize: [name_len(4)][name][msg_len(4)][msg]
        uint32_t name_len = name.size();
        uint32_t msg_len = msg.size();
        std::memcpy(send_buf.data(), &name_len, 4);
        std::memcpy(send_buf.data() + 4, name.data(), name.size());
        std::memcpy(send_buf.data() + 4 + name.size(), &msg_len, 4);
        std::memcpy(send_buf.data() + 4 + name.size() + 4, msg.data(),
                    msg.size());

        // Post send
        ibv_sge sge = {};
        sge.addr = reinterpret_cast<uint64_t>(send_buf.data());
        sge.length = total_size;
        sge.lkey = 0;  // TODO: Get proper MR key

        ibv_send_wr wr = {};
        wr.wr_id = 0;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED;

        ibv_send_wr* bad_wr = nullptr;
        if (ibv_post_send(qp_, &wr, &bad_wr)) {
            LOG(ERROR) << "Failed to post send";
            return false;
        }

        // Store send buffer to prevent early deallocation
        pending_sends_.push_back(std::move(send_buf));
        return true;
    }

    bool receive(std::string& name, std::string& msg) {
        std::lock_guard<std::mutex> lock(recv_mutex_);
        if (received_messages_.empty()) {
            return false;
        }

        auto& front = received_messages_.front();
        name = front.first;
        msg = front.second;
        received_messages_.pop();
        return true;
    }

    int processCompletions() {
        if (!cq_) {
            return 0;
        }

        ibv_wc wc[16];
        int completed = ibv_poll_cq(cq_->cq(), 16, wc);

        if (completed < 0) {
            LOG(ERROR) << "Failed to poll CQ";
            return -1;
        }

        if (completed == 0) {
            return 0;
        }

        for (int i = 0; i < completed; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                LOG(ERROR) << "Work completion failed: " << wc[i].status;
                continue;
            }

            if (wc[i].opcode == IBV_WC_RECV) {
                handleRecv(&wc[i]);
            } else if (wc[i].opcode == IBV_WC_SEND) {
                handleSend(&wc[i]);
            }
        }

        return completed;
    }

   private:
    RdmaTransport& transport_;
    RdmaContext& context_;
    RdmaCQ* cq_ = nullptr;
    ibv_qp* qp_ = nullptr;
    bool connected_;

    std::vector<std::vector<char>> recv_buffers_;
    std::vector<std::vector<char>> pending_sends_;
    std::mutex recv_mutex_;
    std::queue<std::pair<std::string, std::string>> received_messages_;

    void postRecv(int idx) {
        ibv_sge sge = {};
        sge.addr = reinterpret_cast<uint64_t>(recv_buffers_[idx].data());
        sge.length = recv_buffers_[idx].size();
        sge.lkey = 0;  // TODO: Get proper MR key

        ibv_recv_wr wr = {};
        wr.wr_id = idx;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        ibv_recv_wr* bad_wr = nullptr;
        if (ibv_post_recv(qp_, &wr, &bad_wr)) {
            LOG(ERROR) << "Failed to post recv";
        }
    }

    void handleRecv(ibv_wc* wc) {
        int idx = wc->wr_id;
        char* data = recv_buffers_[idx].data();
        size_t len = wc->byte_len;

        // Deserialize: [name_len(4)][name][msg_len(4)][msg]
        if (len < 8) {
            LOG(ERROR) << "Invalid recv message size: " << len;
            postRecv(idx);
            return;
        }

        uint32_t name_len = *reinterpret_cast<uint32_t*>(data);
        if (len < 4 + name_len + 4) {
            LOG(ERROR) << "Invalid recv message format";
            postRecv(idx);
            return;
        }

        std::string name(data + 4, name_len);
        uint32_t msg_len = *reinterpret_cast<uint32_t*>(data + 4 + name_len);
        if (len < 4 + name_len + 4 + msg_len) {
            LOG(ERROR) << "Invalid recv message format (msg too short)";
            postRecv(idx);
            return;
        }

        std::string msg(data + 4 + name_len + 4, msg_len);

        std::lock_guard<std::mutex> lock(recv_mutex_);
        received_messages_.push({name, msg});

        // Repost recv buffer
        postRecv(idx);
    }

    void handleSend(ibv_wc* wc) {
        // Free completed send buffer
        if (wc->wr_id < pending_sends_.size()) {
            pending_sends_.erase(pending_sends_.begin() + wc->wr_id);
        }
    }
};

NotificationChannel::NotificationChannel(RdmaTransport& transport,
                                         RdmaContext& context)
    : impl_(std::make_unique<Impl>(transport, context)) {}

NotificationChannel::~NotificationChannel() = default;

bool NotificationChannel::connect(uint32_t peer_qp_num, uint16_t peer_lid,
                                  const std::vector<uint8_t>& peer_gid) {
    return impl_->connect(peer_qp_num, peer_lid, peer_gid);
}

bool NotificationChannel::send(const std::string& name,
                               const std::string& msg) {
    return impl_->send(name, msg);
}

bool NotificationChannel::receive(std::string& name, std::string& msg) {
    return impl_->receive(name, msg);
}

int NotificationChannel::processCompletions() {
    return impl_->processCompletions();
}

}  // namespace tent
}  // namespace mooncake
