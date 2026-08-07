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

#include "transport/rdma_transport/rdma_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <unordered_map>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_transport/ctrl_channel.h"
#include "transport/rdma_transport/msg_channel.h"

namespace mooncake {

void *RdmaTransport::allocateManagedBuffer(size_t length) {
    if (length == 0 || !globalConfig().rdma_msg_enabled) return nullptr;
    void *addr = nullptr;
    if (posix_memalign(&addr, 64, length)) return nullptr;
    std::memset(addr, 0, length);
    int rc = registerLocalMemoryInternal(addr, length, "cpu:0", true, true,
                                         false, true);
    if (rc) {
        free(addr);
        return nullptr;
    }
    {
        std::lock_guard<std::mutex> lock(managed_mutex_);
        managed_buffers_[reinterpret_cast<uint64_t>(addr)] = {addr, length,
                                                              true};
    }
    return addr;
}

int RdmaTransport::releaseManagedBuffer(void *addr) {
    if (!addr) return ERR_INVALID_ARGUMENT;
    ManagedBuffer mb;
    {
        std::lock_guard<std::mutex> lock(managed_mutex_);
        auto it = managed_buffers_.find(reinterpret_cast<uint64_t>(addr));
        if (it == managed_buffers_.end()) return ERR_ADDRESS_NOT_REGISTERED;
        mb = it->second;
        managed_buffers_.erase(it);
    }
    int rc = unregisterLocalMemory(addr, true);
    if (mb.owned) free(mb.addr);
    return rc;
}

bool RdmaTransport::isLocalManaged(uint64_t addr, size_t length) const {
    std::lock_guard<std::mutex> lock(managed_mutex_);
    for (const auto &entry : managed_buffers_) {
        uint64_t base = entry.first;
        uint64_t end = base + entry.second.length;
        if (addr >= base && addr + length <= end) return true;
    }
    return false;
}

bool RdmaTransport::isRemoteTwoSided(SegmentID target_id, uint64_t offset,
                                     size_t length) const {
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) return false;
    for (const auto &buf : desc->buffers) {
        if (!buf.two_sided) continue;
        if (offset >= buf.addr && offset + length <= buf.addr + buf.length)
            return true;
    }
    return false;
}

bool RdmaTransport::shouldUseTwoSided(const TransferRequest &req) {
    if (!globalConfig().rdma_msg_enabled || !globalConfig().rdma_msg_default)
        return false;
    if (!isLocalManaged(reinterpret_cast<uint64_t>(req.source), req.length))
        return false;
    return isRemoteTwoSided(req.target_id, req.target_offset, req.length);
}

bool RdmaTransport::validateLocalManagedDest(uint64_t dest_addr,
                                             uint32_t length) const {
    return isLocalManaged(dest_addr, length);
}

int RdmaTransport::onSetupMsgChannel(const HandShakeDesc &peer_desc,
                                     HandShakeDesc &local_desc) {
    if (!globalConfig().rdma_msg_enabled) {
        local_desc.reply_msg = "RDMA msg disabled";
        return ERR_INVALID_ARGUMENT;
    }
    if (context_list_.empty()) {
        local_desc.reply_msg = "No local RDMA context for MsgChannel";
        return ERR_DEVICE_NOT_FOUND;
    }
    std::string peer_server_name =
        getServerNameFromNicPath(peer_desc.local_nic_path);
    if (peer_server_name.empty()) {
        local_desc.reply_msg = "Cannot derive peer server name";
        return ERR_INVALID_ARGUMENT;
    }
    std::shared_ptr<MsgChannel> channel;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = msg_channels_.find(peer_server_name);
        if (it != msg_channels_.end() && it->second) {
            it->second->disconnect();
        }
        channel = std::make_shared<MsgChannel>(*this, *context_list_[0],
                                               peer_server_name);
        msg_channels_[peer_server_name] = channel;
    }
    return channel->acceptPassive(peer_desc, local_desc);
}

std::shared_ptr<MsgChannel> RdmaTransport::ensureMsgChannel(
    const std::string &peer_server_name) {
    if (!globalConfig().rdma_msg_enabled || context_list_.empty()) {
        return nullptr;
    }
    std::unique_lock<std::mutex> lock(ctrl_mutex_);
    auto it = msg_channels_.find(peer_server_name);
    if (it != msg_channels_.end() && it->second && it->second->connected()) {
        return it->second;
    }
    auto channel = std::make_shared<MsgChannel>(*this, *context_list_[0],
                                                peer_server_name);
    lock.unlock();
    if (channel->connectActive()) return nullptr;
    lock.lock();
    auto again = msg_channels_.find(peer_server_name);
    if (again != msg_channels_.end() && again->second &&
        again->second->connected()) {
        channel->disconnect();
        return again->second;
    }
    msg_channels_[peer_server_name] = channel;
    return channel;
}

Status RdmaTransport::submitTwoSidedTasks(
    const std::vector<TransferTask *> &tasks) {
    for (auto *task : tasks) {
        assert(task && task->request);
        // One logical slice covering the whole transfer for status tracking.
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = task->request->source;
        slice->length = task->request->length;
        slice->opcode = task->request->opcode;
        slice->rdma.dest_addr = task->request->target_offset;
        slice->task = task;
        slice->target_id = task->request->target_id;
        slice->status = Slice::PENDING;
        task->slice_list.push_back(slice);
        task->total_bytes = task->request->length;
        __sync_fetch_and_add(&task->slice_count, 1);

        int rc = dispatchTwoSidedTask(task);
        if (rc == ERR_TOO_MANY_REQUESTS) {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            waiting_tasks_.push_back(task);
            TwoSidedTaskState st;
            st.task = task;
            st.task_id = next_task_id_.fetch_add(1);
            st.total_bytes = task->request->length;
            st.waiting_credit = true;
            auto desc = metadata_->getSegmentDescByID(task->request->target_id);
            st.peer = desc ? desc->name : "";
            twosided_tasks_[st.task_id] = st;
            // Store task_id in slice ts field unused otherwise for lookup.
            slice->ts = st.task_id;
            continue;
        }
        if (rc) {
            slice->markFailed();
            return Status::InvalidArgument("two-sided submit failed");
        }
    }
    return Status::OK();
}

int RdmaTransport::dispatchTwoSidedTask(TransferTask *task) {
    auto &req = *task->request;
    auto desc = metadata_->getSegmentDescByID(req.target_id);
    if (!desc) return ERR_METADATA;
    const std::string &peer = desc->name;

    // Ensure ctrl+msg channels.
    if (!ensureCtrlChannel(peer)) return ERR_ENDPOINT;
    auto msg = ensureMsgChannel(peer);
    if (!msg) return ERR_ENDPOINT;

    uint64_t session = 0;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = peer_ctrl_state_.find(peer);
        if (it != peer_ctrl_state_.end()) session = it->second.peer_session;
    }
    if (session == 0) session = 1;  // may still be bootstrapping

    size_t max_payload =
        globalConfig().rdma_msg_slot_size > kMsgHeaderSize
            ? globalConfig().rdma_msg_slot_size - kMsgHeaderSize
            : 0;
    if (max_payload == 0) return ERR_INVALID_ARGUMENT;

    uint64_t task_id = 0;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        // Reuse existing state if redispatching a waiting task.
        for (auto &entry : twosided_tasks_) {
            if (entry.second.task == task) {
                task_id = entry.first;
                entry.second.waiting_credit = false;
                entry.second.peer = peer;
                entry.second.peer_session = session;
                break;
            }
        }
        if (task_id == 0) {
            task_id = next_task_id_.fetch_add(1);
            TwoSidedTaskState st;
            st.task = task;
            st.task_id = task_id;
            st.total_bytes = req.length;
            st.peer = peer;
            st.peer_session = session;
            twosided_tasks_[task_id] = st;
            if (!task->slice_list.empty()) task->slice_list[0]->ts = task_id;
        }
    }

    // Credit reserve for full transfer (bounce slots ≈ ceil(len/payload)).
    size_t slots_needed = (req.length + max_payload - 1) / max_payload;
    if (slots_needed == 0) slots_needed = 1;
    if (globalConfig().rdma_credit_enabled) {
        int rc = sender_credit_.tryReserve(
            peer, session,
            {{CreditResource::BounceSlots, slots_needed},
             {CreditResource::BounceBytes, req.length}});
        if (rc) return rc;
    }

    if (req.opcode == TransferRequest::WRITE) {
        uint8_t seq = 0;
        for (size_t off = 0; off < req.length; off += max_payload, ++seq) {
            uint32_t chunk = static_cast<uint32_t>(
                std::min(max_payload, req.length - off));
            const char *src = static_cast<const char *>(req.source) + off;
            int rc = msg->sendDataWrite(task_id, seq, req.target_offset + off,
                                        src, chunk);
            if (rc) return rc;
        }
    } else {
        // READ: send READ_REQs; peer responds with READ_RESP into local source.
        uint8_t seq = 0;
        for (size_t off = 0; off < req.length; off += max_payload, ++seq) {
            uint32_t chunk = static_cast<uint32_t>(
                std::min(max_payload, req.length - off));
            int rc = msg->sendReadReq(task_id, seq, req.target_offset + off,
                                      chunk);
            if (rc) return rc;
        }
    }
    return 0;
}

void RdmaTransport::redispatchWaitingTasks() {
    std::deque<TransferTask *> pending;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        pending.swap(waiting_tasks_);
    }
    for (auto *task : pending) {
        int rc = dispatchTwoSidedTask(task);
        if (rc == ERR_TOO_MANY_REQUESTS) {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            waiting_tasks_.push_back(task);
        } else if (rc) {
            if (!task->slice_list.empty()) task->slice_list[0]->markFailed();
        }
    }
}

void RdmaTransport::completeTwoSidedAck(uint64_t task_id, uint64_t acked_bytes) {
    TransferTask *task = nullptr;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it == twosided_tasks_.end()) return;
        if (acked_bytes > it->second.acked_bytes)
            it->second.acked_bytes = acked_bytes;
        if (it->second.acked_bytes < it->second.total_bytes) return;
        task = it->second.task;
        twosided_tasks_.erase(it);
    }
    if (task && !task->slice_list.empty()) {
        task->slice_list[0]->markSuccess();
    }
}

int RdmaTransport::sendDataAck(const std::string &peer, uint64_t task_id,
                               uint64_t acked_bytes) {
    std::shared_ptr<CtrlChannel> channel;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = ctrl_channels_.find(peer);
        if (it == ctrl_channels_.end()) return ERR_ENDPOINT;
        channel = it->second;
    }
    if (!channel || !channel->connected()) return ERR_ENDPOINT;
    CtrlFrame frame;
    frame.type = CtrlFrameType::DATA_ACK;
    frame.session = local_ctrl_session_id_;
    frame.epoch = 1;
    std::vector<DataAckEntry> acks = {{task_id, acked_bytes}};
    if (encodeDataAckPayload(acks, frame.payload)) return ERR_INVALID_ARGUMENT;
    return channel->sendCtrlFrame(frame);
}

void RdmaTransport::onMsgReceived(const std::string &peer_server_name,
                                  const MsgHeader &hdr, const void *payload) {
    if (hdr.type == MsgType::DATA_WRITE) {
        if (!validateLocalManagedDest(hdr.dest_addr, hdr.length)) {
            LOG(ERROR) << "MsgChannel: illegal DATA_WRITE dest from "
                       << peer_server_name;
            return;
        }
        if (payload && hdr.length) {
            std::memcpy(reinterpret_cast<void *>(hdr.dest_addr), payload,
                        hdr.length);
        }
        // Accumulate per-task received bytes and ACK cumulatively.
        uint64_t cumulative = 0;
        {
            static std::mutex recv_ack_mu;
            static std::unordered_map<uint64_t, uint64_t> recv_acked;
            std::lock_guard<std::mutex> lock(recv_ack_mu);
            recv_acked[hdr.task_id] += hdr.length;
            cumulative = recv_acked[hdr.task_id];
        }
        (void)sendDataAck(peer_server_name, hdr.task_id, cumulative);
        return;
    }
    if (hdr.type == MsgType::READ_REQ) {
        // hdr.dest_addr is remote(=our) source address; respond to peer's
        // request. Peer expects READ_RESP into their local buffer — we don't
        // know their dest here, so encode dest_addr=0 and length; the
        // requester places data at its TransferRequest::source + slice.
        // For MVP: READ_RESP.dest_addr carries the original src offset; the
        // requester maps slice_seq → local dest.
        if (!validateLocalManagedDest(hdr.dest_addr, hdr.length)) {
            LOG(ERROR) << "MsgChannel: illegal READ_REQ src from "
                       << peer_server_name;
            return;
        }
        auto msg = ensureMsgChannel(peer_server_name);
        if (!msg) return;
        (void)msg->sendReadResp(hdr.task_id, hdr.slice_seq, hdr.dest_addr,
                                reinterpret_cast<const void *>(hdr.dest_addr),
                                hdr.length);
        return;
    }
    if (hdr.type == MsgType::READ_RESP) {
        // Place into local task source based on task_id + slice_seq.
        TransferTask *task = nullptr;
        uint64_t local_base = 0;
        {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            auto it = twosided_tasks_.find(hdr.task_id);
            if (it == twosided_tasks_.end() || !it->second.task ||
                !it->second.task->request)
                return;
            task = it->second.task;
            local_base = reinterpret_cast<uint64_t>(task->request->source);
        }
        size_t max_payload =
            globalConfig().rdma_msg_slot_size > kMsgHeaderSize
                ? globalConfig().rdma_msg_slot_size - kMsgHeaderSize
                : 0;
        uint64_t dest = local_base +
                        static_cast<uint64_t>(hdr.slice_seq) * max_payload;
        if (payload && hdr.length) {
            std::memcpy(reinterpret_cast<void *>(dest), payload, hdr.length);
        }
        uint64_t acked = 0;
        {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            auto it = twosided_tasks_.find(hdr.task_id);
            if (it == twosided_tasks_.end()) return;
            it->second.acked_bytes += hdr.length;
            acked = it->second.acked_bytes;
            if (acked >= it->second.total_bytes) {
                task = it->second.task;
                twosided_tasks_.erase(it);
                if (task && !task->slice_list.empty())
                    task->slice_list[0]->markSuccess();
            }
        }
        (void)acked;
        return;
    }
}

}  // namespace mooncake
