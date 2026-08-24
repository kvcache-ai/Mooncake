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

#include "transport/rdma_twosided/rdma_twosided_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_twosided/ctrl_channel.h"
#include "transport/rdma_twosided/msg_channel.h"
#include "transport/rdma_transport/rdma_context.h"

namespace mooncake {

void *RdmaTwoSidedTransport::allocateManagedBuffer(size_t length) {
    if (length == 0 || !globalConfig().rdma_msg_enabled) return nullptr;
    void *addr = nullptr;
    if (posix_memalign(&addr, 64, length)) return nullptr;
    std::memset(addr, 0, length);
    if (registerManagedBufferInternal(addr, length, /*owned=*/true)) {
        free(addr);
        return nullptr;
    }
    return addr;
}

int RdmaTwoSidedTransport::registerManagedBuffer(void *addr, size_t length) {
    return registerManagedBufferInternal(addr, length, /*owned=*/false);
}

int RdmaTwoSidedTransport::registerManagedBufferInternal(void *addr,
                                                         size_t length,
                                                         bool owned) {
    if (!addr || length == 0) return ERR_INVALID_ARGUMENT;
    if (!globalConfig().rdma_msg_enabled) return ERR_INVALID_ARGUMENT;
    const uint64_t base = reinterpret_cast<uint64_t>(addr);
    const uint64_t end = base + length;
    std::lock_guard<std::mutex> lock(managed_mutex_);
    for (const auto &entry : managed_buffers_) {
        uint64_t ebase = entry.first;
        uint64_t eend = ebase + entry.second.length;
        if (!(end <= ebase || base >= eend)) return ERR_ADDRESS_OVERLAPPED;
    }
    // Two-sided buffers stay host memory only: no ibv_reg_mr / BufferDesc.
    managed_buffers_[base] = {addr, length, owned};
    return 0;
}

int RdmaTwoSidedTransport::releaseManagedBuffer(void *addr) {
    if (!addr) return ERR_INVALID_ARGUMENT;
    ManagedBuffer mb;
    {
        std::lock_guard<std::mutex> lock(managed_mutex_);
        auto it = managed_buffers_.find(reinterpret_cast<uint64_t>(addr));
        if (it == managed_buffers_.end()) return ERR_ADDRESS_NOT_REGISTERED;
        mb = it->second;
        managed_buffers_.erase(it);
    }
    if (mb.owned) free(mb.addr);
    return 0;
}

bool RdmaTwoSidedTransport::isLocalManaged(uint64_t addr, size_t length) const {
    std::lock_guard<std::mutex> lock(managed_mutex_);
    for (const auto &entry : managed_buffers_) {
        uint64_t base = entry.first;
        uint64_t end = base + entry.second.length;
        if (addr >= base && addr + length <= end) return true;
    }
    return false;
}

bool RdmaTwoSidedTransport::isRemoteTwoSided(SegmentID target_id,
                                             uint64_t offset,
                                             size_t length) const {
    (void)offset;
    (void)length;
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) return false;
    // Peer capability bit (not per-buffer registration). Dest validity is
    // checked on the receiver via validateLocalManagedDest.
    return desc->supports_two_sided_msg;
}

bool RdmaTwoSidedTransport::shouldUseTwoSided(const TransferRequest &req) {
    if (!globalConfig().rdma_msg_enabled || !globalConfig().rdma_msg_default)
        return false;
    if (!isLocalManaged(reinterpret_cast<uint64_t>(req.source), req.length))
        return false;
    return isRemoteTwoSided(req.target_id, req.target_offset, req.length);
}

bool RdmaTwoSidedTransport::validateLocalManagedDest(uint64_t dest_addr,
                                                     uint32_t length) const {
    return isLocalManaged(dest_addr, length);
}

int RdmaTwoSidedTransport::onSetupMsgChannel(const HandShakeDesc &peer_desc,
                                             HandShakeDesc &local_desc) {
    if (!globalConfig().rdma_msg_enabled) {
        local_desc.reply_msg = "RDMA msg disabled";
        return ERR_INVALID_ARGUMENT;
    }
    if (getContextList().empty()) {
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
        auto &rails = msg_channels_[peer_server_name];
        auto context = selectMsgContext(peer_desc, rails.size());
        if (!context) {
            local_desc.reply_msg = "No local RDMA context for MsgChannel rail";
            return ERR_DEVICE_NOT_FOUND;
        }
        // Replace an existing rail on the same local NIC, if any.
        const std::string local_path = context->nicPath();
        for (auto &existing : rails) {
            if (existing && existing->nicPath() == local_path) {
                existing->disconnect();
                existing = std::make_shared<MsgChannel>(*this, *context,
                                                        peer_server_name);
                channel = existing;
                break;
            }
        }
        if (!channel) {
            channel =
                std::make_shared<MsgChannel>(*this, *context, peer_server_name);
            rails.push_back(channel);
        }
    }
    return channel->acceptPassive(peer_desc, local_desc);
}

std::shared_ptr<RdmaContext> RdmaTwoSidedTransport::selectMsgContext(
    const HandShakeDesc &peer_desc, size_t existing_rail_count) {
    if (getContextList().empty()) return nullptr;
    // Prefer matching peer NIC device name to a local HCA / context.
    const std::string peer_nic =
        getNicNameFromNicPath(peer_desc.local_nic_path);
    if (!peer_nic.empty() && localTopology()) {
        int index = 0;
        for (auto &entry : localTopology()->getHcaList()) {
            if (entry == peer_nic &&
                index < static_cast<int>(getContextList().size())) {
                return getContextList()[index];
            }
            index++;
        }
        for (auto &ctx : getContextList()) {
            if (ctx && ctx->deviceName() == peer_nic) return ctx;
        }
    }
    // Fallback: zip by handshake order (active iterates context list order).
    size_t idx = existing_rail_count % getContextList().size();
    return getContextList()[idx];
}

std::vector<std::shared_ptr<MsgChannel>> RdmaTwoSidedTransport::ensureMsgRails(
    const std::string &peer_server_name) {
    std::vector<std::shared_ptr<MsgChannel>> connected;
    if (!globalConfig().rdma_msg_enabled || getContextList().empty()) {
        return connected;
    }

    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = msg_channels_.find(peer_server_name);
        if (it != msg_channels_.end()) {
            for (auto &rail : it->second) {
                if (rail && rail->connected()) connected.push_back(rail);
            }
            if (connected.size() == getContextList().size()) return connected;
        }
    }

    // Build / reconnect missing rails outside the lock (handshake may block).
    std::vector<std::shared_ptr<MsgChannel>> created;
    created.reserve(getContextList().size());
    for (auto &ctx : getContextList()) {
        if (!ctx) continue;
        bool have = false;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            auto it = msg_channels_.find(peer_server_name);
            if (it != msg_channels_.end()) {
                for (auto &rail : it->second) {
                    if (rail && rail->nicPath() == ctx->nicPath() &&
                        rail->connected()) {
                        have = true;
                        break;
                    }
                }
            }
        }
        if (have) continue;
        auto channel =
            std::make_shared<MsgChannel>(*this, *ctx, peer_server_name);
        if (channel->connectActive()) {
            LOG(WARNING) << "MsgChannel: rail connect failed peer="
                         << peer_server_name << " nic=" << ctx->nicPath();
            continue;
        }
        created.push_back(channel);
    }

    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto &rails = msg_channels_[peer_server_name];
        for (auto &channel : created) {
            bool replaced = false;
            for (auto &existing : rails) {
                if (existing && existing->nicPath() == channel->nicPath()) {
                    if (existing.get() != channel.get()) {
                        existing->disconnect();
                        existing = channel;
                    }
                    replaced = true;
                    break;
                }
            }
            if (!replaced) rails.push_back(channel);
        }
        connected.clear();
        for (auto &rail : rails) {
            if (rail && rail->connected()) connected.push_back(rail);
        }
    }
    return connected;
}

std::shared_ptr<MsgChannel> RdmaTwoSidedTransport::ensureMsgChannel(
    const std::string &peer_server_name) {
    auto rails = ensureMsgRails(peer_server_name);
    if (rails.empty()) return nullptr;
    return rails.front();
}

Status RdmaTwoSidedTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::vector<TransferTask *> one_sided, two_sided;
    one_sided.reserve(task_list.size());
    for (auto *task : task_list) {
        if (task && task->request && shouldUseTwoSided(*task->request))
            two_sided.push_back(task);
        else
            one_sided.push_back(task);
    }
    if (!two_sided.empty()) {
        Status s = submitTwoSidedTasks(two_sided);
        if (!s.ok()) return s;
    }
    if (!one_sided.empty()) return RdmaTransport::submitTransferTask(one_sided);
    return Status::OK();
}

Status RdmaTwoSidedTransport::submitTwoSidedTasks(
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
            // dispatchTwoSidedTask already inserted TwoSidedTaskState; just
            // mark waiting (do not allocate a second task_id for the same
            // task).
            for (auto &entry : twosided_tasks_) {
                if (entry.second.task == task) {
                    entry.second.waiting_credit = true;
                    if (!task->slice_list.empty())
                        task->slice_list[0]->ts = entry.first;
                    break;
                }
            }
            continue;
        }
        if (rc) {
            slice->markFailed();
            return Status::InvalidArgument("two-sided submit failed");
        }
    }
    return Status::OK();
}

int RdmaTwoSidedTransport::dispatchTwoSidedTask(TransferTask *task) {
    if (!task || task->slice_list.empty() || !task->slice_list[0])
        return ERR_INVALID_ARGUMENT;
    // submitTransfer({req}) only keeps request alive for the call; after that
    // task->request dangles. Slice fields are snapshotted while it was valid.
    Slice *slice = task->slice_list[0];
    const auto opcode = slice->opcode;
    void *source = slice->source_addr;
    const size_t length = slice->length;
    const SegmentID target_id = slice->target_id;
    const uint64_t target_offset = slice->rdma.dest_addr;

    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) return ERR_METADATA;
    const std::string &peer = desc->name;

    // Ensure ctrl + multi-rail msg channels.
    if (!ensureCtrlChannel(peer)) return ERR_ENDPOINT;
    auto rails = ensureMsgRails(peer);
    if (rails.empty()) return ERR_ENDPOINT;

    uint64_t session = 0;
    // The ledger rejects mutations whose epoch does not match the session's
    // current generation, so the epoch has to travel with every reserve and
    // rollback for this task.
    uint64_t epoch = 1;
    bool session_known = false;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = peer_ctrl_state_.find(peer);
        if (it != peer_ctrl_state_.end()) {
            session = it->second.peer_session;
            if (it->second.epoch) epoch = it->second.epoch;
        }
    }
    if (session == 0)
        session = 1;  // may still be bootstrapping
    else
        session_known = true;

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
                entry.second.peer_epoch = epoch;
                entry.second.opcode = opcode;
                entry.second.local_buf = source;
                entry.second.total_bytes = length;
                break;
            }
        }
        if (task_id == 0) {
            task_id = next_task_id_.fetch_add(1);
            TwoSidedTaskState st;
            st.task = task;
            st.task_id = task_id;
            st.total_bytes = length;
            st.peer = peer;
            st.peer_session = session;
            st.peer_epoch = epoch;
            st.opcode = opcode;
            st.local_buf = source;
            twosided_tasks_[task_id] = st;
            slice->ts = task_id;
        }
    }

    // Credit reserve for full transfer (bounce slots ≈ ceil(len/payload)).
    size_t slots_needed = (length + max_payload - 1) / max_payload;
    if (slots_needed == 0) slots_needed = 1;
    if (globalConfig().rdma_credit_enabled) {
        // Credits exist only after the peer's CREDIT_GRANT activated the
        // session in the ledger. Dispatching before that is backpressure, not
        // an error, so queue the task; handleCreditGrant redispatches it.
        if (!session_known) return ERR_TOO_MANY_REQUESTS;
        int rc = sender_credit_.tryReserve(
            peer, session, epoch,
            {{CreditResource::BounceSlots, slots_needed},
             {CreditResource::BounceBytes, length}});
        if (rc) {
            for (auto &rail : rails) {
                if (rail) rail->requestExpand();
            }
            return rc;
        }
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it != twosided_tasks_.end()) {
            it->second.credit_reserved = true;
            it->second.reserved_slots = slots_needed;
            it->second.reserved_bytes = length;
            it->second.peer_session = session;
            it->second.peer = peer;
        }
    }

    // Once a chunk is on the wire the task is no longer replayable, so wait in
    // place for a recycled slot rather than rolling back and resending it.
    constexpr auto kMidTransferRetryWindow = std::chrono::milliseconds(200);

    auto spraySend = [&](uint8_t seq, bool allow_wait, auto &&send_fn) -> int {
        const size_t n = rails.size();
        const size_t start = static_cast<size_t>(seq) % n;
        const auto deadline =
            std::chrono::steady_clock::now() + kMidTransferRetryWindow;
        int last_rc = ERR_ENDPOINT;
        for (;;) {
            for (size_t attempt = 0; attempt < n; ++attempt) {
                auto &rail = rails[(start + attempt) % n];
                if (!rail || !rail->connected()) continue;
                last_rc = send_fn(rail);
                if (last_rc == 0) return 0;
                if (last_rc != ERR_TOO_MANY_REQUESTS) return last_rc;
            }
            // All rails out of slots: grow them, then wait for a DATA_ACK to
            // recycle one if the caller cannot requeue.
            for (auto &rail : rails) {
                if (rail) rail->requestExpand();
            }
            if (!allow_wait || std::chrono::steady_clock::now() >= deadline)
                return last_rc ? last_rc : ERR_TOO_MANY_REQUESTS;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    };

    // Safe after partial progress too: the peer never ACKs an incomplete task,
    // and recycles the slots it already consumed on its own.
    auto rollbackReservation = [&]() {
        if (!globalConfig().rdma_credit_enabled) return;
        sender_credit_.rollbackReservation(
            peer, session, epoch,
            {{CreditResource::BounceSlots, slots_needed},
             {CreditResource::BounceBytes, length}});
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it != twosided_tasks_.end()) it->second.credit_reserved = false;
    };

    uint8_t seq = 0;
    size_t sent_chunks = 0;
    for (size_t off = 0; off < length; off += max_payload, ++seq) {
        uint32_t chunk =
            static_cast<uint32_t>(std::min(max_payload, length - off));
        const bool partial = sent_chunks > 0;
        int rc = spraySend(seq, /*allow_wait=*/partial,
                           [&](const std::shared_ptr<MsgChannel> &msg) {
                               if (opcode == TransferRequest::WRITE) {
                                   return msg->sendDataWrite(
                                       task_id, seq, target_offset + off,
                                       static_cast<const char *>(source) + off,
                                       chunk);
                               }
                               return msg->sendReadReq(
                                   task_id, seq, target_offset + off, chunk);
                           });
        if (rc == 0) {
            ++sent_chunks;
            continue;
        }
        rollbackReservation();
        // Requeueing now would resend the prefix already on the wire, so
        // backpressure has to become a hard failure.
        if (partial && rc == ERR_TOO_MANY_REQUESTS) {
            LOG(WARNING) << "MsgChannel: no bounce slot mid-transfer, failing"
                         << " task_id=" << task_id << " peer=" << peer
                         << " sent_chunks=" << sent_chunks;
            return ERR_ENDPOINT;
        }
        return rc;
    }
    // Publish the dispatch under ctrl_idle_mutex_ and wake the ctrl worker:
    // the worker may be parked in its idle wait, which would otherwise delay
    // the DATA_ACK drain by the whole wait period.
    {
        std::lock_guard<std::mutex> lock(ctrl_idle_mutex_);
        twosided_inflight_.fetch_add(1, std::memory_order_acq_rel);
    }
    ctrl_idle_cv_.notify_one();
    return 0;
}

void RdmaTwoSidedTransport::redispatchWaitingTasks() {
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

void RdmaTwoSidedTransport::completeTwoSidedAck(uint64_t task_id,
                                                uint64_t acked_bytes) {
    TransferTask *task = nullptr;
    TwoSidedTaskState finished;
    bool do_rollback = false;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it == twosided_tasks_.end()) return;
        if (acked_bytes > it->second.acked_bytes)
            it->second.acked_bytes = acked_bytes;
        if (it->second.acked_bytes < it->second.total_bytes) return;
        finished = it->second;
        task = it->second.task;
        do_rollback = it->second.credit_reserved;
        twosided_tasks_.erase(it);
    }
    twosided_inflight_.fetch_sub(1, std::memory_order_acq_rel);
    if (do_rollback && globalConfig().rdma_credit_enabled) {
        sender_credit_.rollbackReservation(
            finished.peer, finished.peer_session, finished.peer_epoch,
            {{CreditResource::BounceSlots, finished.reserved_slots},
             {CreditResource::BounceBytes, finished.reserved_bytes}});
        redispatchWaitingTasks();
    }
    if (task && !task->slice_list.empty()) {
        task->slice_list[0]->markSuccess();
    }
}

int RdmaTwoSidedTransport::sendDataAck(const std::string &peer,
                                       uint64_t task_id, uint64_t acked_bytes) {
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

void RdmaTwoSidedTransport::onMsgReceived(const std::string &peer_server_name,
                                          const MsgHeader &hdr,
                                          const void *payload,
                                          MsgChannel *channel) {
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
            std::lock_guard<std::mutex> lock(recv_ack_mutex_);
            recv_acked_bytes_[hdr.task_id] += hdr.length;
            cumulative = recv_acked_bytes_[hdr.task_id];
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
        // Prefer same rail that received READ_REQ to avoid cross-rail hop.
        if (channel && channel->connected()) {
            (void)channel->sendReadResp(
                hdr.task_id, hdr.slice_seq, hdr.dest_addr,
                reinterpret_cast<const void *>(hdr.dest_addr), hdr.length);
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
        // Place into local buffer based on task_id + slice_seq.
        void *local_buf = nullptr;
        size_t total_len = 0;
        TransferRequest::OpCode opcode = TransferRequest::WRITE;
        {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            auto it = twosided_tasks_.find(hdr.task_id);
            if (it == twosided_tasks_.end()) return;
            opcode = it->second.opcode;
            local_buf = it->second.local_buf;
            total_len = it->second.total_bytes;
        }
        if (opcode != TransferRequest::READ) {
            LOG(ERROR) << "MsgChannel: READ_RESP for non-READ task_id="
                       << hdr.task_id << " from " << peer_server_name;
            return;
        }
        if (!local_buf || !hdr.length) return;
        size_t max_payload =
            globalConfig().rdma_msg_slot_size > kMsgHeaderSize
                ? globalConfig().rdma_msg_slot_size - kMsgHeaderSize
                : 0;
        uint64_t offset = static_cast<uint64_t>(hdr.slice_seq) * max_payload;
        if (offset > total_len || hdr.length > total_len - offset) {
            LOG(ERROR) << "MsgChannel: illegal READ_RESP range task_id="
                       << hdr.task_id;
            return;
        }
        if (payload) {
            std::memcpy(static_cast<char *>(local_buf) + offset, payload,
                        hdr.length);
        }
        // Finish through the same path as WRITE: completeTwoSidedAck releases
        // the reservation and redispatches waiting tasks.
        uint64_t cumulative = 0;
        {
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            auto it = twosided_tasks_.find(hdr.task_id);
            if (it == twosided_tasks_.end()) return;
            it->second.acked_bytes += hdr.length;
            cumulative = it->second.acked_bytes;
        }
        completeTwoSidedAck(hdr.task_id, cumulative);
        return;
    }
}

}  // namespace mooncake
