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
#include <unordered_map>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_twosided/ctrl_channel.h"
#include "transport/rdma_twosided/msg_channel.h"
#include "transport/rdma_transport/rdma_context.h"

namespace mooncake {
namespace {
// Backstop for receiver-side ACK bookkeeping. A completed task retires itself
// once all its chunks arrive; this only reclaims tasks that never complete.
// Keep it well above any transfer lifetime, because pruning an entry that is
// still receiving would restart its cumulative count from zero and the sender
// would never see the task complete.
constexpr uint64_t kRecvAckIdleMs = 60000;

uint64_t nowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
}  // namespace

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

    // Absolute deadline for the whole call so repeated waits cannot extend it.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(
                              globalConfig().rdma_notify_connect_timeout_ms);

    auto find_rail = [&](const std::string &nic_path) {
        std::shared_ptr<MsgChannel> found;
        auto it = msg_channels_.find(peer_server_name);
        if (it == msg_channels_.end()) return found;
        for (auto &rail : it->second) {
            if (rail && rail->nicPath() == nic_path && rail->connected()) {
                found = rail;
                break;
            }
        }
        return found;
    };

    std::unique_lock<std::mutex> lock(ctrl_mutex_);
    for (auto &ctx : getContextList()) {
        if (!ctx) continue;
        const std::string nic_path = ctx->nicPath();
        const std::string key = peer_server_name + "|" + nic_path;
        while (!ctrl_stopping_) {
            if (find_rail(nic_path)) break;
            if (msg_connecting_.count(key)) {
                // Another thread is handshaking this rail. A second handshake
                // would make the peer replace its rail, and the SENDs already
                // posted on the replaced one never land: their QP is gone on
                // the peer, and rnr_retry=7 means they never complete either.
                if (ctrl_cv_.wait_until(lock, deadline) ==
                    std::cv_status::timeout) {
                    LOG(WARNING) << "MsgChannel: timed out waiting for "
                                    "in-flight rail connect to "
                                 << peer_server_name << " nic=" << nic_path;
                    break;
                }
                continue;
            }

            {
                // Publishes the in-flight key and, on every exit path
                // including a throwing handshake, clears it and wakes the
                // waiters above.
                RailConnectScope scope(*this, key);
                auto channel =
                    std::make_shared<MsgChannel>(*this, *ctx, peer_server_name);
                int ret = 0;
                {
                    // The handshake blocks and re-enters this transport on the
                    // passive side, so it cannot run under ctrl_mutex_.
                    UnlockGuard unlocked(lock);
                    ret = channel->connectActive();
                }
                if (ret == 0) {
                    auto existing_rail = find_rail(nic_path);
                    if (existing_rail) {
                        // A passive accept published a rail for this NIC while
                        // the lock was released; keep it and drop ours rather
                        // than stranding whatever is already in flight on it.
                        UnlockGuard unlocked(lock);
                        channel->disconnect();
                    } else {
                        auto &rails = msg_channels_[peer_server_name];
                        bool replaced = false;
                        for (auto &rail : rails) {
                            if (rail && rail->nicPath() == nic_path) {
                                rail = channel;
                                replaced = true;
                                break;
                            }
                        }
                        if (!replaced) rails.push_back(channel);
                    }
                } else {
                    LOG(WARNING) << "MsgChannel: rail connect failed peer="
                                 << peer_server_name << " nic=" << nic_path;
                }
            }
            break;
        }
    }

    auto it = msg_channels_.find(peer_server_name);
    if (it != msg_channels_.end()) {
        for (auto &rail : it->second) {
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
            waiting_count_.store(waiting_tasks_.size(),
                                 std::memory_order_relaxed);
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
    // Non-zero when resuming a task that was stopped mid-transfer; its
    // reservation is still held, so it must not be reserved again.
    uint64_t resume_off = 0;
    bool credit_held = false;
    uint64_t held_slots = 0;
    uint64_t held_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        // Reuse existing state if redispatching a waiting task.
        for (auto &entry : twosided_tasks_) {
            if (entry.second.task == task) {
                task_id = entry.first;
                resume_off = entry.second.sent_bytes;
                credit_held = entry.second.credit_reserved;
                held_slots = entry.second.reserved_slots;
                held_bytes = entry.second.reserved_bytes;
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
    uint64_t reserve_bytes = length;
    if (credit_held) {
        // Roll back exactly what was reserved, which may have been capped.
        slots_needed = held_slots;
        reserve_bytes = held_bytes;
    } else if (globalConfig().rdma_credit_enabled) {
        // Credits exist only after the peer's CREDIT_GRANT activated the
        // session in the ledger. Dispatching before that is backpressure, not
        // an error, so queue the task; handleCreditGrant redispatches it.
        if (!session_known) return ERR_TOO_MANY_REQUESTS;
        // A grant never exceeds the peer's bounce pool, so a transfer needing
        // more slots than the whole pool would wait for credits that can never
        // arrive. Reserve at most the full grant and let mid-transfer resume
        // carry the rest.
        uint64_t grant_slots = 0;
        if (sender_credit_.grantTotal(peer, session, epoch,
                                      CreditResource::BounceSlots,
                                      grant_slots) == 0 &&
            grant_slots && slots_needed > grant_slots) {
            slots_needed = grant_slots;
            reserve_bytes = std::min<uint64_t>(
                length, grant_slots * static_cast<uint64_t>(
                                          globalConfig().rdma_msg_slot_size));
        }
        int rc = sender_credit_.tryReserve(
            peer, session, epoch,
            {{CreditResource::BounceSlots, slots_needed},
             {CreditResource::BounceBytes, reserve_bytes}});
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
            it->second.reserved_bytes = reserve_bytes;
            it->second.peer_session = session;
            it->second.peer = peer;
        }
    }

    auto spraySend = [&](uint32_t seq, auto &&send_fn) -> int {
        const size_t n = rails.size();
        const size_t start = static_cast<size_t>(seq) % n;
        int last_rc = ERR_ENDPOINT;
        for (size_t attempt = 0; attempt < n; ++attempt) {
            auto &rail = rails[(start + attempt) % n];
            if (!rail || !rail->connected()) continue;
            last_rc = send_fn(rail);
            if (last_rc == 0) return 0;
            if (last_rc != ERR_TOO_MANY_REQUESTS) return last_rc;
        }
        // Every rail is out of bounce slots: grow them. The caller resumes
        // from the next unsent offset once a DATA_ACK recycles one.
        for (auto &rail : rails) {
            if (rail) rail->requestExpand();
        }
        return last_rc;
    };

    // Safe after partial progress too: the peer never ACKs an incomplete task,
    // and recycles the slots it already consumed on its own.
    auto rollbackReservation = [&]() {
        if (!globalConfig().rdma_credit_enabled) return;
        sender_credit_.rollbackReservation(
            peer, session, epoch,
            {{CreditResource::BounceSlots, slots_needed},
             {CreditResource::BounceBytes, reserve_bytes}});
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it != twosided_tasks_.end()) it->second.credit_reserved = false;
    };

    // Chunk count for the whole task, not just this dispatch: the receiver
    // counts across resumes, so a resumed dispatch must report the same total.
    const uint32_t total_chunks =
        static_cast<uint32_t>((length + max_payload - 1) / max_payload);
    if (resume_off)
        twosided_resume_count_.fetch_add(1, std::memory_order_relaxed);
    for (size_t off = resume_off; off < length; off += max_payload) {
        uint32_t seq = static_cast<uint32_t>(off / max_payload);
        uint32_t chunk =
            static_cast<uint32_t>(std::min(max_payload, length - off));
        int rc = spraySend(seq, [&](const std::shared_ptr<MsgChannel> &msg) {
            if (opcode == TransferRequest::WRITE) {
                return msg->sendDataWrite(
                    task_id, seq, target_offset + off,
                    static_cast<const char *>(source) + off, chunk,
                    total_chunks);
            }
            return msg->sendReadReq(task_id, seq, target_offset + off, chunk);
        });
        if (rc == 0) continue;
        if (rc == ERR_TOO_MANY_REQUESTS && off > 0) {
            // Keep the reservation and record where to resume: replaying the
            // prefix would duplicate chunks the peer already counted.
            std::lock_guard<std::mutex> lock(twosided_mutex_);
            auto it = twosided_tasks_.find(task_id);
            if (it != twosided_tasks_.end()) it->second.sent_bytes = off;
            return rc;
        }
        rollbackReservation();
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
    if (waiting_count_.load(std::memory_order_relaxed) == 0) return;
    // One recycled slot cannot admit the whole queue, so retrying every waiter
    // on every ACK costs O(queue) per event and collapses throughput once the
    // queue is deep. Retry a bounded prefix instead and leave the rest to the
    // next event.
    constexpr size_t kMaxRetryPerEvent = 8;
    std::deque<TransferTask *> pending;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        while (!waiting_tasks_.empty() && pending.size() < kMaxRetryPerEvent) {
            pending.push_back(waiting_tasks_.front());
            waiting_tasks_.pop_front();
        }
        waiting_count_.store(waiting_tasks_.size(), std::memory_order_relaxed);
    }
    std::deque<TransferTask *> requeue;
    for (auto *task : pending) {
        int rc = dispatchTwoSidedTask(task);
        if (rc == ERR_TOO_MANY_REQUESTS) {
            requeue.push_back(task);
        } else if (rc) {
            if (!task->slice_list.empty()) task->slice_list[0]->markFailed();
        }
    }
    if (!requeue.empty()) {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        // At the front: a task that already waited keeps its place in line.
        waiting_tasks_.insert(waiting_tasks_.begin(), requeue.begin(),
                              requeue.end());
        waiting_count_.store(waiting_tasks_.size(), std::memory_order_relaxed);
    }
}

void RdmaTwoSidedTransport::completeTwoSidedAck(uint64_t task_id,
                                                uint64_t acked_bytes) {
    TransferTask *task = nullptr;
    TwoSidedTaskState finished;
    bool do_rollback = false;
    bool completed = false;
    bool slot_recycled = false;
    {
        std::lock_guard<std::mutex> lock(twosided_mutex_);
        auto it = twosided_tasks_.find(task_id);
        if (it == twosided_tasks_.end()) return;
        if (acked_bytes > it->second.acked_bytes)
            it->second.acked_bytes = acked_bytes;
        if (it->second.acked_bytes < it->second.total_bytes) {
            // This ACK freed a bounce slot, which is exactly what a task
            // stopped mid-transfer is queued for.
            slot_recycled = !waiting_tasks_.empty();
        } else {
            finished = it->second;
            task = it->second.task;
            do_rollback = it->second.credit_reserved;
            completed = true;
            twosided_tasks_.erase(it);
        }
    }
    if (!completed) {
        if (slot_recycled) redispatchWaitingTasks();
        return;
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

void RdmaTwoSidedTransport::pruneRecvAckLedger() {
    const uint64_t now = nowMs();
    std::lock_guard<std::mutex> lock(recv_ack_mutex_);
    for (auto it = recv_acked_bytes_.begin(); it != recv_acked_bytes_.end();) {
        if (it->second.last_ms + kRecvAckIdleMs < now)
            it = recv_acked_bytes_.erase(it);
        else
            ++it;
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
            RecvAckKey key{peer_server_name, hdr.session, hdr.task_id};
            auto &state = recv_acked_bytes_[key];
            state.bytes += hdr.length;
            state.chunks++;
            state.last_ms = nowMs();
            cumulative = state.bytes;
            // Retire the entry as soon as the task is whole. Counting chunks
            // works regardless of the order they arrive in across rails, which
            // a last-chunk flag would not. A duplicate landing afterwards just
            // re-creates the entry and its ACK is dropped by the sender, whose
            // task is already gone.
            if (hdr.total_chunks && state.chunks >= hdr.total_chunks)
                recv_acked_bytes_.erase(key);
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
            (void)channel->sendOrQueueReadResp(hdr.task_id, hdr.slice_seq,
                                               hdr.dest_addr, hdr.length);
            return;
        }
        auto msg = ensureMsgChannel(peer_server_name);
        if (!msg) return;
        (void)msg->sendOrQueueReadResp(hdr.task_id, hdr.slice_seq,
                                       hdr.dest_addr, hdr.length);
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
