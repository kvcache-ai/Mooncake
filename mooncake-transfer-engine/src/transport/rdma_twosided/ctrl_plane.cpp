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
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transport/rdma_twosided/ctrl_channel.h"
#include "transport/rdma_twosided/ctrl_frame.h"
#include "transport/rdma_twosided/msg_channel.h"

namespace mooncake {

namespace {

// Pause iterations per idle turn while DATA_ACKs are outstanding. Sized so one
// turn stays well under the ACK round trip, keeping the poll interval short
// without pinning the core.
constexpr int kCtrlSpinPauses = 200;

// Spin hint for the busy-wait above: yields the pipeline to the sibling
// hyperthread on x86 and is a no-op elsewhere.
inline void cpuRelax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

// Releases an already-held lock for the duration of a blocking call and
// reacquires it on scope exit, including when that call throws. Keeps the
// lock state uniform for the enclosing ConnectScope, whose destructor may
// then assume the lock is held.
class UnlockGuard {
   public:
    explicit UnlockGuard(std::unique_lock<std::mutex> &lock) : lock_(lock) {
        DCHECK(lock_.owns_lock());
        lock_.unlock();
    }
    ~UnlockGuard() { lock_.lock(); }

    UnlockGuard(const UnlockGuard &) = delete;
    UnlockGuard &operator=(const UnlockGuard &) = delete;

   private:
    std::unique_lock<std::mutex> &lock_;
};

}  // namespace

RdmaTwoSidedTransport::ConnectScope::ConnectScope(
    RdmaTwoSidedTransport &transport, std::string peer,
    std::shared_ptr<CtrlChannel> channel)
    : transport_(transport),
      peer_(std::move(peer)),
      channel_(std::move(channel)) {
    transport_.ctrl_channels_[peer_] = channel_;
    transport_.ctrl_connecting_[peer_]++;
}

RdmaTwoSidedTransport::ConnectScope::~ConnectScope() {
    auto in_flight = transport_.ctrl_connecting_.find(peer_);
    if (in_flight != transport_.ctrl_connecting_.end() &&
        --in_flight->second <= 0) {
        transport_.ctrl_connecting_.erase(in_flight);
    }
    if (!success_) {
        auto it = transport_.ctrl_channels_.find(peer_);
        if (it != transport_.ctrl_channels_.end() && it->second == channel_) {
            transport_.ctrl_channels_.erase(it);
        }
    }
    transport_.ctrl_cv_.notify_all();
}

int RdmaTwoSidedTransport::onSetupCtrlChannel(const HandShakeDesc &peer_desc,
                                              HandShakeDesc &local_desc) {
    if (!globalConfig().rdma_notify_enabled) {
        local_desc.reply_msg = "RDMA notify disabled";
        return ERR_INVALID_ARGUMENT;
    }
    const auto &contexts = getContextList();
    if (contexts.empty()) {
        local_desc.reply_msg = "No local RDMA context for CtrlChannel";
        return ERR_DEVICE_NOT_FOUND;
    }

    std::string peer_server_name =
        getServerNameFromNicPath(peer_desc.local_nic_path);
    if (peer_server_name.empty()) {
        local_desc.reply_msg = "Cannot derive peer server name from handshake";
        return ERR_INVALID_ARGUMENT;
    }

    std::unique_lock<std::mutex> lock(ctrl_mutex_);
    if (ctrl_stopping_) {
        local_desc.reply_msg = "CtrlChannel plane is shutting down";
        return ERR_ENDPOINT;
    }

    std::shared_ptr<CtrlChannel> old;
    auto it = ctrl_channels_.find(peer_server_name);
    if (it != ctrl_channels_.end()) old = it->second;

    auto channel =
        std::make_shared<CtrlChannel>(*this, *contexts[0], peer_server_name);
    // Publishes the placeholder; retracted automatically unless we succeed.
    ConnectScope scope(*this, peer_server_name, channel);
    int ret = 0;
    {
        UnlockGuard unlocked(lock);
        if (old) old->disconnect();
        ret = channel->acceptPassive(peer_desc, local_desc);
    }
    if (ret == 0) scope.markSuccess();
    return ret;
}

std::shared_ptr<CtrlChannel> RdmaTwoSidedTransport::ensureCtrlChannel(
    const std::string &peer_server_name) {
    const auto &contexts = getContextList();
    if (!globalConfig().rdma_notify_enabled || contexts.empty()) {
        return nullptr;
    }

    // Absolute deadline for the whole call, so retries and repeated waits
    // cannot extend it. On expiry we return nullptr and the caller falls back
    // to OOB notify instead of blocking forever.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(
                              globalConfig().rdma_notify_connect_timeout_ms);

    std::unique_lock<std::mutex> lock(ctrl_mutex_);
    while (true) {
        if (ctrl_stopping_) return nullptr;

        auto it = ctrl_channels_.find(peer_server_name);
        const bool connect_in_flight =
            ctrl_connecting_.find(peer_server_name) != ctrl_connecting_.end();
        if (it != ctrl_channels_.end() && it->second) {
            if (it->second->connected()) return it->second;
            if (!connect_in_flight) {
                // Channel died after connect (error WC, disconnect). Nobody
                // republishes or wakes waiters for it, so reclaim it here and
                // rebuild on the next iteration.
                auto dead = it->second;
                ctrl_channels_.erase(it);
                UnlockGuard unlocked(lock);
                dead->disconnect();
                continue;
            }
        } else if (!connect_in_flight) {
            auto channel = std::make_shared<CtrlChannel>(*this, *contexts[0],
                                                         peer_server_name);
            ConnectScope scope(*this, peer_server_name, channel);
            int ret = 0;
            {
                UnlockGuard unlocked(lock);
                ret = channel->connectActive();
            }
            if (ret) {
                // Report our own failure rather than retrying until the
                // deadline; the caller decides whether to use OOB notify.
                return nullptr;
            }
            auto again = ctrl_channels_.find(peer_server_name);
            if (again == ctrl_channels_.end() || again->second != channel) {
                // A passive accept superseded us while the lock was released.
                UnlockGuard unlocked(lock);
                channel->disconnect();
                continue;
            }
            scope.markSuccess();
            return channel;
        }

        // A connect really is in flight for this peer: wait for its owner to
        // publish or retract it, bounded by the deadline.
        if (ctrl_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            LOG(WARNING) << "CtrlChannel: timed out waiting for in-flight "
                            "connect to "
                         << peer_server_name;
            return nullptr;
        }
    }
}

int RdmaTwoSidedTransport::sendRdmaNotify(const std::string &peer_server_name,
                                          const NotifyDesc &notify) {
    auto channel = ensureCtrlChannel(peer_server_name);
    if (!channel) return ERR_ENDPOINT;
    return channel->sendNotify(notify);
}

void RdmaTwoSidedTransport::onCtrlNotifyReceived(const NotifyDesc &notify) {
    if (meta()) meta()->pushNotify(notify);
}

void RdmaTwoSidedTransport::onCtrlFrameReceived(
    const std::string &peer_server_name, const CtrlFrame &frame) {
    switch (frame.type) {
        case CtrlFrameType::NOTIFY_COMPAT: {
            NotifyDesc notify;
            if (decodeNotifyCompatPayload(frame.payload.data(),
                                          frame.payload.size(), notify) == 0) {
                onCtrlNotifyReceived(notify);
            } else {
                LOG(ERROR) << "CtrlChannel: bad NOTIFY_COMPAT from "
                           << peer_server_name;
            }
            break;
        }
        case CtrlFrameType::SESSION_OPEN:
            handleSessionOpen(peer_server_name, frame);
            break;
        case CtrlFrameType::CREDIT_GRANT:
            handleCreditGrant(peer_server_name, frame);
            break;
        case CtrlFrameType::CREDIT_REQUEST:
            handleCreditRequest(peer_server_name, frame);
            break;
        case CtrlFrameType::DATA_ACK:
            handleDataAck(peer_server_name, frame);
            break;
        case CtrlFrameType::SESSION_CLOSE:
        case CtrlFrameType::CREDIT_PROGRESS:
        case CtrlFrameType::FENCE:
        case CtrlFrameType::DRAIN_ACK:
        case CtrlFrameType::CTRL_ACK:
            VLOG(1) << "CtrlChannel: ignoring frame type "
                    << static_cast<int>(frame.type) << " from "
                    << peer_server_name;
            break;
        default:
            LOG(WARNING) << "CtrlChannel: unknown frame type "
                         << static_cast<int>(frame.type) << " from "
                         << peer_server_name;
            break;
    }
}

void RdmaTwoSidedTransport::startCtrlWorker() {
    if (ctrl_worker_running_.exchange(true)) return;
    ctrl_worker_ = std::thread([this] { ctrlWorkerLoop(); });
}

void RdmaTwoSidedTransport::stopCtrlWorker() {
    {
        // Release waiters before joining: a thread parked on ctrl_cv_ must not
        // outlive the plane it is waiting on. Notify under the lock so a waiter
        // that has not parked yet cannot miss the wakeup.
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        ctrl_stopping_ = true;
        ctrl_cv_.notify_all();
    }
    if (!ctrl_worker_running_.exchange(false)) return;
    if (ctrl_worker_.joinable()) ctrl_worker_.join();
}

void RdmaTwoSidedTransport::ctrlWorkerLoop() {
#ifdef __linux__
    // Linux inflates short sleeps with per-process timer slack (~50us by
    // default), which would dominate DATA_ACK latency. Request minimal slack
    // so the idle wait below approximates its requested duration.
    prctl(PR_SET_TIMERSLACK, 1, 0, 0, 0);
#endif
    while (ctrl_worker_running_.load(std::memory_order_acquire)) {
        std::vector<std::shared_ptr<CtrlChannel>> channels;
        std::vector<std::string> pending_grants;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            channels.reserve(ctrl_channels_.size());
            for (auto &entry : ctrl_channels_) {
                if (entry.second) channels.push_back(entry.second);
            }
            for (auto &entry : peer_ctrl_state_) {
                if (entry.second.grant_pending) {
                    pending_grants.push_back(entry.first);
                }
            }
        }
        int processed = 0;
        for (auto &channel : channels) {
            int ret = channel->pollCompletions(16);
            if (ret > 0) processed += ret;
        }
        for (const auto &peer : pending_grants) {
            if (sendInitialCreditGrant(peer) == 0) processed++;
        }
        if (processed == 0) {
            // Busy-wait while two-sided tasks await DATA_ACK: timer
            // granularity (especially on virtualized hosts) is far coarser
            // than the ACK path, so sleeping here would dominate ACK latency.
            if (twosided_inflight_.load(std::memory_order_acquire) > 0) {
                for (int i = 0; i < kCtrlSpinPauses; ++i) cpuRelax();
            } else {
                // Sleep until a task is dispatched or the idle period elapses.
                // The predicate check and the fetch_add in
                // dispatchTwoSidedTask share ctrl_idle_mutex_, so a task
                // submitted just before the sleep cannot be missed.
                std::unique_lock<std::mutex> lock(ctrl_idle_mutex_);
                ctrl_idle_cv_.wait_for(
                    lock, std::chrono::microseconds(100), [this] {
                        return twosided_inflight_.load(
                                   std::memory_order_acquire) > 0;
                    });
            }
        }
    }
}

void RdmaTwoSidedTransport::handleSessionOpen(
    const std::string &peer_server_name, const CtrlFrame &frame) {
    uint32_t bounce_slots = 0, bounce_slot_size = 0;
    if (decodeSessionOpenPayload(frame.payload.data(), frame.payload.size(),
                                 bounce_slots, bounce_slot_size)) {
        LOG(ERROR) << "CtrlChannel: bad SESSION_OPEN from " << peer_server_name;
        return;
    }

    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto &state = peer_ctrl_state_[peer_server_name];
        state.peer_session = frame.session;
        state.epoch = frame.epoch ? frame.epoch : 1;
        state.peer_bounce_slots = bounce_slots;
        state.peer_bounce_slot_size = bounce_slot_size;
        state.session_open_received = true;

        if (sender_credit_.activate(peer_server_name, frame.session,
                                    state.epoch)) {
            LOG(ERROR) << "CtrlChannel: credit activate failed for "
                       << peer_server_name;
            return;
        }
        // Defer GRANT to ctrl worker so we never sendCtrlFrame inside the
        // recv dispatch stack.
        if (!state.initial_grant_sent && globalConfig().rdma_credit_enabled) {
            state.grant_pending = true;
        }
    }

    LOG(INFO) << "CtrlChannel: SESSION_OPEN from " << peer_server_name
              << " peer_session=" << frame.session
              << " bounce_slots=" << bounce_slots
              << " slot_size=" << bounce_slot_size;
}

int RdmaTwoSidedTransport::sendInitialCreditGrant(
    const std::string &peer_server_name) {
    if (!globalConfig().rdma_credit_enabled) return 0;
    uint64_t epoch = 1;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto &state = peer_ctrl_state_[peer_server_name];
        if (state.initial_grant_sent) return 0;
        state.initial_grant_sent = true;
        state.grant_pending = false;
        epoch = state.epoch ? state.epoch : 1;
    }
    int ret = sendCreditGrant(peer_server_name, epoch);
    if (ret) {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        peer_ctrl_state_[peer_server_name].initial_grant_sent = false;
        peer_ctrl_state_[peer_server_name].grant_pending = true;
    }
    return ret;
}

int RdmaTwoSidedTransport::sendCreditGrant(const std::string &peer_server_name,
                                           uint64_t epoch,
                                           uint64_t bounce_slots) {
    std::shared_ptr<CtrlChannel> channel;
    uint64_t seq = 0;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = ctrl_channels_.find(peer_server_name);
        if (it == ctrl_channels_.end() || !it->second) return ERR_ENDPOINT;
        channel = it->second;
        auto &state = peer_ctrl_state_[peer_server_name];
        if (epoch == 0) epoch = state.epoch;
        seq = state.next_grant_seq;
    }
    if (!channel || !channel->connected()) return ERR_ENDPOINT;

    if (bounce_slots == 0) {
        size_t sum = 0;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            auto it = msg_channels_.find(peer_server_name);
            if (it != msg_channels_.end()) {
                for (auto &rail : it->second) {
                    if (rail && rail->connected()) sum += rail->activeSlots();
                }
            }
        }
        bounce_slots = sum ? sum : globalConfig().rdma_msg_pool_base;
        if (bounce_slots == 0) bounce_slots = globalConfig().rdma_msg_pool_base;
    }

    uint64_t bytes =
        bounce_slots * static_cast<uint64_t>(globalConfig().rdma_msg_slot_size);
    std::vector<CreditAmount> grants = {
        {CreditResource::BounceSlots, bounce_slots},
        {CreditResource::BounceBytes, bytes},
    };
    if (globalConfig().rdma_credit_window_bytes > 0) {
        grants.push_back({CreditResource::DataBytes,
                          globalConfig().rdma_credit_window_bytes});
    }
    if (globalConfig().rdma_credit_window_requests > 0) {
        grants.push_back({CreditResource::RequestSlots,
                          globalConfig().rdma_credit_window_requests});
    }

    CtrlFrame frame;
    frame.type = CtrlFrameType::CREDIT_GRANT;
    frame.session = local_ctrl_session_id_;
    frame.epoch = epoch;
    frame.seq = seq;
    if (encodeCreditGrantPayload(grants, frame.payload))
        return ERR_INVALID_ARGUMENT;

    int ret = channel->sendCtrlFrame(frame);
    if (ret == 0) {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto &state = peer_ctrl_state_[peer_server_name];
        if (state.next_grant_seq == seq) state.next_grant_seq++;
        state.granted_bounce_slots = bounce_slots;
    }
    return ret;
}

void RdmaTwoSidedTransport::handleCreditGrant(
    const std::string &peer_server_name, const CtrlFrame &frame) {
    std::vector<CreditAmount> grants;
    if (decodeCreditGrantPayload(frame.payload.data(), frame.payload.size(),
                                 grants)) {
        LOG(ERROR) << "CtrlChannel: bad CREDIT_GRANT from " << peer_server_name;
        return;
    }
    int disposition = 0;
    // Ensure ledger entry exists before apply (GRANT may race SESSION_OPEN).
    if (sender_credit_.activate(peer_server_name, frame.session, frame.epoch)) {
        LOG(ERROR) << "CtrlChannel: credit activate failed for grant from "
                   << peer_server_name;
        return;
    }
    int ret =
        sender_credit_.applyGrant(peer_server_name, frame.session, frame.epoch,
                                  frame.seq, grants, disposition);
    if (ret) {
        LOG(ERROR) << "CtrlChannel: apply CREDIT_GRANT failed from "
                   << peer_server_name << " ret=" << ret
                   << " session=" << frame.session << " epoch=" << frame.epoch
                   << " seq=" << frame.seq << " grants=" << grants.size();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto &state = peer_ctrl_state_[peer_server_name];
        state.peer_session = frame.session;
        state.epoch = frame.epoch ? frame.epoch : state.epoch;
    }
    LOG(INFO) << "CtrlChannel: CREDIT_GRANT from " << peer_server_name
              << " seq=" << frame.seq << " disposition=" << disposition
              << " grants=" << grants.size();
    redispatchWaitingTasks();
}

void RdmaTwoSidedTransport::handleCreditRequest(
    const std::string &peer_server_name, const CtrlFrame &frame) {
    (void)frame;
    std::lock_guard<std::mutex> lock(ctrl_mutex_);
    peer_ctrl_state_[peer_server_name].expand_requested = true;
    VLOG(1) << "CtrlChannel: CREDIT_REQUEST from " << peer_server_name;
}

int RdmaTwoSidedTransport::sendCreditRequest(
    const std::string &peer_server_name) {
    std::shared_ptr<CtrlChannel> channel;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = ctrl_channels_.find(peer_server_name);
        if (it == ctrl_channels_.end() || !it->second) return ERR_ENDPOINT;
        channel = it->second;
    }
    if (!channel || !channel->connected()) return ERR_ENDPOINT;
    CtrlFrame frame;
    frame.type = CtrlFrameType::CREDIT_REQUEST;
    frame.session = local_ctrl_session_id_;
    frame.epoch = 1;
    return channel->sendCtrlFrame(frame);
}

void RdmaTwoSidedTransport::handleDataAck(const std::string &peer_server_name,
                                          const CtrlFrame &frame) {
    std::vector<DataAckEntry> acks;
    if (decodeDataAckPayload(frame.payload.data(), frame.payload.size(),
                             acks)) {
        LOG(ERROR) << "CtrlChannel: bad DATA_ACK from " << peer_server_name;
        return;
    }
    for (const auto &ack : acks) {
        completeTwoSidedAck(ack.task_id, ack.acked_bytes);
    }
    VLOG(1) << "CtrlChannel: DATA_ACK from " << peer_server_name
            << " entries=" << acks.size();
}

uint64_t RdmaTwoSidedTransport::peerGrantedBounceSlots(
    const std::string &peer_server_name) {
    return sender_credit_.availableForPeer(peer_server_name,
                                           CreditResource::BounceSlots);
}

size_t RdmaTwoSidedTransport::msgBounceActiveSlots(
    const std::string &peer_server_name) {
    std::lock_guard<std::mutex> lock(ctrl_mutex_);
    auto it = msg_channels_.find(peer_server_name);
    if (it == msg_channels_.end()) return 0;
    size_t sum = 0;
    for (auto &rail : it->second) {
        if (rail) sum += rail->activeSlots();
    }
    return sum;
}

void RdmaTwoSidedTransport::startBounceManager() {
    if (bounce_manager_running_.exchange(true)) return;
    bounce_manager_ = std::thread([this] { bounceManagerLoop(); });
}

void RdmaTwoSidedTransport::stopBounceManager() {
    if (!bounce_manager_running_.exchange(false)) return;
    if (bounce_manager_.joinable()) bounce_manager_.join();
}

size_t RdmaTwoSidedTransport::waitingTaskCount() {
    std::lock_guard<std::mutex> lock(twosided_mutex_);
    return waiting_tasks_.size();
}

void RdmaTwoSidedTransport::bounceManagerLoop() {
    while (bounce_manager_running_.load(std::memory_order_acquire)) {
        manageBouncePoolsTick();
        auto tick_ms = globalConfig().rdma_msg_bounce_manager_tick_ms;
        if (tick_ms == 0) tick_ms = 50;
        std::this_thread::sleep_for(std::chrono::milliseconds(tick_ms));
    }
}

void RdmaTwoSidedTransport::manageBouncePoolsTick() {
    if (!globalConfig().rdma_msg_enabled) return;
    auto &cfg = globalConfig();
    const size_t waiting = waitingTaskCount();
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());

    // Ask peers to expand when local transfers are credit-blocked.
    if (waiting >= cfg.rdma_msg_pending_expand_threshold) {
        std::vector<std::string> peers;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            for (auto &entry : ctrl_channels_) {
                if (!entry.second || !entry.second->connected()) continue;
                auto &st = peer_ctrl_state_[entry.first];
                if (st.last_credit_request_ms != 0 &&
                    now_ms < st.last_credit_request_ms +
                                 cfg.rdma_msg_bounce_manager_tick_ms)
                    continue;
                st.last_credit_request_ms = now_ms;
                peers.push_back(entry.first);
            }
        }
        for (const auto &peer : peers) sendCreditRequest(peer);
    }

    // Ensure MsgChannel rails exist for peers that requested expand.
    {
        std::vector<std::string> need_msg;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            for (auto &entry : peer_ctrl_state_) {
                if (!entry.second.expand_requested) continue;
                auto it = msg_channels_.find(entry.first);
                bool have = false;
                if (it != msg_channels_.end()) {
                    for (auto &rail : it->second) {
                        if (rail && rail->connected()) {
                            have = true;
                            break;
                        }
                    }
                }
                if (!have) need_msg.push_back(entry.first);
            }
        }
        for (const auto &peer : need_msg) ensureMsgRails(peer);
    }

    std::vector<
        std::pair<std::string, std::vector<std::shared_ptr<MsgChannel>>>>
        peer_rails;
    std::unordered_map<std::string, bool> expand_requested;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        peer_rails.reserve(msg_channels_.size());
        for (auto &entry : msg_channels_) {
            std::vector<std::shared_ptr<MsgChannel>> rails;
            for (auto &rail : entry.second) {
                if (rail && rail->connected()) rails.push_back(rail);
            }
            if (!rails.empty())
                peer_rails.emplace_back(entry.first, std::move(rails));
        }
        for (auto &entry : peer_ctrl_state_) {
            expand_requested[entry.first] = entry.second.expand_requested;
        }
    }

    for (auto &entry : peer_rails) {
        const std::string &peer = entry.first;
        auto &rails = entry.second;
        size_t active_sum = 0;
        size_t free_send_sum = 0;
        uint64_t waiting_hints = 0;
        for (auto &msg : rails) {
            active_sum += msg->activeSlots();
            free_send_sum += msg->freeSendSlots();
            waiting_hints += msg->waitingHints();
        }
        if (active_sum == 0) continue;
        const size_t free_pct = (free_send_sum * 100) / active_sum;
        const bool peer_asked =
            expand_requested.count(peer) && expand_requested[peer];
        const bool expand_hint =
            waiting_hints > 0 || peer_asked ||
            waiting >= cfg.rdma_msg_pending_expand_threshold;
        const bool low_water =
            free_pct <= cfg.rdma_msg_expand_low_watermark_pct;

        if ((low_water || expand_hint) &&
            active_sum < cfg.rdma_msg_pool_max * rails.size()) {
            bool expanded = false;
            for (auto &msg : rails) {
                const size_t active = msg->activeSlots();
                if (active >= cfg.rdma_msg_pool_max) continue;
                size_t step = cfg.rdma_msg_expand_step;
                if (step == 0) step = 1;
                if (active + step > cfg.rdma_msg_pool_max)
                    step = cfg.rdma_msg_pool_max - active;
                if (step > 0 && msg->expandPool(step) == 0) expanded = true;
            }
            if (expanded) {
                uint64_t epoch = 1;
                size_t new_sum = 0;
                for (auto &msg : rails) new_sum += msg->activeSlots();
                {
                    std::lock_guard<std::mutex> lock(ctrl_mutex_);
                    auto it = peer_ctrl_state_.find(peer);
                    if (it != peer_ctrl_state_.end()) {
                        epoch = it->second.epoch ? it->second.epoch : 1;
                        it->second.high_watermark_since_ms = 0;
                        it->second.expand_requested = false;
                    }
                }
                if (globalConfig().rdma_credit_enabled) {
                    sendCreditGrant(peer, epoch, new_sum);
                }
                redispatchWaitingTasks();
            }
            continue;
        }

        if (peer_asked) {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            auto it = peer_ctrl_state_.find(peer);
            if (it != peer_ctrl_state_.end())
                it->second.expand_requested = false;
        }

        const bool high_water =
            free_pct >= cfg.rdma_msg_shrink_high_watermark_pct &&
            waiting == 0 && waiting_hints == 0 && !peer_asked;
        if (!high_water ||
            active_sum <= cfg.rdma_msg_pool_base * rails.size()) {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            auto it = peer_ctrl_state_.find(peer);
            if (it != peer_ctrl_state_.end()) {
                it->second.high_watermark_since_ms = 0;
            }
            continue;
        }

        uint64_t since = 0;
        uint64_t granted = 0;
        uint64_t epoch = 1;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            auto &state = peer_ctrl_state_[peer];
            if (state.high_watermark_since_ms == 0) {
                state.high_watermark_since_ms = now_ms;
            }
            since = state.high_watermark_since_ms;
            granted = state.granted_bounce_slots;
            epoch = state.epoch ? state.epoch : 1;
        }
        if (now_ms < since + cfg.rdma_msg_shrink_idle_ms) continue;

        size_t step = cfg.rdma_msg_expand_step;
        if (step == 0) step = 1;
        size_t min_total = cfg.rdma_msg_pool_base * rails.size();
        size_t target = active_sum > step ? active_sum - step : min_total;
        if (target < min_total) target = min_total;
        if (target >= active_sum) continue;

        if (globalConfig().rdma_credit_enabled) {
            if (granted == 0 || target < granted) {
                if (sendCreditGrant(peer, epoch, target)) continue;
            }
        }
        // Shrink rails toward a fair per-rail share of the peer total target.
        size_t per_rail = (target + rails.size() - 1) / rails.size();
        if (per_rail < cfg.rdma_msg_pool_base)
            per_rail = cfg.rdma_msg_pool_base;
        for (auto &msg : rails) msg->shrinkPoolToward(per_rail);
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            peer_ctrl_state_[peer].high_watermark_since_ms = now_ms;
        }
    }
}
}  // namespace mooncake
