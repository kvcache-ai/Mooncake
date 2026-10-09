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

namespace mooncake {

namespace {

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
        case CtrlFrameType::SESSION_CLOSE:
        case CtrlFrameType::CREDIT_GRANT:
        case CtrlFrameType::CREDIT_REQUEST:
        case CtrlFrameType::CREDIT_PROGRESS:
        case CtrlFrameType::DATA_ACK:
        case CtrlFrameType::FENCE:
        case CtrlFrameType::DRAIN_ACK:
        case CtrlFrameType::CTRL_ACK:
            VLOG(1) << "CtrlChannel: ignoring frame type "
                    << static_cast<int>(frame.type) << " from "
                    << peer_server_name << " (PR2 notify-only)";
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
    while (ctrl_worker_running_.load(std::memory_order_acquire)) {
        std::vector<std::shared_ptr<CtrlChannel>> channels;
        {
            std::lock_guard<std::mutex> lock(ctrl_mutex_);
            channels.reserve(ctrl_channels_.size());
            for (auto &entry : ctrl_channels_) {
                if (entry.second) channels.push_back(entry.second);
            }
        }
        int processed = 0;
        for (auto &channel : channels) {
            int ret = channel->pollCompletions(16);
            if (ret > 0) processed += ret;
        }
        if (processed == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

}  // namespace mooncake
