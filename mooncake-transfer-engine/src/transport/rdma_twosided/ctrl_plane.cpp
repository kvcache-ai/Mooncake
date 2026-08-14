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

    std::shared_ptr<CtrlChannel> channel;
    std::shared_ptr<CtrlChannel> old;
    {
        std::lock_guard<std::mutex> lock(ctrl_mutex_);
        auto it = ctrl_channels_.find(peer_server_name);
        if (it != ctrl_channels_.end() && it->second) {
            old = it->second;
        }
        channel = std::make_shared<CtrlChannel>(*this, *contexts[0],
                                                peer_server_name);
        ctrl_channels_[peer_server_name] = channel;
    }
    if (old) old->disconnect();
    return channel->acceptPassive(peer_desc, local_desc);
}

std::shared_ptr<CtrlChannel> RdmaTwoSidedTransport::ensureCtrlChannel(
    const std::string &peer_server_name) {
    const auto &contexts = getContextList();
    if (!globalConfig().rdma_notify_enabled || contexts.empty()) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(ctrl_mutex_);
    auto it = ctrl_channels_.find(peer_server_name);
    if (it != ctrl_channels_.end() && it->second && it->second->connected()) {
        return it->second;
    }

    // Serialize active connect per peer to avoid duplicate handshakes.
    auto channel =
        std::make_shared<CtrlChannel>(*this, *contexts[0], peer_server_name);
    // Release lock during handshake I/O; re-check afterwards.
    lock.unlock();
    if (channel->connectActive()) {
        return nullptr;
    }
    lock.lock();
    auto again = ctrl_channels_.find(peer_server_name);
    if (again != ctrl_channels_.end() && again->second &&
        again->second->connected()) {
        // Another thread won the race; keep the already-connected channel.
        channel->disconnect();
        return again->second;
    }
    ctrl_channels_[peer_server_name] = channel;
    return channel;
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
