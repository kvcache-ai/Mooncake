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

#include <mutex>
#include <random>

#include "config.h"
#include "transport/rdma_twosided/ctrl_channel.h"

namespace mooncake {

RdmaTwoSidedTransport::RdmaTwoSidedTransport() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    local_ctrl_session_id_ = gen();
    if (local_ctrl_session_id_ == 0) local_ctrl_session_id_ = 1;
}

RdmaTwoSidedTransport::~RdmaTwoSidedTransport() {
    stopCtrlWorker();
    std::lock_guard<std::mutex> lock(ctrl_mutex_);
    for (auto &entry : ctrl_channels_) {
        if (entry.second) entry.second->disconnect();
    }
    ctrl_channels_.clear();
}

int RdmaTwoSidedTransport::install(std::string &local_server_name,
                                   std::shared_ptr<TransferMetadata> meta,
                                   std::shared_ptr<Topology> topo) {
    int ret = RdmaTransport::install(local_server_name, meta, topo);
    if (ret) return ret;
    if (globalConfig().rdma_notify_enabled) {
        startCtrlWorker();
    }
    return 0;
}

int RdmaTwoSidedTransport::onSetupRdmaConnections(
    const HandShakeDesc &peer_desc, HandShakeDesc &local_desc) {
    if (peer_desc.ctrl_channel) {
        return onSetupCtrlChannel(peer_desc, local_desc);
    }
    return RdmaTransport::onSetupRdmaConnections(peer_desc, local_desc);
}

}  // namespace mooncake
