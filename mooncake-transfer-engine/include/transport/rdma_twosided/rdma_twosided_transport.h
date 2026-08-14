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

#ifndef RDMA_TWOSIDED_TRANSPORT_H_
#define RDMA_TWOSIDED_TRANSPORT_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_twosided/ctrl_frame.h"

namespace mooncake {

class CtrlChannel;

// RDMA transport with a dedicated CtrlChannel for typed notify frames.
// One-sided data path is inherited from RdmaTransport; install name is
// "rdma_twosided" (mutually exclusive with classic "rdma" in MultiTransport).
class RdmaTwoSidedTransport : public RdmaTransport {
    friend class CtrlChannel;

   public:
    using NotifyDesc = TransferMetadata::NotifyDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    RdmaTwoSidedTransport();
    ~RdmaTwoSidedTransport() override;

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "rdma_twosided"; }

    int onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                               HandShakeDesc &local_desc) override;

    int sendRdmaNotify(const std::string &peer_server_name,
                       const NotifyDesc &notify);

    void onCtrlNotifyReceived(const NotifyDesc &notify);
    void onCtrlFrameReceived(const std::string &peer_server_name,
                             const CtrlFrame &frame);

    uint64_t localCtrlSessionId() const { return local_ctrl_session_id_; }

   private:
    int onSetupCtrlChannel(const HandShakeDesc &peer_desc,
                           HandShakeDesc &local_desc);
    std::shared_ptr<CtrlChannel> ensureCtrlChannel(
        const std::string &peer_server_name);

    void startCtrlWorker();
    void stopCtrlWorker();
    void ctrlWorkerLoop();

    std::mutex ctrl_mutex_;
    std::unordered_map<std::string, std::shared_ptr<CtrlChannel>>
        ctrl_channels_;
    std::thread ctrl_worker_;
    std::atomic<bool> ctrl_worker_running_{false};
    uint64_t local_ctrl_session_id_ = 0;
};

}  // namespace mooncake

#endif  // RDMA_TWOSIDED_TRANSPORT_H_
