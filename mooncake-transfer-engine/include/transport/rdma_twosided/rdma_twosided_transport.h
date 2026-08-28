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
#include <condition_variable>
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

    // RAII publisher for one connect attempt. On construction it installs the
    // placeholder entry and marks the peer as having a connect in flight; on
    // destruction it always retracts the marker and wakes waiters, and drops
    // the placeholder unless markSuccess() was called. Callers must hold
    // ctrl_mutex_ for the whole lifetime of the scope, so a blocking handshake
    // has to be wrapped in an UnlockGuard rather than releasing the lock by
    // hand.
    class ConnectScope {
       public:
        ConnectScope(RdmaTwoSidedTransport &transport, std::string peer,
                     std::shared_ptr<CtrlChannel> channel);
        ~ConnectScope();

        ConnectScope(const ConnectScope &) = delete;
        ConnectScope &operator=(const ConnectScope &) = delete;

        void markSuccess() { success_ = true; }

       private:
        RdmaTwoSidedTransport &transport_;
        std::string peer_;
        std::shared_ptr<CtrlChannel> channel_;
        bool success_ = false;
    };

    void startCtrlWorker();
    void stopCtrlWorker();
    void ctrlWorkerLoop();

    std::mutex ctrl_mutex_;
    std::condition_variable ctrl_cv_;
    std::unordered_map<std::string, std::shared_ptr<CtrlChannel>>
        ctrl_channels_;
    // Peers with a connect handshake in flight. Only such peers are worth
    // waiting for: an entry in ctrl_channels_ that is not connected and not
    // listed here is a dead channel, which the next caller reclaims instead of
    // waiting for a wakeup that never comes. Refcounted because an active and
    // a passive connect to the same peer can overlap.
    std::unordered_map<std::string, int> ctrl_connecting_;
    // Set once by stopCtrlWorker() to release waiters during shutdown.
    // Guarded by ctrl_mutex_.
    bool ctrl_stopping_ = false;
    std::thread ctrl_worker_;
    std::atomic<bool> ctrl_worker_running_{false};
    uint64_t local_ctrl_session_id_ = 0;
};

}  // namespace mooncake

#endif  // RDMA_TWOSIDED_TRANSPORT_H_
