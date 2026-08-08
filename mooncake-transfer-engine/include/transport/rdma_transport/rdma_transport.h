// Copyright 2024 KVCache.AI
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

#ifndef RDMA_TRANSPORT_H_
#define RDMA_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/ctrl_frame.h"
#include "transport/rdma_transport/msg_header.h"
#include "transport/rdma_transport/sender_credit.h"
#include "transport/transport.h"

namespace mooncake {

class RdmaContext;
class RdmaEndPoint;
class CtrlChannel;
class MsgChannel;
class TransferMetadata;
class RdmaTransportTestPeer;
class WorkerPool;

class RdmaTransport : public Transport {
    friend class RdmaContext;
    friend class RdmaEndPoint;
    friend class RdmaTransportTestPeer;
    friend class WorkerPool;
    friend class MsgChannel;
    friend class CtrlChannel;

   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;
    using NotifyDesc = TransferMetadata::NotifyDesc;

   public:
    RdmaTransport();

    ~RdmaTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "rdma"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    // TE-managed buffer for the default two-sided path.
    void *allocateManagedBuffer(size_t length);
    int releaseManagedBuffer(void *addr);

   private:
    int registerLocalMemoryInternal(void *addr, size_t length,
                                    const std::string &location,
                                    bool remote_accessible,
                                    bool update_metadata,
                                    bool force_sequential,
                                    bool two_sided = false);

    int unregisterLocalMemoryInternal(void *addr, bool update_metadata,
                                      bool force_sequential);

   public:
    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    SegmentID getSegmentID(const std::string &segment_name);

    int sendRdmaNotify(const std::string &peer_server_name,
                       const NotifyDesc &notify);

    void onCtrlNotifyReceived(const NotifyDesc &notify);
    void onCtrlFrameReceived(const std::string &peer_server_name,
                             const CtrlFrame &frame);
    void onMsgReceived(const std::string &peer_server_name, const MsgHeader &hdr,
                       const void *payload);

    uint64_t localCtrlSessionId() const { return local_ctrl_session_id_; }

    SenderCreditLedger &senderCreditLedger() { return sender_credit_; }

    uint64_t peerGrantedBounceSlots(const std::string &peer_server_name);

    int sendInitialCreditGrant(const std::string &peer_server_name);

   private:
    int allocateLocalSegmentID();

    int refreshLocalDeviceDesc(const std::string &device_name, uint16_t lid,
                               const std::string &gid);

    int preTouchMemory(void *addr, size_t length);

   public:
    int onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                               HandShakeDesc &local_desc);

    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc) {
        return metadata_->sendHandshake(peer_server_name, local_desc,
                                        peer_desc);
    }

   private:
    int initializeRdmaResources();

    int startHandshakeDaemon(std::string &local_server_name);

    int onSetupCtrlChannel(const HandShakeDesc &peer_desc,
                           HandShakeDesc &local_desc);
    int onSetupMsgChannel(const HandShakeDesc &peer_desc,
                          HandShakeDesc &local_desc);

    std::shared_ptr<CtrlChannel> ensureCtrlChannel(
        const std::string &peer_server_name);
    std::shared_ptr<MsgChannel> ensureMsgChannel(
        const std::string &peer_server_name);

    void startCtrlWorker();
    void stopCtrlWorker();
    void ctrlWorkerLoop();

    bool shouldUseTwoSided(const TransferRequest &req);
    bool isLocalManaged(uint64_t addr, size_t length) const;
    bool isRemoteTwoSided(SegmentID target_id, uint64_t offset,
                          size_t length) const;
    Status submitTwoSidedTasks(const std::vector<TransferTask *> &tasks);
    Status submitOneSidedTasks(const std::vector<TransferTask *> &tasks);
    int dispatchTwoSidedTask(TransferTask *task);
    void redispatchWaitingTasks();
    void completeTwoSidedAck(uint64_t task_id, uint64_t acked_bytes);
    int sendDataAck(const std::string &peer, uint64_t task_id,
                    uint64_t acked_bytes);
    bool validateLocalManagedDest(uint64_t dest_addr, uint32_t length) const;

   public:
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            int &buffer_id, int &device_id, int retry_cnt = 0);
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            std::string_view hint, int &buffer_id,
                            int &device_id, int retry_cnt = 0);
    static int selectDeviceByLocalHca(SegmentDesc *desc, uint64_t offset,
                                      size_t length, std::string_view local_hca,
                                      int &buffer_id, int &device_id,
                                      int retry_cnt = 0);

    const std::vector<std::shared_ptr<RdmaContext>> &getContextList() const {
        return context_list_;
    }

   private:
    std::vector<std::shared_ptr<RdmaContext>> context_list_;
    std::shared_ptr<Topology> local_topology_;
    std::string rdma_server_name_;
    std::mutex local_desc_lock_;
    // Mooncake#2017: buffers larger than the device max_mr_size are split into
    // multiple sub-max_mr_size MRs (one BufferDesc per chunk) so that
    // ibv_reg_mr is never silently truncated. unregisterLocalMemory() only
    // receives the base addr, so remember each base buffer's chunk
    // start-addresses for cleanup.
    std::mutex chunk_map_mutex_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> chunk_map_;

    struct PeerCtrlState {
        uint64_t peer_session = 0;
        uint64_t epoch = 1;
        uint64_t next_grant_seq = 1;
        uint32_t peer_bounce_slots = 0;
        uint32_t peer_bounce_slot_size = 0;
        bool session_open_received = false;
        bool initial_grant_sent = false;
        bool grant_pending = false;
    };

    struct ManagedBuffer {
        void *addr = nullptr;
        size_t length = 0;
        bool owned = false;  // allocated by TE
    };

    struct TwoSidedTaskState {
        TransferTask *task = nullptr;
        uint64_t task_id = 0;
        uint64_t total_bytes = 0;
        uint64_t acked_bytes = 0;
        std::string peer;
        uint64_t peer_session = 0;
        bool waiting_credit = false;
        size_t slices_posted = 0;
    };

    int sendCreditGrant(const std::string &peer_server_name, uint64_t epoch);
    void handleSessionOpen(const std::string &peer_server_name,
                           const CtrlFrame &frame);
    void handleCreditGrant(const std::string &peer_server_name,
                           const CtrlFrame &frame);
    void handleDataAck(const std::string &peer_server_name,
                       const CtrlFrame &frame);

    std::mutex ctrl_mutex_;
    std::unordered_map<std::string, std::shared_ptr<CtrlChannel>>
        ctrl_channels_;
    std::unordered_map<std::string, std::shared_ptr<MsgChannel>> msg_channels_;
    std::unordered_map<std::string, PeerCtrlState> peer_ctrl_state_;
    std::thread ctrl_worker_;
    std::atomic<bool> ctrl_worker_running_{false};
    uint64_t local_ctrl_session_id_ = 0;
    SenderCreditLedger sender_credit_;

    mutable std::mutex managed_mutex_;
    std::unordered_map<uint64_t, ManagedBuffer> managed_buffers_;  // addr ->

    std::mutex twosided_mutex_;
    std::unordered_map<uint64_t, TwoSidedTaskState> twosided_tasks_;
    std::deque<TransferTask *> waiting_tasks_;
    std::atomic<uint64_t> next_task_id_{1};
};

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;

}  // namespace mooncake

#endif  // RDMA_TRANSPORT_H_
