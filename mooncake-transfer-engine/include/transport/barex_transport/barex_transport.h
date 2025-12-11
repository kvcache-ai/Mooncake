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

#ifndef BAREX_TRANSPORT_H_
#define BAREX_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <condition_variable>
#include <random>

#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include "transport/barex_transport/barex_context.h"

namespace mooncake {

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;

class TransferMetadata;
class CountDownLatch {
   private:
    int count_;
    std::mutex mtx;
    std::condition_variable cv;

   public:
    CountDownLatch(int count) : count_(count) {};

    void CountDown() {
        std::unique_lock<std::mutex> lk(mtx);
        count_--;
        if (count_ <= 0) {
            cv.notify_all();
        }
    }

    void Wait() {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [this] { return count_ <= 0; });
    }
};
class BarexTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    BarexTransport();

    ~BarexTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "barex"; }

    void setLocalPort(int port) { local_port_ = port; }

    void setPeerPort(int port) { peer_port_ = port; }

    int getLocalPort() { return local_port_; }

    int getPeerPort() { return peer_port_; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int registerLocalMemoryBase(void *addr, size_t length,
                                const std::string &location,
                                bool remote_accessible, bool update_metadata,
                                bool is_gpu);

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    // TRANSFER

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    SegmentID getSegmentID(const std::string &segment_name);

    Status OpenChannel(const std::string &segment_name, SegmentID sid) override;
    Status CheckStatus(SegmentID sid) override;

   private:
    int allocateLocalSegmentID();

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

   public:
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            int &buffer_id, int &device_id, int retry_cnt = 0);

   private:
#ifdef USE_BAREX
    std::vector<std::shared_ptr<BarexContext>> server_context_list_;
    std::vector<std::shared_ptr<BarexContext>> client_context_list_;
    std::shared_ptr<XThreadpool> server_threadpool_;
    std::shared_ptr<XThreadpool> client_threadpool_;
    std::shared_ptr<XSimpleMempool> mempool_;
    std::shared_ptr<XListener> listener_;
    std::shared_ptr<XConnector> connector_;
#endif
    std::shared_ptr<Topology> local_topology_;
    std::mutex buf_mutex_;
    std::map<void *, std::pair<size_t, bool>> buf_length_map_;
    bool use_random_dev_ = false;
    bool barex_use_cpu_ = false;
    int barex_local_device_ = 0;
    int local_port_ = 8089;
    int peer_port_ = 8089;
    std::random_device rd;
};

}  // namespace mooncake

#endif  // BAREX_TRANSPORT_H_