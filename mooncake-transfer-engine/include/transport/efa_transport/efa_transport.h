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

#ifndef EFA_TRANSPORT_H_
#define EFA_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
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
#include "transport/transport.h"

namespace mooncake {

class EfaContext;
class EfaEndPoint;
class TransferMetadata;

class EfaTransport : public Transport {
    friend class EfaContext;
    friend class EfaEndPoint;

   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    EfaTransport();

    ~EfaTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "efa"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

   private:
    // Internal version with force_sequential option to avoid nested parallelism
    int registerLocalMemoryInternal(void *addr, size_t length,
                                    const std::string &location,
                                    bool remote_accessible,
                                    bool update_metadata,
                                    bool force_sequential);

    int unregisterLocalMemoryInternal(void *addr, bool update_metadata,
                                      bool force_sequential);

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

   private:
    int allocateLocalSegmentID();

    int preTouchMemory(void *addr, size_t length);

   public:
    int onSetupEfaConnections(const HandShakeDesc &peer_desc,
                              HandShakeDesc &local_desc);

    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc) {
        return metadata_->sendHandshake(peer_server_name, local_desc,
                                        peer_desc);
    }

    const std::string &local_server_name() const { return local_server_name_; }

    std::shared_ptr<TransferMetadata> meta() { return metadata_; }

   private:
    int initializeEfaResources();

    int startHandshakeDaemon(std::string &local_server_name);

   public:
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            int &buffer_id, int &device_id, int retry_cnt = 0);
    static int selectDevice(SegmentDesc *desc, uint64_t offset, size_t length,
                            std::string_view hint, int &buffer_id,
                            int &device_id, int retry_cnt = 0);

   private:
    // Start/stop CQ polling worker threads
    void startWorkerThreads();
    void stopWorkerThreads();
    void workerThreadFunc(int thread_id);

   private:
    std::vector<std::shared_ptr<EfaContext>> context_list_;
    std::shared_ptr<Topology> local_topology_;

    // CQ polling worker threads
    std::atomic<bool> worker_running_{false};
    std::vector<std::thread> worker_threads_;
};

}  // namespace mooncake

#endif  // EFA_TRANSPORT_H_
