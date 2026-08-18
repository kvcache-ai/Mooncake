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

#ifndef MULTI_TRANSPORT_H_
#define MULTI_TRANSPORT_H_

#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "transport/transport.h"

namespace mooncake {
class TransferEngineImplTestPeer;

class MultiTransport {
    friend class TransferEngineImplTestPeer;

   public:
    using BatchID = Transport::BatchID;
    using TransferRequest = Transport::TransferRequest;
    using TransferStatus = Transport::TransferStatus;
    using TransferStatusEnum = Transport::TransferStatusEnum;
    using BatchDesc = Transport::BatchDesc;

    MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                   std::string &local_server_name);

    ~MultiTransport();

    struct ScatterSubmission {
        BatchID batch_id = static_cast<BatchID>(-1);
        std::vector<size_t> task_sizes;
    };

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

    Status submitScatter(const std::vector<TransferRequest> &entries,
                         ScatterSubmission &submission);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_submitTransfer(BatchID batch_id,
                             const std::vector<TransferRequest> &entries,
                             std::string &proto);
#endif

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getScatterRequestStatuses(
        BatchID batch_id, size_t task_id,
        std::vector<TransferStatusEnum> &request_statuses);

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status);

    Transport *installTransport(const std::string &proto,
                                std::shared_ptr<Topology> topo);

    Transport *getTransport(const std::string &proto);

    /**
     * @brief Check if TCP is the only installed transport.
     *
     * When only TCP transport is available (no RDMA, NVLink, etc.),
     * local memcpy is preferred over TCP loopback for same-host transfers.
     */
    bool isTcpOnly() const;

    std::vector<Transport *> listTransports();

    void *getBaseAddr();

   private:
    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries,
                          std::vector<size_t> *task_sizes);

    Status selectTransport(const TransferRequest &entry, Transport *&transport);

    Status tryFreeBatchID(BatchID batch_id);

    void deferredCleanupLoop();

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_selectTransport(const TransferRequest &entry,
                              Transport *&transport,
                              std::string &preferred_proto);
#endif

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::map<std::string, std::shared_ptr<Transport>> transport_map_;
    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;

    std::mutex deferred_cleanup_mutex_;
    std::condition_variable deferred_cleanup_cv_;
    std::unordered_set<BatchID> deferred_cleanup_batches_;
    bool stop_deferred_cleanup_ = false;
    std::thread deferred_cleanup_thread_;
};
}  // namespace mooncake

#endif  // MULTI_TRANSPORT_H_
