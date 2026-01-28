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

#include <unordered_map>

#include "transport/transport.h"

namespace mooncake {
class MultiTransport {
   public:
    using BatchID = Transport::BatchID;
    using TransferRequest = Transport::TransferRequest;
    using TransferStatus = Transport::TransferStatus;
    using BatchDesc = Transport::BatchDesc;

    MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                   std::string &local_server_name);

    ~MultiTransport();

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status);

    Transport *installTransport(const std::string &proto,
                                std::shared_ptr<Topology> topo);

    Transport *getTransport(const std::string &proto);

    std::vector<Transport *> listTransports();

    void *getBaseAddr();

   private:
    Status selectTransport(const TransferRequest &entry, Transport *&transport);

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::map<std::string, std::shared_ptr<Transport>> transport_map_;
    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;
};
}  // namespace mooncake

#endif  // MULTI_TRANSPORT_H_