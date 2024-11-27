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

#ifndef CXL_TRANSPORT_H_
#define CXL_TRANSPORT_H_

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

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferMetadata;

class CxlTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    CxlTransport();

    ~CxlTransport();

    BatchID allocateBatchID(size_t batch_size) override;

    int submitTransfer(BatchID batch_id,
                       const std::vector<TransferRequest> &entries) override;

    int getTransferStatus(BatchID batch_id, size_t task_id,
                          TransferStatus &status) override;

    int freeBatchID(BatchID batch_id) override;

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta, void **args) override;

    int registerLocalMemory(void *addr, size_t length, const string &location,
                            bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr,
                              bool update_metadata = false) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location) override {
        return 0;
    }

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override {
        return 0;
    }

    const char *getName() const override { return "cxl"; }
};
}  // namespace mooncake

#endif