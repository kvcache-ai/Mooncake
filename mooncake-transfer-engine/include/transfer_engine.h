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

#ifndef MULTI_TRANSFER_ENGINE_H_
#define MULTI_TRANSFER_ENGINE_H_

#include <asm-generic/errno-base.h>
#include <bits/stdint-uintn.h>
#include <limits.h>
#include <string.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentHandle = Transport::SegmentHandle;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;
using BufferEntry = Transport::BufferEntry;

class TransferEngine {
   public:
    TransferEngine() : metadata_(nullptr) {}

    ~TransferEngine() { freeEngine(); }

    int init(const std::string &metadata_conn_string,
             const std::string &local_server_name,
             const std::string &ip_or_host_name, 
             uint64_t rpc_port = 12345);

    int freeEngine();

    Transport *installTransport(const std::string &proto, void **args);

    int uninstallTransport(const std::string &proto);

    SegmentHandle openSegment(const std::string &segment_name);

    int closeSegment(SegmentHandle handle);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location,
                            bool remote_accessible = true,
                            bool update_metadata = true);

    int unregisterLocalMemory(void *addr, bool update_metadata = true);

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location);

    int unregisterLocalMemoryBatch(const std::vector<void *> &addr_list);

    BatchID allocateBatchID(size_t batch_size) {
        return multi_transports_->allocateBatchID(batch_size);
    }

    int freeBatchID(BatchID batch_id) {
        return multi_transports_->freeBatchID(batch_id);
    }

    int submitTransfer(BatchID batch_id,
                       const std::vector<TransferRequest> &entries) {
        return multi_transports_->submitTransfer(batch_id, entries);
    }

    int getTransferStatus(BatchID batch_id, size_t task_id,
                          TransferStatus &status) {
        return multi_transports_->getTransferStatus(batch_id, task_id, status);
    }

    int syncSegmentCache() { return metadata_->syncSegmentCache(); }

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

    bool checkOverlap(void *addr, uint64_t length);

   private:
    struct MemoryRegion {
        void *addr;
        uint64_t length;
        std::string location;
        bool remote_accessible;
    };

    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::shared_ptr<MultiTransport> multi_transports_;
    std::vector<MemoryRegion> local_memory_regions_;
};
}  // namespace mooncake

#endif