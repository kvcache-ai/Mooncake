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
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "metadata/metadata.h"
#include "transport/transport.h"
#include "transport_v1/transport.h"
#include "utility/memory_location.h"

namespace mooncake {
namespace v1 {
using TransferMetadata = mooncake::TransferMetadata;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentHandle = SegmentID;
using TransferRequest = Transport::Request;
using BatchID = Transport::BatchID;
using BufferEntry = Transport::BufferEntry;
class Batch;

class TransferEngine {
   public:
    TransferEngine(bool auto_discover = false)
        : metadata_(nullptr), local_topology_(std::make_shared<Topology>()) {}

    TransferEngine(bool auto_discover, const std::vector<std::string> &filter)
        : metadata_(nullptr),
          local_topology_(std::make_shared<Topology>()),
          filter_(filter) {}

    ~TransferEngine() { freeEngine(); }

    int init(const std::string &metadata_conn_string,
             const std::string &local_server_name,
             const std::string &ip_or_host_name, uint64_t port = 12345) {
        return init(metadata_conn_string, local_server_name);
    }

    int init(const std::string &metadata_conn_string,
             const std::string &local_server_name);

    int freeEngine();

    std::string getLocalIpAndPort();

    int getRpcPort();

    SegmentHandle openSegment(const std::string &segment_name);

    int closeSegment(SegmentHandle handle);

    int registerLocalMemory(BufferEntry &buffer);

    int unregisterLocalMemory(BufferEntry &buffer);

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list);

    int unregisterLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list);

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    int syncSegmentCache(const std::string &segment_name = "") {
        return metadata_->syncSegmentCache(segment_name);
    }

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

   private:
    void lazyFreeBatch();

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::shared_ptr<Transport> transport_;
    std::shared_ptr<Topology> local_topology_;
    std::vector<std::string> filter_;
    std::unordered_set<Batch *> batch_set_;
    std::vector<Batch *> deferred_free_batch_set_;
    std::mutex mutex_;
};
}  // namespace v1
}  // namespace mooncake

#endif