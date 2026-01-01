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

#include "memory_location.h"
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferEngineImpl;
namespace tent {
class TransferEngine;
};
using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentHandle = Transport::SegmentHandle;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;
const static BatchID INVALID_BATCH_ID = UINT64_MAX;
using BufferEntry = Transport::BufferEntry;

class TransferEngine {
   public:
    TransferEngine(bool auto_discover = false);

    TransferEngine(bool auto_discover, const std::vector<std::string>& filter);

    ~TransferEngine();

    int init(const std::string& metadata_conn_string,
             const std::string& local_server_name,
             const std::string& ip_or_host_name = "",
             uint64_t rpc_port = 12345);

    int freeEngine();

    Transport* installTransport(const std::string& proto, void** args);

    int uninstallTransport(const std::string& proto);

    std::string getLocalIpAndPort();

    int getRpcPort();

    SegmentHandle openSegment(const std::string& segment_name);

    Status CheckSegmentStatus(SegmentID sid);

    int closeSegment(SegmentHandle handle);

    int removeLocalSegment(const std::string& segment_name);

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location = kWildcardLocation,
                            bool remote_accessible = true,
                            bool update_metadata = true);

    int unregisterLocalMemory(void* addr, bool update_metadata = true);

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location);

    int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list);

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries);

    Status submitTransferWithNotify(BatchID batch_id,
                                    const std::vector<TransferRequest>& entries,
                                    TransferMetadata::NotifyDesc notify_msg);

    int getNotifies(std::vector<TransferMetadata::NotifyDesc>& notifies);

    int sendNotifyByID(SegmentID target_id,
                       TransferMetadata::NotifyDesc notify_msg);

    int sendNotifyByName(std::string remote_agent,
                         TransferMetadata::NotifyDesc notify_msg);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status);

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus& status);

    Transport* getTransport(const std::string& proto);

    int syncSegmentCache(const std::string& segment_name = "");

    std::shared_ptr<TransferMetadata> getMetadata();

    bool checkOverlap(void* addr, uint64_t length);

    void setAutoDiscover(bool auto_discover);

    void setWhitelistFilters(std::vector<std::string>&& filters);

    int numContexts() const;

    std::shared_ptr<Topology> getLocalTopology();

   private:
    std::shared_ptr<TransferEngineImpl> impl_;
    std::shared_ptr<mooncake::tent::TransferEngine> impl_tent_;
    bool use_tent_{false};
};
}  // namespace mooncake

#endif
