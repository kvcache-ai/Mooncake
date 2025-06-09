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
#include <unordered_map>
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
using TransferRequest = Transport::Request;
using BatchID = Transport::BatchID;
using BufferEntry = Transport::BufferEntry;
class Batch;

enum TransportType { RDMA = 0, SHM, GDS };

const static int kSupportedTransportTypes = 3;

using TEConfig = std::unordered_map<std::string, std::string>;
const std::string TEConfigKeyLocalSegmentName = "local_segment_name";
const std::string TEConfigKeyMetadataConnString = "metadata_conn_string";
const std::string TEConfigKeyBindEthIP = "bind_eth_ip";
const std::string TEConfigKeyBindEthPort = "bind_eth_port";
const std::string TEConfigKeyTopology = "topology";
const std::string TEConfigKeyRdmaDeviceList = "rdma_device_list";

class TransferEngine {
   public:
    TransferEngine();

    TransferEngine(TEConfig &conf);

    ~TransferEngine();

    TransferEngine(const TransferEngine &) = delete;

    TransferEngine &operator=(const TransferEngine &) = delete;

   public:
    bool available() const { return available_; }

    const std::string getEthIP() const;

    uint16_t getEthPort() const;

   public:
    Status exportLocalSegment(std::string &shared_handle);

    Status importRemoteSegment(SegmentID &handle,
                               const std::string &shared_handle);

    Status openRemoteSegment(SegmentID &handle,
                             const std::string &segment_name);

    Status closeRemoteSegment(SegmentID handle);

   public:
    Status registerLocalMemory(BufferEntry &buffer);

    Status unregisterLocalMemory(BufferEntry &buffer);

    Status registerLocalMemoryBatch(
        const std::vector<BufferEntry> &buffer_list);

    Status unregisterLocalMemoryBatch(
        const std::vector<BufferEntry> &buffer_list);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &request_list);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

   private:
    Status construct();

    Status deconstruct();

    Status registerRdmaTransport();

    void lazyFreeBatch();

    TransportType getTransportType(const TransferRequest &request);

   private:
    TEConfig conf_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::shared_ptr<Topology> local_topology_;
    bool available_;

    std::vector<std::shared_ptr<Transport>> transport_list_;
    std::unordered_set<Batch *> batch_set_;
    std::vector<Batch *> deferred_free_batch_set_;
    std::mutex mutex_;
};
}  // namespace v1
}  // namespace mooncake

#endif