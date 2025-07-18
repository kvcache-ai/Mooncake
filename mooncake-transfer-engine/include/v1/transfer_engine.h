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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "v1/common.h"
#include "v1/metadata/metadata.h"
#include "v1/metadata/segment.h"
#include "v1/transport/transport.h"
#include "v1/utility/memory_location.h"

namespace mooncake {
namespace v1 {

class Batch;
class TransferEngine {
   public:
    TransferEngine();

    TransferEngine(std::shared_ptr<ConfigManager> config);

    ~TransferEngine();

    TransferEngine(const TransferEngine &) = delete;

    TransferEngine &operator=(const TransferEngine &) = delete;

   public:
    bool available() const { return available_; }

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

   public:
    Status exportLocalSegment(std::string &shared_handle);

    Status importRemoteSegment(SegmentID &handle,
                               const std::string &shared_handle);

    Status openRemoteSegment(SegmentID &handle,
                             const std::string &segment_name);

    Status closeRemoteSegment(SegmentID handle);

   public:
    Status allocateLocalMemory(void **addr, size_t size,
                               MemoryOptions &options);

    Status freeLocalMemory(void *addr, size_t size);

    Status registerLocalMemory(void *addr, size_t size) {
        MemoryOptions options;
        return registerLocalMemory(addr, size, options);
    }

    Status registerLocalMemory(void *addr, size_t size, MemoryOptions &options);

    Status unregisterLocalMemory(void *addr, size_t size);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request> &request_list);

    Status sendNotify(SegmentID target_id, const NotifyMessage &notify);

    Status getNotifyList(std::vector<NotifyMessage> &notify_list);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status_list);

    std::shared_ptr<SegmentDesc> getSegmentDesc(SegmentID handle);

   private:
    Status construct();

    Status deconstruct();

    Status setupLocalSegment();

    void lazyFreeBatch();

    TransportType getTransportType(const Request &request);

   private:
    struct AllocatedMemory {
        void *addr;
        size_t size;
        Transport *transport;
    };

    struct BatchSet {
        std::unordered_set<Batch *> active;
        std::vector<Batch *> freelist;
    };

   private:
    std::shared_ptr<ConfigManager> conf_;
    std::shared_ptr<MetadataService> metadata_;
    std::shared_ptr<Topology> topology_;
    bool available_;

    std::array<std::unique_ptr<Transport>, kSupportedTransportTypes>
        transport_list_;
    std::unique_ptr<LocalSegmentTracker> local_segment_tracker_;

    ThreadLocalStorage<BatchSet> batch_set_;

    std::vector<AllocatedMemory> allocated_memory_;
    std::mutex mutex_;

    std::string hostname_;
    uint16_t port_;
    bool ipv6_;
    std::string local_segment_name_;
};
}  // namespace v1
}  // namespace mooncake

#endif