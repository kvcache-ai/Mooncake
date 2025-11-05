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

#ifndef TRANSFER_ENGINE_V1_H_
#define TRANSFER_ENGINE_V1_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "v1/common/status.h"
#include "v1/common/types.h"

namespace mooncake {
namespace v1 {
class TransferEngineImpl;
class ConfigManager;
class TransferEngine {
   public:
    TransferEngine();

    TransferEngine(const std::string config_path);

    TransferEngine(std::shared_ptr<ConfigManager> config);

    ~TransferEngine();

    TransferEngine(const TransferEngine &) = delete;

    TransferEngine &operator=(const TransferEngine &) = delete;

   public:
    bool available() const;

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

   public:
    Status exportLocalSegment(std::string &shared_handle);

    Status importRemoteSegment(SegmentID &handle,
                               const std::string &shared_handle);

    Status openSegment(SegmentID &handle, const std::string &segment_name);

    Status closeSegment(SegmentID handle);

    Status getSegmentInfo(SegmentID handle, SegmentInfo &info);

   public:
    Status allocateLocalMemory(void **addr, size_t size,
                               Location location = kWildcardLocation);

    Status freeLocalMemory(void *addr);

    Status registerLocalMemory(void *addr, size_t size,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(void *addr, size_t size = 0);

    // advanced buffer allocate function
    Status allocateLocalMemory(void **addr, size_t size,
                               MemoryOptions &options);

    // advanced buffer register function
    Status registerLocalMemory(void *addr, size_t size, MemoryOptions &options);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request> &request_list);

    Status sendNotification(SegmentID target_id, const Notification &notifi);

    Status receiveNotification(std::vector<Notification> &notifi_list);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status_list);

    Status getTransferStatus(BatchID batch_id, TransferStatus &overall_status);

   private:
    std::unique_ptr<TransferEngineImpl> impl_;
};
}  // namespace v1
}  // namespace mooncake

#endif