// Copyright 2025 KVCache.AI
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

#ifndef TENT_BUFIO_TRANSPORT_H
#define TENT_BUFIO_TRANSPORT_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

class BufIoFileContext;

struct BufIoTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;

    BufIoTask()
        : status_word(TransferStatusEnum::PENDING), transferred_bytes(0) {}
};

struct BufIoSubBatch : public Transport::SubBatch {
    size_t max_size = 0;
    std::vector<BufIoTask> task_list;

    size_t size() const override { return task_list.size(); }
};

class BufIoTransport : public Transport {
   public:
    BufIoTransport();
    ~BufIoTransport() override;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "buf-io"; }

   private:
    std::string getBufIoFilePath(SegmentID handle);
    BufIoFileContext* findFileContext(SegmentID handle);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;

    RWSpinlock file_context_lock_;
    using FileContextMap =
        std::unordered_map<SegmentID, std::shared_ptr<BufIoFileContext>>;
    FileContextMap file_context_map_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_BUFIO_TRANSPORT_H
