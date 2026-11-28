// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_NDS_TRANSPORT_H
#define TENT_NDS_TRANSPORT_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "tent/runtime/transport.h"

namespace mooncake::tent {

class NdsFileContext;
class NdsSubBatch;

// The NDS facade owns process-wide driver state. All instances in this library
// share its lifetime and memory registrations. Application-owned NDS sessions
// must be closed before installing this transport.
class NdsTransport : public Transport {
   public:
    NdsTransport();

    ~NdsTransport() override;

    NdsTransport(const NdsTransport&) = delete;

    NdsTransport& operator=(const NdsTransport&) = delete;

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override;

    Status uninstall() override;

    Status quiesce() override;

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override;

    Status freeSubBatch(SubBatchRef& batch) override;

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override;

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override;

    Status removeMemoryBuffer(BufferDesc& desc) override;

    const char* getName() const override { return "nds"; }

   private:
    // <segment_id, <path, length>>
    using SegmentCache = std::map<SegmentID, std::pair<std::string, uint64_t>>;

    Status getNdsFileInfo(SegmentID target_id, SegmentCache& validated,
                          std::string& path, uint64_t& length);

    Status validateRequest(const Request& request, uint64_t& base,
                           std::string& path, SegmentCache& validated);

    Status openFileContext(const std::string& path, int access,
                           std::shared_ptr<NdsFileContext>& file_ctx_ptr);

    Status acquireBatchContext(NdsSubBatch& batch);

    void appendTask(NdsSubBatch& batch, size_t task_id,
                    const std::shared_ptr<NdsFileContext>& file_ctx_ptr,
                    size_t first_io);

    static void abortSubBatch(NdsSubBatch& batch);

    void submitSubBatch(NdsSubBatch& batch);

    void updateBatchStatus(NdsSubBatch& batch);

    void abortAllSubBatches();

    void abortSubBatchesForBuffer(uint64_t base);

    Status unregisterBuffer(uint64_t addr);

    bool installed_ = false;
    size_t depth_;
    size_t io_bytes_;
    std::shared_ptr<ControlService> metadata_;
    std::map<uint64_t, size_t> buffers_;
    std::set<NdsSubBatch*> batches_;
    std::map<std::pair<std::string, int>, std::shared_ptr<NdsFileContext>>
        files_;
    std::vector<void*> idle_contexts_;
};

}  // namespace mooncake::tent
#endif  // TENT_NDS_TRANSPORT_H
