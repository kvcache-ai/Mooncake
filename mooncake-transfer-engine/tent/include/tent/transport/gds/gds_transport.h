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

#ifndef GDS_TRANSPORT_H_
#define GDS_TRANSPORT_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cufile.h>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

class GdsFileContext;

struct IOParamRange {
    size_t base;
    size_t count;
};

// Wrapper for reusable CUfileBatchHandle_t
// cuFileBatchIOSetUp is expensive, so we reuse handles
struct BatchHandle {
    CUfileBatchHandle_t handle;
    int max_nr;  // max number of batch entries
};

struct GdsSubBatch : public Transport::SubBatch {
    size_t max_size;
    BatchHandle* batch_handle;  // Pointer to reusable handle from pool
    std::vector<IOParamRange> io_param_ranges;
    std::vector<CUfileIOParams_t> io_params;
    std::vector<CUfileIOEvents_t> io_events;
    virtual size_t size() const { return io_param_ranges.size(); }
};

class GdsTransport : public Transport {
   public:
    GdsTransport();

    ~GdsTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "gds"; }

   private:
    std::string getGdsFilePath(SegmentID handle);

    GdsFileContext *findFileContext(SegmentID target_id);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;

    RWSpinlock file_context_lock_;
    using FileContextMap =
        std::unordered_map<SegmentID, std::shared_ptr<GdsFileContext>>;
    FileContextMap file_context_map_;
    size_t io_batch_depth_;

    // Object pool for BatchHandle to avoid frequent cuFileBatchIOSetUp/Destroy
    // CUfileBatchHandle_t is reusable per cuFile API documentation
    std::vector<BatchHandle*> handle_pool_;
    std::mutex handle_pool_lock_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // GDS_TRANSPORT_H_