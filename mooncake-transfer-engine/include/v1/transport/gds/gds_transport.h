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
#include <string>
#include <unordered_map>
#include <utility>

#include <cufile.h>

#include "v1/metadata/metadata.h"
#include "v1/transport/transport.h"

namespace mooncake {
namespace v1 {

class GdsFileContext;

struct GdsSubBatch : public Transport::SubBatch {
    size_t max_size;
    CUfileBatchHandle_t handle;
    std::vector<CUfileIOParams_t> io_params;
    std::vector<CUfileIOEvents_t> io_events;
};

class GdsTransport : public Transport {
   public:
    GdsTransport();

    ~GdsTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<MetadataService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<ConfigManager> conf = nullptr);

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

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<MetadataService> metadata_;
    std::shared_ptr<ConfigManager> conf_;

    RWSpinlock file_context_lock_;
    std::unordered_map<SegmentID, std::shared_ptr<GdsFileContext>>
        file_context_map_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // GDS_TRANSPORT_H_