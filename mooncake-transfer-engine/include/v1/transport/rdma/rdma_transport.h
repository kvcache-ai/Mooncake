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

#ifndef RDMA_TRANSPORT_V1_H_
#define RDMA_TRANSPORT_V1_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "buffers.h"
#include "context.h"
#include "slice.h"
#include "scheduler.h"
#include "v1/metadata/metadata.h"
#include "v1/transport/transport.h"
#include "v1/utility/topology.h"
#include "v1/utility/cluster_topology.h"

namespace mooncake {
namespace v1 {
class RdmaContext;
class RdmaEndPoint;
class Workers;
class EndpointStore;
class LocalBuffers;

using RdmaContextSet = std::vector<std::shared_ptr<RdmaContext>>;

struct RdmaSubBatch : public Transport::SubBatch {
    std::vector<RdmaTask> task_list;
    std::vector<RdmaSlice *> slice_chain;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class RdmaTransport : public Transport {
    friend class Workers;

   public:
    RdmaTransport();

    ~RdmaTransport();

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

    virtual const char *getName() const { return "rdma"; }

   public:
    int onSetupRdmaConnections(const BootstrapDesc &peer_desc,
                               BootstrapDesc &local_desc);

   public:
    Status setupLocalSegment();

   private:
    bool installed_;
    std::shared_ptr<ConfigManager> conf_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<MetadataService> metadata_;
    LocalBufferManager local_buffer_manager_;
    RdmaContextSet context_set_;
    std::unordered_map<std::string, int> context_name_lookup_;
    std::unique_ptr<Workers> workers_;
    std::shared_ptr<RdmaParams> params_;
    std::unique_ptr<ClusterTopology> cluster_topology_;
    std::unique_ptr<Scheduler> scheduler_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_TRANSPORT_V1_H_