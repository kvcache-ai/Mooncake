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
#include "metadata/metadata.h"
#include "transport_v1/transport.h"
#include "utility/topology.h"

namespace mooncake {
namespace v1 {
class RdmaContext;
class RdmaEndPoint;
class Workers;
class EndpointStore;
class LocalBuffers;

using DeviceID = std::string;
using RdmaContextSet =
    std::unordered_map<DeviceID, std::shared_ptr<RdmaContext>>;

struct RdmaResources {
    std::string local_segment_name;
    std::shared_ptr<Topology> local_topology;
    std::shared_ptr<mooncake::TransferMetadata> metadata_manager;
    LocalBufferSet local_buffer_set;
    RdmaContextSet context_set;
};

struct RdmaSlice;

struct RdmaTask {
    RdmaTask() = default;

    ~RdmaTask();

    Transport::Request request;
    Transport::TransferStatus status;

    bool submitted = false;
    RdmaSlice *slices = nullptr;
    int num_slices = 0;
    volatile int finish_slices = 0;  // including success or failed
};

struct RdmaSlice {
    void *source_addr = nullptr;
    uint64_t target_addr = 0;
    size_t length = 0;

    int retry_count = 0;
    RdmaTask *task = nullptr;
    volatile int *quota_counter = nullptr;

    void markSuccess() {
        assert(task);
        __sync_fetch_and_add(&task->status.transferred_bytes, length);
        auto finish_slices = __sync_fetch_and_add(&task->finish_slices, 1);
        if (finish_slices + 1 == task->num_slices) {
            task->status.s = Transport::COMPLETED;
        }
    }

    void markFailed(const std::string &reason) {
        assert(task);
        __sync_fetch_and_add(&task->finish_slices, 1);
        task->status.s = Transport::FAILED;
    }
};

struct RdmaSubBatch : public Transport::SubBatch {
    RdmaSubBatch(size_t max_size) : max_size(max_size) {
        task_list.reserve(max_size);
    }

    std::vector<RdmaTask> task_list;
    const size_t max_size;
};

class RdmaTransport : public Transport {
   public:
    RdmaTransport();

    ~RdmaTransport();

    virtual Status install(
        std::string &local_segment_name,
        std::shared_ptr<mooncake::TransferMetadata> metadata_manager,
        std::shared_ptr<Topology> local_topology);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef &batch, const std::vector<Request> &request_list);

    virtual TransferStatus getTransferStatus(SubBatchRef &batch,
                                             int request_index);

    virtual Status registerLocalMemory(
        const std::vector<BufferEntry> &buffer_list);

    virtual Status unregisterLocalMemory(const std::vector<void *> &addr_list);

    virtual const char *getName() const { return "rdma"; }

   public:
    int startHandshakeDaemon();

    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc);

    int onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                               HandShakeDesc &local_desc);

   public:
    void allocateLocalSegmentID();

    int registerSingleLocalMemory(const BufferEntry &buffer, bool update_meta);

    int unregisterSingleLocalMemory(void *addr, bool update_meta);

   private:
    bool installed_;
    std::shared_ptr<RdmaResources> resources_;
    std::shared_ptr<Workers> workers_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_TRANSPORT_V1_H_