// Copyright 2026 KVCache.AI
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

#ifndef TENT_TPU_TRANSPORT_H_
#define TENT_TPU_TRANSPORT_H_

#include <string>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct TpuTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
};

struct TpuSubBatch : public Transport::SubBatch {
    std::vector<TpuTask> task_list;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

// TpuTransport is the local staging executor for TPU: it performs the
// HBM<->host-DRAM hop of a staged transfer by delegating to
// Platform::copy() (which routes to the PJRT adapter for TPU memory). It never
// touches the network — the host<->host hop is carried by RDMA/TCP, and
// ProxyManager chains the two stages (see findStagingPolicy).
//
// Because TPU HBM is not NIC-addressable, this transport advertises only the
// device<->host capabilities (gpu_to_dram / dram_to_gpu) and leaves gpu_to_gpu
// false so the engine always stages cross-node traffic through host DRAM. It is
// therefore only ever selected for LOCAL_SEGMENT_ID copies.
class TpuTransport : public Transport {
   public:
    TpuTransport();

    ~TpuTransport();

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

    virtual const char *getName() const { return "tpu"; }

   private:
    void startTransfer(TpuTask *task, TpuSubBatch *batch);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TPU_TRANSPORT_H_
