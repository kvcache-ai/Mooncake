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

#include "cufile_context.h"
#include "cufile_desc_pool.h"
#include "v1/metadata/metadata.h"
#include "v1/transport/transport.h"

namespace mooncake {
namespace v1 {

struct GdsTask {
    Request request;
};

struct GdsSubBatch : public Transport::SubBatch {
    std::vector<GdsTask> task_list;
    size_t max_size;
    int desc_idx;
    std::vector<std::tuple<size_t, uint64_t>> task_to_slices;
};

class GdsTransport : public Transport {
   public:
    GdsTransport();

    ~GdsTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<TransferMetadata> metadata_manager,
                           std::shared_ptr<Topology> local_topology);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual void queryOutstandingTasks(SubBatchRef batch,
                                       std::vector<int> &task_id_list);

    virtual Status registerLocalMemory(
        const std::vector<BufferEntry> &buffer_list);

    virtual Status unregisterLocalMemory(
        const std::vector<BufferEntry> &buffer_list);

    virtual const char *getName() const { return "gds"; }

   private:
    void allocateLocalSegmentID();

    void addSliceToCUFileBatch(void *source_addr, uint64_t file_offset,
                               uint64_t slice_len, uint64_t desc_id,
                               Request::OpCode op, CUfileHandle_t fh);

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<TransferMetadata> metadata_manager_;

    std::unordered_map<BatchID, int> batch_to_cufile_desc_;
    std::unordered_map<std::pair<SegmentHandle, uint64_t>,
                       std::shared_ptr<CuFileContext>, pair_hash>
        segment_to_context_;
    std::vector<std::thread> workers_;

    std::shared_ptr<CUFileDescPool> desc_pool_;
    RWSpinlock context_lock_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // GDS_TRANSPORT_H_