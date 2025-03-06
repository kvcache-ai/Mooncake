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

#include "transport/shm_transport/shm_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
const static size_t kDefaultThreadPoolSize = 4;

ShmTransport::ShmTransport() : thread_pool_(kDefaultThreadPoolSize) {
    // TODO
}

ShmTransport::~ShmTransport() {
    // TODO
}

int ShmTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    // TODO currently support transfer in the same process,
    // so segment registration is not needed
    return 0;
}

int ShmTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                    TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) return ERR_INVALID_ARGUMENT;
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return 0;
}

int ShmTransport::submitTransfer(BatchID batch_id,
                                 const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "ShmTransport: Exceed the limitation of current batch's "
                      "capacity";
        return ERR_TOO_MANY_REQUESTS;
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    for (auto &request : entries) {
        if (request.target_id != LOCAL_SEGMENT_ID) {
            LOG(ERROR) << "ShmTransport: Not local segment";
            return ERR_NOT_LOCAL_SEGMENT;
        }
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)request.target_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_count += 1;
        startTransfer(slice);
    }

    return 0;
}

int ShmTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        if (request.target_id != LOCAL_SEGMENT_ID) {
            LOG(ERROR) << "ShmTransport: Not local segment";
            return ERR_NOT_LOCAL_SEGMENT;
        }
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)request.target_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_count += 1;
        startTransfer(slice);
    }
    return 0;
}

void ShmTransport::startTransfer(Slice *slice) {
    thread_pool_.submit([slice]() {
#ifdef USE_CUDA
        if (slice->opcode == TransferRequest::READ)
            cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                       slice->length, cudaMemcpyDefault);
        else
            cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                       slice->length, cudaMemcpyDefault);
#else
        if (slice->opcode == TransferRequest::READ)
            memcpy(slice->source_addr, (void *)slice->local.dest_addr,
                   slice->length);
        else
            memcpy((void *)slice->local.dest_addr, slice->source_addr,
                   slice->length);
#endif
        slice->markSuccess();
    });
}

int ShmTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    return 0;
}

int ShmTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return 0;
}

int ShmTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    return 0;
}

int ShmTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    return 0;
}
}  // namespace mooncake