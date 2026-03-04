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

#include "transport/nvmeof_transport/nvmeof_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <tuple>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/nvmeof_transport/cufile_context.h"
#include "transport/nvmeof_transport/cufile_desc_pool.h"
#include "transport/transport.h"

namespace mooncake {
NVMeoFTransport::NVMeoFTransport() {
    CUFILE_CHECK(cuFileDriverOpen());
    desc_pool_ = std::make_shared<CUFileDescPool>();
}

NVMeoFTransport::~NVMeoFTransport() {}

Transport::TransferStatusEnum from_cufile_transfer_status(
    CUfileStatus_t status) {
    switch (status) {
        case CUFILE_WAITING:
            return Transport::WAITING;
        case CUFILE_PENDING:
            return Transport::PENDING;
        case CUFILE_INVALID:
            return Transport::INVALID;
        case CUFILE_CANCELED:
            return Transport::CANCELED;
        case CUFILE_COMPLETE:
            return Transport::COMPLETED;
        case CUFILE_TIMEOUT:
            return Transport::TIMEOUT;
        case CUFILE_FAILED:
            return Transport::FAILED;
        default:
            return Transport::FAILED;
    }
}

NVMeoFTransport::BatchID NVMeoFTransport::allocateBatchID(size_t batch_size) {
    auto nvmeof_desc = new NVMeoFBatchDesc();
    auto batch_id = Transport::allocateBatchID(batch_size);
    auto &batch_desc = *((BatchDesc *)(batch_id));
    nvmeof_desc->desc_idx_ = desc_pool_->allocCUfileDesc(batch_size);
    nvmeof_desc->transfer_status.reserve(batch_size);
    nvmeof_desc->task_to_slices.reserve(batch_size);
    batch_desc.context = nvmeof_desc;
    return batch_id;
}

Status NVMeoFTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    auto &task = batch_desc.task_list[task_id];
    auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));
    // LOG(DEBUG) << "get t n " << nr;
    // 1. get task -> id map
    TransferStatus transfer_status = {.s = Transport::PENDING,
                                      .transferred_bytes = 0};
    auto [slice_id, slice_num] = nvmeof_desc.task_to_slices[task_id];
    for (size_t i = slice_id; i < slice_id + slice_num; ++i) {
        // LOG(INFO) << "task " << task_id << " i " << i << " upper bound " <<
        // slice_num;
        auto event =
            desc_pool_->getTransferStatus(nvmeof_desc.desc_idx_, slice_id);
        transfer_status.s = from_cufile_transfer_status(event.status);
        // TODO(FIXME): what to do if multi slices have different status?
        if (transfer_status.s == COMPLETED) {
            transfer_status.transferred_bytes += event.ret;
        } else {
            break;
        }
    }
    if (transfer_status.s == COMPLETED) {
        task.is_finished = true;
    }
    status = transfer_status;
    return Status::OK();
}

Status NVMeoFTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    (void)task_list;
    return Status::NotImplemented(
        "NVMeoFTransport::submitTransferTask is not implemented");
}

Status NVMeoFTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));

    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NVMeoFTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NVMeoFTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    size_t slice_id = desc_pool_->getSliceNum(nvmeof_desc.desc_idx_);
    batch_desc.task_list.resize(task_id + entries.size());
    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        segment_desc_map;
    // segment_desc_map[LOCAL_SEGMENT_ID] =
    // getSegmentDescByID(LOCAL_SEGMENT_ID);
    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        auto target_id = request.target_id;

        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                metadata_->getSegmentDescByID(target_id);
            assert(segment_desc_map[target_id] != nullptr);
        }

        auto &desc = segment_desc_map.at(target_id);
        // LOG(INFO) << "desc " << desc->name << " " << desc->protocol;
        assert(desc->protocol == "nvmeof");
        // TODO: solving iterator invalidation due to vector resize
        // Handle File Offset
        uint32_t buffer_id = 0;
        uint64_t segment_start = request.target_offset;
        uint64_t segment_end = request.target_offset + request.length;
        uint64_t current_offset = 0;
        for (auto &buffer_desc : desc->nvmeof_buffers) {
            bool is_overlap = overlap(
                (void *)segment_start, request.length, (void *)current_offset,
                buffer_desc
                    .length);  // this buffer intersects with user's target
            if (is_overlap) {
                // 1. get_slice_start
                uint64_t slice_start = std::max(segment_start, current_offset);
                // 2. slice_end
                uint64_t slice_end =
                    std::min(segment_end, current_offset + buffer_desc.length);
                // 3. init slice and put into TransferTask
                const char *file_path =
                    buffer_desc.local_path_map[local_server_name_].c_str();
                void *source_addr =
                    (char *)request.source + slice_start - segment_start;
                uint64_t file_offset = slice_start - current_offset;
                uint64_t slice_len = slice_end - slice_start;
                addSliceToTask(source_addr, slice_len, file_offset,
                               request.opcode, task, file_path);
                // 4. get cufile handle
                auto buf_key = std::make_pair(target_id, buffer_id);
                CUfileHandle_t fh;
                {
                    // TODO: upgrade
                    RWSpinlock::WriteGuard guard(context_lock_);
                    if (!segment_to_context_.count(buf_key)) {
                        segment_to_context_[buf_key] =
                            std::make_shared<CuFileContext>(file_path);
                    }
                    fh = segment_to_context_.at(buf_key)->getHandle();
                }
                // 5. add cufile request
                addSliceToCUFileBatch(source_addr, file_offset, slice_len,
                                      nvmeof_desc.desc_idx_, request.opcode,
                                      fh);
            }
            ++buffer_id;
            current_offset += buffer_desc.length;
        }

        nvmeof_desc.transfer_status.push_back(
            TransferStatus{.s = PENDING, .transferred_bytes = 0});
        nvmeof_desc.task_to_slices.push_back({slice_id, task.slice_count});
        ++task_id;
        slice_id += task.slice_count;
    }

    desc_pool_->submitBatch(nvmeof_desc.desc_idx_);
    // LOG(INFO) << "submit nr " << slice_id << " start " << start_slice_id;
    return Status::OK();
}

Status NVMeoFTransport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));
    int desc_idx = nvmeof_desc.desc_idx_;
    Status rc = Transport::freeBatchID(batch_id);
    if (rc != Status::OK()) {
        return rc;
    }
    desc_pool_->freeCUfileDesc(desc_idx);
    return Status::OK();
}

int NVMeoFTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> meta,
                             std::shared_ptr<Topology> topo) {
    return Transport::install(local_server_name, meta, topo);
}

int NVMeoFTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    (void)remote_accessible;
    (void)update_metadata;
    CUFILE_CHECK(cuFileBufRegister(addr, length, 0));
    return 0;
}

int NVMeoFTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    (void)update_metadata;
    CUFILE_CHECK(cuFileBufDeregister(addr));
    return 0;
}

void NVMeoFTransport::addSliceToTask(void *source_addr, uint64_t slice_len,
                                     uint64_t target_start,
                                     TransferRequest::OpCode op,
                                     TransferTask &task,
                                     const char *file_path) {
    if (!source_addr || !file_path) {
        LOG(ERROR) << "Invalid source_addr or file_path";
        return;
    }
    Slice *slice = getSliceCache().allocate();
    slice->source_addr = (char *)source_addr;
    slice->length = slice_len;
    slice->opcode = op;
    slice->nvmeof.file_path = file_path;
    slice->nvmeof.start = target_start;
    slice->task = &task;
    slice->status = Slice::PENDING;
    slice->ts = 0;
    task.slice_list.push_back(slice);
    task.total_bytes += slice->length;
    __sync_fetch_and_add(&task.slice_count, 1);
}

void NVMeoFTransport::addSliceToCUFileBatch(
    void *source_addr, uint64_t file_offset, uint64_t slice_len,
    uint64_t desc_id, TransferRequest::OpCode op, CUfileHandle_t fh) {
    CUfileIOParams_t params;
    params.mode = CUFILE_BATCH;
    params.opcode =
        op == Transport::TransferRequest::READ ? CUFILE_READ : CUFILE_WRITE;
    params.cookie = (void *)0;
    params.u.batch.devPtr_base = source_addr;
    params.u.batch.devPtr_offset = 0;
    params.u.batch.file_offset = file_offset;
    params.u.batch.size = slice_len;
    params.fh = fh;
    // LOG(INFO) << "params " << "base " << request.source << " offset " <<
    // request.target_offset << " length " << request.length;
    desc_pool_->pushParams(desc_id, params);
}
}  // namespace mooncake
