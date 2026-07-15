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

NVMeoFTransport::NVMeoFTransport(std::shared_ptr<CUFileDescPool> desc_pool)
    : desc_pool_(std::move(desc_pool)) {}

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
    auto nvmeof_desc = std::make_unique<NVMeoFBatchDesc>();
    auto batch_id = Transport::allocateBatchID(batch_size);
    if (batch_id == static_cast<BatchID>(ERR_MEMORY)) return batch_id;

    auto &batch_desc = *((BatchDesc *)(batch_id));
    nvmeof_desc->desc_idx_ = desc_pool_->allocCUfileDesc(batch_size);
    if (nvmeof_desc->desc_idx_ < 0) {
        Transport::freeBatchID(batch_id);
        return static_cast<BatchID>(ERR_MEMORY);
    }
    nvmeof_desc->transfer_status.reserve(batch_size);
    nvmeof_desc->task_to_slices.reserve(batch_size);
    batch_desc.context = nvmeof_desc.release();
    return batch_id;
}

Status NVMeoFTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    if (batch_id == 0) {
        return Status::InvalidArgument("NVMeoFTransport: Invalid batch ID");
    }
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument("NVMeoFTransport: Task ID out of range");
    }
    if (batch_desc.context == nullptr) {
        return Status::InvalidArgument(
            "NVMeoFTransport: Batch was not allocated by this transport");
    }
    auto &task = batch_desc.task_list[task_id];
    auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));
    if (task_id >= nvmeof_desc.task_to_slices.size()) {
        return Status::InvalidArgument(
            "NVMeoFTransport: Task has no submitted slices");
    }
    const bool all_tasks_finished =
        std::all_of(batch_desc.task_list.begin(), batch_desc.task_list.end(),
                    [](const TransferTask &mapped_task) {
                        return mapped_task.is_finished;
                    });
    if (all_tasks_finished && task.is_finished &&
        task_id < nvmeof_desc.transfer_status.size()) {
        status = nvmeof_desc.transfer_status[task_id];
        return Status::OK();
    }

    thread_local CUFileBatchSnapshot snapshot;
    const auto poll_result =
        desc_pool_->pollBatch(nvmeof_desc.desc_idx_, snapshot);
    if (poll_result == CUFileBatchPollResult::kRetry) {
        if (task.is_finished && task_id < nvmeof_desc.transfer_status.size()) {
            status = nvmeof_desc.transfer_status[task_id];
        } else {
            status = {.s = PENDING, .transferred_bytes = 0};
        }
        return Status::OK();
    }
    if (poll_result == CUFileBatchPollResult::kError) {
        return Status::Context("NVMeoFTransport: Failed to poll cuFile batch");
    }

    if (snapshot.failure_seen && !snapshot.all_terminal) {
        if (task.is_finished && task_id < nvmeof_desc.transfer_status.size()) {
            status = nvmeof_desc.transfer_status[task_id];
            return Status::OK();
        }
        const bool has_pending =
            std::any_of(snapshot.io_events.begin(), snapshot.io_events.end(),
                        [](const CUfileIOEvents_t &event) {
                            return event.status == CUFILE_PENDING;
                        });
        status = {.s = has_pending ? PENDING : WAITING, .transferred_bytes = 0};
        return Status::OK();
    }

    thread_local std::vector<TransferStatus> slice_statuses;
    auto aggregate_task = [&](size_t current_task_id,
                              TransferStatus &task_status,
                              bool &is_finished) -> bool {
        auto [slice_id, slice_num] =
            nvmeof_desc.task_to_slices[current_task_id];
        if (slice_id > snapshot.io_events.size() ||
            slice_num > snapshot.io_events.size() - slice_id) {
            return false;
        }

        slice_statuses.clear();
        slice_statuses.reserve(slice_num);
        for (size_t i = slice_id; i < slice_id + slice_num; ++i) {
            const auto &event = snapshot.io_events[i];
            auto slice_status = from_cufile_transfer_status(event.status);
            if (slice_status == CANCELED &&
                i < snapshot.cancel_requested.size() &&
                snapshot.cancel_requested[i]) {
                slice_status = FAILED;
            }
            slice_statuses.push_back(
                TransferStatus{.s = slice_status,
                               .transferred_bytes =
                                   slice_status == COMPLETED ? event.ret : 0});
        }

        task_status = aggregateTransferStatus(slice_statuses, is_finished);
        return true;
    };

    if (snapshot.all_terminal) {
        if (nvmeof_desc.task_to_slices.size() < batch_desc.task_list.size()) {
            return Status::Context(
                "NVMeoFTransport: Missing task-to-slice mapping");
        }

        std::vector<TransferStatus> final_statuses(batch_desc.task_list.size());
        for (size_t i = 0; i < batch_desc.task_list.size(); ++i) {
            bool is_finished = false;
            if (!aggregate_task(i, final_statuses[i], is_finished) ||
                !is_finished) {
                return Status::Context(
                    "NVMeoFTransport: Invalid task-to-slice mapping");
            }
        }

        nvmeof_desc.transfer_status = std::move(final_statuses);
        for (auto &mapped_task : batch_desc.task_list) {
            mapped_task.is_finished = true;
        }
        status = nvmeof_desc.transfer_status[task_id];
        return Status::OK();
    }

    if (task.is_finished && task_id < nvmeof_desc.transfer_status.size()) {
        status = nvmeof_desc.transfer_status[task_id];
        return Status::OK();
    }

    bool is_finished = false;
    if (!aggregate_task(task_id, status, is_finished)) {
        return Status::Context(
            "NVMeoFTransport: Invalid task-to-slice mapping");
    }
    if (!is_finished) return Status::OK();

    if (nvmeof_desc.transfer_status.size() < batch_desc.task_list.size()) {
        nvmeof_desc.transfer_status.resize(
            batch_desc.task_list.size(),
            TransferStatus{.s = PENDING, .transferred_bytes = 0});
    }
    nvmeof_desc.transfer_status[task_id] = status;
    task.is_finished = true;
    return Status::OK();
}

Status NVMeoFTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    // MultiTransport owns these generic BatchDesc objects, so this transport
    // cannot attach or reclaim the NVMe-specific descriptor required by
    // cuFile. No asynchronous work was started; make the tasks releasable.
    for (auto *task : task_list) {
        if (task != nullptr) task->is_finished = true;
    }
    return Status::NotImplemented(
        "NVMeoFTransport does not support MultiTransport batches");
}

Transport::TransferStatus NVMeoFTransport::aggregateTransferStatus(
    const std::vector<TransferStatus> &slice_statuses, bool &is_finished) {
    TransferStatus result = {.s = COMPLETED, .transferred_bytes = 0};
    is_finished = true;
    bool has_pending = false;

    // Terminal failures use a fixed precedence so the result does not depend
    // on the order in which cuFile reports completions.
    int failure_priority = 0;
    for (const auto &slice_status : slice_statuses) {
        switch (slice_status.s) {
            case COMPLETED:
                result.transferred_bytes += slice_status.transferred_bytes;
                break;
            case WAITING:
                is_finished = false;
                break;
            case PENDING:
                has_pending = true;
                is_finished = false;
                break;
            case INVALID:
                if (failure_priority < 1) {
                    result.s = INVALID;
                    failure_priority = 1;
                }
                break;
            case CANCELED:
                if (failure_priority < 2) {
                    result.s = CANCELED;
                    failure_priority = 2;
                }
                break;
            case TIMEOUT:
                if (failure_priority < 3) {
                    result.s = TIMEOUT;
                    failure_priority = 3;
                }
                break;
            case FAILED:
                result.s = FAILED;
                failure_priority = 4;
                break;
        }
    }

    if (slice_statuses.empty()) {
        result.s = INVALID;
    } else if (!is_finished) {
        result.s = has_pending ? PENDING : WAITING;
    }
    return result;
}

Status NVMeoFTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));

    if (!desc_pool_->isAcceptingSubmissions(nvmeof_desc.desc_idx_)) {
        return Status::BatchBusy(
            "NVMeoFTransport: Batch is draining after a cuFile failure");
    }

    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NVMeoFTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NVMeoFTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    const size_t initial_task_count = batch_desc.task_list.size();
    const size_t initial_status_count = nvmeof_desc.transfer_status.size();
    const size_t initial_mapping_count = nvmeof_desc.task_to_slices.size();
    int initial_slice_count = desc_pool_->getSliceNum(nvmeof_desc.desc_idx_);
    if (initial_slice_count < 0) {
        return Status::Context(
            "NVMeoFTransport: Invalid cuFile batch descriptor");
    }

    auto rollback_unsubmitted = [&]() {
        desc_pool_->discardUnsubmittedParams(nvmeof_desc.desc_idx_);
        nvmeof_desc.transfer_status.resize(initial_status_count);
        nvmeof_desc.task_to_slices.resize(initial_mapping_count);
        batch_desc.task_list.resize(initial_task_count);
    };

    size_t task_id = initial_task_count;
    size_t slice_id = initial_slice_count;
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
                if (addSliceToCUFileBatch(source_addr, file_offset, slice_len,
                                          nvmeof_desc.desc_idx_, request.opcode,
                                          fh) != 0) {
                    rollback_unsubmitted();
                    return Status::Context(
                        "NVMeoFTransport: Failed to append cuFile request");
                }
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

    if (desc_pool_->submitBatch(nvmeof_desc.desc_idx_) != 0) {
        return Status::Context(
            "NVMeoFTransport: Failed to submit cuFile batch");
    }
    // LOG(INFO) << "submit nr " << slice_id << " start " << start_slice_id;
    return Status::OK();
}

Status NVMeoFTransport::freeBatchID(BatchID batch_id) {
    if (batch_id == 0) {
        return Status::InvalidArgument("NVMeoFTransport: Invalid batch ID");
    }
    auto &batch_desc = *((BatchDesc *)(batch_id));
    auto *nvmeof_desc_ptr = (NVMeoFBatchDesc *)(batch_desc.context);
    if (nvmeof_desc_ptr == nullptr) {
        return Status::InvalidArgument(
            "NVMeoFTransport: Batch was not allocated by this transport");
    }
    for (const auto &task : batch_desc.task_list) {
        if (!task.is_finished) {
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }

    int desc_idx = nvmeof_desc_ptr->desc_idx_;
    if (desc_pool_->freeCUfileDesc(desc_idx) != 0) {
        return Status::BatchBusy(
            "BatchID cannot be freed while cuFile I/O is active");
    }
    delete nvmeof_desc_ptr;
    batch_desc.context = nullptr;
    return Transport::freeBatchID(batch_id);
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

int NVMeoFTransport::addSliceToCUFileBatch(void *source_addr,
                                           uint64_t file_offset,
                                           uint64_t slice_len, uint64_t desc_id,
                                           TransferRequest::OpCode op,
                                           CUfileHandle_t fh) {
    CUfileIOParams_t params{};
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
    return desc_pool_->pushParams(desc_id, params);
}
}  // namespace mooncake
