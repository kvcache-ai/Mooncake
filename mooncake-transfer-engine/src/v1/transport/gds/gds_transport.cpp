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

#include "v1/transport/gds/gds_transport.h"

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

#include "v1/common.h"
#include "v1/metadata/metadata.h"
#include "v1/transport/transport.h"

namespace mooncake {
namespace v1 {

GdsTransport::GdsTransport() : installed_(false) {
    static std::once_flag g_once_flag;
    auto fork_init = []() { CUFILE_CHECK(cuFileDriverOpen()); };
    std::call_once(g_once_flag, fork_init);
}

GdsTransport::~GdsTransport() { uninstall(); }

Status GdsTransport::install(std::string &local_segment_name,
                             std::shared_ptr<TransferMetadata> metadata_manager,
                             std::shared_ptr<Topology> local_topology) {
    if (installed_) {
        return Status::InvalidArgument(
            "GDS transport has been installed" MSG_TAIL);
    }

    metadata_manager_ = metadata_manager;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    desc_pool_ = std::make_shared<CUFileDescPool>();
    allocateLocalSegmentID();
    installed_ = true;
    return Status::OK();
}

Status GdsTransport::uninstall() {
    if (installed_) {
        // metadata_manager_->removeSegmentDesc(local_segment_name_);
        metadata_manager_.reset();
        installed_ = false;
    }
    return Status::OK();
}

TransferStatusEnum from_cufile_transfer_status(CUfileStatus_t status) {
    switch (status) {
        case CUFILE_WAITING:
            return WAITING;
        case CUFILE_PENDING:
            return PENDING;
        case CUFILE_INVALID:
            return INVALID;
        case CUFILE_CANCELED:
            return CANCELED;
        case CUFILE_COMPLETE:
            return COMPLETED;
        case CUFILE_TIMEOUT:
            return TIMEOUT;
        case CUFILE_FAILED:
            return FAILED;
        default:
            return FAILED;
    }
}

Status GdsTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto gds_batch = new GdsSubBatch();
    batch = gds_batch;
    gds_batch->task_list.reserve(max_size);
    gds_batch->max_size = max_size;
    gds_batch->desc_idx = desc_pool_->allocCUfileDesc(max_size);
    gds_batch->transfer_status.reserve(max_size);
    gds_batch->task_to_slices.reserve(max_size);
    return Status::OK();
}

Status GdsTransport::freeSubBatch(SubBatchRef &batch) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" MSG_TAIL);
    desc_pool_->freeCUfileDesc(gds_batch->desc_idx);
    delete gds_batch;
    batch = nullptr;
    return Status::OK();
}

Status GdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)gds_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" MSG_TAIL);
    }
    auto &task = gds_batch->task_list[task_id];
    status = {.s = PENDING, .transferred_bytes = 0};
    auto [slice_id, slice_num] = gds_batch->task_to_slices[task_id];
    for (size_t i = slice_id; i < slice_id + slice_num; ++i) {
        auto event =
            desc_pool_->getTransferStatus(gds_batch->desc_idx, slice_id);
        status.s = from_cufile_transfer_status(event.status);
        if (status.s == COMPLETED) {
            status.transferred_bytes += event.ret;
        } else {
            break;
        }
    }
    if (status.s == COMPLETED) {
        task.is_finished = true;
    }
    return Status::OK();
}

void GdsTransport::queryOutstandingTasks(SubBatchRef batch,
                                         std::vector<int> &task_id_list) {
    for (int task_id = 0; task_id < (int)gds_batch->task_list.size();
         ++task_id) {
        auto status = getTransferStatus(batch, task_id);
        if (status.s != TransferStatusEnum::COMPLETED) {
            task_id_list.push_back(task_id);
        }
    }
}

Status GdsTransport::registerLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    for (auto &buffer : buffer_list) {
        CUFILE_CHECK(cuFileBufRegister(buffer.addr, buffer.length, 0));
    }
    return Status::OK();
}

Status GdsTransport::unregisterLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    for (auto &buffer : buffer_list) {
        CUFILE_CHECK(cuFileBufDeregister(buffer.addr));
    }
    return Status::OK();
}

void GdsTransport::allocateLocalSegmentID() {
    // TBD
}

Status GdsTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (gds_batch->task_list.size() + request_list.size() >
        gds_batch->max_size) {
        return Status::TooManyRequests("Exceed batch capacity" MSG_TAIL);
    }

    size_t task_id = gds_batch->task_list.size();
    size_t slice_id = desc_pool_->getSliceNum(gds_batch->desc_idx);
    gds_batch->task_list.resize(task_id + request_list.size());

    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        segment_desc_map;

    for (auto &request : request_list) {
        auto &task = gds_batch->task_list[task_id];
        auto target_id = request.target_id;

        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                metadata_->getSegmentDescByID(target_id);
            assert(segment_desc_map[target_id] != nullptr);
        }

        auto &desc = segment_desc_map.at(target_id);
        assert(desc->protocol == "nvmeof");
        uint32_t buffer_id = 0;
        uint64_t segment_start = request.target_offset;
        uint64_t segment_end = request.target_offset + request.length;
        uint64_t current_offset = 0;
        auto &detail = std::get<FileSegmentDesc>(desc->detail);
        for (auto &buffer_desc : detail.buffers) {
            bool is_overlap =
                overlap((void *)segment_start, request.length,
                        (void *)current_offset, buffer_desc.length);
            if (is_overlap) {
                uint64_t slice_start = std::max(segment_start, current_offset);
                uint64_t slice_end =
                    std::min(segment_end, current_offset + buffer_desc.length);
                const char *file_path =
                    buffer_desc.mounted_path_map[local_segment_name_].c_str();
                void *source_addr =
                    (char *)request.source + slice_start - segment_start;
                uint64_t file_offset = slice_start - current_offset;
                uint64_t slice_len = slice_end - slice_start;
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
                addSliceToCUFileBatch(source_addr, file_offset, slice_len,
                                      gds_batch->desc_idx, request.opcode, fh);
            }
            ++buffer_id;
            current_offset += buffer_desc.length;
        }

        gds_batch->task_to_slices.push_back({slice_id, task.slice_count});
        ++task_id;
        slice_id += task.slice_count;
    }

    desc_pool_->submitBatch(nvmeof_desc.desc_idx_);
    return Status::OK();
}

void GdsTransport::addSliceToCUFileBatch(void *source_addr,
                                         uint64_t file_offset,
                                         uint64_t slice_len, uint64_t desc_id,
                                         Request::OpCode op,
                                         CUfileHandle_t fh) {
    CUfileIOParams_t params;
    params.mode = CUFILE_BATCH;
    params.opcode = (op == Request::READ ? CUFILE_READ : CUFILE_WRITE);
    params.cookie = (void *)0;
    params.u.batch.devPtr_base = source_addr;
    params.u.batch.devPtr_offset = 0;
    params.u.batch.file_offset = file_offset;
    params.u.batch.size = slice_len;
    params.fh = fh;
    desc_pool_->pushParams(desc_id, params);
}

}  // namespace v1
}  // namespace mooncake
