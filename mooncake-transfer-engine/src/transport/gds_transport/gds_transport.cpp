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

#include "transport/gds_transport/gds_transport.h"

#include <bits/stdint-uintn.h>
#include <cufile.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common.h"

namespace mooncake {
class GdsFileContext {
   public:
    explicit GdsFileContext(const std::string &path) : ready_(false) {
        int fd = open(path.c_str(), O_RDWR | O_DIRECT);
        if (fd < 0) {
            PLOG(ERROR) << "Failed to open GDS file " << path;
            return;
        }
        memset(&desc_, 0, sizeof(desc_));
        desc_.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc_.handle.fd = fd;
        auto result = cuFileHandleRegister(&handle_, &desc_);
        if (result.err != CU_FILE_SUCCESS) {
            LOG(ERROR) << "Failed to register GDS file handle: code "
                       << result.err;
            close(fd);
            desc_.handle.fd = -1;
            return;
        }
        ready_ = true;
    }

    GdsFileContext(const GdsFileContext &) = delete;
    GdsFileContext &operator=(const GdsFileContext &) = delete;

    ~GdsFileContext() {
        if (handle_ != nullptr) {
            cuFileHandleDeregister(handle_);
        }
        if (desc_.handle.fd >= 0) {
            close(desc_.handle.fd);
        }
    }

    CUfileHandle_t getHandle() const { return handle_; }

    bool ready() const { return ready_; }

   private:
    CUfileHandle_t handle_ = nullptr;
    CUfileDescr_t desc_{};
    bool ready_;
};

namespace {

constexpr size_t kMaxSliceSize = 16ull << 20;
constexpr size_t kDefaultMaxBatchEntries = 1024;

Transport::TransferStatusEnum parseTransferStatus(CUfileStatus_t status) {
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
        default:
            return Transport::FAILED;
    }
}

}  // namespace

GdsTransport::GdsTransport() : max_batch_entries_(kDefaultMaxBatchEntries) {
    static std::once_flag g_once_flag;
    std::call_once(g_once_flag, []() { cuFileDriverOpen(); });
    desc_pool_ = std::make_shared<CUFileDescPool>(max_batch_entries_);
}

GdsTransport::~GdsTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
}

Transport::BatchID GdsTransport::allocateBatchID(size_t batch_size) {
    auto gds_desc = new GdsBatchDesc();
    auto batch_id = Transport::allocateBatchID(batch_size);
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    gds_desc->desc_idx = desc_pool_->allocCUfileDesc(max_batch_entries_);
    gds_desc->io_param_ranges.reserve(batch_size);
    batch_desc.context = gds_desc;
    return batch_id;
}

Status GdsTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests("Exceed batch capacity");
    }

    const size_t first_task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(first_task_id + entries.size());

    std::vector<TransferTask *> task_list;
    task_list.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        auto &task = batch_desc.task_list[first_task_id + i];
        task.batch_id = batch_id;
        task.request = &entries[i];
        task_list.push_back(&task);
    }
    return submitTransferTask(task_list);
}

Status GdsTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    if (task_list.empty()) {
        return Status::OK();
    }

    auto *batch_desc = reinterpret_cast<BatchDesc *>(task_list[0]->batch_id);
    auto *gds_desc = reinterpret_cast<GdsBatchDesc *>(batch_desc->context);
    if (!gds_desc || gds_desc->desc_idx < 0) {
        return Status::Context("Invalid GDS batch descriptor");
    }

    int first_slice_id = desc_pool_->getSliceNum(gds_desc->desc_idx);
    if (first_slice_id < 0) {
        return Status::Context("Failed to query current GDS slice count");
    }

    size_t estimated_num_params = 0;
    for (auto *task : task_list) {
        assert(task != nullptr);
        assert(task->request != nullptr);
        const auto &request = *task->request;
        estimated_num_params +=
            (request.length + kMaxSliceSize - 1) / kMaxSliceSize;
    }

    if (static_cast<size_t>(first_slice_id) + estimated_num_params >
        max_batch_entries_) {
        return Status::TooManyRequests("Exceed GDS batch capacity");
    }

    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        segment_desc_map;

    for (auto *task : task_list) {
        auto &request = *task->request;
        task->total_bytes = request.length;

        if (!segment_desc_map.count(request.target_id)) {
            segment_desc_map[request.target_id] =
                metadata_->getSegmentDescByID(request.target_id);
        }
        auto desc = segment_desc_map[request.target_id];
        if (!desc) {
            return Status::InvalidArgument("Invalid target segment ID " +
                                           std::to_string(request.target_id));
        }
        if (desc->protocol != "gds" && desc->protocol != "nvmeof") {
            return Status::InvalidArgument("Target segment protocol is not gds");
        }

        IOParamRange range{static_cast<size_t>(
                               desc_pool_->getSliceNum(gds_desc->desc_idx)),
                           0};
        if (static_cast<int>(range.base) < 0) {
            return Status::Context("Failed to query GDS descriptor state");
        }

        uint32_t buffer_id = 0;
        uint64_t segment_start = request.target_offset;
        uint64_t segment_end = request.target_offset + request.length;
        uint64_t current_offset = 0;

        for (auto &buffer_desc : desc->nvmeof_buffers) {
            bool is_overlap = overlap(
                reinterpret_cast<void *>(segment_start), request.length,
                reinterpret_cast<void *>(current_offset), buffer_desc.length);
            if (!is_overlap) {
                ++buffer_id;
                current_offset += buffer_desc.length;
                continue;
            }

            uint64_t overlap_start = std::max(segment_start, current_offset);
            uint64_t overlap_end =
                std::min(segment_end, current_offset + buffer_desc.length);
            std::string file_path = getBufferPath(buffer_desc);
            if (file_path.empty()) {
                return Status::InvalidArgument("Missing GDS file path");
            }

            auto context_key = std::make_pair(request.target_id, buffer_id);
            CUfileHandle_t fh = nullptr;
            {
                RWSpinlock::WriteGuard guard(context_lock_);
                if (!segment_to_context_.count(context_key)) {
                    segment_to_context_[context_key] =
                        std::make_shared<GdsFileContext>(file_path);
                }
                auto context = segment_to_context_[context_key];
                if (!context || !context->ready()) {
                    return Status::InvalidArgument(
                        "Failed to initialize GDS file context");
                }
                fh = context->getHandle();
            }

            for (uint64_t pos = overlap_start; pos < overlap_end;
                 pos += kMaxSliceSize) {
                uint64_t len = std::min(kMaxSliceSize, overlap_end - pos);
                uint64_t source_offset = pos - segment_start;
                uint64_t file_offset = pos - current_offset;
                addSliceToCUFileBatch(request.source, source_offset,
                                      file_offset, len, request.opcode,
                                      gds_desc->desc_idx, fh);
                ++range.count;
            }

            ++buffer_id;
            current_offset += buffer_desc.length;
        }

        if (range.count == 0) {
            return Status::InvalidArgument("Request does not overlap any GDS file buffer");
        }
        gds_desc->io_param_ranges.push_back(range);
    }

    if (desc_pool_->submitBatch(gds_desc->desc_idx) != 0) {
        return Status::Context("Failed to submit GDS batch IO");
    }

    return Status::OK();
}

Status GdsTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    auto *gds_desc = reinterpret_cast<GdsBatchDesc *>(batch_desc.context);
    if (!gds_desc || gds_desc->desc_idx < 0) {
        return Status::Context("Invalid GDS batch descriptor");
    }
    if (task_id >= gds_desc->io_param_ranges.size()) {
        return Status::InvalidArgument("Task ID out of range");
    }

    auto range = gds_desc->io_param_ranges[task_id];
    status.s = PENDING;
    status.transferred_bytes = 0;

    size_t completed_count = 0;
    bool failed = false;
    for (size_t i = range.base; i < range.base + range.count; ++i) {
        auto event = desc_pool_->getTransferStatus(gds_desc->desc_idx, i);
        auto current_status = parseTransferStatus(event.status);
        if (current_status == COMPLETED) {
            ++completed_count;
        } else if (current_status != PENDING && current_status != WAITING) {
            failed = true;
            status.s = current_status;
        }

        if (event.ret > 0) {
            status.transferred_bytes += static_cast<size_t>(event.ret);
        }
    }

    auto &task = batch_desc.task_list[task_id];
    if (completed_count == range.count) {
        status.s = COMPLETED;
        task.is_finished = true;
    } else if (failed) {
        task.is_finished = true;
    }

    return Status::OK();
}

Status GdsTransport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    auto *gds_desc = reinterpret_cast<GdsBatchDesc *>(batch_desc.context);
    int desc_idx = gds_desc ? gds_desc->desc_idx : -1;

    Status rc = Transport::freeBatchID(batch_id);
    if (!rc.ok()) {
        return rc;
    }

    if (desc_idx >= 0) {
        desc_pool_->freeCUfileDesc(desc_idx);
    }
    delete gds_desc;
    return Status::OK();
}

int GdsTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    (void)topo;
    return Transport::install(local_server_name, meta, topo);
}

int GdsTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)location;
    (void)remote_accessible;
    (void)update_metadata;
    auto result = cuFileBufRegister(addr, length, 0);
    if (result.err != CU_FILE_SUCCESS) {
        LOG(ERROR) << "Failed to register GDS buffer: code " << result.err;
        return ERR_INVALID_ARGUMENT;
    }
    return 0;
}

int GdsTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    (void)update_metadata;
    auto result = cuFileBufDeregister(addr);
    if (result.err != CU_FILE_SUCCESS) {
        LOG(ERROR) << "Failed to deregister GDS buffer: code " << result.err;
        return ERR_INVALID_ARGUMENT;
    }
    return 0;
}

int GdsTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (const auto &buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int GdsTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto *addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

std::string GdsTransport::getBufferPath(
    const TransferMetadata::NVMeoFBufferDesc &buffer_desc) const {
    if (!local_server_name_.empty()) {
        auto it = buffer_desc.local_path_map.find(local_server_name_);
        if (it != buffer_desc.local_path_map.end() && !it->second.empty()) {
            return it->second;
        }
    }
    return buffer_desc.file_path;
}

void GdsTransport::addSliceToCUFileBatch(void *source_addr,
                                         uint64_t source_offset,
                                         uint64_t file_offset,
                                         uint64_t slice_len,
                                         TransferRequest::OpCode op,
                                         int desc_idx, CUfileHandle_t fh) {
    CUfileIOParams_t params;
    params.mode = CUFILE_BATCH;
    params.opcode = (op == TransferRequest::READ) ? CUFILE_READ : CUFILE_WRITE;
    params.cookie = nullptr;
    params.u.batch.devPtr_base = source_addr;
    params.u.batch.devPtr_offset = source_offset;
    params.u.batch.file_offset = file_offset;
    params.u.batch.size = slice_len;
    params.fh = fh;
    desc_pool_->pushParams(desc_idx, params);
}

}  // namespace mooncake
