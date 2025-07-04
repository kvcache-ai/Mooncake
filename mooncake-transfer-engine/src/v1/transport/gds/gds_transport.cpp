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
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

namespace mooncake {
namespace v1 {
class GdsFileContext {
   public:
    explicit GdsFileContext(const std::string &path) : ready_(false) {
        int fd = open(path.c_str(), O_RDWR | O_DIRECT);
        if (fd < 0) {
            PLOG(ERROR) << "Failed to open file " << path;
            return;
        }
        memset(&desc_, 0, sizeof(desc_));
        desc_.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc_.handle.fd = fd;
        auto result = cuFileHandleRegister(&handle_, &desc_);
        if (result.err != CU_FILE_SUCCESS) {
            LOG(ERROR) << "Failed to register GDS file handle: " << result.err;
            return;
        }
        ready_ = true;
    }

    GdsFileContext(const GdsFileContext &) = delete;
    GdsFileContext &operator=(const GdsFileContext &) = delete;

    ~GdsFileContext() {
        if (handle_) cuFileHandleDeregister(handle_);
        if (desc_.handle.fd) close(desc_.handle.fd);
    }

    CUfileHandle_t getHandle() const { return handle_; }

    bool ready() const { return ready_; }

   private:
    CUfileHandle_t handle_ = NULL;
    CUfileDescr_t desc_;
    bool ready_;
};

class GdsRunner {
   public:
    GdsRunner(size_t max_tasks);
    ~GdsRunner();

    GdsRunner(const GdsRunner &) = delete;
    GdsRunner &operator=(const GdsRunner &) = delete;

   public:
    bool ready() const { return ready_; }

    int size() const { return io_params_.size(); }

    Status addTask(void *source, GdsFileContext *context, uint64_t offset,
                   uint64_t length, Request::OpCode opcode);

    Status submit();

    Status getStatus(int task_id, TransferStatus &result);

    void reset();

   private:
    const size_t max_tasks_;
    bool ready_;
    CUfileBatchHandle_t handle_;
    std::vector<CUfileIOParams_t> io_params_;
    std::vector<CUfileIOEvents_t> io_events_;
};

GdsRunner::GdsRunner(size_t max_tasks) : max_tasks_(max_tasks) {
    io_params_.reserve(max_tasks_);
    io_events_.resize(max_tasks_);
    auto result = cuFileBatchIOSetUp(&handle_, max_tasks_);
    if (result.err != CU_FILE_SUCCESS) {
        LOG(ERROR) << "Failed to setup GDS batch IO: " << result.err;
        return;
    }
    ready_ = true;
}

GdsRunner::~GdsRunner() {
    if (ready_) {
        cuFileBatchIODestroy(handle_);
        ready_ = false;
    }
}

Status GdsRunner::addTask(void *source, GdsFileContext *context,
                          uint64_t offset, uint64_t length,
                          Request::OpCode opcode) {
    if (io_params_.size() == max_tasks_) {
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    }
    CUfileIOParams_t params;
    params.mode = CUFILE_BATCH;
    params.opcode = (opcode == Request::READ) ? CUFILE_READ : CUFILE_WRITE;
    params.cookie = (void *)0;
    params.u.batch.devPtr_base = source;
    params.u.batch.devPtr_offset = 0;
    params.u.batch.file_offset = offset;
    params.u.batch.size = length;
    params.fh = context->getHandle();
    io_params_.push_back(params);
    return Status::OK();
}

Status GdsRunner::submit() {
    auto result =
        cuFileBatchIOSubmit(handle_, io_params_.size(), io_params_.data(), 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError("Failed to submit GDS batch IO");
    return Status::OK();
}

TransferStatusEnum parseTransferStatus(CUfileStatus_t status) {
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
        default:
            return FAILED;
    }
}

Status GdsRunner::getStatus(int task_id, TransferStatus &status) {
    unsigned num_tasks = io_params_.size();
    if (task_id < 0 || task_id >= (int)num_tasks)
        return Status::InvalidArgument("Invalid task ID");
    auto result = cuFileBatchIOGetStatus(handle_, 0, &num_tasks,
                                         io_events_.data(), nullptr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError("Failed to get GDS batch status");
    auto &event = io_events_[task_id];
    status.s = parseTransferStatus(event.status);
    status.transferred_bytes += event.ret;
    return Status::OK();
}

void GdsRunner::reset() {
    io_params_.clear();
    io_params_.reserve(max_tasks_);
    io_events_.resize(max_tasks_);
}

GdsTransport::GdsTransport() : installed_(false) {
    static std::once_flag g_once_flag;
    auto fork_init = []() { cuFileDriverOpen(); };
    std::call_once(g_once_flag, fork_init);
}

GdsTransport::~GdsTransport() { uninstall(); }

Status GdsTransport::install(std::string &local_segment_name,
                             std::shared_ptr<MetadataService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "GDS transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    return Status::OK();
}

Status GdsTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status GdsTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto gds_batch = new GdsSubBatch();
    batch = gds_batch;
    gds_batch->runner = std::make_shared<GdsRunner>(max_size);
    return Status::OK();
}

Status GdsTransport::freeSubBatch(SubBatchRef &batch) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    delete gds_batch;
    batch = nullptr;
    return Status::OK();
}

std::string GdsTransport::getGdsFilePath(SegmentID target_id) {
    SegmentDescRef desc;
    auto status = metadata_->segmentManager().getRemote(desc, target_id);
    if (!status.ok()) return "";
    auto &detail = std::get<FileSegmentDesc>(desc->detail);
    if (detail.buffers.empty()) return "";
    return detail.buffers[0].path;
}

Status GdsTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    for (auto &request : request_list) {
        GdsFileContext *context = nullptr;
        {
            RWSpinlock::ReadGuard guard(file_context_lock_);
            if (file_context_map_.count(request.target_id))
                context = file_context_map_[request.target_id].get();
        }

        if (!context) {
            RWSpinlock::WriteGuard guard(file_context_lock_);
            if (!file_context_map_.count(request.target_id)) {
                std::string path = getGdsFilePath(request.target_id);
                if (path.empty())
                    return Status::InvalidArgument(
                        "Invalid remote segment" LOC_MARK);
                file_context_map_[request.target_id] =
                    std::make_shared<GdsFileContext>(path);
            }
            context = file_context_map_[request.target_id].get();
        }

        auto status = gds_batch->runner->addTask(
            request.source, context, request.target_offset, request.length,
            request.opcode);

        if (!status.ok()) return status;
    }
    return gds_batch->runner->submit();
}

Status GdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    return gds_batch->runner->getStatus(task_id, status);
}

void GdsTransport::queryOutstandingTasks(SubBatchRef batch,
                                         std::vector<int> &task_id_list) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    for (int task_id = 0; task_id < gds_batch->runner->size(); ++task_id) {
        TransferStatus status;
        getTransferStatus(batch, task_id, status);
        if (status.s != TransferStatusEnum::COMPLETED) {
            task_id_list.push_back(task_id);
        }
    }
}

Status GdsTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    auto result = cuFileBufRegister((void *)desc.addr, desc.length, 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError("Failed to register GDS buffer");
    return Status::OK();
}

Status GdsTransport::removeMemoryBuffer(BufferDesc &desc) {
    auto result = cuFileBufDeregister((void *)desc.addr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError("Failed to register GDS buffer");
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
