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

#include "v1/transport/io_uring/io_uring_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "v1/memory/slab.h"
#include "v1/utility/system.h"

namespace mooncake {
namespace v1 {
class IOUringFileContext {
   public:
    explicit IOUringFileContext(const std::string &path) : ready_(false) {
        fd_ = open(path.c_str(), O_RDWR | O_DIRECT);
        if (fd_ >= 0) {
            ready_ = true;
            return;
        }

        fd_ = open(path.c_str(), O_RDWR);
        if (fd_ < 0) {
            PLOG(ERROR) << "Failed to open file " << path;
            return;
        }

        LOG(WARNING) << "File " << path << " opened in Buffered I/O mode";
        ready_ = true;
    }

    IOUringFileContext(const IOUringFileContext &) = delete;
    IOUringFileContext &operator=(const IOUringFileContext &) = delete;

    ~IOUringFileContext() {
        if (fd_ >= 0) close(fd_);
    }

    int getHandle() const { return fd_; }

    bool ready() const { return ready_; }

   private:
    int fd_;
    bool ready_;
};

IOUringTransport::IOUringTransport() : installed_(false) {}

IOUringTransport::~IOUringTransport() { uninstall(); }

Status IOUringTransport::install(std::string &local_segment_name,
                                 std::shared_ptr<MetadataService> metadata,
                                 std::shared_ptr<Topology> local_topology,
                                 std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "IO Uring transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    return Status::OK();
}

Status IOUringTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status IOUringTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto io_uring_batch = Slab<IOUringSubBatch>::Get().allocate();
    if (!io_uring_batch)
        return Status::InternalError("Unable to allocate IO Uring sub-batch");
    batch = io_uring_batch;
    io_uring_batch->max_size = max_size;
    io_uring_batch->task_list.reserve(max_size);
    int rc = io_uring_queue_init(max_size, &io_uring_batch->ring, 0);
    if (rc)
        return Status::InternalError(
            "io_uring_queue_init failed: " + std::to_string(rc) + LOC_MARK);
    return Status::OK();
}

Status IOUringTransport::freeSubBatch(SubBatchRef &batch) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch *>(batch);
    if (!io_uring_batch)
        return Status::InvalidArgument("Invalid IO Uring sub-batch" LOC_MARK);
    io_uring_queue_exit(&io_uring_batch->ring);
    Slab<IOUringSubBatch>::Get().deallocate(io_uring_batch);
    batch = nullptr;
    return Status::OK();
}

std::string IOUringTransport::getIOUringFilePath(SegmentID target_id) {
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok() || desc->type != SegmentType::File) return "";
    auto &detail = std::get<FileSegmentDesc>(desc->detail);
    if (detail.buffers.empty()) return "";
    return detail.buffers[0].path;
}

Status IOUringTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch *>(batch);
    if (!io_uring_batch)
        return Status::InvalidArgument("Invalid IO Uring sub-batch" LOC_MARK);
    if (request_list.size() + (int)io_uring_batch->task_list.size() >
        io_uring_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        io_uring_batch->task_list.push_back(IOUringTask{});
        auto &task =
            io_uring_batch->task_list[io_uring_batch->task_list.size() - 1];
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        IOUringFileContext *context = nullptr;
        {
            RWSpinlock::ReadGuard guard(file_context_lock_);
            if (file_context_map_.count(request.target_id))
                context = file_context_map_[request.target_id].get();
        }

        if (!context) {
            RWSpinlock::WriteGuard guard(file_context_lock_);
            if (!file_context_map_.count(request.target_id)) {
                std::string path = getIOUringFilePath(request.target_id);
                if (path.empty())
                    return Status::InvalidArgument(
                        "Invalid remote segment" LOC_MARK);
                file_context_map_[request.target_id] =
                    std::make_shared<IOUringFileContext>(path);
            }
            context = file_context_map_[request.target_id].get();
        }

        if (!context->ready())
            return Status::InvalidArgument("Invalid remote segment" LOC_MARK);

        struct io_uring_sqe *sqe = io_uring_get_sqe(&io_uring_batch->ring);
        if (!sqe)
            return Status::InternalError("io_uring_get_sqe failed" LOC_MARK);

        const size_t kPageSize = 4096;
        if (isCudaMemory(request.source) ||
            (uint64_t)request.source % kPageSize) {
            int rc = posix_memalign(&task.buffer, kPageSize, request.length);
            if (rc)
                return Status::InternalError("posix_memalign failed" LOC_MARK);
            if (request.opcode == Request::READ)
                io_uring_prep_read(sqe, context->getHandle(), task.buffer,
                                   request.length, request.target_offset);
            else if (request.opcode == Request::WRITE) {
                genericMemcpy(task.buffer, request.source, request.length);
                io_uring_prep_write(sqe, context->getHandle(), task.buffer,
                                    request.length, request.target_offset);
            }
        } else {
            if (request.opcode == Request::READ)
                io_uring_prep_read(sqe, context->getHandle(), request.source,
                                   request.length, request.target_offset);
            else if (request.opcode == Request::WRITE)
                io_uring_prep_write(sqe, context->getHandle(), request.source,
                                    request.length, request.target_offset);
        }
        sqe->user_data = (uintptr_t)&task;
    }

    int rc = io_uring_submit(&io_uring_batch->ring);
    if (rc != (int32_t)request_list.size())
        return Status::InternalError(
            std::string("io_uring_submit failed: Code ") + std::to_string(rc) +
            LOC_MARK);

    return Status::OK();
}

Status IOUringTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                           TransferStatus &status) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)io_uring_batch->task_list.size())
        return Status::InvalidArgument("Invalid task ID");
    auto &task = io_uring_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    if (task.status_word == TransferStatusEnum::PENDING) {
        struct io_uring_cqe *cqe = nullptr;
        io_uring_peek_cqe(&io_uring_batch->ring, &cqe);
        if (cqe) {
            auto task = (IOUringTask *)cqe->user_data;
            if (task) {
                if (cqe->res < 0)
                    task->status_word = TransferStatusEnum::FAILED;
                else {
                    if (task->buffer) {
                        if (task->request.opcode == Request::READ)
                            genericMemcpy(task->request.source, task->buffer,
                                          task->request.length);
                        free(task->buffer);
                        task->buffer = nullptr;
                    }
                    task->status_word = TransferStatusEnum::COMPLETED;
                    task->transferred_bytes = task->request.length;
                }
            }
            io_uring_cqe_seen(&io_uring_batch->ring, cqe);
        }
    }
    return Status::OK();
}

Status IOUringTransport::addMemoryBuffer(BufferDesc &desc,
                                         const MemoryOptions &options) {
    return Status::OK();
}

Status IOUringTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
