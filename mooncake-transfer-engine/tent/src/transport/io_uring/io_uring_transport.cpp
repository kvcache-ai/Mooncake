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

#include "tent/transport/io_uring/io_uring_transport.h"

#include <cstdint>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "tent/runtime/slab.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {
class IOUringFileContext {
   public:
    explicit IOUringFileContext(const std::string& path) : ready_(false) {
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

    IOUringFileContext(const IOUringFileContext&) = delete;
    IOUringFileContext& operator=(const IOUringFileContext&) = delete;

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

Status IOUringTransport::install(std::string& local_segment_name,
                                 std::shared_ptr<ControlService> metadata,
                                 std::shared_ptr<Topology> local_topology,
                                 std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "IO Uring transport has been installed" LOC_MARK);
    }

    CHECK_STATUS(probeCapabilities());
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 1024) * 1024;
    caps.dram_to_file = true;
    if (Platform::getLoader().type() != "cpu") {
        caps.gpu_to_file = true;
    }
    return Status::OK();
}

Status IOUringTransport::probeCapabilities() {
    struct io_uring probe_ring;
    int rc = io_uring_queue_init(2, &probe_ring, 0);
    if (rc < 0) {
        LOG(INFO) << "IOUringTransport: io_uring_queue_init failed: "
                  << strerror(-rc);
        return Status::InternalError("io_uring not supported on this kernel");
    }
    io_uring_queue_exit(&probe_ring);
    return Status::OK();
}

Status IOUringTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status IOUringTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto io_uring_batch = Slab<IOUringSubBatch>::Get().allocate();
    if (!io_uring_batch)
        return Status::InternalError("Unable to allocate IO Uring sub-batch");
    io_uring_batch->max_size = max_size;
    io_uring_batch->task_list.reserve(max_size);
    int rc = io_uring_queue_init(max_size, &io_uring_batch->ring, 0);
    if (rc) {
        Slab<IOUringSubBatch>::Get().deallocate(io_uring_batch);
        return Status::InternalError(
            std::string("io_uring_queue_init failed: ") + strerror(-rc) +
            LOC_MARK);
    }
    batch = io_uring_batch;
    return Status::OK();
}

Status IOUringTransport::freeSubBatch(SubBatchRef& batch) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch*>(batch);
    if (!io_uring_batch)
        return Status::InvalidArgument("Invalid IO Uring sub-batch" LOC_MARK);
    io_uring_queue_exit(&io_uring_batch->ring);
    Slab<IOUringSubBatch>::Get().deallocate(io_uring_batch);
    batch = nullptr;
    return Status::OK();
}

std::string IOUringTransport::getIOUringFilePath(SegmentID target_id) {
    std::string ret;
    auto status = metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            if (segment->type != SegmentType::File)
                return Status::NeedsRefreshCache(
                    "Segment type is not File" LOC_MARK);
            auto& detail = std::get<FileSegmentDesc>(segment->detail);
            if (detail.buffers.empty())
                return Status::NeedsRefreshCache("No buffers found" LOC_MARK);
            ret = detail.buffers[0].path;
            return Status::OK();
        });
    if (!status.ok()) return "";
    return ret;
}

IOUringFileContext* IOUringTransport::findFileContext(SegmentID target_id) {
    thread_local FileContextMap tl_file_context_map;
    if (tl_file_context_map.count(target_id))
        return tl_file_context_map[target_id].get();

    RWSpinlock::WriteGuard guard(file_context_lock_);
    if (!file_context_map_.count(target_id)) {
        std::string path = getIOUringFilePath(target_id);
        if (path.empty()) return nullptr;
        file_context_map_[target_id] =
            std::make_shared<IOUringFileContext>(path);
    }

    tl_file_context_map = file_context_map_;
    return tl_file_context_map[target_id].get();
}

Status IOUringTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch*>(batch);
    if (!io_uring_batch)
        return Status::InvalidArgument("Invalid IO Uring sub-batch" LOC_MARK);
    if (request_list.size() + (int)io_uring_batch->task_list.size() >
        io_uring_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    const size_t kPageSize = 4096;

    // Phase 1: validate every request and stage alignment-fixing buffers
    // before touching the sub-batch or the SQ ring (all-or-nothing
    // submission contract, see Transport::submitTransferTasks). Any failure
    // here returns with the sub-batch, the SQ ring and the file untouched;
    // staging buffers allocated for earlier requests are freed.
    std::vector<IOUringFileContext*> contexts;
    contexts.reserve(request_list.size());
    std::vector<void*> staging_buffers;
    staging_buffers.reserve(request_list.size());
    for (auto& request : request_list) {
        if (request.opcode != Request::READ &&
            request.opcode != Request::WRITE) {
            for (void* buffer : staging_buffers) free(buffer);
            return Status::InvalidArgument("Unsupported opcode" LOC_MARK);
        }

        IOUringFileContext* context = findFileContext(request.target_id);
        if (!context || !context->ready()) {
            for (void* buffer : staging_buffers) free(buffer);
            return Status::InvalidArgument("Invalid remote segment" LOC_MARK);
        }
        contexts.push_back(context);

        if (Platform::getLoader().getMemoryType(request.source) == MTYPE_CUDA ||
            (uint64_t)request.source % kPageSize) {
            void* buffer = nullptr;
            int rc = posix_memalign(&buffer, kPageSize, request.length);
            if (rc) {
                for (void* staged : staging_buffers) free(staged);
                return Status::InternalError("posix_memalign failed" LOC_MARK);
            }
            staging_buffers.push_back(buffer);
        } else {
            staging_buffers.push_back(nullptr);
        }
    }

    // The capacity check above bounds the task count, but SQEs left queued
    // by a deferred submission (see below) may still occupy the ring.
    if (io_uring_sq_space_left(&io_uring_batch->ring) <
        (int)request_list.size()) {
        for (void* buffer : staging_buffers) free(buffer);
        return Status::TooManyRequests("Insufficient SQE space" LOC_MARK);
    }

    // Phase 2: commit. Tasks are appended and SQEs prepared; from here on
    // this call must not return an error. A positive short io_uring_submit()
    // means part of the batch is already in flight, and an error return
    // would make the engine's failover re-execute it. Keep flushing the
    // remainder instead; a persistent failure defers the flush of the
    // queued SQEs to the next submit attempt from getTransferStatus()'s
    // PENDING path while the tasks stay PENDING.
    for (size_t i = 0; i < request_list.size(); ++i) {
        const auto& request = request_list[i];
        io_uring_batch->task_list.push_back(IOUringTask{});
        auto& task =
            io_uring_batch->task_list[io_uring_batch->task_list.size() - 1];
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.buffer = staging_buffers[i];
        // Vectored ops with a single iovec: IORING_OP_READV/WRITEV have
        // existed since kernel 5.1, while the non-vectored READ/WRITE
        // opcodes used by io_uring_prep_read()/io_uring_prep_write() require
        // kernel 5.6+.
        task.iov = {task.buffer ? task.buffer : request.source, request.length};

        struct io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_batch->ring);
        CHECK(sqe) << "SQE space was verified before preparation";

        if (request.opcode == Request::READ) {
            io_uring_prep_readv(sqe, contexts[i]->getHandle(), &task.iov, 1,
                                request.target_offset);
        } else {
            if (task.buffer)
                Platform::getLoader().copy(task.buffer, request.source,
                                           request.length);
            io_uring_prep_writev(sqe, contexts[i]->getHandle(), &task.iov, 1,
                                 request.target_offset);
        }
        sqe->user_data = (uintptr_t)&task;
    }

    int submitted = 0;
    int transient_retries = 0;
    while (submitted < (int)request_list.size()) {
        int rc = submitSqes(&io_uring_batch->ring);
        if ((rc == -EINTR || rc == -EAGAIN) && transient_retries++ < 3)
            continue;
        if (rc <= 0) {
            LOG(ERROR) << "IOUringTransport: io_uring_submit "
                       << (rc < 0 ? strerror(-rc) : "made no progress")
                       << "; deferring " << request_list.size() - submitted
                       << " SQE(s) to the poll path";
            break;
        }
        submitted += rc;
    }

    return Status::OK();
}

Status IOUringTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                           TransferStatus& status) {
    auto io_uring_batch = dynamic_cast<IOUringSubBatch*>(batch);
    if (task_id < 0 || task_id >= (int)io_uring_batch->task_list.size())
        return Status::InvalidArgument("Invalid task ID");
    auto& task = io_uring_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    if (task.status_word == TransferStatusEnum::PENDING) {
        // Flush SQEs left queued by a deferred submission failure: calling
        // io_uring_submit() on an empty SQ ring is a no-op.
        submitSqes(&io_uring_batch->ring);
        struct io_uring_cqe* cqe = nullptr;
        int err = io_uring_peek_cqe(&io_uring_batch->ring, &cqe);
        if (err == -EAGAIN) return Status::OK();
        if (err || !cqe) {
            return Status::InternalError(
                std::string("io_uring_peek_cqe failed: ") + strerror(-err));
        }
        auto cqe_task = (IOUringTask*)cqe->user_data;
        if (cqe_task) {
            if (cqe->res < 0) {
                LOG(INFO) << "Received an event with error code " << cqe->res;
                cqe_task->status_word = TransferStatusEnum::FAILED;
            } else {
                if (cqe_task->buffer) {
                    if (cqe_task->request.opcode == Request::READ)
                        Platform::getLoader().copy(cqe_task->request.source,
                                                   cqe_task->buffer,
                                                   cqe_task->request.length);

                    free(cqe_task->buffer);
                    cqe_task->buffer = nullptr;
                }
                cqe_task->status_word = TransferStatusEnum::COMPLETED;
                cqe_task->transferred_bytes = cqe_task->request.length;
            }
        }
        io_uring_cqe_seen(&io_uring_batch->ring, cqe);
        batch->notifyProgress();
        status = TransferStatus{task.status_word, task.transferred_bytes};
    }
    return Status::OK();
}

Status IOUringTransport::addMemoryBuffer(BufferDesc& desc,
                                         const MemoryOptions& options) {
    return Status::OK();
}

Status IOUringTransport::removeMemoryBuffer(BufferDesc& desc) {
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
