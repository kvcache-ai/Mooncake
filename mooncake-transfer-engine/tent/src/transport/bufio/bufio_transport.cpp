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

#include "tent/transport/bufio/bufio_transport.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>

#include "tent/common/utils/os.h"
#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {

class BufIoFileContext {
   public:
    explicit BufIoFileContext(const std::string& path)
        : fd_(-1), ready_(false) {
        fd_ = ::open(path.c_str(), O_RDWR);
        if (fd_ < 0) {
            PLOG(ERROR) << "BufIoTransport: failed to open file " << path;
            return;
        }
        ready_ = true;
    }

    BufIoFileContext(const BufIoFileContext&) = delete;
    BufIoFileContext& operator=(const BufIoFileContext&) = delete;

    ~BufIoFileContext() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    int getHandle() const { return fd_; }
    bool ready() const { return ready_; }

   private:
    int fd_;
    bool ready_;
};

BufIoTransport::BufIoTransport() : installed_(false) {}

BufIoTransport::~BufIoTransport() { uninstall(); }

Status BufIoTransport::install(std::string& local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "BufIo transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;

    installed_ = true;
    caps.dram_to_file = true;
    if (Platform::getLoader().type() == "cuda") {
        caps.gpu_to_file = true;
    }

    return Status::OK();
}

Status BufIoTransport::uninstall() {
    if (installed_) {
        RWSpinlock::WriteGuard guard(file_context_lock_);
        file_context_map_.clear();
        metadata_.reset();
        local_topology_.reset();
        conf_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status BufIoTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    if (!installed_) {
        return Status::InvalidArgument(
            "BufIo transport is not installed" LOC_MARK);
    }

    auto* buf_batch = new (std::nothrow) BufIoSubBatch();
    if (!buf_batch) {
        return Status::InternalError(
            "Unable to allocate BufIo sub-batch" LOC_MARK);
    }
    buf_batch->max_size = max_size;
    buf_batch->task_list.reserve(max_size);
    batch = buf_batch;
    return Status::OK();
}

Status BufIoTransport::freeSubBatch(SubBatchRef& batch) {
    auto* buf_batch = dynamic_cast<BufIoSubBatch*>(batch);
    if (!buf_batch) {
        return Status::InvalidArgument("Invalid BufIo sub-batch" LOC_MARK);
    }
    delete buf_batch;
    batch = nullptr;
    return Status::OK();
}

std::string BufIoTransport::getBufIoFilePath(SegmentID target_id) {
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok() || desc->type != SegmentType::File) return "";
    auto& detail = std::get<FileSegmentDesc>(desc->detail);
    if (detail.buffers.empty()) return "";
    return detail.buffers[0].path;
}

BufIoFileContext* BufIoTransport::findFileContext(SegmentID target_id) {
    thread_local FileContextMap tl_file_context_map;

    auto it = tl_file_context_map.find(target_id);
    if (it != tl_file_context_map.end()) {
        return it->second.get();
    }

    {
        RWSpinlock::WriteGuard guard(file_context_lock_);
        auto git = file_context_map_.find(target_id);
        if (git == file_context_map_.end()) {
            std::string path = getBufIoFilePath(target_id);
            if (path.empty()) return nullptr;
            auto ctx = std::make_shared<BufIoFileContext>(path);
            if (!ctx->ready()) {
                return nullptr;
            }
            file_context_map_[target_id] = ctx;
            git = file_context_map_.find(target_id);
        }
        tl_file_context_map = file_context_map_;
        return git->second.get();
    }
}

Status BufIoTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* buf_batch = dynamic_cast<BufIoSubBatch*>(batch);
    if (!buf_batch) {
        return Status::InvalidArgument("Invalid BufIo sub-batch" LOC_MARK);
    }

    if (request_list.size() + buf_batch->task_list.size() >
        buf_batch->max_size) {
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    }

    auto& platform = Platform::getLoader();

    for (const auto& req : request_list) {
        buf_batch->task_list.emplace_back();
        auto& task = buf_batch->task_list.back();
        task.request = req;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;

        BufIoFileContext* ctx = findFileContext(req.target_id);
        if (!ctx || !ctx->ready()) {
            task.status_word = TransferStatusEnum::FAILED;
            LOG(WARNING) << "BufIoTransport: invalid remote segment for id "
                         << req.target_id;
            continue;
        }

        int fd = ctx->getHandle();
        if (fd < 0) {
            task.status_word = TransferStatusEnum::FAILED;
            LOG(WARNING) << "BufIoTransport: invalid file descriptor";
            continue;
        }

        MemoryType mtype = platform.getMemoryType(req.source);
        ssize_t io_ret = 0;

        if (req.opcode == Request::READ) {
            if (mtype == MTYPE_CUDA) {
                std::unique_ptr<uint8_t[]> host_buf(new (std::nothrow)
                                                        uint8_t[req.length]);
                if (!host_buf) {
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }

                io_ret = ::pread(fd, host_buf.get(), req.length,
                                 static_cast<off_t>(req.target_offset));
                if (io_ret < 0) {
                    PLOG(WARNING) << "BufIoTransport: pread failed";
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }

                platform.copy(req.source, host_buf.get(),
                              static_cast<size_t>(io_ret));
            } else {
                io_ret = ::pread(fd, req.source, req.length,
                                 static_cast<off_t>(req.target_offset));
                if (io_ret < 0) {
                    PLOG(WARNING) << "BufIoTransport: pread failed";
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }
            }
        } else if (req.opcode == Request::WRITE) {
            if (mtype == MTYPE_CUDA) {
                std::unique_ptr<uint8_t[]> host_buf(new (std::nothrow)
                                                        uint8_t[req.length]);
                if (!host_buf) {
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }

                platform.copy(host_buf.get(), req.source, req.length);

                io_ret = ::pwrite(fd, host_buf.get(), req.length,
                                  static_cast<off_t>(req.target_offset));
                if (io_ret < 0) {
                    PLOG(WARNING) << "BufIoTransport: pwrite failed";
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }
            } else {
                io_ret = ::pwrite(fd, req.source, req.length,
                                  static_cast<off_t>(req.target_offset));
                if (io_ret < 0) {
                    PLOG(WARNING) << "BufIoTransport: pwrite failed";
                    task.status_word = TransferStatusEnum::FAILED;
                    continue;
                }
            }
        } else {
            task.status_word = TransferStatusEnum::FAILED;
            LOG(WARNING) << "BufIoTransport: unknown opcode " << req.opcode;
            continue;
        }

        task.transferred_bytes = static_cast<size_t>(io_ret < 0 ? 0 : io_ret);
        task.status_word = (io_ret < 0) ? TransferStatusEnum::FAILED
                                        : TransferStatusEnum::COMPLETED;
    }

    return Status::OK();
}

Status BufIoTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus& status) {
    auto* buf_batch = dynamic_cast<BufIoSubBatch*>(batch);
    if (!buf_batch) {
        return Status::InvalidArgument("Invalid BufIo sub-batch" LOC_MARK);
    }
    if (task_id < 0 || task_id >= (int)buf_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID");
    }
    auto& task = buf_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status BufIoTransport::addMemoryBuffer(BufferDesc& /*desc*/,
                                       const MemoryOptions& /*options*/) {
    return Status::OK();
}

Status BufIoTransport::removeMemoryBuffer(BufferDesc& /*desc*/) {
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
