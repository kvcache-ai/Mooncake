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

#include "v1/transport/shm/shm_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#endif

#include "v1/common.h"
#include "v1/metadata/metadata.h"

namespace mooncake {
namespace v1 {

class ShmThreadPool {
   public:
    ShmThreadPool(size_t threadCount)
        : ioService_(),
          work_(asio::make_work_guard(ioService_)),
          stopped_(false) {
        for (size_t i = 0; i < threadCount; ++i) {
            threads_.create_thread(
                boost::bind(&asio::io_service::run, &ioService_));
        }
    }

    ~ShmThreadPool() { stop(); }

    void submit(std::function<void()> task) {
        ioService_.post(std::move(task));
    }

    void stop() {
        if (!stopped_) {
            stopped_ = true;
            ioService_.stop();
            threads_.join_all();
        }
    }

   private:
    asio::io_service ioService_;
    asio::executor_work_guard<asio::io_service::executor_type> work_;
    boost::thread_group threads_;
    bool stopped_;
};

ShmTransport::ShmTransport() : installed_(false) {}

ShmTransport::~ShmTransport() { uninstall(); }

Status ShmTransport::install(std::string &local_segment_name,
                             std::shared_ptr<MetadataService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "SHM transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;

    const static size_t kDefaultThreadPoolSize = 1;
    workers_ = std::make_unique<ShmThreadPool>(kDefaultThreadPoolSize);

    installed_ = true;
    return Status::OK();
}

Status ShmTransport::uninstall() {
    if (installed_) {
        workers_->stop();
        workers_.reset();
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status ShmTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto shm_batch = new ShmSubBatch();
    batch = shm_batch;
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
    return Status::OK();
}

Status ShmTransport::freeSubBatch(SubBatchRef &batch) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
    delete shm_batch;
    batch = nullptr;
    return Status::OK();
}

Status ShmTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
    if (request_list.size() + shm_batch->task_list.size() > shm_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        shm_batch->task_list.push_back(ShmTask{});
        auto &task = shm_batch->task_list[shm_batch->task_list.size() - 1];
        uint64_t target_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task);
    }
    return Status::OK();
}

void ShmTransport::startTransfer(ShmTask *task) {
    workers_->submit([task]() {
#ifdef USE_CUDA
        if (task->request.target_id == LOCAL_SEGMENT_ID) {
            if (task->request.opcode == Request::READ)
                cudaMemcpy(slice->source_addr, (void *)task->target_addr,
                           slice->length, cudaMemcpyDefault);
            else
                cudaMemcpy((void *)task->target_addr, slice->source_addr,
                           slice->length, cudaMemcpyDefault);
        }
#else
        if (task->request.opcode == Request::READ)
            memcpy(task->request.source, (void *)task->target_addr,
                   task->request.length);
        else
            memcpy((void *)task->target_addr, task->request.source,
                   task->request.length);
#endif
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    });
}

Status ShmTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = shm_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

void ShmTransport::queryOutstandingTasks(SubBatchRef batch,
                                         std::vector<int> &task_id_list) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch) return;
    for (int task_id = 0; task_id < (int)shm_batch->task_list.size();
         ++task_id) {
        auto &task = shm_batch->task_list[task_id];
        if (task.status_word != TransferStatusEnum::COMPLETED) {
            task_id_list.push_back(task_id);
        }
    }
}

Status ShmTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    return Status::OK();
}

Status ShmTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

static inline std::string makeRandomMmapFileName(const std::string &parent) {
    std::string result = parent;
    if (result.empty()) result = "/dev/shm/";
    if (result[result.size() - 1] != '/') result += '/';
    result += "mooncake_";
    for (int i = 0; i < 8; ++i) result += 'a' + SimpleRandom::Get().next(26);
    return result;
}

Status ShmTransport::allocateLocalMemory(BufferEntry &buffer, size_t size,
                                         const Location &location) {
    buffer.length = size;
    buffer.location = location;
    auto base_path = conf_->get("transports/shm/shm_base_path", "/dev/shm/");
    buffer.shm_path = makeRandomMmapFileName(base_path);
    buffer.addr = createSharedMemory(buffer.shm_path, size);
    if (!buffer.addr) {
        return Status::InternalError("Failed to allocate shared memory");
    }
    return Status::OK();
}

Status ShmTransport::freeLocalMemory(const BufferEntry &buffer) {
    munmap(buffer.addr, buffer.length);
    return Status::OK();
}

void *ShmTransport::createSharedMemory(const std::string &path, size_t size) {
    int shm_fd = shm_open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (shm_fd == -1) {
        PLOG(ERROR) << "Failed to open shared memory file";
        return nullptr;
    }

    if (ftruncate64(shm_fd, size) == -1) {
        PLOG(ERROR) << "Failed to truncate shared memory file";
        close(shm_fd);
        return nullptr;
    }

    void *mapped_addr =
        mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped_addr == MAP_FAILED) {
        PLOG(ERROR) << "Failed to map shared memory file";
        close(shm_fd);
        return nullptr;
    }

    close(shm_fd);
    return mapped_addr;
}

Status ShmTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    SegmentDescRef desc;
    auto status = metadata_->segmentManager().getRemote(desc, target_id);
    if (!status.ok()) return status;
    int index = 0;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.shm_path.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            std::lock_guard<std::mutex> lock(relocate_mutex_);
            if (!relocate_map_.count(entry.addr)) {
                int shm_fd =
                    shm_open(entry.shm_path.c_str(), O_RDWR, 0644);
                if (shm_fd < 0) {
                    return Status::InternalError(
                        std::string("Failed to open shared memory file ") +
                        entry.shm_path + LOC_MARK);
                }
                auto shm_addr = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                                     MAP_SHARED, shm_fd, 0);
                if (shm_addr == MAP_FAILED) {
                    close(shm_fd);
                    return Status::InternalError(
                        "Failed to map shared memory " LOC_MARK);
                }
                OpenedShmEntry shm_entry;
                shm_entry.shm_fd = shm_fd;
                shm_entry.shm_addr = shm_addr;
                shm_entry.length = length;
                relocate_map_[entry.addr] = shm_entry;
            }
            auto shm_addr = relocate_map_[entry.addr].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return Status::OK();
        }
        index++;
    }
    return Status::InvalidArgument(
        "Requested address is not in registered buffer" LOC_MARK);
}
}  // namespace v1
}  // namespace mooncake
