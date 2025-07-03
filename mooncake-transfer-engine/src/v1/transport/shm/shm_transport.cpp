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
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;

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
        for (auto &relocate_map : relocate_map_) {
            for (auto &entry : relocate_map.second) {
                if (entry.second.is_cuda_ipc) {
#ifdef USE_CUDA
                    cudaIpcCloseMemHandle(entry.second.shm_addr);
#endif
                } else {
                    munmap(entry.second.shm_addr, entry.second.length);
                    close(entry.second.shm_fd);
                }
            }
        }
        relocate_map_.clear();
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
        bool is_cuda_ipc = true;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id, is_cuda_ipc);
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.is_cuda_ipc = is_cuda_ipc;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task);
    }
    return Status::OK();
}

void ShmTransport::startTransfer(ShmTask *task) {
    workers_->submit([task]() {
        if (task->is_cuda_ipc) {
#ifdef USE_CUDA
            cudaError_t err;
            if (task->request.opcode == Request::READ)
                err =
                    cudaMemcpy(task->request.source, (void *)task->target_addr,
                               task->request.length, cudaMemcpyDefault);
            else
                err =
                    cudaMemcpy((void *)task->target_addr, task->request.source,
                               task->request.length, cudaMemcpyDefault);
            if (err == cudaSuccess) {
                task->transferred_bytes = task->request.length;
                task->status_word = TransferStatusEnum::COMPLETED;
            } else
                task->status_word = TransferStatusEnum::FAILED;
#else
            assert(0);
#endif
        } else {
            if (task->request.opcode == Request::READ)
                memcpy(task->request.source, (void *)task->target_addr,
                       task->request.length);
            else
                memcpy((void *)task->target_addr, task->request.source,
                       task->request.length);
            task->transferred_bytes = task->request.length;
            task->status_word = TransferStatusEnum::COMPLETED;
        }
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
#ifdef USE_CUDA
    if (desc.location.starts_with("cuda")) {
        cudaIpcMemHandle_t handle;
        auto err = cudaIpcGetMemHandle(&handle, desc.addr);
        if (err != cudaSuccess) {
            return Status::InternalError("Failed to get cuda memory handle");
        }
        desc.shm_path =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        return Status::OK();
    }
#endif
    if (options.shm_path.empty())
        return Status::OK();  // Return sliently but not regard it as valid
                              // buffer for shared memory transport
    desc.shm_path = options.shm_path;
    // desc.shm_offset = options.shm_offset;
    LOG(INFO) << "Registered shared memory: " << (void *)desc.addr << "--"
              << (void *)(desc.addr + desc.length);
    return Status::OK();
}

Status ShmTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.shm_path.clear();
    // desc.shm_offset = 0;
    return Status::OK();
}

static inline std::string makeRandomMmapFileName(const std::string &parent) {
    std::string result = parent;
    if (!result.empty() && result[result.size() - 1] != '/') result += '/';
    result += "mooncake_";
    for (int i = 0; i < 8; ++i) result += 'a' + SimpleRandom::Get().next(26);
    return result;
}

Status ShmTransport::allocateLocalMemory(void **addr, size_t size,
                                         MemoryOptions &options) {
    auto base_path = conf_->get("transports/shm/shm_base_path", "");
    options.shm_path = makeRandomMmapFileName(base_path);
    options.shm_offset = 0;
    *addr = createSharedMemory(options.shm_path, size);
    if (!(*addr)) {
        return Status::InternalError("Failed to allocate shared memory");
    }
    return Status::OK();
}

Status ShmTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    if (!shm_path_map_.count(addr)) return Status::OK();
    munmap(addr, size);
    shm_unlink(shm_path_map_[addr].c_str());
    shm_path_map_.erase(addr);
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
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    shm_path_map_[mapped_addr] = path;
    return mapped_addr;
}

Status ShmTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id,
                                                 bool &is_cuda_ipc) {
    {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        auto &relocate_map = relocate_map_[target_id];
        for (auto &entry : relocate_map) {
            if (entry.first <= dest_addr &&
                dest_addr + length <= entry.first + entry.second.length) {
                auto shm_addr = entry.second.shm_addr;
                dest_addr = dest_addr - entry.first + ((uint64_t)shm_addr);
                is_cuda_ipc = entry.second.is_cuda_ipc;
                return Status::OK();
            }
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDescRef desc;
    auto status = metadata_->segmentManager().getRemote(desc, target_id);
    if (!status.ok()) return status;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.shm_path.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            auto &relocate_map = relocate_map_[target_id];
            if (!relocate_map.count(entry.addr)) {
                void *shm_addr = nullptr;
                if (entry.location.starts_with("cuda")) {
#ifdef USE_CUDA
                    std::vector<unsigned char> output_buffer;
                    deserializeBinaryData(entry.shm_name, output_buffer);
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    auto err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR) << "Failed to open cuda memory handle: "
                                   << cudaGetErrorString(err);
                        return Status::InternalError(
                            "Failed to open cuda memory handle " LOC_MARK);
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_fd = -1;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    shm_entry.is_cuda_ipc = true;
                    relocate_map[entry.addr] = shm_entry;
#else
                    return Status::NotImplemented(
                        "CUDA supported not enabled in this package " LOC_MARK);
#endif
                } else {
                    int shm_fd = shm_open(entry.shm_path.c_str(), O_RDWR, 0644);
                    if (shm_fd < 0) {
                        return Status::InternalError(
                            std::string("Failed to open shared memory file ") +
                            entry.shm_path + LOC_MARK);
                    }
                    shm_addr =
                        mmap(nullptr, entry.length, PROT_READ | PROT_WRITE,
                             MAP_SHARED, shm_fd, 0);
                    if (shm_addr == MAP_FAILED) {
                        close(shm_fd);
                        return Status::InternalError(
                            "Failed to map shared memory " LOC_MARK);
                    }
                    LOG(INFO)
                        << "Original shared memory: " << (void *)entry.addr
                        << "--" << (void *)(entry.addr + entry.length);
                    LOG(INFO)
                        << "Remapped shared memory: " << (void *)shm_addr
                        << "--" << (void *)((uintptr_t)shm_addr + entry.length);
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_fd = shm_fd;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    shm_entry.is_cuda_ipc = false;
                    relocate_map[entry.addr] = shm_entry;
                }
            }
            auto shm_addr = relocate_map[entry.addr].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            is_cuda_ipc = relocate_map[entry.addr].is_cuda_ipc;
            // LOG(INFO) << desc.use_count();
            return Status::OK();
        }
    }
    return Status::InvalidArgument(
        "Requested address is not in registered buffer" LOC_MARK);
}

bool ShmTransport::taskSupported(const Request &request) {
    if (request.target_id == LOCAL_SEGMENT_ID) return true;
    SegmentDescRef desc;
    auto status =
        metadata_->segmentManager().getRemote(desc, request.target_id);
    if (!status.ok()) return false;
    if (desc->machine_id != machine_id_) return false;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.shm_path.empty() && entry.addr <= request.target_offset &&
            request.target_offset + request.length <=
                entry.addr + entry.length) {
            return true;
        }
    }
    return false;
}
}  // namespace v1
}  // namespace mooncake
