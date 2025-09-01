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
#include <sys/mman.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "v1/common/status.h"
#include "v1/memory/slab.h"
#include "v1/metadata/metadata.h"
#include "v1/utility/random.h"
#include "v1/utility/string_builder.h"

namespace mooncake {
namespace v1 {

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
    installed_ = true;
    cxl_mount_path_ = conf_->get("transports/shm/cxl_mount_path", "");
    async_memcpy_threshold_ =
        conf_->get("transports/shm/async_memcpy_threshold", 4) * 1024;
    return setPeerAccess();
}

Status ShmTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        for (auto &relocate_map : relocate_map_) {
            for (auto &entry : relocate_map.second) {
                if (entry.second.is_cuda_ipc) {
#ifdef USE_CUDA
                    CHECK_CUDA(cudaIpcCloseMemHandle(entry.second.shm_addr));
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
    auto shm_batch = Slab<ShmSubBatch>::Get().allocate();
    if (!shm_batch)
        return Status::InternalError("Unable to allocate SHM sub-batch");
    batch = shm_batch;
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
#ifdef USE_CUDA
    CHECK_CUDA(cudaStreamCreate(&shm_batch->stream));
#endif
    return Status::OK();
}

Status ShmTransport::freeSubBatch(SubBatchRef &batch) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
#ifdef USE_CUDA
    CHECK_CUDA(cudaStreamDestroy(shm_batch->stream));
#endif
    Slab<ShmSubBatch>::Get().deallocate(shm_batch);
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
        startTransfer(&task, shm_batch);
    }
    return Status::OK();
}

void ShmTransport::startTransfer(ShmTask *task, ShmSubBatch *batch) {
#ifdef USE_CUDA
    // cudaSetDevice(task->cuda_id);
    cudaError_t err;
    void *src = nullptr, *dst = nullptr;

    // Determine direction and addresses
    if (task->request.opcode == Request::READ) {
        dst = task->request.source;       // read into source buffer
        src = (void *)task->target_addr;  // from remote
    } else {
        src = task->request.source;  // write from source buffer
        dst = (void *)task->target_addr;
    }

    bool is_async = (task->request.length >= async_memcpy_threshold_);

    // Determine memory types
    cudaPointerAttributes src_attr, dst_attr;
    cudaMemoryType src_type = cudaMemoryTypeHost;
    cudaMemoryType dst_type = cudaMemoryTypeHost;
    if (cudaPointerGetAttributes(&src_attr, src) == cudaSuccess)
        src_type = src_attr.type;
    if (cudaPointerGetAttributes(&dst_attr, dst) == cudaSuccess)
        dst_type = dst_attr.type;

    cudaMemcpyKind kind = cudaMemcpyDefault;  // let CUDA infer if possible
    if (src_type == cudaMemoryTypeDevice && dst_type == cudaMemoryTypeHost)
        kind = cudaMemcpyDeviceToHost;
    else if (src_type == cudaMemoryTypeHost && dst_type == cudaMemoryTypeDevice)
        kind = cudaMemcpyHostToDevice;
    else if (src_type == cudaMemoryTypeDevice &&
             dst_type == cudaMemoryTypeDevice)
        kind = cudaMemcpyDeviceToDevice;
    else if (src_type == cudaMemoryTypeHost && dst_type == cudaMemoryTypeHost)
        kind = cudaMemcpyHostToHost;

    // Select copy method
    if (kind == cudaMemcpyDefault) {
        memcpy(dst, src, task->request.length);
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else if (!is_async) {
        err = cudaMemcpy(dst, src, task->request.length, kind);
        if (err != cudaSuccess)
            task->status_word = TransferStatusEnum::FAILED;
        else {
            task->transferred_bytes = task->request.length;
            task->status_word = TransferStatusEnum::COMPLETED;
        }
    } else {
        err = cudaMemcpyAsync(dst, src, task->request.length, kind,
                              batch->stream);
        if (err != cudaSuccess) task->status_word = TransferStatusEnum::FAILED;
    }
#else
    if (task->request.opcode == Request::READ)
        memcpy(task->request.source, (void *)task->target_addr,
               task->request.length);
    else
        memcpy((void *)task->target_addr, task->request.source,
               task->request.length);
    task->transferred_bytes = task->request.length;
    task->status_word = TransferStatusEnum::COMPLETED;
#endif
}

Status ShmTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = shm_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
#ifdef USE_CUDA
    if (task.is_cuda_ipc && task.status_word == TransferStatusEnum::PENDING) {
        auto err = cudaStreamQuery(shm_batch->stream);
        if (err == cudaSuccess) {
            cudaStreamSynchronize(shm_batch->stream);
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (err != cudaErrorNotReady) {
            task.status_word = TransferStatusEnum::FAILED;
        }
    }
#endif
    return Status::OK();
}

Status ShmTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
#ifdef USE_CUDA
    if (parseLocation(desc.location).first == "cuda") {
        // If the memory region is allocated using cuMemAlloc,
        // we cannot use cudaIpcGetMemHandle, so skip it
        if (options.type == MNNVL) return Status::OK();
        cudaIpcMemHandle_t handle;
        CHECK_CUDA(cudaIpcGetMemHandle(&handle, (void *)desc.addr));
        desc.shm_path =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        return Status::OK();
    }
    if (parseLocation(desc.location).first == "cpu")
        CHECK_CUDA(cudaHostRegister(((void *)desc.addr), desc.length,
                                    cudaHostRegisterDefault));
#endif
    if (options.shm_path.empty())
        return Status::OK();  // Return silently but not regard it as valid
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
#ifdef USE_CUDA
    if (parseLocation(desc.location).first == "cpu") {
        cudaHostUnregister((void *)desc.addr);
    }
#endif
    return Status::OK();
}

static inline std::string randomFileName() {
    std::string result = "mooncake_";
    for (int i = 0; i < 8; ++i) result += 'a' + SimpleRandom::Get().next(26);
    return result;
}

static inline std::string joinPath(const std::string &path,
                                   const std::string &filename) {
    if (path.empty()) return filename;
    if (path[path.size() - 1] == '/') return path + filename;
    return path + "/" + filename;
}

Status ShmTransport::allocateLocalMemory(void **addr, size_t size,
                                         MemoryOptions &options) {
    if (parseLocation(options.location).first == "cuda") {
        return genericAllocateLocalMemory(addr, size, options);
    }
    options.shm_path = randomFileName();
    options.shm_offset = 0;
    *addr = createSharedMemory(options.shm_path, size);
    if (!(*addr)) {
        return Status::InternalError("Failed to allocate shared memory");
    }
    return Status::OK();
}

Status ShmTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    if (!shm_path_map_.count(addr)) {
        return genericFreeLocalMemory(addr, size);
    }
    munmap(addr, size);
    if (cxl_mount_path_.empty())
        shm_unlink(shm_path_map_[addr].c_str());
    else {
        auto full_path = joinPath(cxl_mount_path_, shm_path_map_[addr]);
        unlink(full_path.c_str());
    }
    shm_path_map_.erase(addr);
    return Status::OK();
}

void *ShmTransport::createSharedMemory(const std::string &path, size_t size) {
    int shm_fd = -1;
    if (cxl_mount_path_.empty())
        shm_fd = shm_open(path.c_str(), O_CREAT | O_RDWR, 0644);
    else {
        auto full_path = joinPath(cxl_mount_path_, path);
        shm_fd = open(full_path.c_str(), O_CREAT | O_RDWR, 0644);
    }
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
    thread_local HashMap tl_relocate_map;
    if (tl_relocate_map.empty()) {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        tl_relocate_map = relocate_map_;
    }

    auto &relocate_map = tl_relocate_map[target_id];
    for (auto &entry : relocate_map) {
        if (entry.first <= dest_addr &&
            dest_addr + length <= entry.first + entry.second.length) {
            auto shm_addr = entry.second.shm_addr;
            dest_addr = dest_addr - entry.first + ((uint64_t)shm_addr);
            is_cuda_ipc = entry.second.is_cuda_ipc;
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;

    auto buffer = getBufferDesc(desc, dest_addr, length);
    if (!buffer || buffer->shm_path.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    if (!relocate_map.count(buffer->addr)) {
        void *shm_addr = nullptr;
        auto location = parseLocation(buffer->location);
        if (location.first == "cuda") {
#ifdef USE_CUDA
            std::vector<unsigned char> output_buffer;
            deserializeBinaryData(buffer->shm_path, output_buffer);
            cudaIpcMemHandle_t handle;
            memcpy(&handle, output_buffer.data(), sizeof(handle));
            CHECK_CUDA(cudaIpcOpenMemHandle(&shm_addr, handle,
                                            cudaIpcMemLazyEnablePeerAccess));
            OpenedShmEntry shm_entry;
            shm_entry.shm_fd = -1;
            shm_entry.shm_addr = shm_addr;
            shm_entry.length = buffer->length;
            shm_entry.is_cuda_ipc = true;
            shm_entry.cuda_id = location.second;
            relocate_map[buffer->addr] = shm_entry;
#else
            return Status::NotImplemented(
                "CUDA supported not enabled in this package " LOC_MARK);
#endif
        } else {
            int shm_fd = -1;
            if (cxl_mount_path_.empty())
                shm_fd = shm_open(buffer->shm_path.c_str(), O_RDWR, 0644);
            else {
                auto full_path = joinPath(cxl_mount_path_, buffer->shm_path);
                shm_fd = open(full_path.c_str(), O_CREAT | O_RDWR, 0644);
            }
            if (shm_fd < 0) {
                return Status::InternalError(
                    std::string("Failed to open shared memory file ") +
                    buffer->shm_path + LOC_MARK);
            }
            shm_addr = mmap(nullptr, buffer->length, PROT_READ | PROT_WRITE,
                            MAP_SHARED, shm_fd, 0);
            if (shm_addr == MAP_FAILED) {
                close(shm_fd);
                return Status::InternalError(
                    "Failed to map shared memory " LOC_MARK);
            }
            LOG(INFO) << "Original shared memory: " << (void *)buffer->addr
                      << "--" << (void *)(buffer->addr + buffer->length);
            LOG(INFO) << "Remapped shared memory: " << (void *)shm_addr << "--"
                      << (void *)((uintptr_t)shm_addr + buffer->length);
            OpenedShmEntry shm_entry;
            shm_entry.shm_fd = shm_fd;
            shm_entry.shm_addr = shm_addr;
            shm_entry.length = buffer->length;
            shm_entry.is_cuda_ipc = false;
            relocate_map[buffer->addr] = shm_entry;
        }
    }

    auto shm_addr = relocate_map[buffer->addr].shm_addr;
    dest_addr = dest_addr - buffer->addr + ((uint64_t)shm_addr);
    is_cuda_ipc = relocate_map[buffer->addr].is_cuda_ipc;
    return Status::OK();
}

Status ShmTransport::setPeerAccess() {
#ifdef USE_CUDA
    int device_count = 0;
    CHECK_CUDA(cudaGetDeviceCount(&device_count));
    if (device_count < 2) return Status::OK();
    for (int i = 0; i < device_count; ++i) {
        cudaSetDevice(i);
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            cudaDeviceCanAccessPeer(&can_access, i, j);
            if (!can_access) {
                continue;
            }
            cudaError_t err = cudaDeviceEnablePeerAccess(j, 0);
            if (err != cudaSuccess) {
                if (err == cudaErrorPeerAccessAlreadyEnabled) {
                    cudaGetLastError();
                } else {
                    return Status::InternalError(
                        "cudaDeviceEnablePeerAccess failed");
                }
            }
        }
    }
    return Status::OK();
#else
    return Status::OK();
#endif
}
}  // namespace v1
}  // namespace mooncake
