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

#include "tent/transport/shm/shm_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>
#include <sys/mman.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {

ShmTransport::ShmTransport() : installed_(false) {}

ShmTransport::~ShmTransport() { uninstall(); }

Status ShmTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
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
    caps.dram_to_dram = true;
    return Status::OK();
}

Status ShmTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        for (auto &relocate_map : relocate_map_) {
            for (auto &entry : relocate_map.second) {
                munmap(entry.second.shm_addr, entry.second.length);
                close(entry.second.shm_fd);
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
    return Status::OK();
}

Status ShmTransport::freeSubBatch(SubBatchRef &batch) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
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
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task, shm_batch);
    }
    return Status::OK();
}

void ShmTransport::startTransfer(ShmTask *task, ShmSubBatch *batch) {
    Status status;
    if (task->request.opcode == Request::READ)
        status = Platform::getLoader().copy(task->request.source,
                                            (void *)task->target_addr,
                                            task->request.length);
    else
        status = Platform::getLoader().copy((void *)task->target_addr,
                                            task->request.source,
                                            task->request.length);
    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        task->status_word = TransferStatusEnum::FAILED;
    }
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

Status ShmTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    if (options.shm_path.empty())
        return Status::OK();  // Return silently but not regard it as valid
                              // buffer for shared memory transport
    desc.shm_path = options.shm_path;
    desc.transports.push_back(TransportType::SHM);
    // desc.shm_offset = options.shm_offset;
    LOG(INFO) << "Registered shared memory: " << (void *)desc.addr << "--"
              << (void *)(desc.addr + desc.length);
    return Status::OK();
}

Status ShmTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.shm_path.clear();
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
    LocationParser location(options.location);
    if (location.type() != "cpu") {
        return Status::InvalidArgument("ShmTransport allocates DRAM only");
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
        return Status::InvalidArgument("Memory not allocated by ShmTransport");
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
                                                 uint64_t target_id) {
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
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;

    auto buffer = desc->findBuffer(dest_addr, length);
    if (!buffer || buffer->shm_path.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    if (!relocate_map.count(buffer->addr)) {
        void *shm_addr = nullptr;
        LocationParser location(buffer->location);
        if (location.type() == "cuda") {
            return Status::NotImplemented(
                "CUDA supported not enabled in this package " LOC_MARK);
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
            relocate_map[buffer->addr] = shm_entry;
        }
    }

    auto shm_addr = relocate_map[buffer->addr].shm_addr;
    dest_addr = dest_addr - buffer->addr + ((uint64_t)shm_addr);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
