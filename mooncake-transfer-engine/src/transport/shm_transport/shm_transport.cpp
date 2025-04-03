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

#include "transport/shm_transport/shm_transport.h"

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

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
const static size_t kDefaultThreadPoolSize = 4;

ShmTransport::ShmTransport() : thread_pool_(kDefaultThreadPoolSize) {}

ShmTransport::~ShmTransport() {
    for (auto &entry : remap_entries_) {
        munmap(entry.second.shm_addr, entry.second.length);
        close(entry.second.shm_fd);
    }
    remap_entries_.clear();
}

int ShmTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> metadata,
                          std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "shm";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status ShmTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("memory not registered as mmap");
        }
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        startTransfer(slice);
    }
    return Status::OK();
}

void ShmTransport::startTransfer(Slice *slice) {
    thread_pool_.submit([slice]() {
#ifdef USE_CUDA
        if (slice->target_id == LOCAL_SEGMENT_ID) {
            if (slice->opcode == TransferRequest::READ)
                cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                           slice->length, cudaMemcpyDefault);
            else
                cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                           slice->length, cudaMemcpyDefault);
        }
#else
        if (slice->opcode == TransferRequest::READ)
            memcpy(slice->source_addr, (void *)slice->local.dest_addr,
                   slice->length);
        else
            memcpy((void *)slice->local.dest_addr, slice->source_addr,
                   slice->length);
#endif
        slice->markSuccess();
    });
}

int ShmTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata,
                                      const std::string &shm_path) {
    (void)remote_accessible;
    BufferDesc desc;
    desc.addr = (uint64_t)addr;
    desc.length = length;
    desc.location = location;
    desc.shm_path = shm_path;
    return metadata_->addLocalMemoryBuffer(desc, true);
}

int ShmTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
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

int ShmTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                              uint64_t length,
                                              uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.shm_path.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            if (!remap_entries_.count(entry.addr)) {
                int shm_fd = shm_open(entry.shm_path.c_str(), O_RDWR, 0644);
                if (shm_fd < 0) {
                    PLOG(ERROR) << "Failed to open shared memory file: "
                                << entry.shm_path;
                    return ERR_MEMORY;
                }
                auto shm_addr = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                                     MAP_SHARED, shm_fd, 0);
                if (shm_addr == MAP_FAILED) {
                    PLOG(ERROR) << "Failed to map shared memory file: "
                                << entry.shm_path;
                    close(shm_fd);
                    return ERR_MEMORY;
                }
                OpenedShmEntry shm_entry;
                shm_entry.shm_fd = shm_fd;
                shm_entry.shm_addr = shm_addr;
                shm_entry.length = length;
                remap_entries_[entry.addr] = shm_entry;
            }
            auto shm_addr = remap_entries_[entry.addr].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    return ERR_INVALID_ARGUMENT;
}

int ShmTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false,
                            buffer.shm_path);
    return metadata_->updateLocalSegmentDesc();
}

int ShmTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}
}  // namespace mooncake
