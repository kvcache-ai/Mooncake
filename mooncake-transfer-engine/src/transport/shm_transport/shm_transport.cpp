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

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"


#include <sys/mman.h>
#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <map>
#include <vector>
#include <errno.h>
#include <cstring>
//create shared mem
//one batch[taskid] -> one shared mem

namespace mooncake {
ShmTransport::ShmTransport() {
    // TODO
    //SharedMem_map_ = std::unordered_map<void*, size_t>{{nullptr, nullptr}}; 
    //metadata_ = std::make_shared<TransferMetadata>(); // 在构造函数中初始化 metadata_
}

ShmTransport::~ShmTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();

    // Clean up batches
    SharedMem_map_.clear();
}

void ShmTransport::createSharedMem(void* addr, size_t size, const std::string& location) {
    // 使用 shm_open 创建或打开一个共享内存对象
    int shm_fd = shm_open(location.c_str(), O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        return;
    }

    // 设置共享内存的大小
    if (ftruncate(shm_fd, size) == -1) {
        perror("ftruncate");
        close(shm_fd);
        return;
    }

    // 使用 mmap 将共享内存对象映射到进程的地址空间
    int prot = PROT_READ | PROT_WRITE;
    int flags = MAP_SHARED | MAP_FIXED;
    void *mapped = mmap(addr, size, prot, flags, shm_fd, 0);
    if (mapped == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        return;
    }

    // 检查映射地址是否与期望的地址一致
    if (mapped != addr) {
        fprintf(stderr, "mmap did not map at the desired address.\n");
        munmap(mapped, size);
        close(shm_fd);
        return;
    }

    // 关闭文件描述符，mmap 仍然保持映射
    close(shm_fd);

    return;
}



void ShmTransport::delete_shared_memory(void *addr) {
    size_t length = SharedMem_map_[addr]; 
    if (addr == NULL) {
        fprintf(stderr, "Invalid arguments.\n");
        return;
    }
    // 解除映射
    if (munmap(addr, length) == -1) {
        perror("munmap");
        // 处理错误
    }
}
ShmTransport::BatchID ShmTransport::allocateBatchID(size_t batch_size) {
    //auto shm_desc = new BatchDesc();
    auto batch_id = Transport::allocateBatchID(batch_size);
    auto &batch_desc = *((BatchDesc *)(batch_id));
    batch_desc.batch_size = batch_size;
    batch_desc.task_list.resize(batch_size);
    batch_desc.id = batch_id;
    //batch_desc.context = shm_desc;
    
    return batch_id;
}

int ShmTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                    TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) return ERR_INVALID_ARGUMENT;
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count ==
        (uint64_t)task.slices.size()) {
        if (failed_slice_count)
            status.s = TransferStatusEnum::FAILED;
        else
            status.s = TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return 0;
}

int ShmTransport::submitTransfer(BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));

    // Check if adding new entries would exceed the batch size
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size){
        LOG(ERROR) << "TcpTransport: Exceed the limitation of current batch's "
                      "capacity";
        return ERR_TOO_MANY_REQUESTS;
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    for (const auto& request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        auto target_id = request.target_id;
        ++task_id;
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->local.dest_addr = (void*)request.target_offset;
        slice->status = Slice::PENDING;
        task.slices.push_back(slice);
        startSlice(slice);
    }

    return 0;
}

int ShmTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->local.dest_addr = (void*)request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slices.push_back(slice);
        startSlice(slice);
    }
    return 0;
}

int ShmTransport::freeBatchID(BatchID batch_id) { 
    auto &batch_desc = *((BatchDesc *)(batch_id));
    //auto &nvmeof_desc = *((NVMeoFBatchDesc *)(batch_desc.context));
    //int desc_idx = nvmeof_desc.desc_idx_;
    int rc = Transport::freeBatchID(batch_id);
    if (rc < 0) {
        return -1;
    }
    return 0; }

int ShmTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "shm";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int ShmTransport::install(std::string& local_server_name, std::shared_ptr<TransferMetadata> meta, void** args) {
    // Initialize control shared memory

    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "ShmTransport: cannot allocate local segment";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "ShmTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }
    return 0;
    //return 0;
}

// Assuming metadata_ is a shared pointer to TransferMetadata
// and is initialized properly in the constructor.

int ShmTransport::registerLocalMemory(void* addr, size_t length, const std::string& location, bool remote_accessible, bool update_metadata) {
    // Generate a unique name for the shared memory segment
    //SharedMem_map_[addr] = length;
    //create linux shared memory
    printf("addr %p",addr);
    printf("length %d",length);
    printf("location %s",location.c_str());
    printf("remote_accessible %d",remote_accessible);
    printf("update_metadata %d",update_metadata);
    createSharedMem(addr, length, local_server_name_);
    // Create a BufferDesc and add it to the metadata
    asm volatile ("nop":::"memory");
    BufferDesc buffer_desc;
    asm volatile ("nop %0":: "r"(&buffer_desc) :"memory");
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
    buffer_desc.name = local_server_name_;
    printf("%d",update_metadata);
    int ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret != 0) {
        // Remove the shared memory segment if registration fails
        //deleteSharedMem(addr);

        return ret;
    }
    return 0;
}

int ShmTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    // Generate the unique name for the shared memory segment
    // Remove the buffer's entry from the metadata
    delete_shared_memory(addr);
    int ret = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    
    return 0;
}

void ShmTransport::startSlice(Slice *slice) {
    slice->task->is_finished = false;
    //slice->task->status = TransferTask::RUNNING;
    //slice->task->submit_time = std::chrono::high_resolution_clock::now();
    slice->task->total_bytes += slice->length;
    if(slice->opcode == TransferRequest::WRITE) {
        memcpy((void*)slice->local.dest_addr, slice->source_addr, slice->length);
    }
    else {
        memcpy(slice->source_addr, (void*)slice->local.dest_addr, slice->length);
    }
    //if (slice->task->status == TransferStatusEnum::COMPLETED)
    slice->markSuccess();

}

}  // namespace mooncake