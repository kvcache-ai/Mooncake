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

#include "transport/cxl_transport/cxl_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <memory>
#include <regex>

#include "common.h"
#include "environ.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include <cstring>
#include <fcntl.h>     // For O_RDWR, O_CREAT, etc.
#include <unistd.h>    // For open(), close(), read(), write()
#include <sys/mman.h>  // For mmap, munmap

namespace mooncake {

CxlTransport::CxlTransport() {
    // cxl_dev_path = "/dev/dax0.0";
    // cxl_dev_size = 1024 * 1024 * 1024;
    // get from env
    std::string env_cxl_dev_path = Environ::Get().GetCxlDevPath();

    if (!env_cxl_dev_path.empty()) {
        LOG(INFO) << "MC_CXL_DEV_PATH: " << env_cxl_dev_path;
        cxl_dev_path = (char *)env_cxl_dev_path.c_str();
        cxl_dev_size = cxlGetDeviceSize();
    }
}

CxlTransport::~CxlTransport() {
    if (cxl_base_addr != nullptr && cxl_base_addr != MAP_FAILED &&
        cxl_dev_size != 0) {
        munmap(cxl_base_addr, cxl_dev_size);
    }
    metadata_->removeSegmentDesc(local_server_name_);
}

size_t CxlTransport::cxlGetDeviceSize() {
    // for now, get cxl_shm size from env
    std::string env_cxl_dev_size = Environ::Get().GetCxlDevSize();

    if (!env_cxl_dev_size.empty()) {
        LOG(INFO) << "MC_CXL_DEV_SIZE: " << env_cxl_dev_size;
        char *end = nullptr;
        unsigned long long val = strtoull(env_cxl_dev_size.c_str(), &end, 10);
        if (end != env_cxl_dev_size.c_str() && *end == '\0')
            return static_cast<size_t>(val);
    } else {
        // try to read dev size from sys

        // find "dax*.*" in path
        std::regex dax_pattern(R"(dax\d+\.\d+)");
        std::smatch match;
        std::string dev_name;
        std::string str_cxl_dev_path = std::string(cxl_dev_path);
        if (std::regex_search(str_cxl_dev_path, match, dax_pattern)) {
            dev_name = match.str();
        } else {
            LOG(ERROR) << "Can not find CXL device name in path: "
                       << cxl_dev_path;
            return 0;
        }

        std::string size_path = "/sys/bus/dax/devices/" + dev_name + "/size";
        LOG(INFO) << "Try to get CXL device size from: " << size_path;
        std::ifstream file(size_path);
        if (!file.is_open()) {
            LOG(ERROR) << "CXL size file does not exist";
            return 0;
        }

        std::string content;
        if (!std::getline(file, content)) {
            LOG(ERROR) << "Failed to read from: " << size_path;
            return 0;
        }

        unsigned long long val = strtoull(content.c_str(), nullptr, 10);
        // the content is written by kernel, so it should be a valid ull
        LOG(INFO) << "CXL device size is: " << val;
        return static_cast<size_t>(val);
    }
    return 0;
}

int CxlTransport::cxlMemcpy(void *dest, void *src, size_t size) {
    // Input validation
    if (!src || !dest) {
        LOG(ERROR) << "CxlTransport::cxlMemcpy invalid arguments: null pointer "
                      "provided.";
        return -1;  // null pointer
    }

    // Validate memory bounds using the helper function
    if (!validateMemoryBounds(dest, src, size)) {
        return -1;  // validation failed
    }

    // Perform the memory copy
    std::memcpy(dest, src, size);

    // Memory barriers and cache operations
    if (isAddressInCxlRange(dest) || isAddressInCxlRange(src)) {
        // Ensure memory ordering for CXL operations
        __sync_synchronize();
    }

    return 0;  // success
}

bool CxlTransport::validateMemoryBounds(void *dest, void *src, size_t size) {
    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t dest_ptr = reinterpret_cast<uintptr_t>(dest);
    uintptr_t src_ptr = reinterpret_cast<uintptr_t>(src);

    if (isAddressInCxlRange(dest)) {
        uintptr_t dest_end = dest_ptr + size;
        if (dest_end > end || dest_end < dest_ptr) {
            LOG(ERROR) << "CxlTransport::cxlMemcpy destination out of bounds.";
            return false;
        }
    }

    if (isAddressInCxlRange(src)) {
        uintptr_t src_end = src_ptr + size;
        if (src_end > end || src_end < src_ptr) {
            LOG(ERROR) << "CxlTransport::cxlMemcpy source out of bounds.";
            return false;
        }
    }

    return true;
}

bool CxlTransport::isAddressInCxlRange(void *addr) {
    if (!addr || !cxl_base_addr) return false;

    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);

    return (ptr >= base && ptr < end);
}

int CxlTransport::cxlDevInit() {
    if (!cxl_dev_path || !cxl_dev_size) {
        LOG(ERROR) << "CxlTransport: cxl_dev_path or cxl_dev_size is null.";
        return -1;
    }
    int fd = open(cxl_dev_path, O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "CxlTransport: Cannot open cxl device."
                   << strerror(errno);
        return -1;
    }

    void *ptr =
        mmap(NULL, cxl_dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        close(fd);
        return ERR_MEMORY;
    }
    cxl_base_addr = ptr;
    close(fd);
    return 0;
}

int CxlTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = cxlDevInit();
    if (ret) {
        LOG(ERROR) << "CxlTransport: Mmap cxl device failed.";
        return -1;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot allocate local segment";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    return 0;
}

int CxlTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "cxl";
    desc->cxl_base_addr = (uint64_t)cxl_base_addr;
    desc->cxl_name = cxl_dev_path;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int CxlTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc cxl_buffer_desc;
    cxl_buffer_desc.name = local_server_name_;

    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);
    uintptr_t ptr_end = ptr + length;
    // check addr legal
    if (ptr < base || ptr >= end) {
        errno = EFAULT;
        return -1;
    }
    // check overflow
    if (ptr_end > end || ptr_end < ptr) {
        errno = EOVERFLOW;
        return -1;
    }

    cxl_buffer_desc.offset = (uint64_t)addr - (uint64_t)cxl_base_addr;
    cxl_buffer_desc.length = length;
    return metadata_->addLocalMemoryBuffer(cxl_buffer_desc, update_metadata);
}

int CxlTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int CxlTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int CxlTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status CxlTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "CxlTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status CxlTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "CxlTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "CxlTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_cxl_offset = request.target_offset;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->cxl.dest_addr = (char *)cxl_base_addr + dest_cxl_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        int err;
        if (slice->opcode == TransferRequest::READ)
            // READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                            slice->length);
        else
            // WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void *)slice->cxl.dest_addr, slice->source_addr,
                            slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }

    return Status::OK();
}

Status CxlTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_cxl_offset = request.target_offset;
        task.total_bytes = request.length;

        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->cxl.dest_addr = (char *)cxl_base_addr + dest_cxl_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        int err;
        if (slice->opcode == TransferRequest::READ)
            // READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                            slice->length);
        else
            // WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void *)slice->cxl.dest_addr, slice->source_addr,
                            slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

}  // namespace mooncake
