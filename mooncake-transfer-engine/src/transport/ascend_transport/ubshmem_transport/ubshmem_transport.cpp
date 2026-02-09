// Copyright 2025 Huawei Technologies Co., Ltd
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

#include "transport/ascend_transport/ubshmem_transport/ubshmem_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/syscall.h>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
namespace {
constexpr size_t kIPCHandleKeyLength = 65;
}  // namespace

static bool checkAcl(aclError result, const char *message) {
    if (result != ACL_ERROR_NONE) {
        const char *errMsg = aclGetRecentErrMsg();
        LOG(ERROR) << message << " (Error code: " << result << " - " << errMsg
                   << ")";
        return false;
    }
    return true;
}

static int openIPCHandle(const std::vector<unsigned char> &buffer,
                         void **shm_addr) {
    // Validate buffer size before copying
    if (buffer.size() != kIPCHandleKeyLength) {
        LOG(ERROR) << "UBShmemTransport: buffer size " << buffer.size()
                   << " does not match expected size " << kIPCHandleKeyLength;
        return -1;
    }

    // Copy IPC key from buffer
    char ipc_key[kIPCHandleKeyLength] = {0};
    memcpy(ipc_key, buffer.data(), kIPCHandleKeyLength);

    // Import IPC memory handle
    if (!checkAcl(aclrtIpcMemImportByKey(
                      shm_addr, ipc_key,
                      ACL_RT_IPC_MEM_IMPORT_FLAG_ENABLE_PEER_ACCESS),
                  "UBShmemTransport: aclrtIpcMemImportByKey failed")) {
        return -1;
    }
    return 0;
}

static int openShareableHandle(const std::vector<unsigned char> &buffer,
                               size_t length, void **shm_addr) {
    aclrtMemFabricHandle export_handle = {};
    memcpy(&export_handle, buffer.data(), sizeof(export_handle));

    aclrtDrvMemHandle handle;
    if (!checkAcl(
            aclrtMemImportFromShareableHandleV2(
                &export_handle, ACL_MEM_SHARE_HANDLE_TYPE_FABRIC,
                ACL_RT_VMM_EXPORT_FLAG_DEFAULT, &handle),
            "UBShmemTransport: aclrtMemImportFromShareableHandleV2 failed")) {
        return -1;
    }

    uint64_t page_type = 1;
    if (!checkAcl(
            aclrtReserveMemAddress(shm_addr, length, 0, nullptr, page_type),
            "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return -1;
    }

    if (!checkAcl(aclrtMapMem(*shm_addr, length, 0, handle, 0),
                  "UBShmemTransport: aclrtMemMap failed")) {
        (void)aclrtReleaseMemAddress(*shm_addr);
        (void)aclrtFreePhysical(handle);
        return -1;
    }

    return 0;
}

static int getDeviceFromPointer(void *ptr) {
    if (ptr == nullptr) {
        LOG(ERROR) << "UBShmemTransport: null pointer passed to "
                      "getDeviceFromPointer";
        return -1;
    }

    aclrtPtrAttributes attributes;
    auto err = aclrtPointerGetAttributes(ptr, &attributes);
    if (!checkAcl(err, "UBShmemTransport: aclrtPointerGetAttributes failed")) {
        return -1;
    }

    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return attributes.location.id;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        // Host memory - return -1 to indicate CPU memory
        // This is not an error, just indicates we should use current device
        // context
        if (globalConfig().trace) {
            LOG(INFO) << "UBShmemTransport: pointer " << ptr
                      << " is host memory (type: " << attributes.location.type
                      << "), will use current device context";
        }
        return -1;
    } else {
        LOG(WARNING) << "UBShmemTransport: unknown memory type "
                     << attributes.location.type << " for pointer " << ptr;
        return -1;
    }
}

static int setDeviceContext(void *source_ptr) {
    // Get device ID from source pointer
    int device_id = getDeviceFromPointer(source_ptr);

    // Set device context if we have NPU memory
    if (device_id >= 0) {
        if (!checkAcl(aclrtSetDevice(device_id),
                      "UBShmemTransport: failed to set device context")) {
            return -1;
        }
    }

    return 0;
}

static bool supportFabricMem() {
    const char *use_ipc = getenv("MC_USE_UBSHMEM_IPC");
    if (use_ipc != nullptr && strcmp(use_ipc, "1") == 0) {
        return false;
    }
    uint32_t num_devices = 0;
    if (!checkAcl(aclrtGetDeviceCount(&num_devices),
                  "UBShmemTransport: aclrtGetDeviceCount failed")) {
        return false;
    }

    if (num_devices == 0) {
        LOG(ERROR) << "UBShmemTransport: no device found";
        return false;
    }
    return true;
}

UBShmemTransport::UBShmemTransport() : use_fabric_mem_(supportFabricMem()) {}

UBShmemTransport::~UBShmemTransport() {
    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            if (entry.second.key != nullptr) {
                (void)aclrtIpcMemClose(entry.second.key);
                delete[] entry.second.key;
            }
        }
    }
    remap_entries_.clear();
}

int UBShmemTransport::install(std::string &local_server_name,
                              std::shared_ptr<TransferMetadata> metadata,
                              std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;

    desc->name = local_server_name_;
    desc->protocol = "ubshmem";
    auto ret = aclrtCreateStreamWithConfig(
        &stream_, 0, ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC);
    if (!checkAcl(ret, "Failed to create stream.")) {
        return -1;
    }

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status UBShmemTransport::processTransferRequest(const TransferRequest &request,
                                                TransferTask &task) {
    // Set device context
    int rc = setDeviceContext(request.source);
    if (rc != 0) {
        return Status::InvalidArgument(
            "Failed to set device context for transfer");
    }

    // Relocate shared memory address if needed
    uint64_t dest_addr = request.target_offset;
    if (request.target_id != LOCAL_SEGMENT_ID) {
        int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                             request.target_id);
        if (rc) {
            return Status::Memory("device memory not registered");
        }
    }

    task.total_bytes = request.length;

    // Allocate and configure slice
    Slice *slice = getSliceCache().allocate();
    slice->source_addr = (char *)request.source;
    slice->local.dest_addr = (char *)dest_addr;
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->task = &task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;

    // Always add slice to slice_list for proper tracking and cleanup
    task.slice_list.push_back(slice);

    __sync_fetch_and_add(&task.slice_count, 1);

    // Execute memory copy
    aclError err;
    if (slice->opcode == TransferRequest::READ) {
        err = aclrtMemcpyAsync(slice->source_addr, slice->length,
                               (void *)slice->local.dest_addr, slice->length,
                               ACL_MEMCPY_DEFAULT, stream_);
    } else {
        err = aclrtMemcpyAsync((void *)slice->local.dest_addr, slice->length,
                               slice->source_addr, slice->length,
                               ACL_MEMCPY_DEFAULT, stream_);
    }

    if (!checkAcl(err, "UBShmemTransport: aclrtMemcpyAsync failed")) {
        slice->markFailed();
        return Status::Memory("UBShmemTransport: Memory copy failed");
    }

    // Slice status remains PENDING - will be marked as SUCCESS after
    // synchronization
    return Status::OK();
}

Status UBShmemTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "UBShmemTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "UBShmemTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;

        Status status = processTransferRequest(request, task);
        if (!status.ok()) {
            return status;
        }
    }

    auto err = aclrtSynchronizeStream(stream_);
    if (!checkAcl(err, "UBShmemTransport: aclrtSynchronizeStream failed")) {
        return Status::Memory(
            "UBShmemTransport: Stream synchronization failed");
    }

    // After synchronization, mark all slices as SUCCESS and update task status
    for (size_t i = task_id - entries.size(); i < task_id; ++i) {
        auto &task = batch_desc.task_list[i];
        for (auto *slice : task.slice_list) {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

Status UBShmemTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                           TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();

    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UBShmemTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }

    // Get task and status info
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;

    // Determine completion status
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

Status UBShmemTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (auto *task_ptr : task_list) {
        assert(task_ptr);
        auto &task = *task_ptr;
        assert(task.request);
        auto &request = *task.request;

        Status status = processTransferRequest(request, task);
        if (!status.ok()) {
            return status;
        }
    }

    auto err = aclrtSynchronizeStream(stream_);
    if (!checkAcl(err, "UBShmemTransport: aclrtSynchronizeStream failed")) {
        return Status::Memory(
            "UBShmemTransport: Stream synchronization failed");
    }

    // After synchronization, mark all slices as SUCCESS and update task status
    for (auto *task_ptr : task_list) {
        assert(task_ptr);
        auto &task = *task_ptr;
        for (auto *slice : task.slice_list) {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

int UBShmemTransport::registerLocalMemory(void *addr, size_t length,
                                          const std::string &location,
                                          bool remote_accessible,
                                          bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);

    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }

    // IPC-based memory registration
    if (!use_fabric_mem_) {
        // Get IPC Mem export key
        char ipc_key[kIPCHandleKeyLength] = {0};
        if (!checkAcl(aclrtIpcMemGetExportKey(
                          addr, length, ipc_key, kIPCHandleKeyLength,
                          ACL_RT_IPC_MEM_EXPORT_FLAG_DISABLE_PID_VALIDATION),
                      "UBShmemTransport: aclrtIpcMemGetExportKey failed")) {
            (void)aclrtFree(addr);
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData((const void *)ipc_key, kIPCHandleKeyLength);
        return metadata_->addLocalMemoryBuffer(desc, true);
    }

    // Fabric memory registration
    else {
        // Retain allocation handle
        aclrtDrvMemHandle handle;
        auto ret = aclrtMemRetainAllocationHandle(addr, &handle);
        if (!checkAcl(
                ret,
                "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
            return -1;
        }

        // Export shareable handle
        aclrtMemFabricHandle export_handle_raw = {};
        if (!checkAcl(
                aclrtMemExportToShareableHandleV2(
                    handle, ACL_RT_VMM_EXPORT_FLAG_DISABLE_PID_VALIDATION,
                    ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, &export_handle_raw),
                "UBShmemTransport: aclrtMemExportToShareableHandleV2 failed")) {
            (void)aclrtFreePhysical(handle);
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name = serializeBinaryData((const void *)&export_handle_raw,
                                            sizeof(aclrtMemFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int UBShmemTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int UBShmemTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                  uint64_t length,
                                                  uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);

    // Search for matching buffer entry
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            // Check if already remapped (shared lock)
            remap_lock_.lockShared();
            if (remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                auto shm_addr =
                    remap_entries_[std::make_pair(target_id, entry.addr)]
                        .shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);

                void *shm_addr = nullptr;
                int rc = -1;

                if (!use_fabric_mem_) {
                    rc = openIPCHandle(output_buffer, &shm_addr);
                } else if (output_buffer.size() ==
                               sizeof(aclrtMemFabricHandle) &&
                           use_fabric_mem_) {
                    rc = openShareableHandle(output_buffer, entry.length,
                                             &shm_addr);
                } else {
                    LOG(ERROR) << "Mismatched UBShmem data transfer method";
                    return -1;
                }

                if (rc != 0) {
                    return -1;
                }

                OpenedShmEntry shm_entry;
                shm_entry.shm_addr = shm_addr;
                shm_entry.length = entry.length;

                // For IPC mode, we need to save the key for cleanup
                if (!use_fabric_mem_) {
                    // Validate buffer size before copying
                    if (output_buffer.size() != kIPCHandleKeyLength) {
                        LOG(ERROR) << "UBShmemTransport: buffer size "
                                   << output_buffer.size()
                                   << " does not match expected size "
                                   << kIPCHandleKeyLength;
                        return -1;
                    }
                    shm_entry.key = new char[kIPCHandleKeyLength];
                    memcpy(shm_entry.key, output_buffer.data(),
                           kIPCHandleKeyLength);
                } else {
                    shm_entry.key = nullptr;
                }

                remap_entries_[std::make_pair(target_id, entry.addr)] =
                    shm_entry;
            }

            // Calculate relocated address
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int UBShmemTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc < 0) {
            return rc;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int UBShmemTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc < 0) {
            return rc;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

void *UBShmemTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        if (!checkAcl(aclrtMalloc(&ptr, size, ACL_MEM_MALLOC_HUGE_FIRST),
                      "UBShmemTransport: aclrtMalloc failed")) {
            return nullptr;
        }
        return ptr;
    }

    size_t granularity = 0;
    aclrtPhysicalMemProp prop = {};
    aclrtDrvMemHandle handle;
    void *ptr = nullptr;
    int aclDev;

    if (!checkAcl(aclrtGetDevice(&aclDev),
                  "UBShmemTransport: aclrtGetDevice failed")) {
        return nullptr;
    }

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_HBM_MEM_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = aclDev;
    prop.reserve = 0;

    auto result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (!checkAcl(
            result,
            "UBShmemTransport: Failed to allocate specific device memory")) {
        return nullptr;
    }

    uint64_t page_type = 1;
    result =
        aclrtReserveMemAddress(&ptr, size, granularity, nullptr, page_type);
    if (!checkAcl(result, "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtMapMem failed")) {
        (void)aclrtReleaseMemAddress(ptr);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

void UBShmemTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        aclrtFree(ptr);
        return;
    }

    aclrtDrvMemHandle handle;

    if (!checkAcl(aclrtMemRetainAllocationHandle(ptr, &handle),
                  "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
        return;
    }

    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr);
    (void)aclrtFreePhysical(handle);
}
}  // namespace mooncake
