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
// constexpr auto HIPX_MEM_HANDLE_TYPE_FABRIC =
//     hipMemHandleTypePosixFileDescriptor;

// struct ascendFabricHandle {
//     int fd;
//     int pid;
// };

// RAII wrapper for file descriptor management
// struct FdGuard {
//     int fd = -1;
//     explicit FdGuard(int fd_val) : fd(fd_val) {}
//     ~FdGuard() {
//         if (fd != -1) {
//             close(fd);
//         }
//     }
//     // Disable copy/move
//     FdGuard(const FdGuard &) = delete;
//     FdGuard &operator=(const FdGuard &) = delete;
//     FdGuard(FdGuard &&) = delete;
//     FdGuard &operator=(FdGuard &&) = delete;
// };

static bool checkAcl(aclError result, const char *message) {
    if (result != ACL_ERROR_NONE) {
        LOG(ERROR) << message << " (Error code: " << result << ")";
        return false;
    }
    return true;
}

// static int open_fd(const ascendFabricHandle &export_handle) {
//     int fd = export_handle.fd;
//     int pid = export_handle.pid;

//     int pid_fd = (int)syscall(__NR_pidfd_open, pid, 0);
//     if (pid_fd == -1) {
//         LOG(ERROR) << "UBShmemTransport: pidfd_open error: " << strerror(errno)
//                    << " ( " << pid << " " << fd << ")";
//         return -1;
//     }

//     int open_fd = (int)syscall(__NR_pidfd_getfd, pid_fd, fd, 0);
//     if (open_fd == -1) {
//         LOG(ERROR) << "UBShmemTransport: pidfd_getfd error: " << strerror(errno)
//                    << " ( " << pid << " " << fd << ")";
//         close(pid_fd);
//         return -1;
//     }

//     close(pid_fd);
//     return open_fd;
// }

// static int openIPCHandle(const std::vector<unsigned char> &buffer,
//                          void **shm_addr) {
//     hipIpcMemHandle_t handle;
//     memcpy(&handle, buffer.data(), sizeof(handle));
//     if (!checkAcl(hipIpcOpenMemHandle(shm_addr, handle,
//                                       hipIpcMemLazyEnablePeerAccess),
//                   "UBShmemTransport: hipIpcOpenMemHandle failed")) {
//         return -1;
//     }
//     return 0;
// }

static int openShareableHandle(const std::vector<unsigned char> &buffer,
                               size_t length, void **shm_addr) {
    aslrtMemFabricHandle export_handle = {};
    memcpy(&export_handle, buffer.data(), sizeof(export_handle));

    // Use RAII guard for automatic fd cleanup
    //FdGuard fd_guard(-1);

    // ???
    // if (HIPX_MEM_HANDLE_TYPE_FABRIC == hipMemHandleTypePosixFileDescriptor) {
    //     int opened_fd = open_fd(export_handle);
    //     if (opened_fd == -1) {
    //         LOG(ERROR) << "UBShmemTransport: failed to open fd";
    //         return -1;
    //     }
    //     fd_guard.fd = opened_fd;
    //     export_handle.fd = opened_fd;
    // }

    aclrtDrvMemHandle handle;

    if (!checkAcl(aclrtMemImportFromShareableHandleV2(&export_handle,
                                                  RT_MEM_SHARE_HANDLE_TYPE_FABRIC, 0U, handle),
                  "UBShmemTransport: aclrtMemImportFromShareableHandleV2 failed")) {
        return -1;
    }

    if (!checkAcl(aclrtReserveMemAddress(*shm_addr, length, 0,
                                       nullptr, 0),
                  "UBShmemTransport: aclrtReserveMemAddress failed")) {
        return -1;
    }

    if (!checkAcl(aclrtMemMap(*shm_addr, length, 0, handle, 0),
                  "UBShmemTransport: aclrtMemMap failed")) {
        (void)aclrtReleaseMemAddress(*shm_addr);
        return -1;
    }

    int device_count = 0;
    (void)aclrtGetDeviceCount(&device_count);
    std::vector<aclrtMemAccessDesc> accessDesc(device_count);
    for (int device_id = 0; device_id < device_count; ++device_id) {
        accessDesc[device_id].location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[device_id].location.id = device_id;
        accessDesc[device_id].flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    }

    if (!checkAcl(aclrtMemSetAccess(*shm_addr, length,
                                  accessDesc.data(), device_count),
                  "UBShmemTransport: aclrtMemSetAccess failed")) {
        (void)aclrtUnmapMem(*shm_addr);
        (void)aclrtReleaseMemAddress(*shm_addr, length);
        return -1;
    }

    return 0;
}

static int getDeviceFromPointer(void *ptr) {
    if (!ptr) {
        LOG(ERROR) << "UBShmemTransport: null pointer passed to "
                      "getDeviceFromPointer";
        return -1;
    }

    aclrtPointerAttribute_t attributes;
    auto err = aclrtPointerGetAttributes(ptr, &attributes);
    if (!checkAcl(err, "UBShmemTransport: aclrtPointerGetAttributes failed")) {
        return -1;
    }

    if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        // NPU memory - return device ID
        return attributes.location.id;
    } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        // Host memory - return -1 to indicate CPU memory
        // This is not an error, just indicates we should use current device
        // context
        if (globalConfig().trace) {
            LOG(INFO) << "UBShmemTransport: pointer " << ptr
                      << " is host memory (type: " << attributes.type
                      << "), will use current device context";
        }
        return -1;
    } else {
        LOG(WARNING) << "UBShmemTransport: unknown memory type " << attributes.type
                     << " for pointer " << ptr;
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
    // if (getenv("MC_USE_NVLINK_IPC")) return false;

    int num_devices = 0;
    if (!checkAcl(aclrtGetDeviceCount(&num_devices),
                  "UBShmemTransport: aclrtGetDeviceCount failed")) {
        return false;
    }

    if (num_devices == 0) {
        LOG(ERROR) << "UBShmemTransport: no device found";
        return false;
    }

    // Check if all devices support virtual memory management,
    // which is required for fabric memory operations
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        if (!checkAcl(aclrtGetDevice(device_id),
                      "UBShmemTransport: aclrtGetDevice failed")) {
            return false;
        }
        
        // TODO: 替换为npu逻辑
        // int vmm_supported = 0;
        // auto result = hipDeviceGetAttribute(
        //     &vmm_supported, hipDeviceAttributeVirtualMemoryManagementSupported,
        //     device);
        // if (result != hipSuccess || !vmm_supported) {
        //     LOG(WARNING) << "UBShmemTransport: Device " << device_id
        //                  << " does not support virtual memory management, "
        //                  << "falling back to IPC mode";
        //     return false;
        // }
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
        // for (auto &entry : remap_entries_) {
        //     (void)hipIpcCloseMemHandle(entry.second.shm_addr);
        // }
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

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status UBShmemTransport::processTransferRequest(const TransferRequest &request,
                                            TransferTask &task,
                                            bool add_to_slice_list) {
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
        if (rc) return Status::Memory("device memory not registered");
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

    if (add_to_slice_list) {
        task.slice_list.push_back(slice);
    }

    __sync_fetch_and_add(&task.slice_count, 1);

    // Execute memory copy
    aclError err;
    if (slice->opcode == TransferRequest::READ) {
        err = aclrtMemcpy(slice->source_addr, slice->length, (void *)slice->local.dest_addr,
                        slice->length, ACL_MEMCPY_DEFAULT);
    } else {
        err = aclrtMemcpy((void *)slice->local.dest_addr, slice->length, slice->source_addr,
                        slice->length, ACL_MEMCPY_DEFAULT);
    }

    if (!checkAcl(err, "UBShmemTransport: aclrtMemcpy failed")) {
        slice->markFailed();
        return Status::Memory("UBShmemTransport: Memory copy failed");
    }

    // Synchronize stream to ensure copy is complete
    err = aclrtSynchronizeStream(nullptr);
    if (!checkAcl(err, "UBShmemTransport: aclrtSynchronizeStream failed")) {
        slice->markFailed();
        return Status::Memory("UBShmemTransport: Stream synchronization failed");
    }

    // Mark as successful only after sync completes
    slice->markSuccess();

    return Status::OK();
}

Status UBShmemTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UBShmemTransport: Exceed the limitation of current batch's "
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

        Status status = processTransferRequest(request, task, false);
        if (!status.ok()) {
            return status;
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

        Status status = processTransferRequest(request, task, true);
        if (!status.ok()) {
            return status;
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
        // TODO 
        // // Validate memory type
        // hipPointerAttribute_t attr;
        // if (!checkAcl(hipPointerGetAttributes(&attr, addr),
        //               "UBShmemTransport: hipPointerGetAttributes failed")) {
        //     return -1;
        // }

        // if (attr.type != hipMemoryTypeDevice) {
        //     LOG(ERROR) << "Unsupported memory type, " << addr << " "
        //                << attr.type;
        //     return -1;
        // }

        // // Get IPC handle
        // hipIpcMemHandle_t handle;
        // if (!checkAcl(hipIpcGetMemHandle(&handle, addr),
        //               "UBShmemTransport: hipIpcGetMemHandle failed")) {
        //     return -1;
        // }

        // // Register buffer with metadata
        // (void)remote_accessible;
        // BufferDesc desc;
        // desc.addr = (uint64_t)addr;
        // desc.length = length;
        // desc.name = location;
        // desc.shm_name = serializeBinaryData(&handle, sizeof(hipIpcMemHandle_t));
        // return metadata_->addLocalMemoryBuffer(desc, true);
    }

    // Fabric memory registration
    else {
        // Retain allocation handle
        aclrtDrvMemHandle handle;
        auto ret = aclrtMemRetainAllocationHandle(ptr, &handle);
        if (result != ACL_ERROR_NONE) {
            LOG(WARNING) << "Memory region " << addr
                         << " is not allocated by ascendFabricMemCreate, "
                         << "but it can be used as local buffer";
            return 0;
        }

        // Find whole physical page for memory registration
        void *real_addr;
        size_t real_size;
        // result = hipMemGetAddressRange((hipDeviceptr_t *)&real_addr, &real_size,
        //                                (hipDeviceptr_t)addr);
        // if (result != hipSuccess) {
        //     LOG(WARNING) << "UBShmemTransport: hipMemGetAddressRange failed: "
        //                  << result;
        //     const uint64_t granularity = 2ULL * 1024 * 1024;
        //     real_addr = addr;
        //     real_size = (length + granularity - 1) & ~(granularity - 1);
        // }

        // Export shareable handle
        aclrtMemFabricHandle export_handle_raw = {};
        if (!checkAcl(
                aclrtMemExportToShareableHandleV2(handle, 0U,
                    ACL_MEM_SHARE_HANDLE_TYPE_FABRIC, export_handle_raw),
                "UBShmemTransport: aclrtMemExportToShareableHandleV2 failed")) {
            return -1;
        }
        // export_handle_raw.pid = getpid();

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)real_addr;
        desc.length = real_size;
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

                // IPC分支
                // if (output_buffer.size() == sizeof(hipIpcMemHandle_t) &&
                //     !use_fabric_mem_) {
                //     rc = openIPCHandle(output_buffer, &shm_addr);
                // } else 
                if (output_buffer.size() == sizeof(aclrtMemFabricHandle) &&
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
        if (rc < 0) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

int UBShmemTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc < 0) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

void *UBShmemTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        // SKIP
        // void *ptr = nullptr;
        // if (!checkAcl(aclrtMalloc(&ptr, size),
        //               "UBShmemTransport: hipMalloc failed")) {
        //     return nullptr;
        // }
        // return ptr;
        return nullptr;
    }

    size_t granularity = 0;
    // hipDevice_t currentDev;
    aclrtPhysicalMemProp prop = {}; // npu对应物理内存
    aclrtDrvMemHandle handle;
    void *ptr = nullptr;
    int aclDev;
    int flag = 0;

    if (!checkAcl(aclrtGetDevice(&aclDev), "UBShmemTransport: aclrtGetDevice failed")) {
        return nullptr;
    }

    // if (!checkAcl(hipDeviceGet(&currentDev, hipDev),
    //               "UBShmemTransport: hipDeviceGet failed")) {
    //     return nullptr;
    // }

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = static_cast<int32_t>(aclDev / 4) * 2;
    prop.reserve = 0;

    // hipError_t result = hipDeviceGetAttribute(
    //     &flag, hipDeviceAttributeVirtualMemoryManagementSupported, currentDev);
    // if (!checkAcl(result, "UBShmemTransport: hipDeviceGetAttribute failed")) {
    //     return nullptr;
    // }

    // if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;

    result = aclrtMemGetAllocationGranularity(&prop,
                                            ACL_RT_MEM_ALLOC_GRANULARITY_MINIMUM,
                                            &granularity);
    if (!checkAcl(result,
                  "UBShmemTransport: aclrtMemGetAllocationGranularity failed")) {
        return nullptr;
    }

    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;

    result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (!checkAcl(result, "UBShmemTransport: Failed to allocate specific numa memory")) {
        prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
        prop.location.id = 0;
        result = aclrtMallocPhysical(&handle, size, &prop, 0);
        if (!checkAcl(result, "UBShmemTransport: Failed to allocate memory")) {
            return nullptr;
        }
    }

    result = aclrtReserveMemAddress(ptr, size, granularity,
                                  nullptr, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtMapMem failed")) {
        (void)aclrtReleaseMemAddress(ptr, size);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    int device_count = 0;
    (void)aclrtGetDeviceCount(&device_count);
    std::vector<aclrtMemAccessDesc> accessDesc(device_count);
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE; 
    }

    result = aclrtMemSetAccess(ptr, size, accessDesc.data(),
                             device_count);
    if (!checkAcl(result, "UBShmemTransport: aclrtMemSetAccess failed")) {
        (void)aclrtUnmapMem(ptr);
        (void)aclrtReleaseMemAddress(ptr, size);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

void UBShmemTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        //(void)hipFree(ptr);
        return;
    }

    aclrtDrvMemHandle handle;
    size_t size = 0;

    if (!checkAcl(aclrtMemRetainAllocationHandle(ptr, &handle),
                  "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
        return;
    }

    // hipDeviceptr_t base = nullptr;
    // hipError_t result =
    //     hipMemGetAddressRange(&base, &size, (hipDeviceptr_t)ptr);
    // if (checkAcl(result, "UBShmemTransport: hipMemGetAddressRange")) {
    //     (void)aclrtUnmapMem(ptr);
    //     (void)aclrtReleaseMemAddress(ptr, size);
    // }

    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr, size);
    (void)aclrtFreePhysical(handle);
}
}  // namespace mooncake
