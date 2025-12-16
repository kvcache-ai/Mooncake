// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.
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

#include "transport/hip_transport/hip_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unistd.h>
#include <sys/syscall.h>
#include <cerrno>
#include <cstring>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

// HIP-specific type aliases
constexpr auto HIPX_MEM_HANDLE_TYPE_FABRIC =
    hipMemHandleTypePosixFileDescriptor;

struct hipxFabricHandle {
    int fd;
    int pid;
};

namespace mooncake {
static bool checkHip(hipError_t result, const char *message) {
    if (result != hipSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << hipGetErrorString(result) << ")";
        return false;
    }
    return true;
}

static int open_fd(const hipxFabricHandle &export_handle) {
    int fd = export_handle.fd;
    int pid = export_handle.pid;

    int pid_fd = (int)syscall(__NR_pidfd_open, pid, 0);
    if (pid_fd == -1) {
        LOG(ERROR) << "HIPTransport: pidfd_open error: " << strerror(errno)
                   << " ( " << pid << " " << fd << ")";
        return -1;
    }

    int open_fd = (int)syscall(__NR_pidfd_getfd, pid_fd, fd, 0);
    if (open_fd == -1) {
        LOG(ERROR) << "HIPTransport: pidfd_getfd error: " << strerror(errno)
                   << " ( " << pid << " " << fd << ")";
        close(pid_fd);
        return -1;
    }

    close(pid_fd);
    return open_fd;
}

static int openIPCHandle(const std::vector<unsigned char> &buffer,
                         void **shm_addr) {
    hipIpcMemHandle_t handle;
    memcpy(&handle, buffer.data(), sizeof(handle));
    if (!checkHip(hipIpcOpenMemHandle(shm_addr, handle,
                                      hipIpcMemLazyEnablePeerAccess),
                  "HipTransport: hipIpcOpenMemHandle failed")) {
        return -1;
    }
    return 0;
}

static int openShareableHandle(const std::vector<unsigned char> &buffer,
                               size_t length, void **shm_addr) {
    hipxFabricHandle export_handle;
    memcpy(&export_handle, buffer.data(), sizeof(export_handle));

    int opened_fd = -1;
    if (HIPX_MEM_HANDLE_TYPE_FABRIC == hipMemHandleTypePosixFileDescriptor) {
        opened_fd = open_fd(export_handle);
        if (opened_fd == -1) {
            LOG(ERROR) << "HIPTransport: failed to open fd";
            return -1;
        }
        export_handle.fd = opened_fd;
    }

    hipMemGenericAllocationHandle_t handle;
    if (!checkHip(hipMemImportFromShareableHandle(&handle, &export_handle,
                                                  HIPX_MEM_HANDLE_TYPE_FABRIC),
                  "HipTransport: hipMemImportFromShareableHandle failed")) {
        if (opened_fd != -1) {
            close(opened_fd);
        }
        return -1;
    }

    if (opened_fd != -1) {
        close(opened_fd);
    }

    if (!checkHip(hipMemAddressReserve((hipDeviceptr_t *)shm_addr, length, 0,
                                       nullptr, 0),
                  "HipTransport: hipMemAddressReserve failed")) {
        return -1;
    }

    if (!checkHip(hipMemMap((hipDeviceptr_t)*shm_addr, length, 0, handle, 0),
                  "HipTransport: hipMemMap failed")) {
        (void)hipMemAddressFree((hipDeviceptr_t)*shm_addr, length);
        return -1;
    }

    int device_count = 0;
    (void)hipGetDeviceCount(&device_count);
    std::vector<hipMemAccessDesc> accessDesc(device_count);
    for (int device_id = 0; device_id < device_count; ++device_id) {
        accessDesc[device_id].location.type = hipMemLocationTypeDevice;
        accessDesc[device_id].location.id = device_id;
        accessDesc[device_id].flags = hipMemAccessFlagsProtReadWrite;
    }

    if (!checkHip(hipMemSetAccess((hipDeviceptr_t)*shm_addr, length,
                                  accessDesc.data(), device_count),
                  "HipTransport: hipMemSetAccess failed")) {
        (void)hipMemUnmap((hipDeviceptr_t)*shm_addr, length);
        (void)hipMemAddressFree((hipDeviceptr_t)*shm_addr, length);
        return -1;
    }

    return 0;
}

static bool supportFabricMem() {
    // For HIP transport, prefer HIP-specific env var, but also check NVLINK for
    // backward compatibility.
    if (getenv("MC_USE_HIP_IPC") || getenv("MC_USE_NVLINK_IPC")) return false;

    int num_devices = 0;
    if (!checkHip(hipGetDeviceCount(&num_devices),
                  "HipTransport: hipGetDeviceCount failed")) {
        return false;
    }

    if (num_devices == 0) {
        LOG(ERROR) << "HipTransport: no device found";
        return false;
    }

    // Check if all devices support virtual memory management,
    // which is required for fabric memory operations
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        hipDevice_t device;
        if (!checkHip(hipDeviceGet(&device, device_id),
                      "HipTransport: hipDeviceGet failed")) {
            return false;
        }

        int vmm_supported = 0;
        hipError_t result = hipDeviceGetAttribute(
            &vmm_supported, hipDeviceAttributeVirtualMemoryManagementSupported,
            device);
        if (result != hipSuccess || !vmm_supported) {
            LOG(WARNING) << "HipTransport: Device " << device_id
                         << " does not support virtual memory management, "
                         << "falling back to IPC mode";
            return false;
        }
    }

    return true;
}

HipTransport::HipTransport() : use_fabric_mem_(supportFabricMem()) {}

HipTransport::~HipTransport() {
    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            (void)hipIpcCloseMemHandle(entry.second.shm_addr);
        }
    }
    remap_entries_.clear();
}

int HipTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> metadata,
                          std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;

    desc->name = local_server_name_;
    desc->protocol = "hip";

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status HipTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "HipTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "HipTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;

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
        __sync_fetch_and_add(&task.slice_count, 1);

        // Execute memory copy
        hipError_t err;
        if (slice->opcode == TransferRequest::READ) {
            err = hipMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                            slice->length, hipMemcpyDefault);
        } else {
            err = hipMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                            slice->length, hipMemcpyDefault);
        }

        // Mark completion status
        if (err != hipSuccess) {
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

Status HipTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();

    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "HipTransport::getTransferStatus invalid argument, batch id: " +
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

Status HipTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (auto *task_ptr : task_list) {
        assert(task_ptr);
        auto &task = *task_ptr;
        assert(task.request);
        auto &request = *task.request;

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
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        // Execute memory copy
        hipError_t err;
        if (slice->opcode == TransferRequest::READ) {
            err = hipMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                            slice->length, hipMemcpyDefault);
        } else {
            err = hipMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                            slice->length, hipMemcpyDefault);
        }

        // Mark completion status
        if (err != hipSuccess) {
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

int HipTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);

    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }

    // IPC-based memory registration
    if (!use_fabric_mem_) {
        // Validate memory type
        hipPointerAttribute_t attr;
        if (!checkHip(hipPointerGetAttributes(&attr, addr),
                      "HipTransport: hipPointerGetAttributes failed")) {
            return -1;
        }

        if (attr.type != hipMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return -1;
        }

        // Get IPC handle
        hipIpcMemHandle_t handle;
        if (!checkHip(hipIpcGetMemHandle(&handle, addr),
                      "HipTransport: hipIpcGetMemHandle failed")) {
            return -1;
        }

        // Register buffer with metadata
        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name = serializeBinaryData(&handle, sizeof(hipIpcMemHandle_t));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }

    // Fabric memory registration
    else {
        // Retain allocation handle
        hipMemGenericAllocationHandle_t handle;
        hipError_t result = hipMemRetainAllocationHandle(&handle, addr);
        if (result != hipSuccess) {
            LOG(WARNING) << "Memory region " << addr
                         << " is not allocated by hipMemCreate, "
                         << "but it can be used as local buffer";
            return 0;
        }

        // Find whole physical page for memory registration
        void *real_addr;
        size_t real_size;
        result = hipMemGetAddressRange((hipDeviceptr_t *)&real_addr, &real_size,
                                       (hipDeviceptr_t)addr);
        if (result != hipSuccess) {
            LOG(WARNING) << "HipTransport: hipMemGetAddressRange failed: "
                         << result;
            const uint64_t granularity = 2ULL * 1024 * 1024;
            real_addr = addr;
            real_size = (length + granularity - 1) & ~(granularity - 1);
        }

        // Export shareable handle
        hipxFabricHandle export_handle_raw = {};
        if (!checkHip(
                hipMemExportToShareableHandle(&export_handle_raw, handle,
                                              HIPX_MEM_HANDLE_TYPE_FABRIC, 0),
                "HipTransport: hipMemExportToShareableHandle failed")) {
            return -1;
        }
        export_handle_raw.pid = getpid();

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)real_addr;
        desc.length = real_size;
        desc.name = location;
        desc.shm_name = serializeBinaryData((const void *)&export_handle_raw,
                                            sizeof(hipxFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int HipTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int HipTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
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

                if (output_buffer.size() == sizeof(hipIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    rc = openIPCHandle(output_buffer, &shm_addr);
                } else if (output_buffer.size() == sizeof(hipxFabricHandle) &&
                           use_fabric_mem_) {
                    rc = openShareableHandle(output_buffer, entry.length,
                                             &shm_addr);
                } else {
                    LOG(ERROR) << "Mismatched HIP data transfer method";
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

int HipTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc < 0) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

int HipTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc < 0) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

void *HipTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        if (!checkHip(hipMalloc(&ptr, size),
                      "HipTransport: hipMalloc failed")) {
            return nullptr;
        }
        return ptr;
    }

    size_t granularity = 0;
    hipDevice_t currentDev;
    hipMemAllocationProp prop = {};
    hipMemGenericAllocationHandle_t handle;
    void *ptr = nullptr;
    int hipDev;
    int flag = 0;

    if (!checkHip(hipGetDevice(&hipDev), "HipTransport: hipGetDevice failed")) {
        return nullptr;
    }

    if (!checkHip(hipDeviceGet(&currentDev, hipDev),
                  "HipTransport: hipDeviceGet failed")) {
        return nullptr;
    }

    prop.type = hipMemAllocationTypePinned;
    prop.location.type = hipMemLocationTypeDevice;
    prop.requestedHandleType = HIPX_MEM_HANDLE_TYPE_FABRIC;
    prop.location.id = currentDev;

    hipError_t result = hipDeviceGetAttribute(
        &flag, hipDeviceAttributeVirtualMemoryManagementSupported, currentDev);
    if (!checkHip(result, "HipTransport: hipDeviceGetAttribute failed")) {
        return nullptr;
    }

    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;

    result = hipMemGetAllocationGranularity(&granularity, &prop,
                                            hipMemAllocationGranularityMinimum);
    if (!checkHip(result,
                  "HipTransport: hipMemGetAllocationGranularity failed")) {
        return nullptr;
    }

    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;

    result = hipMemCreate(&handle, size, &prop, 0);
    if (!checkHip(result, "HipTransport: hipMemCreate failed")) {
        return nullptr;
    }

    result = hipMemAddressReserve((hipDeviceptr_t *)&ptr, size, granularity,
                                  nullptr, 0);
    if (!checkHip(result, "HipTransport: hipMemAddressReserve failed")) {
        (void)hipMemRelease(handle);
        return nullptr;
    }

    result = hipMemMap((hipDeviceptr_t)ptr, size, 0, handle, 0);
    if (!checkHip(result, "HipTransport: hipMemMap failed")) {
        (void)hipMemAddressFree((hipDeviceptr_t)ptr, size);
        (void)hipMemRelease(handle);
        return nullptr;
    }

    int device_count = 0;
    (void)hipGetDeviceCount(&device_count);
    std::vector<hipMemAccessDesc> accessDesc(device_count);
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = hipMemLocationTypeDevice;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = hipMemAccessFlagsProtReadWrite;
    }

    result = hipMemSetAccess((hipDeviceptr_t)ptr, size, accessDesc.data(),
                             device_count);
    if (!checkHip(result, "HipTransport: hipMemSetAccess failed")) {
        (void)hipMemUnmap((hipDeviceptr_t)ptr, size);
        (void)hipMemAddressFree((hipDeviceptr_t)ptr, size);
        (void)hipMemRelease(handle);
        return nullptr;
    }

    return ptr;
}

void HipTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        (void)hipFree(ptr);
        return;
    }

    hipMemGenericAllocationHandle_t handle;
    size_t size = 0;

    if (!checkHip(hipMemRetainAllocationHandle(&handle, ptr),
                  "HipTransport: hipMemRetainAllocationHandle failed")) {
        return;
    }

    hipDeviceptr_t base = 0;
    hipError_t result =
        hipMemGetAddressRange(&base, &size, (hipDeviceptr_t)ptr);
    if (checkHip(result, "HipTransport: hipMemGetAddressRange")) {
        (void)hipMemUnmap((hipDeviceptr_t)ptr, size);
        (void)hipMemAddressFree((hipDeviceptr_t)ptr, size);
    }

    (void)hipMemRelease(handle);
}
}  // namespace mooncake
