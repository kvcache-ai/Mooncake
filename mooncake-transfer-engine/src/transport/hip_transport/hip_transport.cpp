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

// Pool configuration constants
// These can be overridden via environment variables:
// - MC_HIP_NUM_STREAMS: number of HIP streams per device
// - MC_HIP_NUM_EVENTS: number of HIP events per device
constexpr int kDefaultNumStreams = 64;
constexpr int kDefaultNumEvents = 64;

// HIP-specific type aliases
constexpr auto HIPX_MEM_HANDLE_TYPE_FABRIC =
    hipMemHandleTypePosixFileDescriptor;

struct hipxFabricHandle {
    int fd;
    int pid;
};

// RAII wrapper for file descriptor management
struct FdGuard {
    int fd = -1;
    explicit FdGuard(int fd_val) : fd(fd_val) {}
    ~FdGuard() {
        if (fd != -1) {
            close(fd);
        }
    }
    // Disable copy/move
    FdGuard(const FdGuard &) = delete;
    FdGuard &operator=(const FdGuard &) = delete;
    FdGuard(FdGuard &&) = delete;
    FdGuard &operator=(FdGuard &&) = delete;
};

static bool checkHip(hipError_t result, const char *message) {
    if (result != hipSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << hipGetErrorString(result) << ")";
        return false;
    }
    return true;
}

// Wrapper around hipMemImportFromShareableHandle to handle ROCm version
// differences. In ROCm 7.1+, the signature was changed:
// the second argument is (void*)(uintptr_t)fd instead of a pointer to the fd
static hipError_t importFromShareableHandle(
    hipMemGenericAllocationHandle_t *handle, hipxFabricHandle *export_handle) {
    static const int runtime_version = []() {
        int version = 0;
        if (!checkHip(hipRuntimeGetVersion(&version),
                      "HipTransport: hipRuntimeGetVersion failed")) {
            return -1;
        }
        return version;
    }();

    if (runtime_version < 0) {
        return hipErrorInvalidValue;
    }

    if (runtime_version >= 70100000) {
        return hipMemImportFromShareableHandle(
            handle, (void *)(uintptr_t)export_handle->fd,
            HIPX_MEM_HANDLE_TYPE_FABRIC);
    } else {
        return hipMemImportFromShareableHandle(handle, export_handle,
                                               HIPX_MEM_HANDLE_TYPE_FABRIC);
    }
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

    // Use RAII guard for automatic fd cleanup
    FdGuard fd_guard(-1);

    if (HIPX_MEM_HANDLE_TYPE_FABRIC == hipMemHandleTypePosixFileDescriptor) {
        int opened_fd = open_fd(export_handle);
        if (opened_fd == -1) {
            LOG(ERROR) << "HIPTransport: failed to open fd";
            return -1;
        }
        fd_guard.fd = opened_fd;
        export_handle.fd = opened_fd;
    }

    hipMemGenericAllocationHandle_t handle;
    if (!checkHip(importFromShareableHandle(&handle, &export_handle),
                  "HipTransport: importFromShareableHandle failed")) {
        return -1;
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

static int getDeviceFromPointer(void *ptr) {
    if (!ptr) {
        LOG(ERROR) << "HipTransport: null pointer passed to "
                      "getDeviceFromPointer";
        return -1;
    }

    hipPointerAttribute_t attributes;
    hipError_t err = hipPointerGetAttributes(&attributes, ptr);
    if (!checkHip(err, "HipTransport: hipPointerGetAttributes failed")) {
        return -1;
    }

    if (attributes.type == hipMemoryTypeDevice) {
        // GPU memory - return device ID
        return attributes.device;
    } else if (attributes.type == hipMemoryTypeHost ||
               attributes.type == hipMemoryTypeUnregistered) {
        // Host memory - return -1 to indicate CPU memory
        // This is not an error, just indicates we should use current device
        // context
        if (globalConfig().trace) {
            LOG(INFO) << "HipTransport: pointer " << ptr
                      << " is host memory (type: " << attributes.type
                      << "), will use current device context";
        }
        return -1;
    } else {
        LOG(WARNING) << "HipTransport: unknown memory type " << attributes.type
                     << " for pointer " << ptr;
        return -1;
    }
}

static int setDeviceContext(void *source_ptr, int &device_id) {
    // Get device ID from source pointer
    device_id = getDeviceFromPointer(source_ptr);

    // For host memory, determine current device or use device 0
    if (device_id < 0) {
        int current_device = 0;
        hipError_t err = hipGetDevice(&current_device);
        if (err == hipSuccess) {
            device_id = current_device;
        } else {
            device_id = 0;
        }
    }

    // Set device context
    if (!checkHip(hipSetDevice(device_id),
                  "HipTransport: failed to set device context")) {
        return -1;
    }

    return 0;
}

static void setupP2PAccess(int num_devices) {
    for (int i = 0; i < num_devices; ++i) {
        if (!checkHip(hipSetDevice(i), "HipTransport: failed to set device")) {
            continue;
        }

        for (int j = 0; j < num_devices; ++j) {
            if (i == j) {
                continue;
            }

            int can_access_peer = 0;
            if (!checkHip(hipDeviceCanAccessPeer(&can_access_peer, i, j),
                          "HipTransport: failed to query peer access")) {
                continue;
            }

            if (can_access_peer) {
                hipError_t result = hipDeviceEnablePeerAccess(j, 0);
                if (result != hipSuccess &&
                    result != hipErrorPeerAccessAlreadyEnabled) {
                    LOG(WARNING) << "HipTransport: failed to enable P2P access "
                                    "from device "
                                 << i << " to device " << j << " ("
                                 << hipGetErrorString(result) << ")";
                }
            } else if (i < j) {
                LOG(WARNING)
                    << "HipTransport: P2P access not available between device "
                    << i << " and device " << j;
            }
        }
    }
}

static int getNumStreams() {
    const char *env = getenv("MC_HIP_NUM_STREAMS");
    if (env) {
        try {
            int value = std::stoi(env);
            if (value > 0) {
                return value;
            }
            LOG(WARNING) << "MC_HIP_NUM_STREAMS value " << value
                         << " must be positive, using default "
                         << kDefaultNumStreams;
        } catch (...) {
            LOG(WARNING) << "Invalid MC_HIP_NUM_STREAMS value, using default "
                         << kDefaultNumStreams;
        }
    }
    return kDefaultNumStreams;
}

static int getNumEvents() {
    const char *env = getenv("MC_HIP_NUM_EVENTS");
    if (env) {
        try {
            int value = std::stoi(env);
            if (value > 0) {
                return value;
            }
            LOG(WARNING) << "MC_HIP_NUM_EVENTS value " << value
                         << " must be positive, using default "
                         << kDefaultNumEvents;
        } catch (...) {
            LOG(WARNING) << "Invalid MC_HIP_NUM_EVENTS value, using default "
                         << kDefaultNumEvents;
        }
    }
    return kDefaultNumEvents;
}

static bool supportFabricMem() {
    // By default, use IPC mode. Fabric memory is enabled only when
    // MC_USE_HIP_IPC=0 or MC_USE_NVLINK_IPC=0 is explicitly set.
    const char *hip_ipc = getenv("MC_USE_HIP_IPC");
    const char *nvlink_ipc = getenv("MC_USE_NVLINK_IPC");

    bool fabric_enabled = (hip_ipc && strcmp(hip_ipc, "0") == 0) ||
                          (nvlink_ipc && strcmp(nvlink_ipc, "0") == 0);

    if (!fabric_enabled) {
        return false;
    }

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

HipTransport::HipTransport()
    : use_fabric_mem_(supportFabricMem()),
      stream_pool_(getNumStreams()),
      event_pool_(getNumEvents()) {
    // Enable P2P access for IPC mode
    if (!use_fabric_mem_) {
        int num_devices = 0;
        if (!checkHip(hipGetDeviceCount(&num_devices),
                      "HipTransport: hipGetDeviceCount failed")) {
            return;
        }

        setupP2PAccess(num_devices);
    }
}

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

Status HipTransport::startAsyncTransfer(const TransferRequest &request,
                                        TransferTask &task,
                                        PendingTransfer &pending) {
    hipError_t err;
    int device_id;

    if (setDeviceContext(request.source, device_id) != 0) {
        return Status::InvalidArgument("Failed to set device context");
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

    task.slice_list.push_back(slice);

    __sync_fetch_and_add(&task.slice_count, 1);

    // Get event and stream from pools
    hipEvent_t event = event_pool_.getEvent(device_id);
    hipStream_t stream = stream_pool_.getNextStream(device_id);

    if (event == nullptr || stream == nullptr) {
        slice->markFailed();
        if (event != nullptr) {
            event_pool_.putEvent(event, device_id);
        }
        return Status::Memory("Failed to get event or stream from pool");
    }

    // Perform async memory copy
    if (slice->opcode == TransferRequest::READ) {
        err = hipMemcpyAsync(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, hipMemcpyDefault, stream);
    } else {
        err = hipMemcpyAsync((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, hipMemcpyDefault, stream);
    }

    if (!checkHip(err, "HipTransport: hipMemcpyAsync failed")) {
        slice->markFailed();
        event_pool_.putEvent(event, device_id);
        return Status::Memory("HipTransport: Async memory copy failed");
    }

    // Record event on the stream
    err = hipEventRecord(event, stream);
    if (!checkHip(err, "HipTransport: hipEventRecord failed")) {
        slice->markFailed();
        event_pool_.putEvent(event, device_id);
        return Status::Memory("HipTransport: Failed to record event");
    }

    // Store pending transfer info
    pending.event = event;
    pending.device_id = device_id;
    pending.slice = slice;

    return Status::OK();
}

void HipTransport::synchronizePendingTransfers(
    std::vector<PendingTransfer> &pending_transfers) {
    for (auto &pt : pending_transfers) {
        // Wait for event to complete
        hipError_t err = hipEventSynchronize(pt.event);
        if (err == hipSuccess) {
            pt.slice->markSuccess();
        } else {
            LOG(ERROR) << "HipTransport: hipEventSynchronize failed: "
                       << hipGetErrorString(err);
            pt.slice->markFailed();
        }

        // Return event to pool
        event_pool_.putEvent(pt.event, pt.device_id);
    }
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

    // Submit async transfers and collect pending transfer info
    std::vector<PendingTransfer> pending_transfers;
    pending_transfers.reserve(entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;

        PendingTransfer pending;
        Status status = startAsyncTransfer(request, task, pending);
        if (!status.ok()) {
            // Clean up any pending transfers we've already created
            for (auto &pt : pending_transfers) {
                event_pool_.putEvent(pt.event, pt.device_id);
            }
            return status;
        }
        pending_transfers.push_back(pending);
    }

    // Synchronize all pending transfers and mark slices as complete
    synchronizePendingTransfers(pending_transfers);

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
    // Submit async transfers and collect pending transfer info
    std::vector<PendingTransfer> pending_transfers;
    pending_transfers.reserve(task_list.size());

    for (auto *task_ptr : task_list) {
        assert(task_ptr);
        auto &task = *task_ptr;
        assert(task.request);
        auto &request = *task.request;

        PendingTransfer pending;
        Status status = startAsyncTransfer(request, task, pending);
        if (!status.ok()) {
            // Clean up any pending transfers we've already created
            for (auto &pt : pending_transfers) {
                event_pool_.putEvent(pt.event, pt.device_id);
            }
            return status;
        }
        pending_transfers.push_back(pending);
    }

    // Synchronize all pending transfers and mark slices as complete
    synchronizePendingTransfers(pending_transfers);

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

    hipDeviceptr_t base = nullptr;
    hipError_t result =
        hipMemGetAddressRange(&base, &size, (hipDeviceptr_t)ptr);
    if (checkHip(result, "HipTransport: hipMemGetAddressRange")) {
        (void)hipMemUnmap((hipDeviceptr_t)ptr, size);
        (void)hipMemAddressFree((hipDeviceptr_t)ptr, size);
    }

    (void)hipMemRelease(handle);
}
}  // namespace mooncake
