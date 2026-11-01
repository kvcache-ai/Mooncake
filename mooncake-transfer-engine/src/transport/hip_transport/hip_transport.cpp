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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <map>
#include <unistd.h>
#include <sys/syscall.h>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "hip_device_guard.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

// Pool configuration constants
// These can be overridden via environment variables:
// - MC_HIP_NUM_STREAMS: number of HIP streams per device
// - MC_HIP_NUM_EVENTS: number of HIP events per device
// - MC_HIP_BATCH_COPY_MIN: small copies per device submission from which they
//   run as one batch copy kernel instead of one hipMemcpyAsync each
//   (0 disables)
// - MC_HIP_BATCH_COPY_GRID: maximum workgroups of one batch copy kernel
constexpr int kDefaultNumStreams = 64;
constexpr int kDefaultNumEvents = 64;
constexpr int kDefaultBatchCopyMin = 16;
constexpr int kDefaultBatchCopyGrid = 256;
// Descriptor slots per device; each covers BatchCopyKernel::kSlotRanges.
constexpr int kBatchCopySlots = 32;
// Larger copies stay on hipMemcpyAsync: the copy engines move them as fast
// as the kernel would (faster over several links), without using CUs.
constexpr uint64_t kBatchCopyMaxLength = 1ull << 20;
// Copies not 16-byte aligned run byte by byte in the kernel, so only short
// ones go there.
constexpr uint64_t kBatchCopyMaxUnaligned = 4096;

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
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    FdGuard(FdGuard&&) = delete;
    FdGuard& operator=(FdGuard&&) = delete;
};

static bool checkHip(hipError_t result, const char* message) {
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
    hipMemGenericAllocationHandle_t* handle, hipxFabricHandle* export_handle) {
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
            handle, (void*)(uintptr_t)export_handle->fd,
            HIPX_MEM_HANDLE_TYPE_FABRIC);
    } else {
        return hipMemImportFromShareableHandle(handle, export_handle,
                                               HIPX_MEM_HANDLE_TYPE_FABRIC);
    }
}

static int open_fd(const hipxFabricHandle& export_handle) {
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

static int openIPCHandle(const std::vector<unsigned char>& buffer,
                         void** shm_addr) {
    hipIpcMemHandle_t handle;
    memcpy(&handle, buffer.data(), sizeof(handle));
    if (!checkHip(hipIpcOpenMemHandle(shm_addr, handle,
                                      hipIpcMemLazyEnablePeerAccess),
                  "HipTransport: hipIpcOpenMemHandle failed")) {
        return -1;
    }
    return 0;
}

static int openShareableHandle(const std::vector<unsigned char>& buffer,
                               size_t length, void** shm_addr) {
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

    if (!checkHip(hipMemAddressReserve((hipDeviceptr_t*)shm_addr, length, 0,
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

static int getDeviceFromPointer(void* ptr) {
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

// Device allocations already looked up in one submission. Its buffers
// usually fall in a few allocations, so most need no HIP call.
struct DeviceAllocation {
    uint64_t begin;
    uint64_t end;
    int device_id;
};

// Device that owns ptr, and whether ptr is device memory. Host memory gets
// fallback_device.
static int getBufferDevice(void* ptr, int fallback_device,
                           std::vector<DeviceAllocation>& allocations,
                           bool& device_memory) {
    const uint64_t addr = (uint64_t)ptr;
    for (const auto& allocation : allocations) {
        if (addr >= allocation.begin && addr < allocation.end) {
            device_memory = true;
            return allocation.device_id;
        }
    }
    int device_id = getDeviceFromPointer(ptr);
    device_memory = device_id >= 0;
    if (!device_memory) return fallback_device;
    hipDeviceptr_t base = nullptr;
    size_t size = 0;
    if (allocations.size() < 16 &&
        hipMemGetAddressRange(&base, &size, (hipDeviceptr_t)ptr) ==
            hipSuccess) {
        allocations.push_back(
            {(uint64_t)base, (uint64_t)base + size, device_id});
    }
    return device_id;
}

// Returns whether every device can access every other one.
static bool setupP2PAccess(int num_devices) {
    bool all_enabled = true;
    // The loop switches devices; the guard restores the caller's on return.
    HipDeviceGuard device_guard;

    auto clearStickyPeerAccessError = [](int src_device, int dst_device) {
        // hipDeviceEnablePeerAccess may leave hipErrorPeerAccessAlreadyEnabled
        // in the runtime's last-error slot. Clear it so subsequent PyTorch HIP
        // calls (for example torch.empty during the next connector init) do not
        // observe a stale error.
        hipError_t last_error = hipGetLastError();
        if (last_error != hipSuccess &&
            last_error != hipErrorPeerAccessAlreadyEnabled) {
            LOG(WARNING) << "HipTransport: unexpected sticky HIP error after "
                            "enabling P2P access from device "
                         << src_device << " to device " << dst_device << " ("
                         << hipGetErrorString(last_error) << ")";
        }
    };

    for (int i = 0; i < num_devices; ++i) {
        if (!checkHip(hipSetDevice(i), "HipTransport: failed to set device")) {
            all_enabled = false;
            continue;
        }

        for (int j = 0; j < num_devices; ++j) {
            if (i == j) {
                continue;
            }

            int can_access_peer = 0;
            if (!checkHip(hipDeviceCanAccessPeer(&can_access_peer, i, j),
                          "HipTransport: failed to query peer access")) {
                all_enabled = false;
                continue;
            }
            all_enabled = all_enabled && can_access_peer;

            if (can_access_peer) {
                hipError_t result = hipDeviceEnablePeerAccess(j, 0);
                if (result == hipErrorPeerAccessAlreadyEnabled) {
                    clearStickyPeerAccessError(i, j);
                } else if (result != hipSuccess) {
                    all_enabled = false;
                    clearStickyPeerAccessError(i, j);
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
    return all_enabled;
}

static int getNumStreams() {
    const char* env = getenv("MC_HIP_NUM_STREAMS");
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
    const char* env = getenv("MC_HIP_NUM_EVENTS");
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

static int getNonNegativeEnv(const char* name, int default_value) {
    const char* env = getenv(name);
    if (env) {
        try {
            int value = std::stoi(env);
            if (value >= 0) {
                return value;
            }
            LOG(WARNING) << name << " value " << value
                         << " must not be negative, using default "
                         << default_value;
        } catch (...) {
            LOG(WARNING) << "Invalid " << name << " value, using default "
                         << default_value;
        }
    }
    return default_value;
}

static bool supportFabricMem() {
    // By default, use IPC mode. Fabric memory is enabled only when
    // MC_USE_HIP_IPC=0 or MC_USE_NVLINK_IPC=0 is explicitly set.
    const char* hip_ipc = getenv("MC_USE_HIP_IPC");
    const char* nvlink_ipc = getenv("MC_USE_NVLINK_IPC");

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
      all_peer_access_(false),
      stream_pool_(getNumStreams()),
      event_pool_(getNumEvents()),
      batch_copy_min_copies_(
          getNonNegativeEnv("MC_HIP_BATCH_COPY_MIN", kDefaultBatchCopyMin)),
      batch_copy_(kBatchCopySlots,
                  std::max(1, getNonNegativeEnv("MC_HIP_BATCH_COPY_GRID",
                                                kDefaultBatchCopyGrid))) {
    // Enable P2P access for IPC mode
    if (!use_fabric_mem_) {
        int num_devices = 0;
        if (!checkHip(hipGetDeviceCount(&num_devices),
                      "HipTransport: hipGetDeviceCount failed")) {
            return;
        }

        all_peer_access_ = setupP2PAccess(num_devices);
    }
}

HipTransport::~HipTransport() {
    if (use_fabric_mem_) {
        for (auto& entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto& entry : remap_entries_) {
            (void)hipIpcCloseMemHandle(entry.second.shm_addr);
        }
    }
    remap_entries_.clear();
}

int HipTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> metadata,
                          std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    // Compose with any local segment another transport (e.g. RDMA) already
    // installed instead of overwriting it, so a single-node segment can
    // advertise both protocols (e.g. "rdma,hip"). Work on a copy (the map may
    // hand back a descriptor other threads are reading) and publish the new
    // one atomically via addLocalSegment.
    auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    if (old_desc) *desc = *old_desc;

    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (desc->protocol.empty()) {
        desc->protocol = "hip";
    } else if (desc->protocol.find("hip") == std::string::npos) {
        desc->protocol += ",hip";
    }
#else
    desc->protocol = "hip";
#endif

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

namespace {

// One event recorded after a device's copies. Each of their slices holds a
// reference; the last one to finish returns the event to the pool.
struct HipCompletionEvent {
    hipEvent_t event;
    int device_id;
    EventPool* pool;
    std::atomic<size_t> refs;
    // Set once a query saw the event complete, so the other slices of the
    // submission need not query it again.
    std::atomic<bool> done{false};
};

void releaseCompletionEvent(HipCompletionEvent* completion) {
    if (completion->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        completion->pool->putEvent(completion->event, completion->device_id);
        delete completion;
    }
}

// Only the caller that takes the event may query it and finish the slice.
HipCompletionEvent* takeCompletionEvent(Transport::Slice* slice) {
    return static_cast<HipCompletionEvent*>(__atomic_exchange_n(
        &slice->local.hip_event, nullptr, __ATOMIC_ACQ_REL));
}

// Slice cleanup hook: drop the reference of a slice freed while in flight.
// Finishes the slice if its copies are done (with wait, once they are), and
// returns whether it is finished.
bool finishSlice(Transport::Slice* slice, bool wait) {
    HipCompletionEvent* completion = takeCompletionEvent(slice);
    if (completion == nullptr) return true;
    hipError_t err = hipSuccess;
    if (!completion->done.load(std::memory_order_acquire)) {
        err = wait ? hipEventSynchronize(completion->event)
                   : hipEventQuery(completion->event);
        if (err == hipSuccess) {
            completion->done.store(true, std::memory_order_release);
        }
    }
    if (err == hipErrorNotReady) {
        // Still in flight; hand the event back for the next poll.
        __atomic_store_n(&slice->local.hip_event, completion, __ATOMIC_RELEASE);
        return false;
    }
    if (checkHip(err, "HipTransport: waiting for the copy event failed")) {
        slice->markSuccess();
    } else {
        slice->markFailed();
    }
    releaseCompletionEvent(completion);
    return true;
}

void releaseSliceEvent(Transport::Slice* slice) {
    if (auto* completion = takeCompletionEvent(slice)) {
        releaseCompletionEvent(completion);
    }
}

}  // namespace

void HipTransport::submitDeviceCopies(int device_id,
                                      const std::vector<Slice*>& slices,
                                      const std::vector<bool>& device_memory) {
    hipStream_t stream = stream_pool_.getNextStream(device_id);
    hipEvent_t event = stream ? event_pool_.getEvent(device_id) : nullptr;
    if (event == nullptr) {
        LOG(ERROR) << "HipTransport: failed to get a stream or event on device "
                   << device_id;
        for (Slice* slice : slices) slice->markFailed();
        return;
    }

    // Small copies go to the batch copy kernel when there are enough of
    // them, each with the index of its slice. Copies to different targets are
    // interleaved so that the workgroups running at any moment write over
    // several links.
    std::vector<size_t> small;
    std::vector<size_t> memcpy_slices;
    for (size_t i = 0; i < slices.size(); ++i) {
        const Slice* slice = slices[i];
        const bool aligned =
            ((uint64_t)slice->source_addr | (uint64_t)slice->local.dest_addr) %
                16 ==
            0;
        if (device_memory[i] && slice->length <= kBatchCopyMaxLength &&
            (aligned || slice->length <= kBatchCopyMaxUnaligned)) {
            small.push_back(i);
        } else {
            memcpy_slices.push_back(i);
        }
    }
    std::vector<BatchCopyKernel::Range> ranges;
    std::vector<size_t> range_slices;
    if (all_peer_access_ && batch_copy_min_copies_ > 0 &&
        small.size() >= batch_copy_min_copies_ &&
        batch_copy_.available(device_id)) {
        std::map<uint64_t, std::vector<size_t>> by_target;
        for (size_t i : small) by_target[slices[i]->target_id].push_back(i);
        for (size_t k = 0; range_slices.size() < small.size(); ++k) {
            for (auto& [target, indices] : by_target) {
                if (k >= indices.size()) continue;
                const Slice* slice = slices[indices[k]];
                uint64_t local = (uint64_t)slice->source_addr;
                uint64_t remote = (uint64_t)slice->local.dest_addr;
                if (slice->opcode == TransferRequest::READ) {
                    ranges.push_back({remote, local, slice->length});
                } else {
                    ranges.push_back({local, remote, slice->length});
                }
                range_slices.push_back(indices[k]);
            }
        }
    } else {
        memcpy_slices.insert(memcpy_slices.end(), small.begin(), small.end());
    }

    size_t launched = 0;
    if (!ranges.empty()) {
        launched = batch_copy_.enqueue(device_id, stream, ranges.data(),
                                       ranges.size());
    }

    std::vector<bool> failed(slices.size(), false);
    auto copy = [&](size_t i, void* dst, const void* src, size_t len) {
        if (!checkHip(hipMemcpyAsync(dst, src, len, hipMemcpyDefault, stream),
                      "HipTransport: hipMemcpyAsync failed")) {
            failed[i] = true;
        }
    };
    // Ranges of a failed launch.
    for (size_t r = launched; r < ranges.size(); ++r) {
        copy(range_slices[r], (void*)ranges[r].dst, (const void*)ranges[r].src,
             ranges[r].len);
    }
    for (size_t i : memcpy_slices) {
        Slice* slice = slices[i];
        if (slice->opcode == TransferRequest::READ) {
            copy(i, slice->source_addr, slice->local.dest_addr, slice->length);
        } else {
            copy(i, slice->local.dest_addr, slice->source_addr, slice->length);
        }
    }

    std::vector<Slice*> posted;
    for (size_t i = 0; i < slices.size(); ++i) {
        if (failed[i]) {
            slices[i]->markFailed();
        } else {
            posted.push_back(slices[i]);
        }
    }
    if (posted.empty()) {
        event_pool_.putEvent(event, device_id);
        return;
    }

    if (!checkHip(hipEventRecord(event, stream),
                  "HipTransport: hipEventRecord failed")) {
        // Without an event, drain the copies in flight before failing them.
        event_pool_.putEvent(event, device_id);
        (void)hipStreamSynchronize(stream);
        for (Slice* slice : posted) slice->markFailed();
        return;
    }

    auto* completion =
        new HipCompletionEvent{event, device_id, &event_pool_, {posted.size()}};
    for (Slice* slice : posted) {
        slice->status = Slice::POSTED;
        __atomic_store_n(&slice->local.hip_event, completion, __ATOMIC_RELEASE);
    }
}

Status HipTransport::submitTasks(
    const std::vector<std::pair<TransferTask*, const TransferRequest*>>&
        tasks) {
    // Resolve every request first, so a bad one fails the whole submission
    // instead of leaving earlier copies in flight behind an error. Switch to
    // the device that will run the copy (the local buffer's) before the first
    // relocation of a target opens its IPC handle, which hipIpcOpenMemHandle
    // maps for the current device.
    HipDeviceGuard device_guard;
    int caller_device = 0;
    if (hipGetDevice(&caller_device) != hipSuccess) caller_device = 0;
    int current_device = caller_device;
    std::vector<uint64_t> dest_addrs(tasks.size());
    std::vector<int> device_ids(tasks.size(), -1);
    // Whether both buffers are device memory, which the batch copy kernel
    // needs: it cannot dereference pageable host memory.
    std::vector<bool> device_memory(tasks.size(), false);
    Status status = Status::OK();
    std::vector<DeviceAllocation> allocations;
    // The buffer last matched for a target, and its offset to the mapping.
    struct Mapped {
        uint64_t target_id;
        uint64_t begin;
        uint64_t end;
        uint64_t delta;
    };
    std::vector<Mapped> mapped;
    for (size_t i = 0; i < tasks.size() && status.ok(); ++i) {
        const TransferRequest& request = *tasks[i].second;
        uint64_t& dest = dest_addrs[i];
        dest = request.target_offset;
        bool source_on_device = false;
        device_ids[i] = getBufferDevice(request.source, caller_device,
                                        allocations, source_on_device);
        if (request.target_id == LOCAL_SEGMENT_ID) {
            bool dest_on_device = false;
            getBufferDevice((void*)dest, caller_device, allocations,
                            dest_on_device);
            device_memory[i] = source_on_device && dest_on_device;
            continue;
        }
        // Targets of other processes are mapped device memory.
        device_memory[i] = source_on_device;
        auto it = std::find_if(mapped.begin(), mapped.end(), [&](auto& m) {
            return m.target_id == request.target_id && dest >= m.begin &&
                   dest + request.length <= m.end;
        });
        if (it != mapped.end()) {
            dest += it->delta;
            continue;
        }
        if (device_ids[i] != current_device) {
            if (!checkHip(hipSetDevice(device_ids[i]),
                          "HipTransport: failed to set device context")) {
                status =
                    Status::InvalidArgument("failed to set device context");
                break;
            }
            current_device = device_ids[i];
        }
        const uint64_t original = dest;
        uint64_t buffer_addr = 0, buffer_length = 0;
        if (relocateSharedMemoryAddress(dest, request.length, request.target_id,
                                        &buffer_addr, &buffer_length)) {
            status = Status::Memory("device memory not registered");
        } else {
            mapped.push_back({request.target_id, buffer_addr,
                              buffer_addr + buffer_length, dest - original});
        }
    }

    std::map<int, std::vector<Slice*>> device_slices;
    std::map<int, std::vector<bool>> device_slice_memory;
    for (size_t i = 0; i < tasks.size(); ++i) {
        TransferTask& task = *tasks[i].first;
        const TransferRequest& request = *tasks[i].second;
        task.total_bytes = request.length;

        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->local.dest_addr = (char*)dest_addrs[i];
        slice->local.hip_event = nullptr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        slice->cleanup_callback = releaseSliceEvent;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        if (!status.ok()) {
            // Every task gets a failed slice so none reads as COMPLETED.
            slice->markFailed();
            continue;
        }
        device_slices[device_ids[i]].push_back(slice);
        device_slice_memory[device_ids[i]].push_back(device_memory[i]);
    }

    for (auto& [device_id, slices] : device_slices) {
        if (!checkHip(hipSetDevice(device_id),
                      "HipTransport: failed to set device context")) {
            for (Slice* slice : slices) slice->markFailed();
            continue;
        }
        submitDeviceCopies(device_id, slices, device_slice_memory[device_id]);
    }

#ifdef USE_EVENT_DRIVEN_COMPLETION
    // Event-driven waiters (e.g. Mooncake Store) block on the batch's
    // condition variable and never call getTransferStatus, which is what
    // finishes a slice here. Finish the copies before returning instead, as
    // before submission became asynchronous.
    for (auto& [device_id, slices] : device_slices) {
        for (Slice* slice : slices) finishSlice(slice, true);
    }
#endif
    return status;
}

Status HipTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "HipTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "HipTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::vector<std::pair<TransferTask*, const TransferRequest*>> tasks;
    for (auto& request : entries) {
        tasks.emplace_back(&batch_desc.task_list[task_id++], &request);
    }
    return submitTasks(tasks);
}

Status HipTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();

    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "HipTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }

    // Get task and status info
    auto& task = batch_desc.task_list[task_id];
    for (Slice* slice : task.slice_list) finishSlice(slice, false);
    uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    // Completion counters publish the preceding byte updates.
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);

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
    const std::vector<TransferTask*>& task_list) {
    std::vector<std::pair<TransferTask*, const TransferRequest*>> tasks;
    for (auto* task_ptr : task_list) {
        assert(task_ptr && task_ptr->request);
        tasks.emplace_back(task_ptr, task_ptr->request);
    }
    return submitTasks(tasks);
}

int HipTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);

    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }

    // IPC-based memory registration
    if (!use_fabric_mem_) {
        // Only device memory can be exported via HIP IPC. Callers that register
        // a batch across all transports (e.g. SGLang's PD metadata/aux buffers,
        // which live in host memory) also hand those host buffers to this
        // transport. Skip non-device memory gracefully and let RDMA/TCP
        // register it; returning an error would roll back the entire
        // multi-protocol batch and tear down every session.
        hipPointerAttribute_t attr;
        hipError_t attr_err = hipPointerGetAttributes(&attr, addr);
        if (attr_err != hipSuccess || attr.type != hipMemoryTypeDevice) {
            // Clear any sticky error the failed query latched so it does not
            // poison subsequent HIP calls made by the caller.
            (void)hipGetLastError();
            if (globalConfig().trace) {
                LOG(INFO) << "HipTransport: skipping non-device memory " << addr
                          << ", leaving it to other transports";
            }
            return 0;
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
#ifdef ENABLE_MULTI_PROTOCOL
        desc.protocol = "hip";
#endif
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
        void* real_addr;
        size_t real_size;
        result = hipMemGetAddressRange((hipDeviceptr_t*)&real_addr, &real_size,
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
        desc.shm_name = serializeBinaryData((const void*)&export_handle_raw,
                                            sizeof(hipxFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int HipTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int HipTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                              uint64_t length,
                                              uint64_t target_id,
                                              uint64_t* buffer_addr,
                                              uint64_t* buffer_length) {
    auto desc = metadata_->getSegmentDescByID(target_id);

    // Search for matching buffer entry
    for (auto& entry : desc->buffers) {
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
                if (buffer_addr) *buffer_addr = entry.addr;
                if (buffer_length) *buffer_length = entry.length;
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);

                void* shm_addr = nullptr;
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
            if (buffer_addr) *buffer_addr = entry.addr;
            if (buffer_length) *buffer_length = entry.length;
            return 0;
        }
    }
    LOG(ERROR) << "Requested address " << (void*)dest_addr << " to "
               << (void*)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int HipTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

int HipTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc && !first_error) first_error = rc;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

void* HipTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void* ptr = nullptr;
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
    void* ptr = nullptr;
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

    result = hipMemAddressReserve((hipDeviceptr_t*)&ptr, size, granularity,
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

void HipTransport::freePinnedLocalMemory(void* ptr) {
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
