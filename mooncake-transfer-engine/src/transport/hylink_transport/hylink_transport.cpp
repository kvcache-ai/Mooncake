// Copyright 2026 KVCache.AI
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

#include "transport/hylink_transport/hylink_transport.h"

#include <glog/logging.h>

#include <array>
#include <cstring>
#include <exception>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "hip_device_guard.h"

namespace mooncake {

namespace {

constexpr uint32_t kHylinkMagic = 0x4B4C5948u;  // 'HYLK'
constexpr uint16_t kHylinkVersion = 1;
constexpr uint16_t kFlagFabric = 2u;
constexpr auto kDtkFabricHandleType =
    static_cast<hipMemAllocationHandleType>(0x8);
constexpr size_t kDtkFabricHandleBytes = 512;
using DtkFabricHandle = std::array<unsigned char, kDtkFabricHandleBytes>;

#pragma pack(push, 1)
struct HylinkBlobHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t ipc_bytes;
    uint32_t fabric_bytes;
};
#pragma pack(pop)

bool checkHip(hipError_t result, const char* message) {
    if (result != hipSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << hipGetErrorString(result) << ")";
        return false;
    }
    return true;
}

bool deviceSupportsFabric(int device) {
    static std::mutex mu;
    static std::unordered_map<int, bool> cache;
    {
        std::lock_guard<std::mutex> lock(mu);
        auto it = cache.find(device);
        if (it != cache.end()) return it->second;
    }

    bool supported = false;
    int vmm = 0;
    hipError_t err = hipDeviceGetAttribute(
        &vmm, hipDeviceAttributeVirtualMemoryManagementSupported, device);
    if (err == hipSuccess && vmm) {
        hipMemAllocationProp prop{};
        prop.type = hipMemAllocationTypePinned;
        prop.location.type = hipMemLocationTypeDevice;
        prop.location.id = device;
        prop.requestedHandleType = kDtkFabricHandleType;
        size_t granularity = 0;
        err = hipMemGetAllocationGranularity(
            &granularity, &prop, hipMemAllocationGranularityMinimum);
        size_t probe_size = 4096;
        if (err == hipSuccess && granularity > 0) {
            probe_size = granularity;
        } else {
            (void)hipGetLastError();
        }
        hipMemGenericAllocationHandle_t handle = nullptr;
        err = hipMemCreate(&handle, probe_size, &prop, 0);
        if (err == hipSuccess) {
            (void)hipMemRelease(handle);
            supported = true;
        } else {
            (void)hipGetLastError();
        }
    } else if (err != hipSuccess) {
        (void)hipGetLastError();
    }

    if (!supported) {
        static std::once_flag logged;
        std::call_once(logged, [err]() {
            LOG(INFO) << "HylinkTransport: DTK fabric is not available ("
                      << hipGetErrorString(err) << ")";
        });
    }
    std::lock_guard<std::mutex> lock(mu);
    cache[device] = supported;
    return supported;
}

int deviceFromLocation(const std::string& location) {
    auto pos = location.rfind(':');
    if (pos == std::string::npos || pos + 1 >= location.size()) return -1;
    try {
        return std::stoi(location.substr(pos + 1));
    } catch (const std::exception&) {
        return -1;
    }
}

int getDeviceFromPointer(void* ptr) {
    hipPointerAttribute_t attributes;
    hipError_t err = hipPointerGetAttributes(&attributes, ptr);
    if (err != hipSuccess) {
        (void)hipGetLastError();
        return -1;
    }
    if (attributes.type == hipMemoryTypeDevice) return attributes.device;
    return -1;
}

int setDeviceContext(void* source_ptr, int& device_id) {
    device_id = getDeviceFromPointer(source_ptr);
    if (device_id < 0) {
        int current = 0;
        if (hipGetDevice(&current) == hipSuccess) {
            device_id = current;
        } else {
            device_id = 0;
            (void)hipGetLastError();
        }
    }
    return checkHip(hipSetDevice(device_id),
                    "HylinkTransport: failed to set device context")
               ? 0
               : -1;
}

std::string encodeFabric(const DtkFabricHandle& fabric) {
    HylinkBlobHeader header{};
    header.magic = kHylinkMagic;
    header.version = kHylinkVersion;
    header.flags = kFlagFabric;
    header.ipc_bytes = 0;
    header.fabric_bytes = static_cast<uint32_t>(kDtkFabricHandleBytes);
    std::vector<unsigned char> blob(sizeof(header) + header.fabric_bytes);
    std::memcpy(blob.data(), &header, sizeof(header));
    std::memcpy(blob.data() + sizeof(header), fabric.data(), fabric.size());
    return serializeBinaryData(blob.data(), blob.size());
}

bool decodeFabric(const std::string& encoded, DtkFabricHandle& fabric) {
    std::vector<unsigned char> blob;
    try {
        deserializeBinaryData(encoded, blob);
    } catch (const std::exception&) {
        return false;
    }
    if (blob.size() < sizeof(HylinkBlobHeader)) return false;
    HylinkBlobHeader header{};
    std::memcpy(&header, blob.data(), sizeof(header));
    if (header.magic != kHylinkMagic || header.version != kHylinkVersion) {
        return false;
    }
    if ((header.flags & kFlagFabric) == 0) return false;
    if (header.fabric_bytes != kDtkFabricHandleBytes) {
        return false;
    }
    const size_t need = sizeof(header) + header.ipc_bytes + header.fabric_bytes;
    if (blob.size() < need) return false;
    std::memcpy(fabric.data(), blob.data() + sizeof(header) + header.ipc_bytes,
                fabric.size());
    return true;
}

bool enablePeerAccess() {
    int device_count = 0;
    int saved = 0;
    if (!checkHip(hipGetDevice(&saved),
                  "HylinkTransport: hipGetDevice failed") ||
        !checkHip(hipGetDeviceCount(&device_count),
                  "HylinkTransport: hipGetDeviceCount failed")) {
        return false;
    }
    if (device_count < 2) return true;
    for (int i = 0; i < device_count; ++i) {
        if (!checkHip(hipSetDevice(i),
                      "HylinkTransport: hipSetDevice failed")) {
            (void)hipSetDevice(saved);
            return false;
        }
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            if (!checkHip(hipDeviceCanAccessPeer(&can_access, i, j),
                          "HylinkTransport: hipDeviceCanAccessPeer failed")) {
                (void)hipSetDevice(saved);
                return false;
            }
            if (!can_access) continue;
            hipError_t err = hipDeviceEnablePeerAccess(j, 0);
            if (err == hipErrorPeerAccessAlreadyEnabled) {
                (void)hipGetLastError();
            } else if (err != hipSuccess) {
                LOG(ERROR) << "hipDeviceEnablePeerAccess failed: "
                           << hipGetErrorString(err);
                (void)hipSetDevice(saved);
                return false;
            }
        }
    }
    if (!checkHip(hipSetDevice(saved),
                  "HylinkTransport: failed to restore HIP device")) {
        return false;
    }
    return true;
}

}  // namespace

std::mutex HylinkTransport::fabric_alloc_mutex_;
std::unordered_set<void*> HylinkTransport::fabric_allocs_;

HylinkTransport::HylinkTransport() = default;

HylinkTransport::~HylinkTransport() {
    RWSpinlock::WriteGuard guard(remap_lock_);
    for (auto& segment : remap_) {
        for (auto& buffer : segment.second) {
            for (auto& device : buffer.second) closeMapping(device.second);
        }
    }
    remap_.clear();
    std::lock_guard<std::mutex> streams(stream_pool_mutex_);
    for (auto& pool : stream_pools_) {
        for (hipStream_t stream : pool.second) (void)hipStreamDestroy(stream);
    }
    stream_pools_.clear();
}

void* HylinkTransport::allocateFabricMemory(size_t size) {
    int device = 0;
    if (hipGetDevice(&device) != hipSuccess) return nullptr;
    if (!deviceSupportsFabric(device)) return nullptr;

    hipMemAllocationProp prop{};
    prop.type = hipMemAllocationTypePinned;
    prop.location.type = hipMemLocationTypeDevice;
    prop.location.id = device;
    prop.requestedHandleType = kDtkFabricHandleType;

    size_t granularity = 0;
    hipError_t err = hipMemGetAllocationGranularity(
        &granularity, &prop, hipMemAllocationGranularityMinimum);
    if (err != hipSuccess || granularity == 0) {
        (void)hipGetLastError();
        return nullptr;
    }
    size = (size + granularity - 1) & ~(granularity - 1);

    hipMemGenericAllocationHandle_t handle = nullptr;
    err = hipMemCreate(&handle, size, &prop, 0);
    if (err != hipSuccess) {
        (void)hipGetLastError();
        LOG(WARNING) << "HylinkTransport: DTK fabric allocation failed ("
                     << hipGetErrorString(err) << ")";
        return nullptr;
    }

    void* ptr = nullptr;
    err = hipMemAddressReserve((hipDeviceptr_t*)&ptr, size, 0, nullptr, 0);
    if (err == hipSuccess) {
        err = hipMemMap((hipDeviceptr_t)ptr, size, 0, handle, 0);
    }
    if (err == hipSuccess) {
        int device_count = 0;
        (void)hipGetDeviceCount(&device_count);
        std::vector<hipMemAccessDesc> access(device_count);
        for (int i = 0; i < device_count; ++i) {
            access[i].location.type = hipMemLocationTypeDevice;
            access[i].location.id = i;
            access[i].flags = hipMemAccessFlagsProtReadWrite;
        }
        err = hipMemSetAccess((hipDeviceptr_t)ptr, size, access.data(),
                              access.size());
    }
    if (err != hipSuccess) {
        if (ptr) {
            (void)hipMemUnmap((hipDeviceptr_t)ptr, size);
            (void)hipMemAddressFree((hipDeviceptr_t)ptr, size);
        }
        (void)hipMemRelease(handle);
        (void)hipGetLastError();
        LOG(WARNING) << "HylinkTransport: DTK fabric allocation failed ("
                     << hipGetErrorString(err) << ")";
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(fabric_alloc_mutex_);
    fabric_allocs_.insert(ptr);
    return ptr;
}

void HylinkTransport::freeFabricMemory(void* addr) {
    if (!addr) return;
    {
        std::lock_guard<std::mutex> lock(fabric_alloc_mutex_);
        if (fabric_allocs_.erase(addr) == 0) return;
    }
    hipMemGenericAllocationHandle_t handle = nullptr;
    if (hipMemRetainAllocationHandle(&handle, addr) == hipSuccess && handle) {
        hipDeviceptr_t base = 0;
        size_t size = 0;
        if (hipMemGetAddressRange(&base, &size, (hipDeviceptr_t)addr) ==
            hipSuccess) {
            (void)hipMemUnmap((hipDeviceptr_t)addr, size);
            (void)hipMemAddressFree((hipDeviceptr_t)addr, size);
        }
        (void)hipMemRelease(handle);
    } else {
        (void)hipGetLastError();
    }
}

int HylinkTransport::install(std::string& local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    (void)topology;
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    int device_count = 0;
    if (!checkHip(hipGetDeviceCount(&device_count),
                  "HylinkTransport: hipGetDeviceCount failed") ||
        device_count <= 0) {
        LOG(ERROR) << "HylinkTransport: no DTK device found";
        return ERR_INVALID_ARGUMENT;
    }
    for (int device = 0; device < device_count; ++device) {
        if (!deviceSupportsFabric(device)) {
            LOG(ERROR) << "HylinkTransport: DTK fabric VMM is not supported on "
                          "device "
                       << device;
            return ERR_INVALID_ARGUMENT;
        }
    }
    if (!enablePeerAccess()) return ERR_INVALID_ARGUMENT;

    auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    if (old_desc) *desc = *old_desc;
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (desc->protocol.empty()) {
        desc->protocol = "hylink";
    } else if (desc->protocol.find("hylink") == std::string::npos) {
        desc->protocol += ",hylink";
    }
#else
    desc->protocol = "hylink";
#endif
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    LOG(INFO) << "HylinkTransport installed (DTK fabric handle)";
    return 0;
}

hipStream_t HylinkTransport::acquireStream(int device_id) {
    std::lock_guard<std::mutex> lock(stream_pool_mutex_);
    auto& streams = stream_pools_[device_id];
    if (streams.empty()) {
        HipDeviceGuard guard(device_id);
        if (!guard.set_ok()) return nullptr;
        for (int i = 0; i < kStreamsPerDevice; ++i) {
            hipStream_t stream = nullptr;
            if (hipStreamCreate(&stream) == hipSuccess) {
                streams.push_back(stream);
            } else {
                (void)hipGetLastError();
            }
        }
        if (streams.empty()) {
            stream_pools_.erase(device_id);
            return nullptr;
        }
    }
    size_t& cursor = stream_cursors_[device_id];
    hipStream_t stream = streams[cursor % streams.size()];
    ++cursor;
    return stream;
}

void HylinkTransport::closeMapping(OpenedMapping& mapping) {
    if (mapping.shm_addr && mapping.length) {
        (void)hipMemUnmap((hipDeviceptr_t)mapping.shm_addr, mapping.length);
        (void)hipMemAddressFree((hipDeviceptr_t)mapping.shm_addr,
                                mapping.length);
    }
    if (mapping.fabric_handle) (void)hipMemRelease(mapping.fabric_handle);
    mapping.shm_addr = nullptr;
    mapping.fabric_handle = nullptr;
    mapping.length = 0;
}

Status HylinkTransport::startAsyncTransfer(const TransferRequest& request,
                                           TransferTask& task,
                                           PendingTransfer& pending) {
    int device_id = 0;
    if (setDeviceContext(request.source, device_id) != 0) {
        return Status::InvalidArgument("Failed to set device context");
    }

    uint64_t dest_addr = request.target_offset;
    if (request.target_id != LOCAL_SEGMENT_ID) {
        int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                             request.target_id);
        if (rc) return Status::Memory("device memory not registered");
    }

    task.total_bytes = request.length;
    Slice* slice = getSliceCache().allocate();
    slice->source_addr = (char*)request.source;
    slice->local.dest_addr = (char*)dest_addr;
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->task = &task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;
    task.slice_list.push_back(slice);
    __sync_fetch_and_add(&task.slice_count, 1);

    hipStream_t stream = acquireStream(device_id);
    if (!stream) {
        slice->markFailed();
        return Status::Memory("Failed to get a Hylink copy stream");
    }

    void* src = nullptr;
    void* dst = nullptr;
    if (slice->opcode == TransferRequest::READ) {
        dst = slice->source_addr;
        src = (void*)slice->local.dest_addr;
    } else {
        src = slice->source_addr;
        dst = (void*)slice->local.dest_addr;
    }
    hipError_t err =
        hipMemcpyAsync(dst, src, slice->length, hipMemcpyDefault, stream);
    if (!checkHip(err, "HylinkTransport: hipMemcpyAsync failed")) {
        slice->markFailed();
        return Status::Memory("HylinkTransport: async memory copy failed");
    }

    hipEvent_t event = nullptr;
    err = hipEventCreateWithFlags(&event, hipEventDisableTiming);
    if (err == hipSuccess) err = hipEventRecord(event, stream);
    if (!checkHip(err, "HylinkTransport: hipEventRecord failed")) {
        slice->markFailed();
        if (event) (void)hipEventDestroy(event);
        return Status::Memory("HylinkTransport: failed to record event");
    }

    pending.event = event;
    pending.device_id = device_id;
    pending.slice = slice;
    return Status::OK();
}

void HylinkTransport::synchronizePendingTransfers(
    std::vector<PendingTransfer>& pending_transfers) {
    for (auto& pt : pending_transfers) {
        HipDeviceGuard guard(pt.device_id);
        hipError_t err = hipEventSynchronize(pt.event);
        if (err == hipSuccess) {
            pt.slice->markSuccess();
        } else {
            LOG(ERROR) << "HylinkTransport: hipEventSynchronize failed: "
                       << hipGetErrorString(err);
            pt.slice->markFailed();
        }
        (void)hipEventDestroy(pt.event);
    }
}

Status HylinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "HylinkTransport: batch capacity exceeded";
        return Status::InvalidArgument(
            "HylinkTransport: batch capacity exceeded, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<PendingTransfer> pending_transfers;
    pending_transfers.reserve(entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        PendingTransfer pending;
        Status status = startAsyncTransfer(request, task, pending);
        if (!status.ok()) {
            for (auto& pt : pending_transfers) (void)hipEventDestroy(pt.event);
            return status;
        }
        pending_transfers.push_back(pending);
    }
    synchronizePendingTransfers(pending_transfers);
    return Status::OK();
}

Status HylinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument(
            "HylinkTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
    if (success_slice_count + failed_slice_count == task.slice_count) {
        status.s = failed_slice_count ? TransferStatusEnum::FAILED
                                      : TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status HylinkTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::vector<PendingTransfer> pending_transfers;
    pending_transfers.reserve(task_list.size());
    for (auto* task_ptr : task_list) {
        auto& task = *task_ptr;
        auto& request = *task.request;
        PendingTransfer pending;
        Status status = startAsyncTransfer(request, task, pending);
        if (!status.ok()) {
            for (auto& pt : pending_transfers) (void)hipEventDestroy(pt.event);
            return status;
        }
        pending_transfers.push_back(pending);
    }
    synchronizePendingTransfers(pending_transfers);
    return Status::OK();
}

int HylinkTransport::registerLocalMemory(void* addr, size_t length,
                                         const std::string& location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    (void)remote_accessible;
    std::lock_guard<std::mutex> lock(register_mutex_);

    hipPointerAttribute_t attr;
    hipError_t attr_err = hipPointerGetAttributes(&attr, addr);
    if (attr_err != hipSuccess || attr.type != hipMemoryTypeDevice) {
        (void)hipGetLastError();
        if (globalConfig().trace) {
            LOG(INFO) << "HylinkTransport: skipping non-device memory " << addr;
        }
        return 0;
    }

    int saved = 0;
    (void)hipGetDevice(&saved);
    int device_id = deviceFromLocation(location);
    if (device_id < 0) device_id = attr.device;
    if (hipSetDevice(device_id) != hipSuccess) {
        (void)hipGetLastError();
        return -1;
    }

    hipDeviceptr_t base = nullptr;
    size_t alloc_size = 0;
    hipError_t range_err = hipMemGetAddressRange(
        &base, &alloc_size, reinterpret_cast<hipDeviceptr_t>(addr));
    if (range_err != hipSuccess) {
        (void)hipGetLastError();
        base = reinterpret_cast<hipDeviceptr_t>(addr);
        alloc_size = length;
    }

    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(base);
    desc.length = alloc_size;
    desc.name = location;
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "hylink";
#endif

    auto cached = registered_exports_.find(desc.addr);
    if (cached == registered_exports_.end()) {
        hipMemGenericAllocationHandle_t handle = nullptr;
        hipError_t retain_err = hipMemRetainAllocationHandle(&handle, base);
        if (retain_err != hipSuccess || !handle) {
            (void)hipGetLastError();
            LOG(ERROR) << "HylinkTransport: memory at " << addr
                       << " is not a DTK fabric allocation";
            (void)hipSetDevice(saved);
            return -1;
        }
        DtkFabricHandle fabric{};
        hipError_t export_err = hipMemExportToShareableHandle(
            fabric.data(), handle, kDtkFabricHandleType, 0);
        (void)hipMemRelease(handle);
        if (export_err != hipSuccess) {
            (void)hipGetLastError();
            LOG(ERROR) << "HylinkTransport: DTK fabric export failed: "
                       << hipGetErrorString(export_err);
            (void)hipSetDevice(saved);
            return -1;
        }
        cached =
            registered_exports_.emplace(desc.addr, encodeFabric(fabric)).first;
    }
    desc.shm_name = cached->second;
    (void)hipSetDevice(saved);
    return metadata_->addLocalMemoryBuffer(desc, update_metadata);
}

int HylinkTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    hipDeviceptr_t base = nullptr;
    size_t alloc_size = 0;
    uint64_t key = reinterpret_cast<uint64_t>(addr);
    if (hipMemGetAddressRange(&base, &alloc_size,
                              reinterpret_cast<hipDeviceptr_t>(addr)) ==
        hipSuccess) {
        key = reinterpret_cast<uint64_t>(base);
    } else {
        (void)hipGetLastError();
    }
    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        registered_exports_.erase(key);
    }
    return metadata_->removeLocalMemoryBuffer(reinterpret_cast<void*>(key),
                                              update_metadata);
}

int HylinkTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    int device_id = 0;
    if (hipGetDevice(&device_id) != hipSuccess) {
        (void)hipGetLastError();
        return ERR_INVALID_ARGUMENT;
    }
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        LOG(ERROR) << "HylinkTransport: missing segment " << target_id;
        return ERR_INVALID_ARGUMENT;
    }

    for (auto& entry : desc->buffers) {
        if (entry.shm_name.empty() || entry.addr > dest_addr ||
            dest_addr + length > entry.addr + entry.length) {
            continue;
        }
#ifdef ENABLE_MULTI_PROTOCOL
        if (!entry.protocol.empty() && entry.protocol != "hylink") continue;
#endif

        {
            remap_lock_.lockShared();
            auto segment = remap_.find(target_id);
            if (segment != remap_.end()) {
                auto buffer = segment->second.find(entry.addr);
                if (buffer != segment->second.end()) {
                    auto mapping = buffer->second.find(device_id);
                    if (mapping != buffer->second.end()) {
                        dest_addr = dest_addr - entry.addr +
                                    reinterpret_cast<uint64_t>(
                                        mapping->second.shm_addr);
                        remap_lock_.unlockShared();
                        return 0;
                    }
                }
            }
            remap_lock_.unlockShared();
        }

        RWSpinlock::WriteGuard guard(remap_lock_);
        auto& device_map = remap_[target_id][entry.addr];
        auto existing = device_map.find(device_id);
        if (existing == device_map.end()) {
            DtkFabricHandle fabric{};
            if (!decodeFabric(entry.shm_name, fabric)) {
                LOG(ERROR) << "HylinkTransport: malformed fabric handle";
                return -1;
            }
            OpenedMapping opened;
            opened.length = entry.length;
            hipError_t err = hipMemImportFromShareableHandle(
                &opened.fabric_handle, fabric.data(), kDtkFabricHandleType);
            if (err != hipSuccess) {
                LOG(ERROR) << "HylinkTransport: import failed: "
                           << hipGetErrorString(err);
                return -1;
            }
            err = hipMemAddressReserve((hipDeviceptr_t*)&opened.shm_addr,
                                       opened.length, 0, nullptr, 0);
            if (err != hipSuccess) {
                closeMapping(opened);
                LOG(ERROR) << "HylinkTransport: hipMemAddressReserve failed: "
                           << hipGetErrorString(err);
                return -1;
            }
            err = hipMemMap((hipDeviceptr_t)opened.shm_addr, opened.length, 0,
                            opened.fabric_handle, 0);
            if (err != hipSuccess) {
                closeMapping(opened);
                LOG(ERROR) << "HylinkTransport: hipMemMap failed: "
                           << hipGetErrorString(err);
                return -1;
            }
            if (hipSetDevice(device_id) != hipSuccess) {
                closeMapping(opened);
                (void)hipGetLastError();
                return -1;
            }
            hipMemAccessDesc access{};
            access.location.type = hipMemLocationTypeDevice;
            access.location.id = device_id;
            access.flags = hipMemAccessFlagsProtReadWrite;
            err = hipMemSetAccess((hipDeviceptr_t)opened.shm_addr,
                                  opened.length, &access, 1);
            if (err != hipSuccess) {
                (void)hipGetLastError();
                closeMapping(opened);
                LOG(ERROR) << "HylinkTransport: hipMemSetAccess failed: "
                           << hipGetErrorString(err);
                return -1;
            }
            LOG(INFO) << "HylinkTransport: opened DTK fabric mapping for "
                      << (void*)entry.addr << " on device " << device_id;
            device_map.emplace(device_id, opened);
            existing = device_map.find(device_id);
        }
        dest_addr = dest_addr - entry.addr +
                    reinterpret_cast<uint64_t>(existing->second.shm_addr);
        return 0;
    }
    LOG(ERROR) << "Requested address " << (void*)dest_addr << " to "
               << (void*)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int HylinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

int HylinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc && !first_error) first_error = rc;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

}  // namespace mooncake
