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

#include "tent/transport/hylink/hylink_transport.h"

#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include <exception>
#include <vector>

#include "tent/common/utils/string_builder.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

constexpr uint32_t kHylinkMagic = 0x4B4C5948u;  // 'HYLK' little-endian
constexpr uint16_t kHylinkVersion = 1;
constexpr uint16_t kFlagIpc = 1u;
constexpr uint16_t kFlagFabric = 2u;

namespace {

// MC_HYLINK_USE_VMM=1 (default) exports and opens a DTK fabric handle.
// =0 uses HIP IPC. Both peers must use the same setting.
bool hylinkUseVmm() {
    const char* value = getenv("MC_HYLINK_USE_VMM");
    return !(value && std::strcmp(value, "0") == 0);
}

// Probe each device once: the VMM attribute, then a tiny hipMemCreate with
// the DTK fabric handle type. Later allocations reuse the result instead of
// failing hipMemCreate on every buffer.
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
        if (err == hipSuccess && granularity > 0)
            probe_size = granularity;
        else
            (void)hipGetLastError();
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

}  // namespace

#pragma pack(push, 1)
struct HylinkBlobHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t ipc_bytes;
    uint32_t fabric_bytes;
};
#pragma pack(pop)

std::string HylinkTransport::encodeExport(const ExportedHandle& exported) {
    HylinkBlobHeader header{};
    header.magic = kHylinkMagic;
    header.version = kHylinkVersion;
    if (exported.has_ipc) {
        header.flags |= kFlagIpc;
        header.ipc_bytes = sizeof(hipIpcMemHandle_t);
    }
    if (exported.has_fabric) {
        header.flags |= kFlagFabric;
        header.fabric_bytes = kDtkFabricHandleBytes;
    }
    std::vector<unsigned char> blob(sizeof(header) + header.ipc_bytes +
                                    header.fabric_bytes);
    std::memcpy(blob.data(), &header, sizeof(header));
    size_t offset = sizeof(header);
    if (exported.has_ipc) {
        std::memcpy(blob.data() + offset, &exported.ipc, sizeof(exported.ipc));
        offset += sizeof(exported.ipc);
    }
    if (exported.has_fabric) {
        std::memcpy(blob.data() + offset, exported.fabric.data(),
                    exported.fabric.size());
    }
    return serializeBinaryData(blob.data(), blob.size());
}

bool HylinkTransport::decodeExport(const std::string& encoded,
                                   ExportedHandle& exported) {
    exported = {};
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
    const size_t need = sizeof(header) + header.ipc_bytes + header.fabric_bytes;
    if (blob.size() < need) return false;
    size_t offset = sizeof(header);
    if (header.flags & kFlagIpc) {
        if (header.ipc_bytes != sizeof(hipIpcMemHandle_t)) return false;
        std::memcpy(&exported.ipc, blob.data() + offset, sizeof(exported.ipc));
        exported.has_ipc = true;
        offset += header.ipc_bytes;
    }
    if (header.flags & kFlagFabric) {
        if (header.fabric_bytes != kDtkFabricHandleBytes) return false;
        std::memcpy(exported.fabric.data(), blob.data() + offset,
                    exported.fabric.size());
        exported.has_fabric = true;
    }
    return exported.has_ipc || exported.has_fabric;
}

namespace {

// DTK device locations are "hip:N". "dtk:N" is accepted as the same device.
bool isDtkDeviceLocation(const std::string& type) {
    return type == "hip" || type == "dtk";
}

Status syncWithCallerStream() {
    int device_id = 0;
    CHECK_HIP(hipGetDevice(&device_id));
    hipEvent_t event = nullptr;
    CHECK_HIP(hipEventCreateWithFlags(&event, hipEventDisableTiming));
    hipError_t record = hipEventRecord(event, hipStreamPerThread);
    hipError_t wait = hipSuccess;
    if (record == hipSuccess) wait = hipEventSynchronize(event);
    (void)hipEventDestroy(event);
    if (record != hipSuccess || wait != hipSuccess) {
        return Status::InternalError(
            "unable to synchronize caller HIP stream" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace

HylinkTransport::HylinkTransport() = default;

HylinkTransport::~HylinkTransport() { uninstall(); }

std::mutex HylinkTransport::fabric_alloc_mutex_;
std::unordered_set<void*> HylinkTransport::fabric_allocs_;

void* HylinkTransport::allocateFabricMemory(size_t size) {
    int device = 0;
    if (hipGetDevice(&device) != hipSuccess) return nullptr;

    // IPC mode allocates with hipMalloc. On the fabric path a failed probe
    // stops here; there is no hipMalloc fallback and no fabric mapping.
    if (!hylinkUseVmm()) {
        void* ptr = nullptr;
        if (hipMalloc(&ptr, size) != hipSuccess) return nullptr;
        return ptr;
    }
    if (!deviceSupportsFabric(device)) return nullptr;

    hipMemAllocationProp prop{};
    prop.type = hipMemAllocationTypePinned;
    prop.location.type = hipMemLocationTypeDevice;
    prop.location.id = device;
    prop.requestedHandleType = kDtkFabricHandleType;

    size_t granularity = 0;
    hipError_t err = hipMemGetAllocationGranularity(
        &granularity, &prop, hipMemAllocationGranularityMinimum);
    if (err == hipSuccess) {
        size = (size + granularity - 1) & ~(granularity - 1);
        hipMemGenericAllocationHandle_t handle = nullptr;
        err = hipMemCreate(&handle, size, &prop, 0);
        if (err == hipSuccess) {
            void* ptr = nullptr;
            err = hipMemAddressReserve((hipDeviceptr_t*)&ptr, size, 0, nullptr,
                                       0);
            if (err == hipSuccess)
                err = hipMemMap((hipDeviceptr_t)ptr, size, 0, handle, 0);
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
            } else {
                // Remember VMM pointers: hipMemRetainAllocationHandle only
                // accepts VMM allocations on DTK, so the free path must not
                // probe hipMalloc fallbacks.
                std::lock_guard<std::mutex> lock(fabric_alloc_mutex_);
                fabric_allocs_.insert(ptr);
                return ptr;
            }
        }
        (void)hipGetLastError();
        LOG(WARNING) << "HylinkTransport: DTK fabric allocation failed ("
                     << hipGetErrorString(err) << ")";
    } else {
        (void)hipGetLastError();
    }
    return nullptr;
}

void HylinkTransport::freeFabricMemory(void* addr) {
    if (!addr) return;
    {
        std::lock_guard<std::mutex> lock(fabric_alloc_mutex_);
        if (fabric_allocs_.erase(addr) == 0) {
            // hipMalloc fallback buffer.
            (void)hipFree(addr);
            return;
        }
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

Status HylinkTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "hylink transport has already been installed" LOC_MARK);
    }
    int device_count = 0;
    CHECK_HIP(hipGetDeviceCount(&device_count));
    if (device_count <= 0) {
        return Status::InvalidArgument(
            "hylink transport requires a DTK device" LOC_MARK);
    }
    if (hylinkUseVmm()) {
        for (int device = 0; device < device_count; ++device) {
            if (!deviceSupportsFabric(device)) {
                return Status::InvalidArgument(
                    "hylink fabric VMM is not supported on this "
                    "device" LOC_MARK);
            }
        }
    }
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;

    auto peer_status = setPeerAccess();
    if (!peer_status.ok()) {
        metadata_.reset();
        local_topology_.reset();
        conf_.reset();
        return peer_status;
    }
    installed_ = true;
    LOG(INFO) << "HylinkTransport installed ("
              << (hylinkUseVmm() ? "DTK fabric handle" : "HIP IPC") << ")";
    return Status::OK();
}

Status HylinkTransport::uninstall() {
    if (!installed_) return Status::OK();
    {
        RWSpinlock::WriteGuard guard(relocate_lock_);
        for (auto& segment : relocate_map_) {
            for (auto& entry : segment.second)
                for (auto& dev : entry.second) closeMapping(dev.second);
        }
        relocate_map_.clear();
        relocate_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    {
        std::lock_guard<std::mutex> lock(stream_pool_mutex_);
        int saved = 0;
        const bool have_device = hipGetDevice(&saved) == hipSuccess;
        for (auto& entry : stream_pools_) {
            if (have_device) (void)hipSetDevice(entry.first);
            for (auto stream : entry.second) (void)hipStreamDestroy(stream);
        }
        stream_pools_.clear();
        stream_cursors_.clear();
        if (have_device) (void)hipSetDevice(saved);
    }
    metadata_.reset();
    installed_ = false;
    return Status::OK();
}

void HylinkTransport::closeMapping(OpenedMapping& mapping) {
    if (!mapping.shm_addr) return;
    if (mapping.kind == HylinkHandleKind::kIpc) {
        hipError_t err = hipIpcCloseMemHandle(mapping.shm_addr);
        if (err != hipSuccess) {
            LOG(WARNING) << "HylinkTransport: hipIpcCloseMemHandle failed: "
                         << hipGetErrorString(err);
        }
    } else if (mapping.kind == HylinkHandleKind::kFabric) {
        // Unmap first. The imported handle stays alive until the mapping is
        // gone, then hipMemRelease drops it.
        (void)hipMemUnmap((hipDeviceptr_t)mapping.shm_addr, mapping.length);
        (void)hipMemAddressFree((hipDeviceptr_t)mapping.shm_addr,
                                mapping.length);
        if (mapping.fabric_handle) (void)hipMemRelease(mapping.fabric_handle);
    }
    mapping.shm_addr = nullptr;
    mapping.fabric_handle = nullptr;
    mapping.kind = HylinkHandleKind::kNone;
}

Status HylinkTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto* hylink_batch = Slab<HylinkSubBatch>::Get().allocate();
    if (!hylink_batch) {
        return Status::InternalError("Unable to allocate hylink sub-batch");
    }
    batch = hylink_batch;
    hylink_batch->task_list.reserve(max_size);
    hylink_batch->max_size = max_size;
    return Status::OK();
}

Status HylinkTransport::freeSubBatch(SubBatchRef& batch) {
    auto* hylink_batch = dynamic_cast<HylinkSubBatch*>(batch);
    if (!hylink_batch) {
        return Status::InvalidArgument("Invalid hylink sub-batch" LOC_MARK);
    }
    Slab<HylinkSubBatch>::Get().deallocate(hylink_batch);
    batch = nullptr;
    return Status::OK();
}

Status HylinkTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* hylink_batch = dynamic_cast<HylinkSubBatch*>(batch);
    if (!hylink_batch) {
        return Status::InvalidArgument("Invalid hylink sub-batch" LOC_MARK);
    }
    if (request_list.size() + hylink_batch->task_list.size() >
        hylink_batch->max_size) {
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    }

    CHECK_STATUS(syncWithCallerStream());

    SegmentDescRef local_segment = metadata_->segmentManager().getLocal();
    if (!local_segment) {
        return Status::InternalError("Local segment not found" LOC_MARK);
    }

    const size_t original_task_count = hylink_batch->task_list.size();
    auto rollback_tasks = [&]() {
        for (size_t i = original_task_count; i < hylink_batch->task_list.size();
             ++i) {
            auto& task = hylink_batch->task_list[i];
            if (task.completion_event)
                (void)hipEventDestroy(task.completion_event);
        }
        hylink_batch->task_list.resize(original_task_count);
    };

    for (const auto& request : request_list) {
        hylink_batch->task_list.push_back(HylinkTask{});
        auto status = startAsyncTransfer(request, local_segment,
                                         hylink_batch->task_list.back());
        if (!status.ok()) {
            LOG(ERROR) << "HylinkTransport: submit task failed: "
                       << status.ToString();
            rollback_tasks();
            return status;
        }
    }
    return Status::OK();
}

Status HylinkTransport::startAsyncTransfer(const Request& request,
                                           const SegmentDescRef& local_segment,
                                           HylinkTask& task) {
    BufferDesc* buf = local_segment->findBuffer(
        reinterpret_cast<uint64_t>(request.source), request.length);
    if (!buf) {
        return Status::InvalidArgument(
            "Unregistered buffer: source pointer not in any registered "
            "buffer" LOC_MARK);
    }
    // Bind the task to its source buffer's device, mirroring the classic
    // hip transport's setDeviceContext: the remote handle import, the copy
    // and the completion event all share one device context. Host-source
    // tasks keep the caller's current device.
    int saved_device = 0;
    const bool have_saved_device = hipGetDevice(&saved_device) == hipSuccess;
    int device_id = LocationParser(buf->location).index();
    if (device_id < 0) {
        device_id = have_saved_device ? saved_device : 0;
    }
    const bool switch_device = have_saved_device && saved_device != device_id;
    if (switch_device && hipSetDevice(device_id) != hipSuccess) {
        return Status::InternalError(
            "hylink failed to set the source buffer's device" LOC_MARK);
    }

    Status status = Status::OK();
    uint64_t target_addr = request.target_offset;
    if (request.target_id != LOCAL_SEGMENT_ID) {
        status = relocateSharedMemoryAddress(target_addr, request.length,
                                             request.target_id, device_id);
    }
    if (status.ok()) {
        task.device_id = device_id;
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        status = submitTaskCopy(task);
    }
    if (switch_device) (void)hipSetDevice(saved_device);
    return status;
}

hipStream_t HylinkTransport::acquireStream(int device_id) {
    std::lock_guard<std::mutex> lock(stream_pool_mutex_);
    auto& streams = stream_pools_[device_id];
    if (streams.empty()) {
        int saved = 0;
        const bool have_device = hipGetDevice(&saved) == hipSuccess;
        if (have_device) (void)hipSetDevice(device_id);
        for (int i = 0; i < kStreamsPerDevice; ++i) {
            hipStream_t stream = nullptr;
            if (hipStreamCreate(&stream) == hipSuccess) {
                streams.push_back(stream);
            } else {
                (void)hipGetLastError();
            }
        }
        if (have_device) (void)hipSetDevice(saved);
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

Status HylinkTransport::submitTaskCopy(HylinkTask& task) {
    hipStream_t stream = acquireStream(task.device_id);
    if (!stream) {
        return Status::InternalError(
            "hylink failed to create a copy stream" LOC_MARK);
    }
    void* src = nullptr;
    void* dst = nullptr;
    if (task.request.opcode == Request::READ) {
        dst = task.request.source;
        src = reinterpret_cast<void*>(task.target_addr);
    } else {
        src = task.request.source;
        dst = reinterpret_cast<void*>(task.target_addr);
    }
    hipError_t err =
        hipMemcpyAsync(dst, src, task.request.length, hipMemcpyDefault, stream);
    if (err != hipSuccess) {
        std::string message = hipGetErrorString(err);
        (void)hipGetLastError();
        return Status::InternalError(std::string("hylink async copy failed: ") +
                                     message + LOC_MARK);
    }
    hipEvent_t event = nullptr;
    err = hipEventCreateWithFlags(&event, hipEventDisableTiming);
    if (err == hipSuccess) err = hipEventRecord(event, stream);
    if (err != hipSuccess) {
        std::string message = hipGetErrorString(err);
        (void)hipGetLastError();
        if (event) (void)hipEventDestroy(event);
        return Status::InternalError(
            std::string("hylink event record failed: ") + message + LOC_MARK);
    }
    task.completion_event = event;
    return Status::OK();
}

Status HylinkTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto* hylink_batch = dynamic_cast<HylinkSubBatch*>(batch);
    if (!hylink_batch || task_id < 0 ||
        task_id >= static_cast<int>(hylink_batch->task_list.size())) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto& task = hylink_batch->task_list[task_id];
    if (task.status_word == TransferStatusEnum::PENDING) {
        if (!task.completion_event) {
            task.status_word = TransferStatusEnum::FAILED;
        } else {
            auto err = hipEventQuery(task.completion_event);
            if (err == hipSuccess) {
                task.transferred_bytes = task.request.length;
                task.status_word = TransferStatusEnum::COMPLETED;
            } else if (err != hipErrorNotReady) {
                LOG(ERROR) << "HylinkTransport: hipEventQuery failed: "
                           << hipGetErrorString(err);
                task.status_word = TransferStatusEnum::FAILED;
            }
        }
    }
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status HylinkTransport::exportDeviceBuffer(BufferDesc& desc,
                                           const LocationParser& location) {
    int saved_dev = 0;
    CHECK_HIP(hipGetDevice(&saved_dev));
    if (location.index() >= 0 && saved_dev != location.index()) {
        CHECK_HIP(hipSetDevice(location.index()));
    }
    auto restore = [&]() {
        if (location.index() >= 0 && saved_dev != location.index()) {
            (void)hipSetDevice(saved_dev);
        }
    };

    hipDeviceptr_t base = nullptr;
    size_t alloc_size = 0;
    hipError_t range_err = hipMemGetAddressRange(
        &base, &alloc_size, reinterpret_cast<hipDeviceptr_t>(desc.addr));
    if (range_err != hipSuccess) {
        (void)hipGetLastError();
        base = reinterpret_cast<hipDeviceptr_t>(desc.addr);
        alloc_size = desc.length;
    }

    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        auto iter =
            registered_base_addrs_.find(reinterpret_cast<uint64_t>(base));
        if (iter != registered_base_addrs_.end()) {
            desc.addr = reinterpret_cast<uint64_t>(base);
            desc.length = alloc_size;
            desc.shm_path = iter->second;
            if (std::find(desc.transports.begin(), desc.transports.end(),
                          TransportType::HYLINK) == desc.transports.end()) {
                desc.transports.push_back(TransportType::HYLINK);
            }
            restore();
            return Status::OK();
        }
    }

    ExportedHandle exported;
    if (hylinkUseVmm()) {
        hipMemGenericAllocationHandle_t alloc_handle = nullptr;
        hipError_t retain_err = hipMemRetainAllocationHandle(
            &alloc_handle, reinterpret_cast<void*>(base));
        if (retain_err == hipSuccess && alloc_handle) {
            DtkFabricHandle fabric{};
            hipError_t export_err = hipMemExportToShareableHandle(
                fabric.data(), alloc_handle, kDtkFabricHandleType, 0);
            if (export_err == hipSuccess) {
                exported.fabric = fabric;
                exported.has_fabric = true;
            } else {
                (void)hipGetLastError();
                LOG(INFO) << "HylinkTransport: DTK fabric export failed: "
                          << hipGetErrorString(export_err);
            }
            (void)hipMemRelease(alloc_handle);
        } else {
            (void)hipGetLastError();
            LOG(INFO) << "HylinkTransport: hipMemRetainAllocationHandle failed "
                         "for 0x"
                      << std::hex << reinterpret_cast<uint64_t>(base)
                      << std::dec << ": " << hipGetErrorString(retain_err);
        }
    } else {
        hipError_t ipc_err =
            hipIpcGetMemHandle(&exported.ipc, reinterpret_cast<void*>(base));
        if (ipc_err == hipSuccess) {
            exported.has_ipc = true;
        } else {
            (void)hipGetLastError();
            LOG(INFO) << "HylinkTransport: hipIpcGetMemHandle failed for 0x"
                      << std::hex << reinterpret_cast<uint64_t>(base)
                      << std::dec << ": " << hipGetErrorString(ipc_err);
        }
    }

    if (!exported.has_ipc && !exported.has_fabric) {
        restore();
        return Status::InternalError(
            std::string(hylinkUseVmm()
                            ? "hylink export failed: DTK fabric handle"
                            : "hylink export failed: HIP IPC handle") +
            LOC_MARK);
    }

    desc.addr = reinterpret_cast<uint64_t>(base);
    desc.length = alloc_size;
    desc.shm_path = encodeExport(exported);
    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        auto [it, inserted] =
            registered_base_addrs_.emplace(desc.addr, desc.shm_path);
        if (!inserted) desc.shm_path = it->second;
    }
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::HYLINK) == desc.transports.end()) {
        desc.transports.push_back(TransportType::HYLINK);
    }
    restore();
    return Status::OK();
}

Status HylinkTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    (void)options;
    LocationParser location(desc.location);
    if (!isDtkDeviceLocation(location.type())) {
        // Host and foreign-device buffers stay with RDMA/TCP/SHM.
        return Status::OK();
    }
    return exportDeviceBuffer(desc, location);
}

Status HylinkTransport::removeMemoryBuffer(BufferDesc& desc) {
    LocationParser location(desc.location);
    if (isDtkDeviceLocation(location.type())) {
        int saved_dev = 0;
        (void)hipGetDevice(&saved_dev);
        if (location.index() >= 0 && saved_dev != location.index()) {
            (void)hipSetDevice(location.index());
        }
        hipDeviceptr_t base = nullptr;
        size_t alloc_size = 0;
        uint64_t key = desc.addr;
        if (hipMemGetAddressRange(
                &base, &alloc_size,
                reinterpret_cast<hipDeviceptr_t>(desc.addr)) == hipSuccess) {
            key = reinterpret_cast<uint64_t>(base);
        } else {
            (void)hipGetLastError();
        }
        if (location.index() >= 0 && saved_dev != location.index()) {
            (void)hipSetDevice(saved_dev);
        }
        std::lock_guard<std::mutex> lock(register_mutex_);
        registered_base_addrs_.erase(key);
        desc.shm_path.clear();
        desc.transports.erase(
            std::remove(desc.transports.begin(), desc.transports.end(),
                        TransportType::HYLINK),
            desc.transports.end());
    }
    return Status::OK();
}

Status HylinkTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                    uint64_t length,
                                                    uint64_t target_id,
                                                    int device_id,
                                                    HylinkHandleKind* kind) {
    thread_local RelocateMap tl_relocate_map;
    thread_local uint64_t tl_epoch = 0;
    const uint64_t epoch = relocate_epoch_.load(std::memory_order_acquire);
    if (tl_epoch != epoch) {
        tl_relocate_map.clear();
        tl_epoch = epoch;
    }
    auto cached = tl_relocate_map.find(target_id);
    if (cached != tl_relocate_map.end()) {
        for (auto& entry : cached->second) {
            if (entry.second.empty()) continue;
            const auto& any = entry.second.begin()->second;
            if (entry.first > dest_addr ||
                dest_addr + length > entry.first + any.length)
                continue;
            // An imported fabric VA can be granted only once, to one device.
            // A miss here opens a new import+map for this GPU.
            const int map_key =
                any.kind == HylinkHandleKind::kFabric ? device_id : -1;
            auto found = entry.second.find(map_key);
            if (found == entry.second.end()) break;
            const auto& mapping = found->second;
            dest_addr = dest_addr - entry.first +
                        reinterpret_cast<uint64_t>(mapping.shm_addr);
            if (kind) *kind = mapping.kind;
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    BufferDesc* buffer = nullptr;
    bool same_machine = false;
    SegmentDescRef pin;
    CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
        target_id, pin, [&](SegmentDesc* segment) {
            buffer = segment->findBuffer(dest_addr, length);
            if (!buffer || buffer->shm_path.empty()) {
                return Status::NeedsRefreshCache(
                    "hylink target buffer has no export handle" LOC_MARK);
            }
            same_machine = !segment->machine_id.empty() &&
                           segment->machine_id == machine_id_;
            return Status::OK();
        }));

    auto segment_entries = relocate_map_.find(target_id);
    // Fabric: one import+map+SetAccess per local GPU. IPC is shared (key -1).
    const int map_key = hylinkUseVmm() ? device_id : -1;
    const bool already_open =
        segment_entries != relocate_map_.end() &&
        segment_entries->second.count(buffer->addr) != 0 &&
        segment_entries->second.at(buffer->addr).count(map_key) != 0;
    if (!already_open) {
        ExportedHandle exported;
        if (!decodeExport(buffer->shm_path, exported)) {
            return Status::InvalidArgument(
                "hylink target buffer has a malformed handle" LOC_MARK);
        }

        // MC_HYLINK_USE_VMM selects the handle. Default is the DTK fabric
        // mapping. =0 opens a HIP IPC handle, which cannot cross machines.
        void* mapped = nullptr;
        OpenedMapping opened;
        opened.length = buffer->length;
        if (!hylinkUseVmm()) {
            if (!exported.has_ipc) {
                return Status::InvalidArgument(
                    "hylink IPC path requires a HIP IPC handle "
                    "(MC_HYLINK_USE_VMM=0)" LOC_MARK);
            }
            if (!same_machine) {
                return Status::InvalidArgument(
                    "hylink IPC handle cannot cross machines" LOC_MARK);
            }
            CHECK_HIP(hipIpcOpenMemHandle(&mapped, exported.ipc,
                                          hipIpcMemLazyEnablePeerAccess));
            opened.kind = HylinkHandleKind::kIpc;
            LOG(INFO) << "HylinkTransport: opened intra-node IPC mapping for "
                      << (void*)buffer->addr;
        } else if (exported.has_fabric) {
            const size_t map_length = buffer->length;
            hipMemGenericAllocationHandle_t handle = nullptr;
            CHECK_HIP(hipMemImportFromShareableHandle(
                &handle, exported.fabric.data(), kDtkFabricHandleType));
            hipError_t err = hipMemAddressReserve((hipDeviceptr_t*)&mapped,
                                                  map_length, 0, nullptr, 0);
            if (err != hipSuccess) {
                (void)hipMemRelease(handle);
                return Status::InternalError(
                    std::string("hipMemAddressReserve failed: ") +
                    hipGetErrorString(err) + LOC_MARK);
            }
            err = hipMemMap((hipDeviceptr_t)mapped, map_length, 0, handle, 0);
            if (err != hipSuccess) {
                (void)hipMemAddressFree((hipDeviceptr_t)mapped, map_length);
                (void)hipMemRelease(handle);
                return Status::InternalError(std::string("hipMemMap failed: ") +
                                             hipGetErrorString(err) + LOC_MARK);
            }
            opened.kind = HylinkHandleKind::kFabric;
            opened.fabric_handle = handle;
            LOG(INFO) << "HylinkTransport: opened "
                      << (same_machine ? "same-node" : "cross-node")
                      << " DTK fabric mapping for " << (void*)buffer->addr;
        } else {
            return Status::InvalidArgument(
                "hylink target buffer has no usable handle" LOC_MARK);
        }
        opened.shm_addr = mapped;
        if (opened.kind == HylinkHandleKind::kFabric) {
            hipMemAccessDesc access{};
            access.location.type = hipMemLocationTypeDevice;
            access.location.id = device_id;
            access.flags = hipMemAccessFlagsProtReadWrite;
            hipError_t access_err = hipMemSetAccess(
                (hipDeviceptr_t)opened.shm_addr, opened.length, &access, 1);
            if (access_err != hipSuccess) {
                (void)hipGetLastError();
                closeMapping(opened);
                return Status::InternalError(
                    std::string("hipMemSetAccess failed: ") +
                    hipGetErrorString(access_err) + LOC_MARK);
            }
        }
        relocate_map_[target_id][buffer->addr][map_key] = opened;
    }
    tl_relocate_map = relocate_map_;
    tl_epoch = relocate_epoch_.load(std::memory_order_acquire);

    auto& mapping = relocate_map_.at(target_id).at(buffer->addr).at(map_key);
    auto shm_addr = mapping.shm_addr;
    if (kind) *kind = mapping.kind;
    dest_addr = dest_addr - buffer->addr + reinterpret_cast<uint64_t>(shm_addr);
    return Status::OK();
}

Status HylinkTransport::setPeerAccess() {
    int device_count = 0;
    int saved = 0;
    CHECK_HIP(hipGetDevice(&saved));
    CHECK_HIP(hipGetDeviceCount(&device_count));
    if (device_count < 2) return Status::OK();
    for (int i = 0; i < device_count; ++i) {
        CHECK_HIP(hipSetDevice(i));
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            CHECK_HIP(hipDeviceCanAccessPeer(&can_access, i, j));
            if (!can_access) continue;
            hipError_t err = hipDeviceEnablePeerAccess(j, 0);
            if (err == hipErrorPeerAccessAlreadyEnabled) {
                (void)hipGetLastError();
            } else if (err != hipSuccess) {
                (void)hipSetDevice(saved);
                return Status::InternalError(
                    std::string("hipDeviceEnablePeerAccess failed: ") +
                    hipGetErrorString(err) + LOC_MARK);
            }
        }
    }
    CHECK_HIP(hipSetDevice(saved));
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
