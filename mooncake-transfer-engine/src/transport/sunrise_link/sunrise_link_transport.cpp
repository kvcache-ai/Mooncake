// Copyright 2024 KVCache.AI

#include "transport/sunrise_link_transport/sunrise_link_transport.h"

#include <dlfcn.h>
#include <glog/logging.h>
#include <ptml.h>
#include <tang_runtime_api.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>

#include "common.h"
#include "common/serialization.h"

namespace {

static std::string TangRtSharedObjectPath(const char* soname) {
    const char* lib_dir = std::getenv("MC_TANGRT_LIB_DIR");
    if (lib_dir && lib_dir[0]) {
        std::string d(lib_dir);
        while (!d.empty() && d.back() == '/') d.pop_back();
        return d + "/" + soname;
    }
    const char* root = std::getenv("MC_TANGRT_ROOT");
    std::string r = (root && root[0]) ? std::string(root)
                                      : std::string("/usr/local/tangrt");
    while (!r.empty() && r.back() == '/') r.pop_back();
    const char* arch = std::getenv("MC_TANGRT_LIB_ARCH");
    std::string arch_dir =
        (arch && arch[0]) ? std::string(arch) : std::string("linux-x86_64");
    return r + "/lib/" + arch_dir + "/" + soname;
}

constexpr int kMaxTangDevLocks = 64;

static int devSlot(int dev) {
    if (dev < 0) return -1;
    return dev % kMaxTangDevLocks;
}

static std::mutex& devMutex(int slot) {
    static std::mutex table[kMaxTangDevLocks];
    static std::mutex fallback;
    if (slot < 0 || slot >= kMaxTangDevLocks) return fallback;
    return table[slot];
}

struct ScopedTangDeviceOps {
    std::unique_lock<std::mutex> lo_lock;
    std::unique_lock<std::mutex> hi_lock;

    explicit ScopedTangDeviceOps(int src_dev, int dst_dev) {
        if (src_dev < 0 && dst_dev < 0) return;
        if (src_dev < 0 || dst_dev < 0) {
            int slot = devSlot(src_dev >= 0 ? src_dev : dst_dev);
            lo_lock = std::unique_lock<std::mutex>(devMutex(slot));
            return;
        }
        int slot_lo = devSlot(src_dev);
        int slot_hi = devSlot(dst_dev);
        if (slot_lo == slot_hi) {
            lo_lock = std::unique_lock<std::mutex>(devMutex(slot_lo));
            return;
        }
        if (slot_lo > slot_hi) std::swap(slot_lo, slot_hi);
        lo_lock =
            std::unique_lock<std::mutex>(devMutex(slot_lo), std::defer_lock);
        hi_lock =
            std::unique_lock<std::mutex>(devMutex(slot_hi), std::defer_lock);
        std::lock(lo_lock, hi_lock);
    }
};

static tangMemcpyKind MemcpyKindForPointers(tangMemoryType st,
                                            tangMemoryType dt) {
    if (st == tangMemoryTypeDevice && dt == tangMemoryTypeHost) {
        return tangMemcpyDeviceToHost;
    }
    if (st == tangMemoryTypeHost && dt == tangMemoryTypeDevice) {
        return tangMemcpyHostToDevice;
    }
    if (st == tangMemoryTypeDevice && dt == tangMemoryTypeDevice) {
        return tangMemcpyDeviceToDevice;
    }
    return tangMemcpyHostToHost;
}

struct SavedTangDevice {
    int dev{-1};
    SavedTangDevice() { tangGetDevice(&dev); }
    ~SavedTangDevice() {
        if (dev >= 0) tangSetDevice(dev);
    }
    SavedTangDevice(const SavedTangDevice&) = delete;
    SavedTangDevice& operator=(const SavedTangDevice&) = delete;
};

static void NormalizeMappedHostPointer(void** ptr,
                                       tangPointerAttributes* attr) {
    if (!ptr || !*ptr || !attr) return;
    if (attr->type != tangMemoryTypeHost) return;

    void* dev_ptr = nullptr;
    tangError_t ret = tangHostGetDevicePointer(&dev_ptr, *ptr, 0);
    if (ret != tangSuccess || !dev_ptr) return;

    tangPointerAttributes dev_attr = {};
    tangPointerGetAttributes(&dev_attr, dev_ptr);
    if (dev_attr.type == tangMemoryTypeDevice) {
        *ptr = dev_ptr;
        *attr = dev_attr;
    }
}

static tangError_t QueryPointerAttrsBestEffort(void* ptr,
                                               tangPointerAttributes* attr,
                                               int preferred_dev) {
    if (!ptr || !attr) return tangErrorInvalidValue;
    SavedTangDevice saved;
    tangError_t ret = tangPointerGetAttributes(attr, ptr);
    if (ret == tangSuccess) return ret;

    if (preferred_dev >= 0) {
        tangSetDevice(preferred_dev);
        ret = tangPointerGetAttributes(attr, ptr);
        if (ret == tangSuccess) return ret;
    }

    int dev_count = 0;
    if (tangGetDeviceCount(&dev_count) != tangSuccess || dev_count <= 0) {
        return ret;
    }
    for (int d = 0; d < dev_count; ++d) {
        tangSetDevice(d);
        ret = tangPointerGetAttributes(attr, ptr);
        if (ret == tangSuccess) return ret;
    }
    return ret;
}

static tangError_t doTangCopy(void* dst, int dst_dev, void* src, int src_dev,
                              size_t length,
                              tangMemoryType src_type = tangMemoryTypeDevice,
                              tangMemoryType dst_type = tangMemoryTypeDevice) {
    if (src_dev >= 0 && dst_dev >= 0 && src_dev != dst_dev) {
        tangError_t ret = tangMemcpyPeer(dst, dst_dev, src, src_dev, length);
        if (ret != tangSuccess) {
            ret = tangMemcpyPeer_v2(dst, dst_dev, src, src_dev, length);
        }
        return ret;
    }
    if (src_dev >= 0 && dst_dev >= 0 && src_dev == dst_dev) {
        ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        tangSetDevice(src_dev);
        tangError_t ret =
            tangMemcpy(dst, src, length, tangMemcpyDeviceToDevice);
        if (saved_dev >= 0) tangSetDevice(saved_dev);
        return ret;
    }
    ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
    int saved_dev = -1;
    tangGetDevice(&saved_dev);
    if (src_dev >= 0) {
        tangSetDevice(src_dev);
    } else if (dst_dev >= 0) {
        tangSetDevice(dst_dev);
    }
    tangError_t ret =
        tangMemcpy(dst, src, length, MemcpyKindForPointers(src_type, dst_type));
    if (saved_dev >= 0) tangSetDevice(saved_dev);
    return ret;
}

static int ParseSunriseDeviceId(const std::string& location) {
    auto pos = location.rfind(':');
    if (pos == std::string::npos) return -1;
    const char* value = location.c_str() + pos + 1;
    if (*value == '\0') return -1;

    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') return -1;
    if (parsed < 0 || parsed > std::numeric_limits<int>::max()) return -1;
    return static_cast<int>(parsed);
}

static bool RequestRangeFitsInBuffer(uint64_t dest_addr, uint64_t length,
                                     uint64_t buffer_addr,
                                     uint64_t buffer_length) {
    if (dest_addr < buffer_addr) return false;

    uint64_t offset = dest_addr - buffer_addr;
    if (offset > buffer_length) return false;
    return length <= buffer_length - offset;
}

}  // namespace

namespace mooncake {

int SunriseLinkTransport::parseDeviceIdForTest(const std::string& location) {
    return ParseSunriseDeviceId(location);
}

bool SunriseLinkTransport::requestRangeFitsInBufferForTest(
    uint64_t dest_addr, uint64_t length, uint64_t buffer_addr,
    uint64_t buffer_length) {
    return RequestRangeFitsInBuffer(dest_addr, length, buffer_addr,
                                    buffer_length);
}

SunriseLinkTransport::SunriseLinkTransport() = default;

SunriseLinkTransport::~SunriseLinkTransport() {
    for (auto& entry : remap_entries_) {
        if (entry.second.shm_addr) tangIpcCloseMemHandle(entry.second.shm_addr);
    }
    remap_entries_.clear();
    {
        std::lock_guard<std::mutex> guard(runtime_mutex_);
        unloadRuntime();
    }
}

int SunriseLinkTransport::install(std::string& local_server_name,
                                  std::shared_ptr<TransferMetadata> metadata,
                                  std::shared_ptr<Topology> topology) {
    (void)topology;
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    if (!loadRuntime()) {
        LOG(ERROR) << "SunriseLinkTransport: failed to load Tang/PTML runtime";
        return -1;
    }

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "sunrise_link";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

bool SunriseLinkTransport::loadRuntime() {
    std::lock_guard<std::mutex> guard(runtime_mutex_);
    if (runtime_loaded_) return true;

    runtime_.ptml_handle =
        dlopen(TangRtSharedObjectPath("libptml_shared.so").c_str(),
               RTLD_NOW | RTLD_GLOBAL);
    if (!runtime_.ptml_handle) {
        LOG(ERROR) << "SunriseLinkTransport: failed to load PTML: "
                   << dlerror();
        unloadRuntime();
        return false;
    }

    runtime_.tang_handle =
        dlopen(TangRtSharedObjectPath("libtangrt_shared.so").c_str(),
               RTLD_NOW | RTLD_GLOBAL);
    if (!runtime_.tang_handle) {
        LOG(ERROR) << "SunriseLinkTransport: failed to load Tang runtime: "
                   << dlerror();
        unloadRuntime();
        return false;
    }

    ptmlReturn_t ret = ptmlInit();
    if (ret != PTML_SUCCESS) {
        LOG(ERROR) << "SunriseLinkTransport: ptmlInit failed: " << ret;
        unloadRuntime();
        return false;
    }
    ret = ptmlPtlinkEnableAll();
    if (ret != PTML_SUCCESS) {
        LOG(WARNING) << "SunriseLinkTransport: ptmlPtlinkEnableAll failed: "
                     << ret;
    }

    runtime_loaded_ = true;
    return true;
}

void SunriseLinkTransport::unloadRuntime() {
    if (runtime_.tang_handle) {
        dlclose(runtime_.tang_handle);
        runtime_.tang_handle = nullptr;
    }
    if (runtime_.ptml_handle) {
        dlclose(runtime_.ptml_handle);
        runtime_.ptml_handle = nullptr;
    }
    runtime_loaded_ = false;
}

int SunriseLinkTransport::inferRegisteredDevice(void* ptr) const {
    uint64_t p = reinterpret_cast<uint64_t>(ptr);
    std::lock_guard<std::mutex> guard(register_mutex_);
    for (const auto& entry : registered_regions_) {
        uint64_t base = reinterpret_cast<uint64_t>(entry.first);
        if (base <= p && p < base + entry.second.length)
            return entry.second.gpu_id;
    }
    return -1;
}

int SunriseLinkTransport::inferPointerDeviceBestEffort(
    void* ptr, int preferred_dev) const {
    int dev = inferRegisteredDevice(ptr);
    if (dev >= 0) return dev;

    tangPointerAttributes attr = {};
    tangError_t ret = QueryPointerAttrsBestEffort(ptr, &attr, preferred_dev);
    if (ret != tangSuccess) return -1;

    NormalizeMappedHostPointer(&ptr, &attr);
    return attr.type == tangMemoryTypeDevice ? attr.device : -1;
}

int SunriseLinkTransport::registerLocalMemory(void* addr, size_t length,
                                              const std::string& location,
                                              bool remote_accessible,
                                              bool update_metadata) {
    (void)remote_accessible;
    int preferred_dev = ParseSunriseDeviceId(location);
    tangPointerAttributes attr = {};
    tangError_t err = QueryPointerAttrsBestEffort(addr, &attr, preferred_dev);
    if (err != tangSuccess || attr.type != tangMemoryTypeDevice) {
        LOG(ERROR) << "SunriseLinkTransport: unsupported memory for register"
                   << " addr=" << addr << " location=" << location
                   << " preferred_dev=" << preferred_dev << " err=" << err
                   << " type=" << attr.type << " device=" << attr.device;
        return -1;
    }

    tangIpcMemHandle_t handle;
    int saved_dev = -1;
    tangGetDevice(&saved_dev);
    int handle_dev = preferred_dev >= 0 ? preferred_dev : attr.device;
    if (handle_dev >= 0) {
        tangError_t sd = tangSetDevice(handle_dev);
        if (sd != tangSuccess) {
            LOG(ERROR) << "SunriseLinkTransport: tangSetDevice before "
                       << "tangIpcGetMemHandle failed: " << sd << " "
                       << tangGetErrorString(sd) << " device=" << handle_dev;
            if (saved_dev >= 0) tangSetDevice(saved_dev);
            return -1;
        }
    }
    err = tangIpcGetMemHandle(&handle, addr);
    if (saved_dev >= 0) tangSetDevice(saved_dev);
    if (err != tangSuccess) {
        LOG(ERROR) << "SunriseLinkTransport: tangIpcGetMemHandle failed: "
                   << err << " " << tangGetErrorString(err);
        return -1;
    }

    {
        std::lock_guard<std::mutex> guard(register_mutex_);
        if (registered_base_addrs_.count(reinterpret_cast<uint64_t>(addr))) {
            return 0;
        }
        registered_base_addrs_.insert(reinterpret_cast<uint64_t>(addr));
        registered_regions_[addr] = RegisteredRegion{length, attr.device};
    }

    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(addr);
    desc.length = length;
    desc.name = location;
    desc.shm_name = serializeBinaryData(&handle, sizeof(handle));
    return metadata_->addLocalMemoryBuffer(desc, update_metadata);
}

int SunriseLinkTransport::unregisterLocalMemory(void* addr,
                                                bool update_metadata) {
    {
        std::lock_guard<std::mutex> guard(register_mutex_);
        registered_base_addrs_.erase(reinterpret_cast<uint64_t>(addr));
        registered_regions_.erase(addr);
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int SunriseLinkTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    for (const auto& buffer : buffer_list) {
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    }
    return metadata_->updateLocalSegmentDesc();
}

int SunriseLinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto* addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

int SunriseLinkTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                      uint64_t length,
                                                      uint64_t target_id,
                                                      int* target_gpu_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    for (auto& entry : desc->buffers) {
        if (!entry.shm_name.empty() &&
            RequestRangeFitsInBuffer(dest_addr, length, entry.addr,
                                     entry.length)) {
            remap_lock_.lockShared();
            auto key = std::make_pair(target_id, entry.addr);
            if (remap_entries_.count(key)) {
                auto opened = remap_entries_[key];
                remap_lock_.unlockShared();
                if (target_gpu_id) *target_gpu_id = opened.gpu_id;
                dest_addr = dest_addr - entry.addr +
                            reinterpret_cast<uint64_t>(opened.shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard guard(remap_lock_);
            if (!remap_entries_.count(key)) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                if (output_buffer.size() != sizeof(tangIpcMemHandle_t)) {
                    LOG(ERROR)
                        << "SunriseLinkTransport: invalid IPC handle size";
                    return -1;
                }
                tangIpcMemHandle_t handle;
                memcpy(&handle, output_buffer.data(), sizeof(handle));
                void* shm_addr = nullptr;
                int remote_gpu_id = ParseSunriseDeviceId(entry.name);
                int saved_dev = -1;
                tangGetDevice(&saved_dev);
                if (remote_gpu_id >= 0) {
                    tangError_t sd = tangSetDevice(remote_gpu_id);
                    if (sd != tangSuccess) {
                        LOG(ERROR)
                            << "SunriseLinkTransport: tangSetDevice "
                            << "before tangIpcOpenMemHandle failed: " << sd
                            << " " << tangGetErrorString(sd)
                            << " device=" << remote_gpu_id;
                        if (saved_dev >= 0) tangSetDevice(saved_dev);
                        return -1;
                    }
                }
                tangError_t err = tangIpcOpenMemHandle(
                    &shm_addr, handle, tangIpcMemLazyEnablePeerAccess);
                if (saved_dev >= 0) tangSetDevice(saved_dev);
                if (err != tangSuccess) {
                    LOG(ERROR)
                        << "SunriseLinkTransport: tangIpcOpenMemHandle "
                        << "failed: " << err << " " << tangGetErrorString(err);
                    return -1;
                }
                OpenedShmEntry shm_entry;
                shm_entry.shm_addr = shm_addr;
                shm_entry.length = entry.length;
                shm_entry.gpu_id = remote_gpu_id;
                remap_entries_[key] = shm_entry;
            }

            auto opened = remap_entries_[key];
            if (target_gpu_id) *target_gpu_id = opened.gpu_id;
            dest_addr = dest_addr - entry.addr +
                        reinterpret_cast<uint64_t>(opened.shm_addr);
            return 0;
        }
    }
    LOG(ERROR)
        << "SunriseLinkTransport: requested address not found in segment";
    return ERR_INVALID_ARGUMENT;
}

int SunriseLinkTransport::startCopy(void* src, void* dst, size_t length,
                                    int remote_dev, int local_dev) {
    tangPointerAttributes src_attr = {};
    tangPointerAttributes dst_attr = {};
    tangError_t src_ret =
        QueryPointerAttrsBestEffort(src, &src_attr, local_dev);
    tangError_t dst_ret =
        QueryPointerAttrsBestEffort(dst, &dst_attr, remote_dev);
    NormalizeMappedHostPointer(&src, &src_attr);
    NormalizeMappedHostPointer(&dst, &dst_attr);

    int src_dev =
        (src_ret == tangSuccess && src_attr.type == tangMemoryTypeDevice)
            ? src_attr.device
            : -1;
    int dst_dev =
        (dst_ret == tangSuccess && dst_attr.type == tangMemoryTypeDevice)
            ? dst_attr.device
            : -1;

    tangMemoryType src_type =
        src_ret == tangSuccess ? src_attr.type : tangMemoryTypeHost;
    tangMemoryType dst_type =
        dst_ret == tangSuccess ? dst_attr.type : tangMemoryTypeHost;

    tangError_t ret =
        doTangCopy(dst, dst_dev, src, src_dev, length, src_type, dst_type);
    if (ret != tangSuccess) {
        LOG(ERROR) << "SunriseLinkTransport: tang copy failed: " << ret << " "
                   << tangGetErrorString(ret) << " src_dev=" << src_dev
                   << " dst_dev=" << dst_dev;
        return -1;
    }
    return 0;
}

int SunriseLinkTransport::submitRequest(TransferTask& task,
                                        const TransferRequest& request,
                                        bool keep_slice_ref) {
    uint64_t dest_addr = request.target_offset;
    int target_gpu_id = -1;
    bool is_ipc = (request.target_id != LOCAL_SEGMENT_ID);
    if (is_ipc) {
        int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                             request.target_id, &target_gpu_id);
        if (rc) return rc;
    }

    int local_gpu_id = inferPointerDeviceBestEffort(request.source, -1);

    task.total_bytes = request.length;
    Slice* slice = getSliceCache().allocate();
    slice->source_addr = static_cast<char*>(request.source);
    slice->local.dest_addr = reinterpret_cast<void*>(dest_addr);
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->task = &task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;
    if (keep_slice_ref) task.slice_list.push_back(slice);
    __sync_fetch_and_add(&task.slice_count, 1);

    void* src = nullptr;
    void* dst = nullptr;
    if (request.opcode == TransferRequest::READ) {
        src = reinterpret_cast<void*>(dest_addr);
        dst = request.source;
    } else {
        src = request.source;
        dst = reinterpret_cast<void*>(dest_addr);
    }

    if (is_ipc && target_gpu_id >= 0) {
        int remote_dev = target_gpu_id;
        void* local_ptr = request.opcode == TransferRequest::WRITE ? src : dst;
        tangPointerAttributes local_attr = {};
        tangError_t local_ret =
            QueryPointerAttrsBestEffort(local_ptr, &local_attr, local_gpu_id);
        if (local_ret == tangSuccess) {
            NormalizeMappedHostPointer(&local_ptr, &local_attr);
        }
        if (local_ret == tangSuccess &&
            local_attr.type == tangMemoryTypeDevice) {
            int local_dev = local_attr.device;
            if (request.opcode == TransferRequest::WRITE) {
                src = local_ptr;
            } else {
                dst = local_ptr;
            }
            int src_dev = (request.opcode == TransferRequest::WRITE)
                              ? local_dev
                              : remote_dev;
            int dst_dev = (request.opcode == TransferRequest::WRITE)
                              ? remote_dev
                              : local_dev;

            tangError_t ret =
                doTangCopy(dst, dst_dev, src, src_dev, request.length);
            if (ret != tangSuccess) {
                if (src_dev == dst_dev) {
                    LOG(ERROR)
                        << "SunriseLink IPC same-device copy failed "
                        << "(IPC memory is read-only on same device, "
                        << "use cross-GPU for WRITE): " << ret << " "
                        << tangGetErrorString(ret) << " src_dev=" << src_dev
                        << " dst_dev=" << dst_dev;
                } else {
                    LOG(ERROR)
                        << "SunriseLinkTransport: tang copy failed: " << ret
                        << " " << tangGetErrorString(ret)
                        << " src_dev=" << src_dev << " dst_dev=" << dst_dev;
                }
                slice->markFailed();
                return -1;
            }

            slice->markSuccess();
            return 0;
        }
    }

    if (startCopy(src, dst, request.length, target_gpu_id, local_gpu_id) != 0) {
        slice->markFailed();
        return -1;
    }

    slice->markSuccess();
    return 0;
}

Status SunriseLinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::InvalidArgument(
            "SunriseLinkTransport: exceed batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    for (const auto& request : entries) {
        auto& task = batch_desc.task_list[task_id++];
        if (submitRequest(task, request, false) != 0) {
            return Status::Memory("sunrise_link transfer failed");
        }
    }
    return Status::OK();
}

Status SunriseLinkTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (auto* task_ptr : task_list) {
        if (!task_ptr || !task_ptr->request) {
            return Status::InvalidArgument(
                "SunriseLinkTransport: invalid transfer task");
        }
        if (submitRequest(*task_ptr, *task_ptr->request, true) != 0) {
            return Status::Memory("sunrise_link transfer failed");
        }
    }
    return Status::OK();
}

Status SunriseLinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                               TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument(
            "SunriseLinkTransport::getTransferStatus invalid task id");
    }

    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        status.s = failed_slice_count ? TransferStatusEnum::FAILED
                                      : TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

}  // namespace mooncake
