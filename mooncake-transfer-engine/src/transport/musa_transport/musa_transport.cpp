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

#include "transport/musa_transport/musa_transport.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "memory_location.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

namespace {

/// Per-device CUDA stream pool (thread-local).
/// Each thread maintains a map of device_id → stream.
/// The copy destination's device is queried via cudaPointerGetAttributes, and
/// the stream for that device is used for DMA copy submission.
struct CudaStreamEntry {
    cudaStream_t stream;
    int device_id;
};

class PerDeviceStreamPool {
   public:
    CudaStreamEntry getOrCreate(int device_id) {
        auto it = pool_.find(device_id);
        if (it != pool_.end()) return it->second;

        int saved_device = 0;
        cudaGetDevice(&saved_device);
        if (cudaSetDevice(device_id) != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaSetDevice(" << device_id
                       << ") failed when creating stream";
            return {nullptr, -1};
        }
        CudaStreamEntry entry;
        entry.device_id = device_id;
        cudaError_t err =
            cudaStreamCreateWithFlags(&entry.stream, cudaStreamNonBlocking);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create MUSA CUDA stream on device "
                       << device_id << ": " << cudaGetErrorString(err);
        cudaSetDevice(saved_device);

        cudaDeviceProp prop;
        std::string pci = "unknown";
        if (cudaGetDeviceProperties(&prop, device_id) == cudaSuccess) {
            pci = std::string(prop.name) + " PCI " +
                  std::to_string(prop.pciBusID) + ":" +
                  std::to_string(prop.pciDeviceID);
        }
        const char *visible =
#ifdef USE_MUSA
            getenv("MUSA_VISIBLE_DEVICES");
        constexpr const char *visible_name = "MUSA_VISIBLE_DEVICES";
#else
            getenv("CUDA_VISIBLE_DEVICES");
        constexpr const char *visible_name = "CUDA_VISIBLE_DEVICES";
#endif
        LOG(INFO) << "MusaTransport: MUSA CUDA stream created on device "
                  << device_id << " [physical: " << pci << "] " << visible_name
                  << "=" << (visible ? visible : "(not set)")
                  << " pid=" << getpid();
        pool_[device_id] = entry;
        return entry;
    }
    ~PerDeviceStreamPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto &kv : pool_) {
            cudaSetDevice(kv.first);
            if (kv.second.stream) cudaStreamDestroy(kv.second.stream);
        }
        cudaSetDevice(saved_device);
    }

   private:
    std::unordered_map<int, CudaStreamEntry> pool_;
};

static thread_local PerDeviceStreamPool tl_device_stream_pool;

/// Per-device event pool (thread-local). Caches one event per device to
/// avoid repeated create/destroy on device switches, and ensures proper
/// cleanup when the thread exits.
class PerDeviceEventPool {
   public:
    cudaEvent_t getOrCreate(int device_id) {
        auto it = pool_.find(device_id);
        if (it != pool_.end()) return it->second;
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        if (cudaSetDevice(device_id) != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaSetDevice(" << device_id
                       << ") failed when creating event";
            return nullptr;
        }
        cudaEvent_t event = nullptr;
        cudaError_t err =
            cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create MUSA sync event on device "
                       << device_id << ": " << cudaGetErrorString(err);
        cudaSetDevice(saved_device);
        pool_[device_id] = event;
        return event;
    }
    ~PerDeviceEventPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto &kv : pool_) {
            cudaSetDevice(kv.first);
            if (kv.second) cudaEventDestroy(kv.second);
        }
        cudaSetDevice(saved_device);
    }

   private:
    std::unordered_map<int, cudaEvent_t> pool_;
};

static thread_local PerDeviceEventPool tl_device_event_pool;

// cudaMemcpyAsync is issued against the current device context on MUSA.  Keep
// the caller's device intact while submitting work to a per-device stream.
class ScopedCudaDevice {
   public:
    explicit ScopedCudaDevice(int device) {
        status_ = cudaGetDevice(&saved_device_);
        if (status_ != cudaSuccess) return;
        if (saved_device_ != device) {
            status_ = cudaSetDevice(device);
            changed_ = status_ == cudaSuccess;
        }
    }

    ~ScopedCudaDevice() {
        if (changed_) {
            cudaError_t err = cudaSetDevice(saved_device_);
            if (err != cudaSuccess) {
                LOG(ERROR) << "MusaTransport: failed to restore device "
                           << saved_device_ << ": " << cudaGetErrorString(err);
            }
        }
    }

    bool ok() const { return status_ == cudaSuccess; }

   private:
    int saved_device_{-1};
    cudaError_t status_{cudaSuccess};
    bool changed_{false};
};

static int getDeviceForPointer(const void *ptr) {
    cudaPointerAttributes attr;
    if (!ptr || cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

static Status synchronizeCallerBuffers(
    const std::vector<const void *> &local_buffers) {
    if (local_buffers.empty()) return Status::OK();

    int current_device = -1;
    cudaError_t err = cudaGetDevice(&current_device);
    if (err != cudaSuccess) {
        return Status::Context("cudaGetDevice failed: " +
                               std::string(cudaGetErrorString(err)));
    }

    std::map<int, bool> devices;
    for (const void *buffer : local_buffers) {
        int device = getDeviceForPointer(buffer);
        devices[device >= 0 ? device : current_device] = true;
    }

    for (const auto &[device, unused] : devices) {
        (void)unused;
        ScopedCudaDevice guard(device);
        if (!guard.ok()) {
            return Status::Context("failed to select caller buffer device " +
                                   std::to_string(device));
        }
        cudaEvent_t sync_event = tl_device_event_pool.getOrCreate(device);
        if (!sync_event) {
            return Status::Context(
                "failed to create caller sync event on "
                "device " +
                std::to_string(device));
        }
        err = cudaEventRecord(sync_event, cudaStreamPerThread);
        if (err != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaEventRecord failed on device "
                       << device << ": " << cudaGetErrorString(err);
            return Status::Context("cudaEventRecord failed: " +
                                   std::string(cudaGetErrorString(err)));
        }
        err = cudaEventSynchronize(sync_event);
        if (err != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaEventSynchronize failed on "
                          "device "
                       << device << ": " << cudaGetErrorString(err);
            return Status::Context("cudaEventSynchronize failed: " +
                                   std::string(cudaGetErrorString(err)));
        }
    }
    return Status::OK();
}

static int deviceFromLocation(const std::string &location) {
    if (location.rfind(GPU_PREFIX, 0) != 0) return -1;
    const char *begin = location.c_str() + GPU_PREFIX.size();
    if (*begin == '\0') return -1;

    errno = 0;
    char *end = nullptr;
    long value = std::strtol(begin, &end, 10);
    if (errno == ERANGE || end == begin || *end != '\0' || value < 0 ||
        value > std::numeric_limits<int>::max()) {
        return -1;
    }

    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || value >= count) return -1;
    return static_cast<int>(value);
}

static bool openOnMetadataDevice() {
    static const bool enabled = [] {
        const char *value = std::getenv("MC_MUSA_IPC_OPEN_DEVICE");
        if (value && std::string(value) == "metadata") {
            const char *musa_visible = std::getenv("MUSA_VISIBLE_DEVICES");
            const char *mthreads_visible =
                std::getenv("MTHREADS_VISIBLE_DEVICES");
            LOG(INFO) << "MusaTransport: opening IPC handles on the metadata "
                         "device; peers must use identical ordered physical "
                         "device lists (MUSA_VISIBLE_DEVICES="
                      << (musa_visible ? musa_visible : "(not set)")
                      << ", MTHREADS_VISIBLE_DEVICES="
                      << (mthreads_visible ? mthreads_visible : "(not set)")
                      << ")";
            return true;
        }
        if (!value || std::string(value) == "current") {
            LOG(INFO) << "MusaTransport: opening IPC handles on the current "
                         "device";
            return false;
        }
        LOG(WARNING) << "MusaTransport: unknown MC_MUSA_IPC_OPEN_DEVICE="
                     << value << ", using current";
        return false;
    }();
    return enabled;
}

static bool localVisibilityMappingIsConsistent() {
    const char *musa_visible = std::getenv("MUSA_VISIBLE_DEVICES");
    const char *mthreads_visible = std::getenv("MTHREADS_VISIBLE_DEVICES");
    if (!musa_visible && !mthreads_visible) return true;
    if (!musa_visible || !mthreads_visible) {
        LOG(ERROR) << "MusaTransport: metadata-device IPC requires both "
                      "MUSA_VISIBLE_DEVICES and MTHREADS_VISIBLE_DEVICES to be "
                      "set, or both to be unset";
        return false;
    }
    if (std::string(musa_visible) == std::string(mthreads_visible)) return true;
    LOG(ERROR) << "MusaTransport: MUSA_VISIBLE_DEVICES=" << musa_visible
               << " differs from MTHREADS_VISIBLE_DEVICES=" << mthreads_visible
               << "; refusing metadata-device IPC mapping";
    return false;
}

enum class MusaCopyApi { Auto, Default, TransferBatch };

static MusaCopyApi copyApi() {
    static const MusaCopyApi api = [] {
        const char *value = std::getenv("MC_MUSA_COPY_API");
        if (!value || std::string(value) == "auto") return MusaCopyApi::Auto;
        if (std::string(value) == "default") return MusaCopyApi::Default;
        if (std::string(value) == "transfer_batch")
            return MusaCopyApi::TransferBatch;
        LOG(WARNING) << "MusaTransport: unknown MC_MUSA_COPY_API=" << value
                     << ", using auto";
        return MusaCopyApi::Auto;
    }();
    return api;
}

static const char *copyApiName(MusaCopyApi api) {
    switch (api) {
        case MusaCopyApi::Auto:
            return "auto";
        case MusaCopyApi::Default:
            return "default";
        case MusaCopyApi::TransferBatch:
            return "transfer_batch";
    }
    return "unknown";
}

static size_t transferBatchMinBytes() {
    static const size_t min_bytes = [] {
        constexpr size_t kDefaultMinBytes = 1024ULL * 1024ULL;
        const char *value = std::getenv("MC_MUSA_TRANSFER_BATCH_MIN_BYTES");
        if (!value) return kDefaultMinBytes;
        errno = 0;
        char *end = nullptr;
        unsigned long long parsed = std::strtoull(value, &end, 0);
        if (errno == ERANGE || end == value || *end != '\0' ||
            parsed > std::numeric_limits<size_t>::max()) {
            LOG(WARNING)
                << "MusaTransport: invalid MC_MUSA_TRANSFER_BATCH_MIN_BYTES="
                << value << ", using " << kDefaultMinBytes;
            return kDefaultMinBytes;
        }
        return static_cast<size_t>(parsed);
    }();
    return min_bytes;
}

static bool shouldUseTransferBatch(MusaCopyApi api,
                                   const std::vector<size_t> &sizes) {
    if (api == MusaCopyApi::Default) return false;
    if (api == MusaCopyApi::TransferBatch) return true;
    const size_t min_bytes = transferBatchMinBytes();
    for (size_t size : sizes) {
        if (size < min_bytes) return false;
    }
    return true;
}

static std::string normalizeMemoryLocation(const void *addr,
                                           const std::string &location) {
    if (location.rfind(GPU_PREFIX, 0) == 0 || location != kWildcardLocation)
        return location;

    const int device = getDeviceForPointer(addr);
    if (device >= 0) return GPU_PREFIX + std::to_string(device);

    int current_device = -1;
    if (cudaGetDevice(&current_device) == cudaSuccess && current_device >= 0) {
        LOG(WARNING) << "MusaTransport: could not query wildcard buffer owner; "
                        "using current device "
                     << current_device;
        return GPU_PREFIX + std::to_string(current_device);
    }
    LOG(ERROR) << "MusaTransport: could not resolve wildcard buffer location "
                  "for IPC registration";
    return location;
}

static int selectStreamDevice(const void *destination) {
    // MUSA P2P IPC reaches the best path when the DMA stream belongs to the
    // copy destination: remote device for WRITE and local device for READ.
    int device = getDeviceForPointer(destination);
    if (device < 0) cudaGetDevice(&device);
    return device < 0 ? 0 : device;
}

static bool submitBatchCopies(const std::vector<void *> &srcs,
                              const std::vector<void *> &dsts,
                              const std::vector<size_t> &sizes,
                              cudaStream_t stream, size_t &fail_index) {
    const MusaCopyApi api = copyApi();
    static const bool logged_api = [api] {
        LOG(INFO) << "MusaTransport: copy API " << copyApiName(api)
                  << ", transfer-batch min bytes " << transferBatchMinBytes();
        return true;
    }();
    (void)logged_api;
    if (!shouldUseTransferBatch(api, sizes)) return false;

    if (srcs.empty() || srcs.size() != dsts.size() ||
        srcs.size() != sizes.size()) {
        fail_index = std::numeric_limits<size_t>::max();
        LOG(ERROR) << "MusaTransport: invalid batch-copy vectors";
        return true;
    }

#if defined(MUSA_VERSION) && MUSA_VERSION >= 50200
    std::vector<MUdeviceptr> driver_srcs(srcs.size());
    std::vector<MUdeviceptr> driver_dsts(dsts.size());
    std::vector<size_t> driver_sizes(sizes.begin(), sizes.end());
    for (size_t i = 0; i < srcs.size(); ++i) {
        driver_srcs[i] = reinterpret_cast<MUdeviceptr>(srcs[i]);
        driver_dsts[i] = reinterpret_cast<MUdeviceptr>(dsts[i]);
    }

    MUmemcpyAttributes attributes{};
    attributes.srcAccessOrder = MU_MEMCPY_SRC_ACCESS_ORDER_STREAM;
    size_t attributes_index = 0;
    fail_index = std::numeric_limits<size_t>::max();
    MUresult result = muMemoryTransferBatchAsync(
        driver_dsts.data(), driver_srcs.data(), driver_sizes.data(),
        driver_srcs.size(), &attributes, &attributes_index, 1, &fail_index,
        reinterpret_cast<MUstream>(stream));
    if (result != MUSA_SUCCESS) {
        LOG(ERROR) << "MusaTransport: muMemoryTransferBatchAsync failed: "
                   << result;
        return true;
    }
    fail_index = srcs.size();
    return true;
#else
    static const bool warned_once = [] {
        LOG(WARNING) << "MusaTransport: muMemoryTransferBatchAsync requires "
                        "MUSA SDK 5.2 or newer; using per-slice copies";
        return true;
    }();
    (void)warned_once;
    return false;
#endif
}

static cudaError_t openIpcMemHandle(void **address, cudaIpcMemHandle_t handle,
                                    const std::string &location,
                                    int &opened_device) {
    int saved_device = -1;
    cudaError_t saved_err = cudaGetDevice(&saved_device);
    if (saved_err != cudaSuccess) return saved_err;

    opened_device = saved_device;
    if (openOnMetadataDevice()) {
        const int metadata_device = deviceFromLocation(location);
        if (metadata_device >= 0) {
            if (!localVisibilityMappingIsConsistent())
                return musaErrorInvalidDevice;
            opened_device = metadata_device;
        } else if (location == kWildcardLocation) {
            LOG(WARNING) << "MusaTransport: wildcard IPC location; opening on "
                            "the current device for compatibility";
        } else {
            LOG(ERROR) << "MusaTransport: invalid metadata device in "
                       << location;
            return musaErrorInvalidDevice;
        }
        if (opened_device != saved_device) {
            cudaError_t set_err = cudaSetDevice(opened_device);
            if (set_err != cudaSuccess) return set_err;
        }
    }

    cudaError_t open_err =
        cudaIpcOpenMemHandle(address, handle, cudaIpcMemLazyEnablePeerAccess);
    cudaError_t restore_err = cudaSuccess;
    if (opened_device != saved_device)
        restore_err = cudaSetDevice(saved_device);

    if (restore_err != cudaSuccess) {
        if (open_err == cudaSuccess) {
            cudaError_t set_err = cudaSetDevice(opened_device);
            if (set_err == cudaSuccess) {
                cudaIpcCloseMemHandle(*address);
                cudaSetDevice(saved_device);
            }
        }
        return restore_err;
    }
    return open_err;
}

static cudaError_t closeIpcMemHandle(void *address, int opened_device) {
    int saved_device = -1;
    cudaError_t saved_err = cudaGetDevice(&saved_device);
    if (saved_err != cudaSuccess) return saved_err;

    if (opened_device >= 0 && opened_device != saved_device) {
        cudaError_t set_err = cudaSetDevice(opened_device);
        if (set_err != cudaSuccess) return set_err;
    }
    cudaError_t close_err = cudaIpcCloseMemHandle(address);
    if (opened_device >= 0 && opened_device != saved_device) {
        cudaError_t restore_err = cudaSetDevice(saved_device);
        if (close_err == cudaSuccess) close_err = restore_err;
    }
    return close_err;
}

}  // anonymous namespace

using Slice = Transport::Slice;

static void submitBatchMemcpy(const std::vector<Slice *> &slices,
                              const std::vector<void *> &srcs,
                              const std::vector<void *> &dsts,
                              const std::vector<size_t> &sizes,
                              cudaStream_t stream) {
    if (slices.empty()) return;

    const size_t count = slices.size();

    size_t fail_index = std::numeric_limits<size_t>::max();
    if (submitBatchCopies(srcs, dsts, sizes, stream, fail_index)) {
        const size_t posted =
            fail_index == count ? count : (fail_index < count ? fail_index : 0);
        for (size_t i = 0; i < posted; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
        for (size_t i = posted; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) slices[i]->markFailed();
        }
        return;
    }

    for (size_t i = 0; i < count; ++i) {
        cudaError_t err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (err != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaMemcpyAsync failed at index " << i
                       << ": " << cudaGetErrorString(err);
            slices[i]->markFailed();
            continue;
        }
        slices[i]->status = Slice::POSTED;
        slices[i]->local.cuda_stream = (void *)stream;
    }
}

struct CopyGroup {
    std::vector<Slice *> slices;
    std::vector<void *> srcs;
    std::vector<void *> dsts;
    std::vector<size_t> sizes;
    cudaStream_t stream{nullptr};
};

// A single Mooncake batch may contain requests targeting different GPU
// ordinals. Group by destination device so every copy uses the matching MUSA
// stream and context.
static bool submitBatchMemcpyByDevice(
    const std::vector<Slice *> &slices, const std::vector<void *> &srcs,
    const std::vector<void *> &dsts, const std::vector<size_t> &sizes,
    std::vector<std::pair<Slice *, int>> &slice_devices, std::string &error) {
    slice_devices.clear();
    if (slices.empty()) return true;
    if (srcs.size() != slices.size() || dsts.size() != slices.size() ||
        sizes.size() != slices.size()) {
        error = "MusaTransport: invalid copy vector sizes";
        return false;
    }

    auto fail_pending = [](const std::vector<Slice *> &failed_slices) {
        for (Slice *slice : failed_slices) {
            if (slice && slice->status == Slice::PENDING) slice->markFailed();
        }
    };

    auto prepare_stream = [&](int device, cudaStream_t &stream) {
        CudaStreamEntry stream_entry =
            tl_device_stream_pool.getOrCreate(device);
        if (!stream_entry.stream) {
            error =
                "MusaTransport: failed to create transfer stream on device " +
                std::to_string(device);
            return false;
        }
        ScopedCudaDevice guard(device);
        if (!guard.ok()) {
            error = "MusaTransport: failed to select transfer device " +
                    std::to_string(device);
            return false;
        }
        stream = stream_entry.stream;
        return true;
    };

    auto submit_group =
        [&](int device, const std::vector<Slice *> &group_slices,
            const std::vector<void *> &group_srcs,
            const std::vector<void *> &group_dsts,
            const std::vector<size_t> &group_sizes, cudaStream_t stream) {
            ScopedCudaDevice guard(device);
            if (!guard.ok()) {
                error = "MusaTransport: failed to select transfer device " +
                        std::to_string(device);
                return false;
            }
            submitBatchMemcpy(group_slices, group_srcs, group_dsts, group_sizes,
                              stream);
            return true;
        };

    // Preserve the MUSA hot path and avoid allocation/copy overhead for the
    // common MUSA case where every entry selects the same destination device.
    int first_device = selectStreamDevice(dsts.front());
    if (first_device < 0) {
        error = "MusaTransport: failed to select a stream device";
        fail_pending(slices);
        return false;
    }

    bool homogeneous = true;
    std::vector<int> devices;
    devices.reserve(slices.size());
    devices.push_back(first_device);
    for (size_t i = 1; i < slices.size(); ++i) {
        int device = selectStreamDevice(dsts[i]);
        if (device < 0) {
            error = "MusaTransport: failed to select a stream device";
            fail_pending(slices);
            return false;
        }
        devices.push_back(device);
        homogeneous = homogeneous && device == first_device;
    }

    if (homogeneous) {
        cudaStream_t stream = nullptr;
        if (!prepare_stream(first_device, stream) ||
            !submit_group(first_device, slices, srcs, dsts, sizes, stream)) {
            fail_pending(slices);
            return false;
        }
        for (Slice *slice : slices)
            slice_devices.emplace_back(slice, first_device);
        return true;
    }

    std::map<int, CopyGroup> groups;
    auto append_to_group = [&](size_t i, int device) {
        auto &group = groups[device];
        group.slices.push_back(slices[i]);
        group.srcs.push_back(srcs[i]);
        group.dsts.push_back(dsts[i]);
        group.sizes.push_back(sizes[i]);
    };
    for (size_t i = 0; i < slices.size(); ++i) append_to_group(i, devices[i]);

    // Preflight every stream/context before submitting any group, so a setup
    // failure cannot leave a mixed-device task half POSTED and half PENDING.
    for (auto &[device, group] : groups) {
        if (!prepare_stream(device, group.stream)) {
            fail_pending(slices);
            return false;
        }
    }

    for (auto &[device, group] : groups) {
        if (!submit_group(device, group.slices, group.srcs, group.dsts,
                          group.sizes, group.stream)) {
            fail_pending(slices);
            return false;
        }
    }
    for (const auto &[device, group] : groups) {
        for (Slice *slice : group.slices)
            slice_devices.emplace_back(slice, device);
    }
    return true;
}

MusaTransport::MusaTransport() = default;

void MusaTransport::rememberSliceDevices(
    const std::vector<std::pair<Slice *, int>> &slices) {
    std::lock_guard<std::mutex> lock(slice_device_mutex_);
    for (const auto &[slice, device] : slices) {
        if (slice && slice->status == Slice::POSTED)
            slice_devices_[slice] = device;
    }
}

int MusaTransport::sliceDevice(const Slice *slice, const void *destination) {
    {
        std::lock_guard<std::mutex> lock(slice_device_mutex_);
        auto it = slice_devices_.find(slice);
        if (it != slice_devices_.end()) return it->second;
    }
    return selectStreamDevice(destination);
}

void MusaTransport::forgetSliceDevice(const Slice *slice) {
    std::lock_guard<std::mutex> lock(slice_device_mutex_);
    slice_devices_.erase(slice);
}

MusaTransport::~MusaTransport() {
    for (auto &entry : remap_entries_) {
        cudaError_t close_err =
            closeIpcMemHandle(entry.second.shm_addr, entry.second.device_id);
        if (close_err != cudaSuccess) {
            LOG(ERROR) << "MusaTransport: cudaIpcCloseMemHandle failed: "
                       << cudaGetErrorString(close_err);
        }
    }
    remap_entries_.clear();
}

int MusaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> metadata,
                           std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (old_desc) *desc = *old_desc;
    desc->name = local_server_name_;
    if (desc->protocol.empty()) {
        desc->protocol = "musa";
    } else if (desc->protocol.find("musa") == std::string::npos) {
        desc->protocol += ",musa";
    }
#else
    desc->protocol = "musa";
#endif
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status MusaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "MusaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "MusaTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    if (entries.empty()) return Status::OK();

    // Complete every fallible preflight before extending task_list. This keeps
    // failed submissions releasable and avoids zero-slice tasks being reported
    // as completed.
    std::vector<const void *> local_buffers;
    local_buffers.reserve(entries.size());
    for (const auto &request : entries) local_buffers.push_back(request.source);
    Status sync_status = synchronizeCallerBuffers(local_buffers);
    if (!sync_status.ok()) return sync_status;

    std::vector<uint64_t> dest_addrs;
    dest_addrs.reserve(entries.size());
    for (const auto &request : entries) {
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        dest_addrs.push_back(dest_addr);
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (size_t index = 0; index < entries.size(); ++index) {
        const auto &request = entries[index];
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_addr = dest_addrs[index];
        task.batch_id = batch_id;
        task.transport_ = this;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void *src = (request.opcode == TransferRequest::READ)
                        ? (void *)slice->local.dest_addr
                        : (void *)slice->source_addr;
        void *dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void *)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Let the backend choose the submission device after IPC address
    // relocation, grouping a mixed-target batch by the selected device.
    std::vector<std::pair<Slice *, int>> slice_devices;
    std::string submit_error;
    if (!submitBatchMemcpyByDevice(slices, srcs, dsts, sizes, slice_devices,
                                   submit_error)) {
        return Status::Context(submit_error);
    }
    rememberSliceDevices(slice_devices);

    return Status::OK();
}

Status MusaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "MusaTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    // Poll POSTED slices on the destination-owned MUSA device context.
    std::map<int, std::unordered_map<cudaStream_t, cudaError_t>>
        stream_status_cache;
    for (auto *slice : task.slice_list) {
        if (slice && slice->status == Slice::POSTED) {
            cudaStream_t stream = (cudaStream_t)slice->local.cuda_stream;
            const void *destination =
                slice->opcode == TransferRequest::READ
                    ? slice->source_addr
                    : static_cast<void *>(slice->local.dest_addr);
            const int stream_device = sliceDevice(slice, destination);
            auto &device_cache = stream_status_cache[stream_device];
            auto it = device_cache.find(stream);
            cudaError_t cuda_err;
            if (it == device_cache.end()) {
                ScopedCudaDevice guard(stream_device);
                if (!guard.ok()) {
                    forgetSliceDevice(slice);
                    slice->markFailed();
                    continue;
                }
                cuda_err = cudaStreamQuery(stream);
                device_cache[stream] = cuda_err;
            } else {
                cuda_err = it->second;
            }
            if (cuda_err == cudaSuccess) {
                forgetSliceDevice(slice);
                slice->markSuccess();
            } else if (cuda_err != cudaErrorNotReady) {
                forgetSliceDevice(slice);
                slice->markFailed();
            }
        }
    }
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

Status MusaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    if (task_list.empty()) return Status::OK();
    for (TransferTask *task : task_list) {
        if (!task || !task->request)
            return Status::InvalidArgument("invalid MUSA transfer task");
    }

    auto fail_tasks = [&task_list]() {
        for (TransferTask *task : task_list) {
            if (task->slice_list.empty()) {
                const auto &request = *task->request;
                Slice *slice = getSliceCache().allocate();
                slice->task = task;
                slice->source_addr = request.source;
                slice->length = request.length;
                slice->opcode = request.opcode;
                slice->status = Slice::PENDING;
                task->slice_list.push_back(slice);
                task->slice_count = task->slice_count + 1;
            }
            for (Slice *slice : task->slice_list) {
                if (slice->status == Slice::PENDING) slice->markFailed();
            }
#ifndef USE_EVENT_DRIVEN_COMPLETION
            task->is_finished = true;
#endif
        }
    };

    std::vector<const void *> local_buffers;
    std::vector<uint64_t> dest_addrs;
    local_buffers.reserve(task_list.size());
    dest_addrs.reserve(task_list.size());
    for (TransferTask *task : task_list) {
        const auto &request = *task->request;
        local_buffers.push_back(request.source);
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) {
                fail_tasks();
                return Status::Memory("device memory not registered");
            }
        }
        dest_addrs.push_back(dest_addr);
    }
    Status sync_status = synchronizeCallerBuffers(local_buffers);
    if (!sync_status.ok()) {
        fail_tasks();
        return sync_status;
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (size_t index = 0; index < task_list.size(); ++index) {
        auto &task = *task_list[index];
        auto &request = *task.request;
        uint64_t dest_addr = dest_addrs[index];
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void *src = (request.opcode == TransferRequest::READ)
                        ? (void *)slice->local.dest_addr
                        : (void *)slice->source_addr;
        void *dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void *)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: See submitTransfer() for backend-specific stream grouping.
    std::vector<std::pair<Slice *, int>> slice_devices;
    std::string submit_error;
    if (!submitBatchMemcpyByDevice(slices, srcs, dsts, sizes, slice_devices,
                                   submit_error)) {
        return Status::Context(submit_error);
    }
    rememberSliceDevices(slice_devices);

    return Status::OK();
}

int MusaTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, addr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MusaTransport: cudaPointerGetAttributes failed";
        return -1;
    }

    if (attr.type != cudaMemoryTypeDevice) {
        LOG(ERROR) << "Unsupported memory type, " << addr << " " << attr.type;
        return -1;
    }

    cudaIpcMemHandle_t handle;
    err = cudaIpcGetMemHandle(&handle, addr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "MusaTransport: cudaIpcGetMemHandle failed";
        return -1;
    }

    (void)remote_accessible;
    BufferDesc desc;
    desc.addr = (uint64_t)addr;
    desc.length = length;
    desc.name = normalizeMemoryLocation(addr, location);
    desc.shm_name = serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "musa";
#endif
    return metadata_->addLocalMemoryBuffer(desc, update_metadata);
}

int MusaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int MusaTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                               uint64_t length,
                                               uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
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
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t)) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    int ipc_device = -1;
                    cudaError_t err = openIpcMemHandle(&shm_addr, handle,
                                                       entry.name, ipc_device);
                    if (err != cudaSuccess) {
                        LOG(ERROR) << "MusaTransport: "
                                      "cudaIpcOpenMemHandle failed: "
                                   << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    shm_entry.device_id = ipc_device;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched MUSA IPC handle";
                    return -1;
                }
            }
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int MusaTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int MusaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int first_error = 0;
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

void *MusaTransport::allocatePinnedLocalMemory(size_t size) {
    void *ptr = nullptr;
    cudaError_t err = cudaMalloc(&ptr, size);
    if (err != cudaSuccess)
        LOG(ERROR) << "MusaTransport: cudaMalloc failed: "
                   << cudaGetErrorString(err);
    return ptr;
}

void MusaTransport::freePinnedLocalMemory(void *ptr) {
    if (ptr) cudaFree(ptr);
}
}  // namespace mooncake
