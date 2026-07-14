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

#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

static bool checkCudaErrorReturn(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        return false;
    }
    return true;
}

namespace mooncake {

namespace {

/// Per-device CUDA stream pool (thread-local).
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
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                       << device_id << ") failed";
            return {nullptr, -1};
        }
        CudaStreamEntry entry;
        entry.device_id = device_id;
        cudaError_t err =
            cudaStreamCreateWithFlags(&entry.stream, cudaStreamNonBlocking);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create NVLink CUDA stream on device "
                       << device_id << ": " << cudaGetErrorString(err);
        cudaSetDevice(saved_device);
        cudaDeviceProp prop;
        std::string pci = "unknown";
        if (cudaGetDeviceProperties(&prop, device_id) == cudaSuccess) {
            pci = std::string(prop.name) + " PCI " +
                  std::to_string(prop.pciBusID) + ":" +
                  std::to_string(prop.pciDeviceID);
        }
        const char *visible = getenv("CUDA_VISIBLE_DEVICES");
        LOG(INFO) << "IntraNodeNvlinkTransport: NVLink CUDA stream created on "
                  << "device " << device_id << " [physical: " << pci
                  << "] CUDA_VISIBLE_DEVICES="
                  << (visible ? visible : "(not set)") << " pid=" << getpid();
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
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                       << device_id << ") failed when creating event";
            return nullptr;
        }
        cudaEvent_t event = nullptr;
        cudaError_t err =
            cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create NVLink sync event on device "
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

static cudaEvent_t getCallerSyncEvent() {
    int current_device = 0;
    cudaGetDevice(&current_device);
    return tl_device_event_pool.getOrCreate(current_device);
}

static int getDeviceForPointer(const void *ptr) {
    cudaPointerAttributes attr;
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

static CudaStreamEntry getStreamForRequest(const void *source) {
    int device_id = getDeviceForPointer(source);
    if (device_id < 0) {
        cudaGetDevice(&device_id);
        if (device_id < 0) device_id = 0;
    }
    return tl_device_stream_pool.getOrCreate(device_id);
}

static double readDurationEnv(const char *name, double default_value,
                              bool allow_nonpositive) {
    const char *raw = std::getenv(name);
    if (!raw || !*raw) return default_value;

    errno = 0;
    char *end = nullptr;
    double value = std::strtod(raw, &end);
    constexpr double kMinDurationSeconds = 1e-9;
    constexpr double kMaxDurationSeconds =
        static_cast<double>(std::numeric_limits<int64_t>::max()) / 1e9;
    bool valid =
        errno != ERANGE && end != raw && *end == '\0' && std::isfinite(value) &&
        std::abs(value) <= kMaxDurationSeconds &&
        (allow_nonpositive ? (value <= 0 || value >= kMinDurationSeconds)
                           : value >= kMinDurationSeconds);
    if (!valid) {
        LOG(WARNING) << "IntraNodeNvlinkTransport: ignoring invalid " << name
                     << "='" << raw << "'; using " << default_value;
        return default_value;
    }
    return value;
}

}  // anonymous namespace

using Slice = Transport::Slice;

namespace {

struct BatchCompletion {
    cudaEvent_t event = nullptr;
    int device_id = -1;
    std::atomic<uint64_t> remaining_slices{0};
    std::atomic_bool reusable{true};
};

class CompletionEventPool {
   public:
    cudaError_t acquire(int device_id, cudaEvent_t *event) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto &events = events_[device_id];
            if (!events.empty()) {
                *event = events.back();
                events.pop_back();
                return cudaSuccess;
            }
        }
        return cudaEventCreateWithFlags(event, cudaEventDisableTiming);
    }

    void recycle(int device_id, cudaEvent_t event) {
        std::lock_guard<std::mutex> lock(mutex_);
        events_[device_id].push_back(event);
    }

    ~CompletionEventPool() {
        int previous_device = -1;
        cudaGetDevice(&previous_device);
        for (auto &device_events : events_) {
            cudaSetDevice(device_events.first);
            for (auto event : device_events.second) cudaEventDestroy(event);
        }
        if (previous_device >= 0) cudaSetDevice(previous_device);
    }

   private:
    std::mutex mutex_;
    std::unordered_map<int, std::vector<cudaEvent_t>> events_;
};

static CompletionEventPool completion_event_pool;

class AggressiveWriteGuard {
   public:
    explicit AggressiveWriteGuard(RWSpinlock &lock) : lock_(lock) {
        lock_.writeLockAggressive();
    }
    ~AggressiveWriteGuard() { lock_.unlock(); }

    AggressiveWriteGuard(const AggressiveWriteGuard &) = delete;
    AggressiveWriteGuard &operator=(const AggressiveWriteGuard &) = delete;

   private:
    RWSpinlock &lock_;
};

enum class CompletionAttachResult {
    kAttached,
    kCompletedSynchronously,
    kUnsafeFailure,
};

static CompletionAttachResult attachBatchCompletion(
    const std::vector<Slice *> &slices, cudaStream_t stream, int device_id) {
    uint64_t posted_count = 0;
    for (auto *slice : slices) {
        if (slice->status == Slice::POSTED) ++posted_count;
    }
    if (posted_count == 0) return CompletionAttachResult::kAttached;

    auto *completion = new BatchCompletion;
    completion->device_id = device_id;
    completion->remaining_slices.store(posted_count, std::memory_order_relaxed);

    int previous_device = -1;
    cudaError_t setup_error = cudaGetDevice(&previous_device);
    if (setup_error == cudaSuccess) setup_error = cudaSetDevice(device_id);
    if (setup_error == cudaSuccess) {
        setup_error =
            completion_event_pool.acquire(device_id, &completion->event);
    }
    if (setup_error == cudaSuccess) {
        setup_error = cudaEventRecord(completion->event, stream);
    }

    if (setup_error == cudaSuccess) {
        for (auto *slice : slices) {
            if (slice->status == Slice::POSTED) {
                slice->local.cuda_completion = completion;
            }
        }
        cudaError_t restore_error = cudaSetDevice(previous_device);
        if (restore_error != cudaSuccess) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: failed to restore device "
                       << previous_device
                       << " after recording batch completion: "
                       << cudaGetErrorString(restore_error);
        }
        return CompletionAttachResult::kAttached;
    }

    LOG(ERROR) << "IntraNodeNvlinkTransport: failed to record batch "
                  "completion event: "
               << cudaGetErrorString(setup_error);
    cudaError_t sync_error = cudaStreamSynchronize(stream);
    bool quiescent = sync_error == cudaSuccess;
    if (!quiescent) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cannot establish stream "
                      "quiescence after completion-event failure: "
                   << cudaGetErrorString(sync_error);
    }
    if (completion->event) cudaEventDestroy(completion->event);
    if (previous_device >= 0) cudaSetDevice(previous_device);
    delete completion;

    return quiescent ? CompletionAttachResult::kCompletedSynchronously
                     : CompletionAttachResult::kUnsafeFailure;
}

static void releaseBatchCompletion(BatchCompletion *completion) {
    if (!completion || completion->remaining_slices.fetch_sub(
                           1, std::memory_order_acq_rel) != 1) {
        return;
    }

    int previous_device = -1;
    cudaError_t error = cudaGetDevice(&previous_device);
    if (error == cudaSuccess) error = cudaSetDevice(completion->device_id);
    if (error == cudaSuccess && completion->reusable.load()) {
        completion_event_pool.recycle(completion->device_id, completion->event);
    } else if (error == cudaSuccess) {
        error = cudaEventDestroy(completion->event);
    }
    if (error != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to destroy batch "
                      "completion event: "
                   << cudaGetErrorString(error);
    }
    if (previous_device >= 0) cudaSetDevice(previous_device);
    delete completion;
}

}  // anonymous namespace

/// Submit batched memcpy operations using cudaMemcpyBatchAsync when available
/// (CUDA 12.8+), falling back to per-slice cudaMemcpyAsync otherwise.
/// Uses cudaMemcpySrcAccessOrderStream to ensure source data visibility for
/// P2P copies. This attribute is REQUIRED — without it, the GPU does not
/// insert the necessary memory barriers for P2P access, causing segfaults.
/// The caller must also establish GPU-level stream synchronization
/// (via cudaEventRecord + cudaStreamWaitEvent) before calling this function.
/// Individual slice errors are returned through handle_failure while
/// successfully submitted ones are marked POSTED.
template <typename FailureHandler>
static void submitBatchMemcpy(const std::vector<Slice *> &slices,
                              const std::vector<void *> &srcs,
                              const std::vector<void *> &dsts,
                              const std::vector<size_t> &sizes,
                              cudaStream_t stream,
                              FailureHandler &&handle_failure) {
    if (slices.empty()) return;

    const size_t count = slices.size();
    cudaError_t err = cudaSuccess;

    // Log the active memcpy path once per process lifetime
    static const bool logged_once = [] {
#if CUDART_VERSION >= 13000
        LOG(INFO) << "IntraNodeNvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 13.0 path)";
#elif CUDART_VERSION >= 12080
        LOG(INFO) << "IntraNodeNvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 12.8 path)";
#else
        LOG(INFO)
            << "IntraNodeNvlinkTransport: using per-slice cudaMemcpyAsync "
            << "(CUDA < 12.8 fallback path)";
#endif
        return true;
    }();
    (void)logged_once;

#if CUDART_VERSION >= 12080
    // srcAccessOrderStream is REQUIRED for P2P copies — without it, the GPU
    // does not insert necessary memory barriers for cross-device access,
    // resulting in segmentation faults. The caller also establishes a
    // GPU-level dependency via cudaEventRecord(cudaStreamPerThread) +
    // cudaStreamWaitEvent(nvlink_stream) to ensure source data is coherent
    // before the memcpy starts; these two mechanisms are complementary.
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    // cudaMemcpyBatchAsync in CUDA 12.8 takes non-const size_t* for sizes
    std::vector<size_t> mutable_sizes(sizes);
    size_t fail_idx = count;
#endif

#if CUDART_VERSION >= 13000
    err = cudaMemcpyBatchAsync(const_cast<const void **>(dsts.data()),
                               const_cast<const void **>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, stream);
    if (err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                   << "failed: " << cudaGetErrorString(err);
        // CUDA >= 13.0 does not return fail_idx. Synchronize once to determine
        // whether it is safe for the caller to release mapping references for
        // the slices that must all be reported as failed.
        cudaError_t sync_error = cudaStreamSynchronize(stream);
        bool failed_slices_quiescent = sync_error == cudaSuccess;
        if (!failed_slices_quiescent) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cannot establish stream "
                          "quiescence after batch submission failure: "
                       << cudaGetErrorString(sync_error);
        }
        for (size_t i = 0; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                handle_failure(slices[i], failed_slices_quiescent);
            }
        }
        return;
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
    }
#elif CUDART_VERSION >= 12080
    err = cudaMemcpyBatchAsync(const_cast<void **>(dsts.data()),
                               const_cast<void **>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, &fail_idx, stream);
    if (err != cudaSuccess) {
        if (fail_idx < count) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed at index " << fail_idx
                       << " (src=" << srcs[fail_idx]
                       << ", dst=" << dsts[fail_idx]
                       << ", size=" << sizes[fail_idx]
                       << "): " << cudaGetErrorString(err);
        } else {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed: " << cudaGetErrorString(err);
        }
        // Copies [0, fail_idx) were submitted successfully → POSTED.
        // Copy [fail_idx] failed → FAILED.
        // Copies (fail_idx, count) were never submitted → FAILED.
        for (size_t i = 0; i < fail_idx; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
        for (size_t i = fail_idx; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                handle_failure(slices[i], true);
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
    }
#else
    // Fallback for CUDA < 12.8: submit each memcpy individually
    for (size_t i = 0; i < count; ++i) {
        auto single_err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (single_err != cudaSuccess) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyAsync failed at "
                       << "index " << i << ": "
                       << cudaGetErrorString(single_err);
            handle_failure(slices[i], true);
            continue;
        }
        slices[i]->status = Slice::POSTED;
        slices[i]->local.cuda_stream = (void *)stream;
    }
    return;  // Slice states already set above
#endif
}

static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(
                cudaGetDeviceCount(&cached_num_devices),
                "IntraNodeNvlinkTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(
            cudaDeviceCanAccessPeer(&canAccessPeer, src_device_id,
                                    dst_device_id),
            "IntraNodeNvlinkTransport: failed to query peer access")) {
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(
            cudaSetDevice(src_device_id),
            "IntraNodeNvlinkTransport: failed to set device")) {
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p access "
                      "(Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;

        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(
            cudaSetDevice(dst_device_id),
            "IntraNodeNvlinkTransport: failed to set device")) {
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p access "
                      "(Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;

        return false;
    }

    return true;
}
IntraNodeNvlinkTransport::IntraNodeNvlinkTransport() {
    import_ttl_s_ = readDurationEnv("MC_NVLINK_IMPORT_TTL", 120.0, true);
    evict_interval_s_ =
        readDurationEnv("MC_NVLINK_EVICT_INTERVAL", 30.0, false);

    running_.store(true);
    if (import_ttl_s_ > 0) {
        evict_thread_ = std::thread(&IntraNodeNvlinkTransport::evictLoop, this);
        LOG(INFO) << "IntraNodeNvlinkTransport: idle-import evictor on (ttl "
                  << import_ttl_s_ << "s, scan " << evict_interval_s_ << "s)";
    } else {
        LOG(INFO) << "IntraNodeNvlinkTransport: idle-import evictor OFF (ttl "
                  << import_ttl_s_
                  << " <= 0); imported IPC mappings held until process exit";
    }
}

// IntraNodeNvlinkTransport::IntraNodeNvlinkTransport() :
// use_fabric_mem_(supportFabricMem()) {}
//     int num_devices = getNumDevices();
//     if (globalConfig().trace) {
//         LOG(INFO) << "IntraNodeNvlinkTransport: use_fabric_mem_:" <<
//         use_fabric_mem_
//                   << ", num_devices: " << num_devices;
//     }

//     for (int src_device_id = 0; src_device_id < num_devices; ++src_device_id)
//     {
//         for (int dst_device_id = src_device_id + 1; dst_device_id <
//         num_devices;
//              ++dst_device_id) {
//             if (enableP2PAccess(src_device_id, dst_device_id)) {
//                 if (globalConfig().trace) {
//                     LOG(INFO)
//                         << "IntraNodeNvlinkTransport: enabled p2p access
//                         between device "
//                         << src_device_id << " and " << dst_device_id;
//                 }
//             } else {
//                 LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p
//                 access "
//                               "between device "
//                            << src_device_id << " and " << dst_device_id;
//             }
//         }
//     }
// }

bool IntraNodeNvlinkTransport::releaseImportedEntry(OpenedShmEntry &entry) {
    if (entry.in_flight.load(std::memory_order_relaxed) != 0) {
        LOG(WARNING) << "IntraNodeNvlinkTransport: refusing to release an "
                        "imported mapping with in-flight transfers";
        return false;
    }
    if (!entry.mapped) return true;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    if (!entry.import_context) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: imported IPC mapping has no "
                      "CUDA context";
        return false;
    }
    CUcontext previous_context = nullptr;
    CUresult result = cuCtxGetCurrent(&previous_context);
    if (result == CUDA_SUCCESS) {
        result = cuCtxSetCurrent(entry.import_context);
    }
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to activate IPC "
                      "import context during release: "
                   << result;
        return false;
    }
    cudaError_t error = cudaIpcCloseMemHandle(entry.shm_addr);
    if (error == cudaSuccess) entry.mapped = false;
    result = cuCtxSetCurrent(previous_context);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to restore CUDA "
                      "context after IPC release: "
                   << result;
    }
    if (error != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaIpcCloseMemHandle failed "
                      "during release: "
                   << cudaGetErrorString(error);
        return false;
    }
    return true;
#else
    int previous_device = -1;
    cudaError_t error = cudaGetDevice(&previous_device);
    if (error != cudaSuccess || entry.device_id < 0) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cannot select the import "
                      "device before cudaIpcCloseMemHandle";
        return false;
    }
    error = cudaSetDevice(entry.device_id);
    if (error != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                   << entry.device_id
                   << ") failed before cudaIpcCloseMemHandle: "
                   << cudaGetErrorString(error);
        return false;
    }
    error = cudaIpcCloseMemHandle(entry.shm_addr);
    cudaError_t restore_error = cudaSetDevice(previous_device);
    if (restore_error != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                   << previous_device
                   << ") failed after cudaIpcCloseMemHandle: "
                   << cudaGetErrorString(restore_error);
    }
    if (error != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaIpcCloseMemHandle failed "
                      "during release: "
                   << cudaGetErrorString(error);
        return false;
    }
    entry.mapped = false;
    return true;
#endif
}

static inline int64_t steadyNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void IntraNodeNvlinkTransport::releaseMappingRef(uint64_t target_id,
                                                 uint64_t mapping_base_addr) {
    RWSpinlock::ReadGuard lock_guard(remap_lock_);
    auto it = remap_entries_.find({target_id, mapping_base_addr});
    if (it == remap_entries_.end()) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: mapping reference not found "
                      "for target "
                   << target_id << ", base " << (void *)mapping_base_addr;
        return;
    }

    auto &entry = it->second;
    uint64_t previous = entry.in_flight.load(std::memory_order_relaxed);
    while (previous != 0 &&
           !entry.in_flight.compare_exchange_weak(previous, previous - 1,
                                                  std::memory_order_relaxed)) {
    }
    if (previous == 0) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: mapping reference underflow "
                      "for target "
                   << target_id << ", base " << (void *)mapping_base_addr;
    } else if (previous == 1) {
        entry.last_active_ns.store(steadyNowNs(), std::memory_order_relaxed);
    }
}

void IntraNodeNvlinkTransport::releaseSliceMappingRef(Slice *slice) {
    if (!slice || slice->target_id == LOCAL_SEGMENT_ID) return;
    releaseMappingRef(slice->target_id, slice->local.mapping_base_addr);
}

int IntraNodeNvlinkTransport::retryPendingReleases() {
    std::vector<OpenedShmEntry> retry_entries;
    {
        AggressiveWriteGuard lock_guard(remap_lock_);
        retry_entries.swap(pending_releases_);
    }

    int released = 0;
    for (auto &entry : retry_entries) {
        bool previous_attempt_done = false;
        if (entry.release_state) {
            std::lock_guard<std::mutex> lock(entry.release_state->mutex);
            previous_attempt_done = entry.release_state->done;
        }
        if (!entry.release_state || previous_attempt_done) {
            entry.release_state = std::make_shared<ReleaseState>();
            AggressiveWriteGuard lock_guard(remap_lock_);
            auto it =
                remap_entries_.find({entry.target_id, entry.mapping_base_addr});
            if (it != remap_entries_.end() && it->second.releasing) {
                it->second.release_state = entry.release_state;
            }
        }
        bool success = releaseImportedEntry(entry);
        {
            AggressiveWriteGuard lock_guard(remap_lock_);
            auto key = std::make_pair(entry.target_id, entry.mapping_base_addr);
            if (success) {
                remap_entries_.erase(key);
            } else {
                auto it = remap_entries_.find(key);
                if (it != remap_entries_.end()) it->second = entry;
                pending_releases_.push_back(entry);
            }
        }
        {
            std::lock_guard<std::mutex> lock(entry.release_state->mutex);
            entry.release_state->failed = !success;
            entry.release_state->done = true;
        }
        entry.release_state->cv.notify_all();
        if (success) {
            ++released;
        }
    }
    return released;
}

int IntraNodeNvlinkTransport::evictIdleSegments(double ttl_s) {
    if (ttl_s <= 0) return 0;

    const int64_t now_ns = steadyNowNs();
    const int64_t ttl_ns = static_cast<int64_t>(ttl_s * 1e9);
    {
        AggressiveWriteGuard lock_guard(remap_lock_);
        for (auto it = remap_entries_.begin(); it != remap_entries_.end();) {
            int64_t last_active =
                it->second.last_active_ns.load(std::memory_order_relaxed);
            bool idle =
                it->second.in_flight.load(std::memory_order_relaxed) == 0 &&
                !it->second.releasing && last_active != 0 &&
                now_ns - last_active > ttl_ns;
            if (idle) {
                it->second.releasing = true;
                it->second.release_state = std::make_shared<ReleaseState>();
                pending_releases_.push_back(it->second);
            }
            ++it;
        }
    }

    return retryPendingReleases();
}

void IntraNodeNvlinkTransport::evictLoop() {
    while (running_.load()) {
        {
            std::unique_lock<std::mutex> lock(evict_cv_mutex_);
            evict_cv_.wait_for(lock,
                               std::chrono::duration<double>(evict_interval_s_),
                               [this] { return !running_.load(); });
        }
        if (!running_.load()) break;

        int released = evictIdleSegments(import_ttl_s_);
        if (released > 0) {
            LOG(INFO) << "IntraNodeNvlinkTransport: released " << released
                      << " idle imported IPC mapping(s) (idle > "
                      << import_ttl_s_ << "s)";
        }
    }
}

IntraNodeNvlinkTransport::~IntraNodeNvlinkTransport() {
    {
        std::lock_guard<std::mutex> lock(evict_cv_mutex_);
        running_.store(false);
    }
    evict_cv_.notify_all();
    if (evict_thread_.joinable()) evict_thread_.join();

    {
        RWSpinlock::WriteGuard lock_guard(remap_lock_);
        for (auto &entry : remap_entries_) {
            if (!entry.second.releasing) {
                entry.second.releasing = true;
                entry.second.release_state = std::make_shared<ReleaseState>();
                pending_releases_.push_back(entry.second);
            }
        }
    }
    retryPendingReleases();
    size_t pending_count = 0;
    {
        RWSpinlock::ReadGuard lock_guard(remap_lock_);
        pending_count = pending_releases_.size();
    }
    if (pending_count != 0) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: " << pending_count
                   << " imported mapping(s) remain after teardown";
    }
}

int IntraNodeNvlinkTransport::install(
    std::string &local_server_name, std::shared_ptr<TransferMetadata> metadata,
    std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink_intra";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status IntraNodeNvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: Exceed the limitation of "
                      "current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "IntraNodeNvlinkTransport: Exceed the limitation of capacity, "
            "batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    // Get per-device transfer stream for the source buffer's device.
    CudaStreamEntry stream_entry =
        getStreamForRequest(entries.empty() ? nullptr : entries[0].source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream) return Status::Context("Failed to create NVLink CUDA stream");
    // Synchronize with caller's GPU work via cudaEventSynchronize
    // (CPU-blocking) to avoid expensive cross-device cudaStreamWaitEvent on
    // non-NVIDIA GPUs.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventSynchronize failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_addr = request.target_offset;
        uint64_t mapping_base_addr = 0;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id,
                                                 mapping_base_addr);
            if (rc) {
                for (auto *prepared_slice : slices) {
                    releaseSliceMappingRef(prepared_slice);
                }
                return Status::Memory("device memory not registered");
            }
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->local.cuda_completion = nullptr;
        slice->local.mapping_base_addr = mapping_base_addr;
        slice->local.cuda_device_id = stream_entry.device_id;
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

    // Phase 2: Submit all memcpy operations
    std::vector<Slice *> refs_to_release;
    std::vector<std::pair<Slice *, bool>> terminal_slices;
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream,
                      [&](Slice *slice, bool quiescent) {
                          if (quiescent) refs_to_release.push_back(slice);
                          terminal_slices.push_back({slice, false});
                      });
    CompletionAttachResult completion_result =
        attachBatchCompletion(slices, stream, stream_entry.device_id);
    if (completion_result == CompletionAttachResult::kCompletedSynchronously) {
        for (auto *slice : slices) {
            if (slice->status == Slice::POSTED) {
                refs_to_release.push_back(slice);
                terminal_slices.push_back({slice, true});
            }
        }
    } else if (completion_result == CompletionAttachResult::kUnsafeFailure) {
        for (auto *slice : slices) {
            if (slice->status == Slice::POSTED) {
                terminal_slices.push_back({slice, false});
            }
        }
    }
    for (auto *slice : refs_to_release) releaseSliceMappingRef(slice);
    for (const auto &terminal : terminal_slices) {
        if (terminal.second)
            terminal.first->markSuccess();
        else
            terminal.first->markFailed();
    }

    return Status::OK();
}

Status IntraNodeNvlinkTransport::getTransferStatus(BatchID batch_id,
                                                   size_t task_id,
                                                   TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "IntraNodeNvlinkTransport::getTransportStatus invalid argument, "
            "batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    // Poll the completion event recorded after this submission. Unlike querying
    // the shared per-device stream, this lets an earlier batch complete even
    // while newer batches remain queued on that stream.
    struct EventStatus {
        cudaError_t query_result;
        bool quiescent;
    };
    std::unordered_map<BatchCompletion *, EventStatus> event_status_cache;
    std::vector<std::pair<Slice *, bool>> terminal_slices;
    for (auto *slice : task.slice_list) {
        if (slice && __atomic_load_n(&slice->status, __ATOMIC_ACQUIRE) ==
                         Slice::POSTED) {
            Slice::SliceStatus expected = Slice::POSTED;
            if (!__atomic_compare_exchange_n(
                    &slice->status, &expected, Slice::PENDING, false,
                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                continue;
            }
            auto *completion =
                static_cast<BatchCompletion *>(slice->local.cuda_completion);
            if (!completion) {
                LOG(ERROR) << "IntraNodeNvlinkTransport: POSTED slice has no "
                              "completion event";
                __atomic_store_n(&slice->status, Slice::POSTED,
                                 __ATOMIC_RELEASE);
                continue;
            }
            auto it = event_status_cache.find(completion);
            EventStatus event_status;
            if (it == event_status_cache.end()) {
                int previous_device = -1;
                cudaError_t cuda_err = cudaGetDevice(&previous_device);
                if (cuda_err != cudaSuccess ||
                    cudaSetDevice(completion->device_id) != cudaSuccess) {
                    LOG(ERROR)
                        << "IntraNodeNvlinkTransport: cannot activate device "
                        << completion->device_id
                        << " to query the batch completion event";
                    __atomic_store_n(&slice->status, Slice::POSTED,
                                     __ATOMIC_RELEASE);
                    continue;
                }
                event_status.query_result = cudaEventQuery(completion->event);
                event_status.quiescent =
                    event_status.query_result == cudaSuccess;
                if (event_status.query_result != cudaSuccess &&
                    event_status.query_result != cudaErrorNotReady) {
                    completion->reusable.store(false,
                                               std::memory_order_relaxed);
                    cudaError_t sync_error =
                        cudaEventSynchronize(completion->event);
                    event_status.quiescent = sync_error == cudaSuccess;
                    if (sync_error != cudaSuccess) {
                        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to "
                                      "establish batch quiescence after event "
                                      "query error: "
                                   << cudaGetErrorString(sync_error);
                    }
                }
                cudaError_t restore_error = cudaSetDevice(previous_device);
                if (restore_error != cudaSuccess) {
                    LOG(ERROR) << "IntraNodeNvlinkTransport: failed to restore "
                                  "device "
                               << previous_device
                               << " after querying the completion event: "
                               << cudaGetErrorString(restore_error);
                }
                event_status_cache[completion] = event_status;
            } else {
                event_status = it->second;
            }
            cudaError_t cuda_err = event_status.query_result;
            if (cuda_err != cudaSuccess && cuda_err != cudaErrorNotReady) {
                LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventQuery failed "
                              "on device "
                           << completion->device_id << ": "
                           << cudaGetErrorString(cuda_err);
            }
            if (cuda_err != cudaErrorNotReady) {
                if (cuda_err == cudaSuccess) {
                    releaseSliceMappingRef(slice);
                    terminal_slices.push_back({slice, true});
                } else {
                    if (event_status.quiescent) {
                        releaseSliceMappingRef(slice);
                    }
                    // If synchronization also failed, keep the mapping pinned:
                    // the DMA state is unknown and teardown will refuse to
                    // unmap it while the reference remains outstanding.
                    terminal_slices.push_back({slice, false});
                }
                releaseBatchCompletion(completion);
            } else {
                __atomic_store_n(&slice->status, Slice::POSTED,
                                 __ATOMIC_RELEASE);
            }
        }
    }
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    uint64_t transferred_bytes = task.transferred_bytes;
    for (const auto &terminal : terminal_slices) {
        if (terminal.second) {
            ++success_slice_count;
            transferred_bytes += terminal.first->length;
        } else {
            ++failed_slice_count;
        }
    }
    status.transferred_bytes = transferred_bytes;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
#ifndef USE_EVENT_DRIVEN_COMPLETION
    for (const auto &terminal : terminal_slices) {
        if (terminal.second)
            terminal.first->markSuccess();
        else
            terminal.first->markFailed();
    }
    if (success_slice_count + failed_slice_count == task.slice_count) {
        task.is_finished = true;
    }
#else
    for (const auto &terminal : terminal_slices) {
        if (terminal.second)
            terminal.first->markSuccess();
        else
            terminal.first->markFailed();
    }
#endif
    return Status::OK();
}

Status IntraNodeNvlinkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    // Get per-device transfer stream. See submitTransfer() for rationale.
    CudaStreamEntry stream_entry = getStreamForRequest(
        task_list.empty() ? nullptr : task_list[0]->request->source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream) return Status::Context("Failed to create NVLink CUDA stream");
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventSynchronize failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_addr = request.target_offset;
        uint64_t mapping_base_addr = 0;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id,
                                                 mapping_base_addr);
            if (rc) {
                for (auto *prepared_slice : slices) {
                    releaseSliceMappingRef(prepared_slice);
                }
                return Status::Memory("device memory not registered");
            }
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->local.cuda_completion = nullptr;
        slice->local.mapping_base_addr = mapping_base_addr;
        slice->local.cuda_device_id = stream_entry.device_id;
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

    // Phase 2: Submit all memcpy operations
    std::vector<Slice *> refs_to_release;
    std::vector<std::pair<Slice *, bool>> terminal_slices;
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream,
                      [&](Slice *slice, bool quiescent) {
                          if (quiescent) refs_to_release.push_back(slice);
                          terminal_slices.push_back({slice, false});
                      });
    CompletionAttachResult completion_result =
        attachBatchCompletion(slices, stream, stream_entry.device_id);
    if (completion_result == CompletionAttachResult::kCompletedSynchronously) {
        for (auto *slice : slices) {
            if (slice->status == Slice::POSTED) {
                refs_to_release.push_back(slice);
                terminal_slices.push_back({slice, true});
            }
        }
    } else if (completion_result == CompletionAttachResult::kUnsafeFailure) {
        for (auto *slice : slices) {
            if (slice->status == Slice::POSTED) {
                terminal_slices.push_back({slice, false});
            }
        }
    }
    for (auto *slice : refs_to_release) releaseSliceMappingRef(slice);
    for (const auto &terminal : terminal_slices) {
        if (terminal.second)
            terminal.first->markSuccess();
        else
            terminal.first->markFailed();
    }

    return Status::OK();
}

int IntraNodeNvlinkTransport::registerLocalMemory(void *addr, size_t length,
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
        LOG(ERROR)
            << "IntraNodeNvlinkTransport: cudaPointerGetAttributes failed";
        return -1;
    }

    if (attr.type != cudaMemoryTypeDevice) {
        LOG(ERROR) << "Unsupported memory type, " << addr << " " << attr.type;
        return -1;
    }

    // Resolve the true cudaMalloc base address. Framework caching allocators
    // (PyTorch, etc.) sub-allocate tensors within larger cudaMalloc segments.
    // cudaIpcGetMemHandle always returns a handle for the entire segment, so
    // we must register at segment granularity for correct IPC relocation.
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);
    if (cu_err != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cuMemGetAddressRange failed "
                   << "for addr " << addr << " (error " << cu_err << ")";
        return -1;
    }

    // Skip if this cudaMalloc block is already registered
    if (registered_base_addrs_.count((uint64_t)base_ptr)) {
        return 0;
    }

    cudaIpcMemHandle_t handle;
    err = cudaIpcGetMemHandle(&handle, (void *)base_ptr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaIpcGetMemHandle failed";
        return -1;
    }

    (void)remote_accessible;
    BufferDesc desc;
    desc.addr = (uint64_t)base_ptr;
    desc.length = alloc_size;
    desc.name = location;
    desc.shm_name = serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
    int rc = metadata_->addLocalMemoryBuffer(desc, true);
    if (rc == 0) {
        registered_base_addrs_.insert((uint64_t)base_ptr);
    }
    return rc;
}

int IntraNodeNvlinkTransport::unregisterLocalMemory(void *addr,
                                                    bool update_metadata) {
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);

    void *key_ptr = addr;
    if (cu_err == CUDA_SUCCESS) {
        key_ptr = (void *)base_ptr;
    } else {
        LOG(WARNING)
            << "IntraNodeNvlinkTransport: cuMemGetAddressRange failed for "
            << "addr " << addr << " during unregister (error " << cu_err
            << "). Memory may already be freed, using provided address.";
    }

    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        registered_base_addrs_.erase((uint64_t)key_ptr);
    }
    return metadata_->removeLocalMemoryBuffer(key_ptr, update_metadata);
}

int IntraNodeNvlinkTransport::relocateSharedMemoryAddress(
    uint64_t &dest_addr, uint64_t length, uint64_t target_id,
    uint64_t &mapping_base_addr) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
        retry_mapping:
            remap_lock_.lockShared();
            auto key = std::make_pair(target_id, entry.addr);
            auto cache_it = remap_entries_.find(key);
            if (cache_it != remap_entries_.end()) {
                if (cache_it->second.releasing) {
                    auto release_state = cache_it->second.release_state;
                    remap_lock_.unlockShared();
                    if (!release_state) {
                        std::this_thread::yield();
                        goto retry_mapping;
                    }
                    std::unique_lock<std::mutex> lock(release_state->mutex);
                    release_state->cv.wait(lock,
                                           [&] { return release_state->done; });
                    if (release_state->failed) return -1;
                    goto retry_mapping;
                }
                auto &mapped = cache_it->second;
                mapped.last_active_ns.store(steadyNowNs(),
                                            std::memory_order_relaxed);
                mapped.in_flight.fetch_add(1, std::memory_order_relaxed);
                auto shm_addr = mapped.shm_addr;
                remap_lock_.unlockShared();
                mapping_base_addr = entry.addr;
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();
            std::shared_ptr<ReleaseState> release_state;
            bool releasing_entry = false;
            {
                RWSpinlock::WriteGuard lock_guard(remap_lock_);
                auto existing = remap_entries_.find(key);
                if (existing != remap_entries_.end() &&
                    existing->second.releasing) {
                    releasing_entry = true;
                    release_state = existing->second.release_state;
                } else if (existing == remap_entries_.end()) {
                    std::vector<unsigned char> output_buffer;
                    deserializeBinaryData(entry.shm_name, output_buffer);
                    if (output_buffer.size() == sizeof(cudaIpcMemHandle_t)) {
                        cudaIpcMemHandle_t handle;
                        memcpy(&handle, output_buffer.data(), sizeof(handle));
                        void *shm_addr = nullptr;
                        cudaError_t err = cudaIpcOpenMemHandle(
                            &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                        if (err != cudaSuccess) {
                            LOG(ERROR) << "IntraNodeNvlinkTransport: "
                                          "cudaIpcOpenMemHandle failed: "
                                       << cudaGetErrorString(err);
                            return -1;
                        }
                        OpenedShmEntry shm_entry;
                        shm_entry.shm_addr = shm_addr;
                        shm_entry.length = entry.length;
                        shm_entry.target_id = target_id;
                        shm_entry.mapping_base_addr = entry.addr;
                        shm_entry.mapped = true;
                        err = cudaGetDevice(&shm_entry.device_id);
                        if (err != cudaSuccess) {
                            LOG(ERROR)
                                << "IntraNodeNvlinkTransport: cudaGetDevice "
                                   "failed after cudaIpcOpenMemHandle: "
                                << cudaGetErrorString(err);
                            cudaError_t close_error =
                                cudaIpcCloseMemHandle(shm_addr);
                            if (close_error != cudaSuccess) {
                                LOG(ERROR)
                                    << "IntraNodeNvlinkTransport: cleanup "
                                       "after cudaGetDevice failure also "
                                       "failed: "
                                    << cudaGetErrorString(close_error);
                            }
                            return -1;
                        }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                        CUresult context_result =
                            cuCtxGetCurrent(&shm_entry.import_context);
                        if (context_result != CUDA_SUCCESS ||
                            !shm_entry.import_context) {
                            LOG(ERROR) << "IntraNodeNvlinkTransport: "
                                          "cuCtxGetCurrent failed after "
                                          "cudaIpcOpenMemHandle: "
                                       << context_result;
                            cudaError_t close_error =
                                cudaIpcCloseMemHandle(shm_addr);
                            if (close_error != cudaSuccess) {
                                LOG(ERROR)
                                    << "IntraNodeNvlinkTransport: cleanup "
                                       "after cuCtxGetCurrent failure also "
                                       "failed: "
                                    << cudaGetErrorString(close_error);
                            }
                            return -1;
                        }
#endif
                        remap_entries_[key] = shm_entry;
                    } else {
                        LOG(ERROR) << "Mismatched NVLink data transfer method";
                        return -1;
                    }
                }
                if (!releasing_entry) {
                    auto &mapped = remap_entries_[key];
                    mapped.last_active_ns.store(steadyNowNs(),
                                                std::memory_order_relaxed);
                    mapped.in_flight.fetch_add(1, std::memory_order_relaxed);
                    auto shm_addr = mapped.shm_addr;
                    mapping_base_addr = entry.addr;
                    dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                    return 0;
                }
            }
            if (!release_state) {
                std::this_thread::yield();
                goto retry_mapping;
            }
            std::unique_lock<std::mutex> lock(release_state->mutex);
            release_state->cv.wait(lock, [&] { return release_state->done; });
            if (release_state->failed) return -1;
            goto retry_mapping;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int IntraNodeNvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int IntraNodeNvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

void *IntraNodeNvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    void *ptr = nullptr;
    cudaError_t res = cudaMalloc(&ptr, size);
    if (res == cudaSuccess) {
        LOG(INFO) << "IntraNodeNvlinkTransport: Falling back to cudaMalloc for "
                  << size << " bytes (memory will NOT be exportable)";
        return ptr;
    } else {
        LOG(ERROR)
            << "IntraNodeNvlinkTransport: cudaMalloc failed during fallback: "
            << cudaGetErrorString(res);
        return nullptr;
    }
}

void IntraNodeNvlinkTransport::freePinnedLocalMemory(void *ptr) {
    cudaFree(ptr);
    return;
}

}  // namespace mooncake
