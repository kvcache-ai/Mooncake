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

#include "transport/nvlink_transport/nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
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
/// Each thread maintains a map of device_id → stream.
/// The source buffer's device is queried via cudaPointerGetAttributes,
/// and the stream for THAT device is used for DMA copy submission.
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
            LOG(ERROR) << "NvlinkTransport: cudaSetDevice(" << device_id
                       << ") failed when creating stream";
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
        const char *visible =
#ifdef USE_MUSA
            getenv("MUSA_VISIBLE_DEVICES");
        constexpr const char *visible_name = "MUSA_VISIBLE_DEVICES";
#else
            getenv("CUDA_VISIBLE_DEVICES");
        constexpr const char *visible_name = "CUDA_VISIBLE_DEVICES";
#endif
        LOG(INFO) << "NvlinkTransport: NVLink CUDA stream created on device "
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
            LOG(ERROR) << "NvlinkTransport: cudaSetDevice(" << device_id
                       << ") failed when creating event";
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
                LOG(ERROR) << "NvlinkTransport: failed to restore device "
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
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

class DefaultNvlinkTransportPolicy final : public GpuIpcTransportPolicy {
   public:
    const char *protocol() const override { return "nvlink"; }
    const char *displayName() const override { return "NvlinkTransport"; }

    int selectStreamDevice(const void *local_buffer, const void * /*source*/,
                           const void * /*destination*/) const override {
        int device_id = getDeviceForPointer(local_buffer);
        if (device_id < 0) cudaGetDevice(&device_id);
        return device_id < 0 ? 0 : device_id;
    }

    cudaError_t openIpcMemHandle(void **address, cudaIpcMemHandle_t handle,
                                 const std::string & /*location*/,
                                 int &opened_device) const override {
        // Keep the legacy CUDA path byte-for-byte in terms of context
        // ownership.  The recorded device is only needed by policies (MUSA)
        // whose IPC close operation is context-sensitive.
        opened_device = -1;
        return cudaIpcOpenMemHandle(address, handle,
                                    cudaIpcMemLazyEnablePeerAccess);
    }

    cudaError_t closeIpcMemHandle(void *address,
                                  int /*opened_device*/) const override {
        return cudaIpcCloseMemHandle(address);
    }
};

static std::shared_ptr<GpuIpcTransportPolicy> makeDefaultNvlinkPolicy() {
    static std::shared_ptr<GpuIpcTransportPolicy> policy =
        std::make_shared<DefaultNvlinkTransportPolicy>();
    return policy;
}

}  // anonymous namespace

using Slice = Transport::Slice;

/// Submit batched memcpy operations using cudaMemcpyBatchAsync when available
/// (CUDA 12.8+), falling back to per-slice cudaMemcpyAsync otherwise.
/// Uses cudaMemcpySrcAccessOrderStream to ensure source data visibility for
/// P2P copies. This attribute is REQUIRED — without it, the GPU does not
/// insert the necessary memory barriers for P2P access, causing segfaults.
/// The caller must also establish GPU-level stream synchronization
/// (via cudaEventRecord + cudaStreamWaitEvent) before calling this function.
/// Individual slice errors are tracked so that slices whose memcpy failed
/// are marked as FAILED while successfully submitted ones are POSTED.
static void submitBatchMemcpy(const std::vector<Slice *> &slices,
                              const std::vector<void *> &srcs,
                              const std::vector<void *> &dsts,
                              const std::vector<size_t> &sizes,
                              const GpuIpcTransportPolicy *policy,
                              cudaStream_t stream, int stream_device) {
    if (slices.empty()) return;

    const size_t count = slices.size();

    // A vendor policy may provide a lower-CPU batch primitive.  MUSA uses
    // muMemoryTransferBatchAsync here; CUDA keeps the existing runtime paths
    // below.  A handled failure is terminal for this group: retrying with
    // per-slice copies could duplicate operations already accepted by the
    // driver.
    if (policy) {
        size_t fail_index = std::numeric_limits<size_t>::max();
        if (policy->submitBatchCopies(srcs, dsts, sizes, stream, fail_index)) {
            if (fail_index == count) {
                for (size_t i = 0; i < count; ++i) {
                    slices[i]->status = Slice::POSTED;
                    slices[i]->local.cuda_stream = (void *)stream;
                    slices[i]->local.cuda_device = stream_device;
                }
            } else {
                const size_t posted = fail_index < count ? fail_index : 0;
                for (size_t i = 0; i < posted; ++i) {
                    slices[i]->status = Slice::POSTED;
                    slices[i]->local.cuda_stream = (void *)stream;
                    slices[i]->local.cuda_device = stream_device;
                }
                for (size_t i = posted; i < count; ++i) {
                    if (slices[i]->status == Slice::PENDING) {
                        slices[i]->markFailed();
                    }
                }
            }
            return;
        }
    }

    // Log the active memcpy path once per process lifetime
    static const bool logged_once = [] {
#if CUDART_VERSION >= 13000
        LOG(INFO) << "NvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 13.0 path)";
#elif CUDART_VERSION >= 12080
        LOG(INFO) << "NvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 12.8 path)";
#else
        LOG(INFO) << "NvlinkTransport: using per-slice cudaMemcpyAsync "
                  << "(CUDA < 12.8 fallback path)";
#endif
        return true;
    }();
    (void)logged_once;

#if CUDART_VERSION >= 12080
    cudaError_t err = cudaSuccess;
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
        LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
                   << "failed: " << cudaGetErrorString(err);
        // CUDA >= 13.0 does not return fail_idx; conservatively mark all
        // as FAILED since we cannot determine which copies succeeded.
        for (size_t i = 0; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
            slices[i]->local.cuda_device = stream_device;
        }
    }
#elif CUDART_VERSION >= 12080
    err = cudaMemcpyBatchAsync(const_cast<void **>(dsts.data()),
                               const_cast<void **>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, &fail_idx, stream);
    if (err != cudaSuccess) {
        if (fail_idx < count) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed at index " << fail_idx
                       << " (src=" << srcs[fail_idx]
                       << ", dst=" << dsts[fail_idx]
                       << ", size=" << sizes[fail_idx]
                       << "): " << cudaGetErrorString(err);
        } else {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed: " << cudaGetErrorString(err);
        }
        // Copies [0, fail_idx) were submitted successfully → POSTED.  The
        // runtime does not guarantee a useful index on every error; an
        // out-of-range value therefore means that no slice is safe to mark
        // POSTED.
        // Copy [fail_idx] failed → FAILED.
        // Copies (fail_idx, count) were never submitted → FAILED.
        const size_t posted = fail_idx < count ? fail_idx : 0;
        for (size_t i = 0; i < posted; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
            slices[i]->local.cuda_device = stream_device;
        }
        for (size_t i = posted; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
            slices[i]->local.cuda_device = stream_device;
        }
    }
#else
    // Fallback for CUDA < 12.8: submit each memcpy individually
    for (size_t i = 0; i < count; ++i) {
        auto single_err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (single_err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyAsync failed at "
                       << "index " << i << ": "
                       << cudaGetErrorString(single_err);
            slices[i]->markFailed();
            continue;
        }
        slices[i]->status = Slice::POSTED;
        slices[i]->local.cuda_stream = (void *)stream;
        slices[i]->local.cuda_device = stream_device;
    }
    return;  // Slice states already set above
#endif
}

struct CopyGroup {
    std::vector<Slice *> slices;
    std::vector<void *> srcs;
    std::vector<void *> dsts;
    std::vector<size_t> sizes;
    cudaStream_t stream{nullptr};
};

// A single Mooncake batch may contain requests targeting different GPU
// ordinals.  MUSA groups by its policy-selected destination device so every
// copy uses the matching stream/context.  NVIDIA intentionally keeps its
// legacy behavior: one stream selected from the first request-local buffer.
static bool submitBatchMemcpyByDevice(
    const std::shared_ptr<GpuIpcTransportPolicy> &policy,
    const std::vector<Slice *> &slices, const std::vector<void *> &srcs,
    const std::vector<void *> &dsts, const std::vector<size_t> &sizes,
    std::string &error) {
    if (slices.empty()) return true;
    if (srcs.size() != slices.size() || dsts.size() != slices.size() ||
        sizes.size() != slices.size()) {
        error =
            std::string(policy->displayName()) + ": invalid copy vector sizes";
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
            error = std::string(policy->displayName()) +
                    ": failed to create transfer stream on device " +
                    std::to_string(device);
            return false;
        }
        if (policy->requiresStreamDeviceGuard()) {
            ScopedCudaDevice guard(device);
            if (!guard.ok()) {
                error = std::string(policy->displayName()) +
                        ": failed to select transfer device " +
                        std::to_string(device);
                return false;
            }
        }
        stream = stream_entry.stream;
        return true;
    };

    auto submit_group =
        [&](int device, const std::vector<Slice *> &group_slices,
            const std::vector<void *> &group_srcs,
            const std::vector<void *> &group_dsts,
            const std::vector<size_t> &group_sizes, cudaStream_t stream) {
            if (policy->requiresStreamDeviceGuard()) {
                ScopedCudaDevice guard(device);
                if (!guard.ok()) {
                    error = std::string(policy->displayName()) +
                            ": failed to select transfer device " +
                            std::to_string(device);
                    return false;
                }
                submitBatchMemcpy(group_slices, group_srcs, group_dsts,
                                  group_sizes, policy.get(), stream, device);
                return true;
            }
            submitBatchMemcpy(group_slices, group_srcs, group_dsts, group_sizes,
                              policy.get(), stream, device);
            return true;
        };

    // Preserve the NVLink hot path and avoid allocation/copy overhead for the
    // common MUSA case where every entry selects the same destination device.
    int first_device = policy->selectStreamDevice(slices.front()->source_addr,
                                                  srcs.front(), dsts.front());
    if (first_device < 0) {
        error = std::string(policy->displayName()) +
                ": policy returned an invalid stream device";
        fail_pending(slices);
        return false;
    }

    bool homogeneous = true;
    std::vector<int> devices;
    if (policy->groupTransfersByDevice()) {
        devices.reserve(slices.size());
        devices.push_back(first_device);
        for (size_t i = 1; i < slices.size(); ++i) {
            int device = policy->selectStreamDevice(slices[i]->source_addr,
                                                    srcs[i], dsts[i]);
            if (device < 0) {
                error = std::string(policy->displayName()) +
                        ": policy returned an invalid stream device";
                fail_pending(slices);
                return false;
            }
            devices.push_back(device);
            homogeneous = homogeneous && device == first_device;
        }
    }

    if (!policy->groupTransfersByDevice() || homogeneous) {
        cudaStream_t stream = nullptr;
        if (!prepare_stream(first_device, stream) ||
            !submit_group(first_device, slices, srcs, dsts, sizes, stream)) {
            fail_pending(slices);
            return false;
        }
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
    return true;
}

static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(
                cudaGetDeviceCount(&cached_num_devices),
                "NvlinkTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

static bool supportFabricMem() {
    if (getenv("MC_USE_NVLINK_IPC")) return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDeviceCount failed: "
                   << cudaGetErrorString(err);
        return false;
    }
    if (num_devices == 0) {
        LOG(ERROR) << "NvlinkTransport: no device found";
        return false;
    }

#ifdef USE_CUDA
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        cuDeviceGetAttribute(&device_support_fabric_mem,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             device_id);
        if (!device_support_fabric_mem) {
            return false;
        }
    }
#endif
    return true;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(cudaDeviceCanAccessPeer(
                                  &canAccessPeer, src_device_id, dst_device_id),
                              "NvlinkTransport: failed to query peer access")) {
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "NvlinkTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(src_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(dst_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    return true;
}

NvlinkTransport::NvlinkTransport()
    : NvlinkTransport(makeDefaultNvlinkPolicy(), supportFabricMem()) {}

NvlinkTransport::NvlinkTransport(std::shared_ptr<GpuIpcTransportPolicy> policy,
                                 bool use_fabric_mem)
    : use_fabric_mem_(use_fabric_mem), policy_(std::move(policy)) {
    if (!policy_) LOG(FATAL) << "NvlinkTransport requires a runtime policy";
}
//     int num_devices = getNumDevices();
//     if (globalConfig().trace) {
//         LOG(INFO) << "NvlinkTransport: use_fabric_mem_:" << use_fabric_mem_
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
//                         << "NvlinkTransport: enabled p2p access between
//                         device "
//                         << src_device_id << " and " << dst_device_id;
//                 }
//             } else {
//                 LOG(ERROR) << "NvlinkTransport: failed to enable p2p access "
//                               "between device "
//                            << src_device_id << " and " << dst_device_id;
//             }
//         }
//     }
// }

NvlinkTransport::~NvlinkTransport() {
    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            cudaError_t close_err = policy_->closeIpcMemHandle(
                entry.second.shm_addr, entry.second.device_id);
            if (close_err != cudaSuccess) {
                LOG(ERROR) << policy_->displayName()
                           << ": cudaIpcCloseMemHandle failed: "
                           << cudaGetErrorString(close_err);
            }
        }
    }
    remap_entries_.clear();
}

int NvlinkTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (policy_->preserveExistingMetadata()) {
        auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (old_desc) *desc = *old_desc;
        desc->name = local_server_name_;
        if (desc->protocol.empty()) {
            desc->protocol = policy_->protocol();
        } else if (desc->protocol.find(policy_->protocol()) ==
                   std::string::npos) {
            desc->protocol += ",";
            desc->protocol += policy_->protocol();
        }
    } else {
        desc->protocol = policy_->protocol();
    }
#else
    desc->protocol = policy_->protocol();
#endif
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status NvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    // Synchronize with the caller's GPU work (e.g., PyTorch gather operations)
    // that produced the source data. We use cudaEventSynchronize (CPU-blocking)
    // instead of cudaStreamWaitEvent to avoid expensive cross-device event
    // operations on non-NVIDIA GPUs. The CPU blocking is acceptable because
    // the transfer depends on the caller's work anyway — they cannot overlap.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
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
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
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
    std::string submit_error;
    if (!submitBatchMemcpyByDevice(policy_, slices, srcs, dsts, sizes,
                                   submit_error)) {
        return Status::Context(submit_error);
    }

    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    // Poll POSTED slices for async completion via cudaStreamQuery.  MUSA's
    // destination-owned stream may differ from the caller's current device;
    // its policy switches context for the query while CUDA keeps the legacy
    // direct-query behavior.
    std::map<int, std::unordered_map<cudaStream_t, cudaError_t>>
        stream_status_cache;
    for (auto *slice : task.slice_list) {
        if (slice && slice->status == Slice::POSTED) {
            cudaStream_t stream = (cudaStream_t)slice->local.cuda_stream;
            const int stream_device = slice->local.cuda_device;
            auto &device_cache = stream_status_cache[stream_device];
            auto it = device_cache.find(stream);
            cudaError_t cuda_err;
            if (it == device_cache.end()) {
                if (policy_->requiresStreamDeviceGuard()) {
                    ScopedCudaDevice guard(stream_device);
                    if (!guard.ok()) {
                        slice->markFailed();
                        continue;
                    }
                    cuda_err = cudaStreamQuery(stream);
                } else {
                    cuda_err = cudaStreamQuery(stream);
                }
                device_cache[stream] = cuda_err;
            } else {
                cuda_err = it->second;
            }
            if (cuda_err == cudaSuccess) {
                slice->markSuccess();
            } else if (cuda_err != cudaErrorNotReady) {
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

Status NvlinkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    // Synchronize with caller's GPU work via cudaEventSynchronize.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
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
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
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
    std::string submit_error;
    if (!submitBatchMemcpyByDevice(policy_, slices, srcs, dsts, sizes,
                                   submit_error)) {
        return Status::Context(submit_error);
    }

    return Status::OK();
}

int NvlinkTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    if (!use_fabric_mem_) {
        cudaPointerAttributes attr;
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed";
            return -1;
        }

        if (attr.type != cudaMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return -1;
        }

        cudaIpcMemHandle_t handle;
        err = cudaIpcGetMemHandle(&handle, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcGetMemHandle failed";
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = policy_->normalizeMemoryLocation(addr, location);
        desc.shm_name =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
#ifdef ENABLE_MULTI_PROTOCOL
        if (policy_->preserveExistingMetadata())
            desc.protocol = policy_->protocol();
#endif
        return metadata_->addLocalMemoryBuffer(desc, true);
    } else {
        CUmemGenericAllocationHandle handle;
        auto result = cuMemRetainAllocationHandle(&handle, addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "Memory region " << addr
                         << " is not allocated by cuMemCreate, "
                         << "but it can be used as local buffer";
            return 0;
        }

        // Find whole physical page for memory registration
        void *real_addr;
        size_t real_size;
        result = cuMemGetAddressRange((CUdeviceptr *)&real_addr, &real_size,
                                      (CUdeviceptr)addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "NvlinkTransport: cuMemGetAddressRange failed: "
                         << result;
            const uint64_t granularity = 2 * 1024 * 1024;
            real_addr = addr;
            real_size = (length + granularity - 1) & ~(granularity - 1);
        }

        CUmemFabricHandle export_handle;
        result = cuMemExportToShareableHandle(&export_handle, handle,
                                              CU_MEM_HANDLE_TYPE_FABRIC, 0);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemExportToShareableHandle failed: "
                << result;
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)real_addr;  // (uint64_t)addr;
        desc.length = real_size;          // length;
        desc.name = policy_->normalizeMemoryLocation(addr, location);
        desc.shm_name =
            serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
#ifdef ENABLE_MULTI_PROTOCOL
        if (policy_->preserveExistingMetadata())
            desc.protocol = policy_->protocol();
#endif
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int NvlinkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int NvlinkTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
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
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    int ipc_device = -1;
                    cudaError_t err = policy_->openIpcMemHandle(
                        &shm_addr, handle, entry.name, ipc_device);
                    if (err != cudaSuccess) {
                        LOG(ERROR) << policy_->displayName()
                                   << ": cudaIpcOpenMemHandle failed: "
                                   << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
#ifdef USE_MUSA
                    shm_entry.device_id = ipc_device;
#endif
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
                    CUmemFabricHandle export_handle;
                    memcpy(&export_handle, output_buffer.data(),
                           sizeof(export_handle));
                    void *shm_addr = nullptr;
                    CUmemGenericAllocationHandle handle;
                    auto result = cuMemImportFromShareableHandle(
                        &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: "
                                      "cuMemImportFromShareableHandle failed: "
                                   << result;
                        return -1;
                    }
                    result = cuMemAddressReserve((CUdeviceptr *)&shm_addr,
                                                 entry.length, 0, 0, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemAddressReserve failed: "
                            << result;
                        return -1;
                    }
                    result = cuMemMap((CUdeviceptr)shm_addr, entry.length, 0,
                                      handle, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return -1;
                    }

                    int device_count;
                    cudaGetDeviceCount(&device_count);
                    CUmemAccessDesc accessDesc[device_count];
                    for (int device_id = 0; device_id < device_count;
                         ++device_id) {
                        accessDesc[device_id].location.type =
                            CU_MEM_LOCATION_TYPE_DEVICE;
                        accessDesc[device_id].location.id = device_id;
                        accessDesc[device_id].flags =
                            CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                    }
                    result = cuMemSetAccess((CUdeviceptr)shm_addr, entry.length,
                                            accessDesc, device_count);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: "
                                   << result;
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched NVLink data transfer method";
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

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int first_error = 0;
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

void *NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        cudaMalloc(&ptr, size);
        return ptr;
    }
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int cudaDev;
    int flag = 0;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }
    CUresult result = cuDeviceGet(&currentDev, cudaDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGet failed: " << result;
        return nullptr;
    }
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.location.id = currentDev;
    result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGetAttribute failed: "
                   << result;
        return nullptr;
    }
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    result = cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAllocationGranularity failed: "
                   << result;
        return nullptr;
    }
    // fix size
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemCreate failed: " << result;
        return nullptr;
    }
    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemAddressReserve failed: " << result;
        cuMemRelease(handle);
        return nullptr;
    }
    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    int device_count;
    cudaGetDeviceCount(&device_count);
    CUmemAccessDesc accessDesc[device_count];
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }
    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc, device_count);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: " << result;
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    return ptr;
}

void NvlinkTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        cudaFree(ptr);
        return;
    }
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemRetainAllocationHandle failed: "
                   << result;
        return;
    }
    result = cuMemGetAddressRange(NULL, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
}
}  // namespace mooncake
