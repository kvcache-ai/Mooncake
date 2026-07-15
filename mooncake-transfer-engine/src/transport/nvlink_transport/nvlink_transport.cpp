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
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

static bool checkCudaErrorReturn(cudaError_t result, const char* message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        return false;
    }
    return true;
}

namespace mooncake {

#if defined(USE_MNNVL) && defined(USE_CUDA)
namespace {

struct OwnedVmmRange {
    size_t length = 0;
    size_t owners = 0;
};

struct OwnedVmmRangeRegistry {
    std::mutex mutex;
    std::unordered_map<uintptr_t, OwnedVmmRange> ranges;
};

OwnedVmmRangeRegistry& ownedVmmRangeRegistry() {
    // HOST_NUMA allocations can be released by Store's atexit handler after
    // ordinary function-local static objects have already been destroyed. Keep
    // this
    // tiny process-lifetime registry alive so late cleanup never observes a
    // destructed mutex or map.
    static auto* registry = new OwnedVmmRangeRegistry();
    return *registry;
}

struct CleanupPendingVmmOwnerNode {
    std::unique_ptr<NvlinkVmmAllocation> owner;
    CleanupPendingVmmOwnerNode* next = nullptr;
};

struct CleanupPendingVmmOwnerRegistry {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    CleanupPendingVmmOwnerNode* head = nullptr;
    size_t count = 0;
};

CleanupPendingVmmOwnerRegistry& cleanupPendingVmmOwnerRegistry() {
    // A factory rollback can outlive ordinary static destruction if CUDA keeps
    // rejecting a teardown stage. The intrusive nodes are preallocated before
    // CUDA ownership is acquired and intentionally live until a later retry or
    // process exit, so recording a failed cleanup cannot itself allocate.
    static auto* registry = new CleanupPendingVmmOwnerRegistry();
    return *registry;
}

class CleanupPendingRegistryGuard {
   public:
    explicit CleanupPendingRegistryGuard(CleanupPendingVmmOwnerRegistry& owner)
        : owner_(owner) {
        while (owner_.lock.test_and_set(std::memory_order_acquire)) {
        }
    }

    ~CleanupPendingRegistryGuard() {
        owner_.lock.clear(std::memory_order_release);
    }

   private:
    CleanupPendingVmmOwnerRegistry& owner_;
};

void quarantineCleanupPendingVmmOwner(
    std::unique_ptr<CleanupPendingVmmOwnerNode> node) noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingRegistryGuard guard(registry);
    node->next = registry.head;
    registry.head = node.release();
    ++registry.count;
}

size_t cleanupPendingVmmOwnerCount() noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingRegistryGuard guard(registry);
    return registry.count;
}

bool retryCleanupPendingVmmOwners() noexcept {
    auto& registry = cleanupPendingVmmOwnerRegistry();
    CleanupPendingVmmOwnerNode* pending = nullptr;
    {
        CleanupPendingRegistryGuard guard(registry);
        pending = registry.head;
        registry.head = nullptr;
        registry.count = 0;
    }

    bool all_released = true;
    while (pending != nullptr) {
        std::unique_ptr<CleanupPendingVmmOwnerNode> node(pending);
        pending = pending->next;
        node->next = nullptr;

        Status status;
        try {
            status = node->owner->Release();
        } catch (const std::exception& error) {
            status = Status::Memory(
                std::string("cleanup-pending VMM retry threw: ") +
                error.what());
        } catch (...) {
            status = Status::Memory(
                "cleanup-pending VMM retry threw an unknown exception");
        }
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: cleanup-pending owner retry "
                          "failed; retaining CUDA ownership: "
                       << status;
            all_released = false;
            quarantineCleanupPendingVmmOwner(std::move(node));
        }
    }
    return all_released;
}

struct ProcessLifetimeRetainedHandleQuarantine {
    std::mutex mutex;
    std::vector<uint64_t> handles;
};

ProcessLifetimeRetainedHandleQuarantine&
processLifetimeRetainedHandleQuarantine() {
    // A CUDA driver teardown failure must not make Mooncake forget that the
    // retained handle is still live. Intentionally keep the ownership marker
    // until process exit after the transport and CUDA adapter are gone.
    static auto* quarantine = new ProcessLifetimeRetainedHandleQuarantine();
    return *quarantine;
}

void quarantineRetainedHandleForProcessLifetime(uint64_t handle) noexcept {
    try {
        auto& quarantine = processLifetimeRetainedHandleQuarantine();
        std::lock_guard<std::mutex> lock(quarantine.mutex);
        quarantine.handles.push_back(handle);
    } catch (const std::exception& error) {
        // Never let allocation failure escape NvlinkTransport's destructor.
        // The CUDA handle itself remains live until process exit even if the
        // diagnostic ownership marker cannot be allocated.
        LOG(ERROR) << "NvlinkTransport: unable to record process-lifetime "
                      "retained-handle quarantine: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: unknown failure while recording "
                      "process-lifetime retained-handle quarantine";
    }
}

}  // namespace
#endif

struct NvlinkTransport::ConsumerMetrics {
    std::array<std::atomic<uint64_t>, 2> mapping_cache_total{};
    std::atomic<uint64_t> lazy_import_duration_us_total{0};
    std::atomic<uint64_t> lazy_import_observations_total{0};
    std::array<std::atomic<uint64_t>, 5> failures_total{};
    std::array<std::atomic<uint64_t>, 4> transfer_results_total{};
};

void NvlinkTransport::observeCacheLookup(bool hit) {
    consumer_metrics_->mapping_cache_total[hit ? 0 : 1].fetch_add(
        1, std::memory_order_relaxed);
}

void NvlinkTransport::observeLazyImportLatency(uint64_t duration_us) {
    consumer_metrics_->lazy_import_duration_us_total.fetch_add(
        std::max<uint64_t>(duration_us, 1), std::memory_order_relaxed);
    consumer_metrics_->lazy_import_observations_total.fetch_add(
        1, std::memory_order_relaxed);
}

void NvlinkTransport::observeConsumerFailure(ConsumerFailureStage stage) {
    consumer_metrics_->failures_total[static_cast<size_t>(stage)].fetch_add(
        1, std::memory_order_relaxed);
}

void NvlinkTransport::observeTransferResult(TransferRequest::OpCode operation,
                                            bool success) {
    const size_t operation_offset = operation == TransferRequest::READ ? 0 : 2;
    consumer_metrics_
        ->transfer_results_total[operation_offset + (success ? 0 : 1)]
        .fetch_add(1, std::memory_order_relaxed);
}

bool NvlinkTransport::observeTransferResultOnce(TransferTask& task,
                                                bool success) {
    if (!task.operation_initialized) return false;
    if (__atomic_exchange_n(&task.transport_result_observed, true,
                            __ATOMIC_ACQ_REL)) {
        return false;
    }
    observeTransferResult(task.operation, success);
    return true;
}

void NvlinkTransport::finalizeTransferResult(TransferTask& task, bool success) {
    if (observeTransferResultOnce(task, success) && !success) {
        observeConsumerFailure(ConsumerFailureStage::COPY);
    }
}

void NvlinkTransport::finalizeSubmissionFailure(TransferTask& task,
                                                bool copy_failure) {
    const bool first_result = observeTransferResultOnce(task, false);
    if (copy_failure && first_result) {
        observeConsumerFailure(ConsumerFailureStage::COPY);
    }
    // Publish task/batch completion only after its result and exact failure
    // category are stable. Otherwise the event-driven batch fast path could
    // win the result CAS and misclassify a non-copy failure as copy.
    markSubmissionFailed(task);
}

void NvlinkTransport::appendMetrics(std::string& output) {
    if (!output.empty() && output.back() != '\n') output.push_back('\n');
    auto append_header = [&output](const char* name, const char* help) {
        output.append("# HELP ").append(name).append(" ").append(help).append(
            "\n# TYPE ");
        output.append(name).append(" counter\n");
    };
    auto append_sample = [&output](const char* name, const char* labels,
                                   uint64_t value) {
        output.append(name);
        if (labels != nullptr && labels[0] != '\0') {
            output.append("{").append(labels).append("}");
        }
        output.append(" ").append(std::to_string(value)).append("\n");
    };

    constexpr const char* cache_name =
        "mooncake_nvlink_consumer_mapping_cache_total";
    append_header(cache_name, "NVLink Consumer mapping cache lookups");
    append_sample(cache_name, "result=\"hit\"",
                  consumer_metrics_->mapping_cache_total[0].load(
                      std::memory_order_relaxed));
    append_sample(cache_name, "result=\"miss\"",
                  consumer_metrics_->mapping_cache_total[1].load(
                      std::memory_order_relaxed));

    constexpr const char* duration_name =
        "mooncake_nvlink_consumer_lazy_import_duration_us_total";
    append_header(duration_name,
                  "Cumulative NVLink Consumer lazy import duration in "
                  "microseconds");
    append_sample(duration_name, nullptr,
                  consumer_metrics_->lazy_import_duration_us_total.load(
                      std::memory_order_relaxed));
    constexpr const char* observations_name =
        "mooncake_nvlink_consumer_lazy_import_observations_total";
    append_header(observations_name,
                  "Observed NVLink Consumer lazy import attempts");
    append_sample(observations_name, nullptr,
                  consumer_metrics_->lazy_import_observations_total.load(
                      std::memory_order_relaxed));

    constexpr const char* failure_name =
        "mooncake_nvlink_consumer_failures_total";
    constexpr std::array<const char*, 5> failure_labels = {
        "stage=\"import\"", "stage=\"reserve\"", "stage=\"map\"",
        "stage=\"set_access\"", "stage=\"copy\""};
    append_header(failure_name, "NVLink Consumer failures by stage");
    for (size_t index = 0; index < failure_labels.size(); ++index) {
        append_sample(failure_name, failure_labels[index],
                      consumer_metrics_->failures_total[index].load(
                          std::memory_order_relaxed));
    }

    constexpr const char* transfer_name =
        "mooncake_nvlink_consumer_transfer_results_total";
    constexpr std::array<const char*, 4> transfer_labels = {
        "operation=\"read\",result=\"success\"",
        "operation=\"read\",result=\"failure\"",
        "operation=\"write\",result=\"success\"",
        "operation=\"write\",result=\"failure\""};
    append_header(transfer_name, "NVLink Consumer transfer results");
    for (size_t index = 0; index < transfer_labels.size(); ++index) {
        append_sample(transfer_name, transfer_labels[index],
                      consumer_metrics_->transfer_results_total[index].load(
                          std::memory_order_relaxed));
    }
}

namespace {

class ScopedLatencyObservation {
   public:
    explicit ScopedLatencyObservation(std::function<void(uint64_t)> observer)
        : observer_(std::move(observer)),
          start_(std::chrono::steady_clock::now()) {}

    ScopedLatencyObservation(const ScopedLatencyObservation&) = delete;
    ScopedLatencyObservation& operator=(const ScopedLatencyObservation&) =
        delete;

    ~ScopedLatencyObservation() {
        const auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_)
                .count();
        observer_(static_cast<uint64_t>(std::max<int64_t>(duration_us, 1)));
    }

   private:
    std::function<void(uint64_t)> observer_;
    std::chrono::steady_clock::time_point start_;
};

#if defined(USE_MNNVL) && defined(USE_CUDA)

Status cudaDriverFailure(const char* stage, CUresult result) {
    return Status::Memory(std::string(stage) + " failed with CUDA result " +
                          std::to_string(static_cast<int>(result)));
}

Status missingDriverFunction(const char* name) {
    return Status::InvalidArgument(
        std::string("missing CUDA driver adapter: ") + name);
}

bool isPowerOfTwo(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

template <typename DriverApi>
Status buildAllocationProp(const NvlinkVmmAllocation::Options& options,
                           const DriverApi& api, CUmemAllocationProp& prop) {
    prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.requestedHandleTypes = options.fabric_exportable
                                    ? CU_MEM_HANDLE_TYPE_FABRIC
                                    : static_cast<CUmemAllocationHandleType>(0);

    if (options.location_type == NvlinkVmmAllocation::LocationType::HOST_NUMA) {
        prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
        prop.location.id = options.location_id;
        return Status::OK();
    }

    if (!api.device_get) return missingDriverFunction("cuDeviceGet");
    if (!api.device_get_attribute)
        return missingDriverFunction("cuDeviceGetAttribute");

    CUdevice device;
    CUresult result = api.device_get(&device, options.location_id);
    if (result != CUDA_SUCCESS) return cudaDriverFailure("cuDeviceGet", result);

    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = device;

    int gpu_direct_rdma_supported = 0;
    result = api.device_get_attribute(
        &gpu_direct_rdma_supported,
        CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED, device);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuDeviceGetAttribute(GPU Direct RDMA)",
                                 result);
    if (gpu_direct_rdma_supported) prop.allocFlags.gpuDirectRDMACapable = 1;
    return Status::OK();
}

struct VmmAllocationOwnerRegistry {
    std::mutex mutex;
    std::unordered_map<void*, std::unique_ptr<NvlinkVmmAllocation>> owners;
};

VmmAllocationOwnerRegistry& vmmAllocationOwnerRegistry() {
    // Legacy pinned allocations may intentionally remain quarantined until
    // process exit after a CUDA cleanup failure. Avoid running their owners'
    // destructors against already-destroyed provenance state.
    static auto* registry = new VmmAllocationOwnerRegistry();
    return *registry;
}

#endif

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
        const char* visible = getenv("CUDA_VISIBLE_DEVICES");
        LOG(INFO) << "NvlinkTransport: NVLink CUDA stream created on device "
                  << device_id << " [physical: " << pci
                  << "] CUDA_VISIBLE_DEVICES="
                  << (visible ? visible : "(not set)") << " pid=" << getpid();
        pool_[device_id] = entry;
        return entry;
    }
    ~PerDeviceStreamPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto& kv : pool_) {
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
        for (auto& kv : pool_) {
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

static int getDeviceForPointer(const void* ptr) {
    cudaPointerAttributes attr;
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

static CudaStreamEntry getStreamForRequest(const void* source) {
    int device_id = getDeviceForPointer(source);
    if (device_id < 0) {
        cudaGetDevice(&device_id);
        if (device_id < 0) device_id = 0;
    }
    return tl_device_stream_pool.getOrCreate(device_id);
}

}  // anonymous namespace

#if defined(USE_MNNVL) && defined(USE_CUDA)
class NvlinkTransport::FabricMappingAttempt {
   public:
    explicit FabricMappingAttempt(NvlinkTransport& transport)
        : transport_(transport) {}
    FabricMappingAttempt(const FabricMappingAttempt&) = delete;
    FabricMappingAttempt& operator=(const FabricMappingAttempt&) = delete;

    ~FabricMappingAttempt() {
        if (cleanup_.mapped || cleanup_.address_reserved ||
            cleanup_.handle_owned) {
            transport_.ReleaseOrQuarantineFabricMapping(
                cleanup_, "lazy Fabric import failure");
        }
    }

    CUmemGenericAllocationHandle* handleOut() { return &cleanup_.handle; }
    CUmemGenericAllocationHandle handle() const { return cleanup_.handle; }
    void markHandleOwned() { cleanup_.handle_owned = true; }

    CUdeviceptr* addressOut() { return &cleanup_.address; }
    CUdeviceptr address() const { return cleanup_.address; }
    void markAddressReserved(size_t length) {
        cleanup_.length = length;
        cleanup_.address_reserved = true;
    }
    void markMapped() { cleanup_.mapped = true; }

    CUresult releaseHandle() {
        if (!cleanup_.handle_owned) return CUDA_SUCCESS;
        CUresult result =
            transport_.fabric_driver_api_.mem_release(cleanup_.handle);
        if (result == CUDA_SUCCESS) {
            cleanup_.handle_owned = false;
            cleanup_.handle = 0;
        }
        return result;
    }

    void transferMappingOwnership() {
        cleanup_.mapped = false;
        cleanup_.address_reserved = false;
        cleanup_.address = 0;
        cleanup_.length = 0;
    }

   private:
    NvlinkTransport& transport_;
    FabricMappingCleanup cleanup_;
};

class NvlinkTransport::RetainedHandleGuard {
   public:
    RetainedHandleGuard(NvlinkTransport& transport,
                        CUmemGenericAllocationHandle handle,
                        const char* failure_stage)
        : transport_(transport),
          handle_(handle),
          failure_stage_(failure_stage) {}

    RetainedHandleGuard(const RetainedHandleGuard&) = delete;
    RetainedHandleGuard& operator=(const RetainedHandleGuard&) = delete;

    ~RetainedHandleGuard() {
        if (owned_) {
            transport_.ReleaseOrQuarantineRetainedHandle(handle_,
                                                         failure_stage_);
        }
    }

    void TransferOwnership() { owned_ = false; }

   private:
    NvlinkTransport& transport_;
    CUmemGenericAllocationHandle handle_ = 0;
    const char* failure_stage_ = nullptr;
    bool owned_ = true;
};

bool NvlinkTransport::CleanupFabricMapping(FabricMappingCleanup& cleanup,
                                           const char* failure_stage) noexcept {
    try {
        // Cleanup is a staged state machine. A later CUDA primitive is unsafe
        // until the preceding ownership stage has completed successfully.
        if (cleanup.mapped) {
            if (!fabric_driver_api_.mem_unmap) {
                LOG(ERROR) << "NvlinkTransport: missing cuMemUnmap while "
                           << failure_stage;
                return false;
            }
            const CUresult result =
                fabric_driver_api_.mem_unmap(cleanup.address, cleanup.length);
            if (result != CUDA_SUCCESS) {
                LOG(ERROR) << "NvlinkTransport: cuMemUnmap failed while "
                           << failure_stage << ": " << result;
                return false;
            }
            cleanup.mapped = false;
        }

        if (cleanup.address_reserved) {
            if (!fabric_driver_api_.mem_address_free) {
                LOG(ERROR) << "NvlinkTransport: missing cuMemAddressFree while "
                           << failure_stage;
                return false;
            }
            const CUresult result = fabric_driver_api_.mem_address_free(
                cleanup.address, cleanup.length);
            if (result != CUDA_SUCCESS) {
                LOG(ERROR) << "NvlinkTransport: cuMemAddressFree failed while "
                           << failure_stage << ": " << result;
                return false;
            }
            cleanup.address_reserved = false;
            cleanup.address = 0;
        }

        if (cleanup.handle_owned) {
            if (!fabric_driver_api_.mem_release) {
                LOG(ERROR) << "NvlinkTransport: missing cuMemRelease while "
                           << failure_stage;
                return false;
            }
            const CUresult result =
                fabric_driver_api_.mem_release(cleanup.handle);
            if (result != CUDA_SUCCESS) {
                LOG(ERROR) << "NvlinkTransport: cuMemRelease failed while "
                           << failure_stage << ": " << result;
                return false;
            }
            cleanup.handle_owned = false;
            cleanup.handle = 0;
        }

        cleanup.length = 0;
        return true;
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: CUDA cleanup adapter threw while "
                   << failure_stage << ": " << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: CUDA cleanup adapter threw while "
                   << failure_stage;
    }
    return false;
}

bool NvlinkTransport::RetryQuarantinedFabricMappings() noexcept {
    auto retained = quarantined_fabric_mappings_.begin();
    for (auto it = quarantined_fabric_mappings_.begin();
         it != quarantined_fabric_mappings_.end(); ++it) {
        if (!CleanupFabricMapping(*it, "retrying lazy Fabric cleanup")) {
            if (retained != it) *retained = *it;
            ++retained;
        }
    }
    quarantined_fabric_mappings_.erase(retained,
                                       quarantined_fabric_mappings_.end());
    return quarantined_fabric_mappings_.empty();
}

void NvlinkTransport::PreserveProcessLifetimeFabricCleanup(
    FabricMappingCleanup cleanup) noexcept {
    struct ProcessLifetimeFabricCleanupQuarantine {
        std::mutex mutex;
        std::vector<FabricMappingCleanup> cleanups;
    };
    try {
        static auto* quarantine = new ProcessLifetimeFabricCleanupQuarantine();
        std::lock_guard<std::mutex> lock(quarantine->mutex);
        quarantine->cleanups.push_back(cleanup);
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: could not record process-lifetime "
                      "Fabric cleanup ownership: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: could not record process-lifetime "
                      "Fabric cleanup ownership";
    }
}

void NvlinkTransport::ReleaseOrQuarantineFabricMapping(
    FabricMappingCleanup cleanup, const char* failure_stage) noexcept {
    if (CleanupFabricMapping(cleanup, failure_stage)) return;

    try {
        // relocateSharedMemoryAddress() reserves this slot before importing a
        // handle. The catch remains as a destructor-safe last line of defense.
        quarantined_fabric_mappings_.push_back(cleanup);
        LOG(ERROR) << "NvlinkTransport: retaining failed Fabric cleanup for "
                      "a later retry";
    } catch (...) {
        PreserveProcessLifetimeFabricCleanup(cleanup);
        LOG(ERROR) << "NvlinkTransport: preserving failed Fabric cleanup in "
                      "the process-lifetime quarantine";
    }
}
#endif

#if defined(USE_MNNVL) && defined(USE_CUDA)

NvlinkVmmAllocation::DriverApi NvlinkVmmAllocation::ProductionDriverApi() {
    DriverApi api;
    api.device_get_count = [](int* count) { return cuDeviceGetCount(count); };
    api.device_get = [](CUdevice* device, int ordinal) {
        return cuDeviceGet(device, ordinal);
    };
    api.device_get_attribute = [](int* value, CUdevice_attribute attribute,
                                  CUdevice device) {
        return cuDeviceGetAttribute(value, attribute, device);
    };
    api.mem_get_allocation_granularity =
        [](size_t* granularity, const CUmemAllocationProp* prop,
           CUmemAllocationGranularity_flags flags) {
            return cuMemGetAllocationGranularity(granularity, prop, flags);
        };
    api.mem_create = [](CUmemGenericAllocationHandle* handle, size_t size,
                        const CUmemAllocationProp* prop,
                        unsigned long long flags) {
        return cuMemCreate(handle, size, prop, flags);
    };
    api.mem_address_reserve = [](CUdeviceptr* ptr, size_t size,
                                 size_t alignment, CUdeviceptr addr,
                                 unsigned long long flags) {
        return cuMemAddressReserve(ptr, size, alignment, addr, flags);
    };
    api.mem_map = [](CUdeviceptr ptr, size_t size, size_t offset,
                     CUmemGenericAllocationHandle handle,
                     unsigned long long flags) {
        return cuMemMap(ptr, size, offset, handle, flags);
    };
    api.mem_set_access = [](CUdeviceptr ptr, size_t size,
                            const CUmemAccessDesc* desc, size_t count) {
        return cuMemSetAccess(ptr, size, desc, count);
    };
    api.mem_unmap = [](CUdeviceptr ptr, size_t size) {
        return cuMemUnmap(ptr, size);
    };
    api.mem_address_free = [](CUdeviceptr ptr, size_t size) {
        return cuMemAddressFree(ptr, size);
    };
    api.mem_release = [](CUmemGenericAllocationHandle handle) {
        return cuMemRelease(handle);
    };
    api.mem_retain_allocation_handle = [](CUmemGenericAllocationHandle* handle,
                                          void* ptr) {
        return cuMemRetainAllocationHandle(handle, ptr);
    };
    api.mem_get_allocation_properties_from_handle =
        [](CUmemAllocationProp* prop, CUmemGenericAllocationHandle handle) {
            return cuMemGetAllocationPropertiesFromHandle(prop, handle);
        };
    api.mem_get_address_range = [](CUdeviceptr* base, size_t* size,
                                   CUdeviceptr ptr) {
        return cuMemGetAddressRange(base, size, ptr);
    };
    api.mem_export_to_shareable_handle =
        [](void* shareable_handle, CUmemGenericAllocationHandle handle,
           CUmemAllocationHandleType type, unsigned long long flags) {
            return cuMemExportToShareableHandle(shareable_handle, handle, type,
                                                flags);
        };
    api.mem_import_from_shareable_handle =
        [](CUmemGenericAllocationHandle* handle, void* shareable_handle,
           CUmemAllocationHandleType type) {
            return cuMemImportFromShareableHandle(handle, shareable_handle,
                                                  type);
        };
    return api;
}

bool NvlinkVmmAllocation::RegisterOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto& ranges = registry.ranges;
    const uintptr_t address = reinterpret_cast<uintptr_t>(base);
    auto [it, inserted] = ranges.emplace(address, OwnedVmmRange{length, 0});
    if (!inserted && it->second.length != length) return false;
    ++it->second.owners;
    return true;
}

bool NvlinkVmmAllocation::UnregisterOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto& ranges = registry.ranges;
    const auto it = ranges.find(reinterpret_cast<uintptr_t>(base));
    if (it == ranges.end() || it->second.length != length ||
        it->second.owners == 0) {
        LOG(ERROR) << "NvlinkVmmAllocation: owned range registry mismatch";
        return false;
    }
    if (--it->second.owners == 0) ranges.erase(it);
    return true;
}

bool NvlinkVmmAllocation::IsExactOwnedRange(void* base, size_t length) {
    if (base == nullptr || length == 0) return false;
    auto& registry = ownedVmmRangeRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    const auto& ranges = registry.ranges;
    const auto it = ranges.find(reinterpret_cast<uintptr_t>(base));
    return it != ranges.end() && it->second.length == length &&
           it->second.owners > 0;
}

NvlinkVmmAllocation::NvlinkVmmAllocation(NvlinkVmmAllocation&& other) noexcept
    : base_(other.base_),
      length_(other.length_),
      granularity_(other.granularity_),
      va_alignment_(other.va_alignment_),
      location_type_(other.location_type_),
      location_id_(other.location_id_),
      fabric_exportable_(other.fabric_exportable_),
      mapped_(other.mapped_),
      address_reserved_(other.address_reserved_),
      handle_owned_(other.handle_owned_),
      allocation_handle_(other.allocation_handle_),
      owned_range_registered_(other.owned_range_registered_),
      driver_api_(std::move(other.driver_api_)) {
    other.base_ = nullptr;
    other.length_ = 0;
    other.granularity_ = 0;
    other.va_alignment_ = 0;
    other.mapped_ = false;
    other.address_reserved_ = false;
    other.handle_owned_ = false;
    other.allocation_handle_ = 0;
    other.owned_range_registered_ = false;
}

NvlinkVmmAllocation::~NvlinkVmmAllocation() { reset(); }

Status NvlinkVmmAllocation::Release() {
    const CUdeviceptr ptr = reinterpret_cast<CUdeviceptr>(base_);

    // Cleanup-pending Fabric memory must stop satisfying provenance checks
    // before the first unmap attempt. Otherwise a failed unmap would leave a
    // range eligible for a new remote-accessible registration while teardown
    // is already in progress. This registry transition is idempotent and does
    // not need to be repeated by later release retries.
    if (owned_range_registered_) {
        if (!UnregisterOwnedRange(base_, length_)) {
            return Status::Memory(
                "HOST_NUMA VMM owned-range provenance removal failed");
        }
        owned_range_registered_ = false;
    }

    // These operations are deliberately serialized in reverse creation order.
    // A later stage must not run after an earlier stage fails: for example, a
    // still-mapped VA cannot safely be returned to the CUDA address allocator.
    if (mapped_) {
        if (!driver_api_.mem_unmap) return missingDriverFunction("cuMemUnmap");
        CUresult result = driver_api_.mem_unmap(ptr, length_);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemUnmap", result);
        mapped_ = false;
    }

    if (address_reserved_) {
        if (!driver_api_.mem_address_free)
            return missingDriverFunction("cuMemAddressFree");
        CUresult result = driver_api_.mem_address_free(ptr, length_);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemAddressFree", result);
        address_reserved_ = false;
        base_ = nullptr;
    }

    if (handle_owned_) {
        if (!driver_api_.mem_release)
            return missingDriverFunction("cuMemRelease");
        CUresult result = driver_api_.mem_release(
            static_cast<CUmemGenericAllocationHandle>(allocation_handle_));
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemRelease", result);
        handle_owned_ = false;
        allocation_handle_ = 0;
    }

    base_ = nullptr;
    length_ = 0;
    granularity_ = 0;
    va_alignment_ = 0;
    return Status::OK();
}

void NvlinkVmmAllocation::reset() noexcept {
    try {
        Status status = Release();
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup retained "
                          "unreleased CUDA ownership: "
                       << status;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup threw: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkVmmAllocation: best-effort cleanup threw an "
                      "unknown exception";
    }
}

bool NvlinkVmmAllocation::RetryCleanupPendingOwners() {
    return retryCleanupPendingVmmOwners();
}

size_t NvlinkVmmAllocation::CleanupPendingOwnerCount() {
    return cleanupPendingVmmOwnerCount();
}

Status NvlinkVmmAllocation::CheckStrictFabricCapability() {
    return CheckStrictFabricCapabilityWithDriverApi(ProductionDriverApi());
}

Status NvlinkVmmAllocation::CheckStrictFabricCapabilityWithDriverApi(
    const DriverApi& api) {
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr) {
        return Status::NotSupportedTransport(
            "MC_USE_NVLINK_IPC disables Fabric VMM allocations");
    }
    if (!api.device_get_count) return missingDriverFunction("cuDeviceGetCount");
    if (!api.device_get) return missingDriverFunction("cuDeviceGet");
    if (!api.device_get_attribute)
        return missingDriverFunction("cuDeviceGetAttribute");

    int device_count = 0;
    CUresult result = api.device_get_count(&device_count);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuDeviceGetCount", result);
    if (device_count <= 0)
        return Status::NotSupportedTransport(
            "Fabric VMM requires at least one visible CUDA device");

    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
        CUdevice device;
        result = api.device_get(&device, ordinal);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure("cuDeviceGet", result);

        int fabric_supported = 0;
        result = api.device_get_attribute(
            &fabric_supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
            device);
        if (result != CUDA_SUCCESS)
            return cudaDriverFailure(
                "cuDeviceGetAttribute(Fabric handle support)", result);
        if (!fabric_supported) {
            return Status::NotSupportedTransport(
                "visible CUDA device " + std::to_string(ordinal) +
                " does not support Fabric allocation handles");
        }
    }
    return Status::OK();
}

Status NvlinkVmmAllocation::GetAllocationGranularity(LocationType location_type,
                                                     int location_id,
                                                     bool fabric_exportable,
                                                     size_t& granularity) {
    return GetAllocationGranularityWithDriverApi(
        location_type, location_id, fabric_exportable, ProductionDriverApi(),
        granularity);
}

Status NvlinkVmmAllocation::GetAllocationGranularityWithDriverApi(
    LocationType location_type, int location_id, bool fabric_exportable,
    const DriverApi& api, size_t& granularity) {
    granularity = 0;
    if (location_id < 0)
        return Status::InvalidArgument("VMM location id must be non-negative");
    if (!api.mem_get_allocation_granularity)
        return missingDriverFunction("cuMemGetAllocationGranularity");

    Options options;
    options.location_type = location_type;
    options.location_id = location_id;
    options.fabric_exportable = fabric_exportable;
    CUmemAllocationProp prop = {};
    Status status = buildAllocationProp(options, api, prop);
    if (!status.ok()) return status;

    CUresult result = api.mem_get_allocation_granularity(
        &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS)
        return cudaDriverFailure("cuMemGetAllocationGranularity", result);
    if (!isPowerOfTwo(granularity)) {
        granularity = 0;
        return Status::Memory(
            "CUDA VMM allocation granularity is zero or not a power of two");
    }
    return Status::OK();
}

Status NvlinkVmmAllocation::Create(
    const Options& options, std::unique_ptr<NvlinkVmmAllocation>& allocation) {
    return CreateWithDriverApi(options, ProductionDriverApi(), allocation);
}

Status NvlinkVmmAllocation::CreateWithDriverApi(
    const Options& options, const DriverApi& api,
    std::unique_ptr<NvlinkVmmAllocation>& allocation) {
    allocation.reset();
    if (!RetryCleanupPendingOwners()) {
        return Status::Memory(
            "a previous VMM factory rollback is still pending cleanup");
    }
    if (options.requested_length == 0)
        return Status::InvalidArgument(
            "VMM allocation length must be greater than zero");
    if (options.location_id < 0)
        return Status::InvalidArgument("VMM location id must be non-negative");
    if (!api.mem_create) return missingDriverFunction("cuMemCreate");
    if (!api.mem_address_reserve)
        return missingDriverFunction("cuMemAddressReserve");
    if (!api.mem_map) return missingDriverFunction("cuMemMap");
    if (!api.mem_set_access) return missingDriverFunction("cuMemSetAccess");
    if (!api.mem_unmap) return missingDriverFunction("cuMemUnmap");
    if (!api.mem_address_free) return missingDriverFunction("cuMemAddressFree");
    if (!api.mem_release) return missingDriverFunction("cuMemRelease");
    if (!api.device_get_count) return missingDriverFunction("cuDeviceGetCount");
    if (!api.device_get) return missingDriverFunction("cuDeviceGet");

    if (options.fabric_exportable) {
        Status status = CheckStrictFabricCapabilityWithDriverApi(api);
        if (!status.ok()) return status;
    }

    size_t granularity = 0;
    Status status = GetAllocationGranularityWithDriverApi(
        options.location_type, options.location_id, options.fabric_exportable,
        api, granularity);
    if (!status.ok()) return status;

    size_t va_alignment = granularity;
    if (options.required_va_alignment != 0) {
        if (!isPowerOfTwo(options.required_va_alignment) ||
            options.required_va_alignment < granularity ||
            options.required_va_alignment % granularity != 0) {
            return Status::InvalidArgument(
                "required VA alignment must be a power-of-two multiple of "
                "CUDA allocation granularity");
        }
        va_alignment = options.required_va_alignment;
    }

    const size_t remainder = options.requested_length % va_alignment;
    size_t length = options.requested_length;
    if (remainder != 0) {
        const size_t padding = va_alignment - remainder;
        if (length > std::numeric_limits<size_t>::max() - padding)
            return Status::InvalidArgument(
                "VMM allocation length overflows during alignment");
        length += padding;
    }

    CUmemAllocationProp prop = {};
    status = buildAllocationProp(options, api, prop);
    if (!status.ok()) return status;

    std::unique_ptr<CleanupPendingVmmOwnerNode> cleanup_node;
    std::unique_ptr<NvlinkVmmAllocation> owner;
    try {
        // Preallocate the intrusive quarantine node before acquiring any CUDA
        // resource. Moving this node into the process-lifetime registry cannot
        // fail even under memory pressure during rollback.
        cleanup_node = std::make_unique<CleanupPendingVmmOwnerNode>();
        owner = std::unique_ptr<NvlinkVmmAllocation>(new NvlinkVmmAllocation());
    } catch (const std::exception& error) {
        return Status::Memory(std::string("VMM owner allocation failed: ") +
                              error.what());
    }
    owner->length_ = length;
    owner->granularity_ = granularity;
    owner->va_alignment_ = va_alignment;
    owner->location_type_ = options.location_type;
    owner->location_id_ = options.location_id;
    owner->fabric_exportable_ = options.fabric_exportable;
    owner->driver_api_ = api;

    auto rollback = [&](Status cause) -> Status {
        Status cleanup;
        try {
            cleanup = owner->Release();
        } catch (const std::exception& error) {
            cleanup = Status::Memory(std::string("VMM rollback threw: ") +
                                     error.what());
        } catch (...) {
            cleanup = Status::Memory("VMM rollback threw an unknown exception");
        }
        if (!cleanup.ok()) {
            LOG(ERROR) << "NvlinkVmmAllocation: factory rollback failed; "
                          "quarantining complete CUDA ownership for retry: "
                       << cleanup;
            cleanup_node->owner = std::move(owner);
            quarantineCleanupPendingVmmOwner(std::move(cleanup_node));
        }
        return cause;
    };

    CUmemGenericAllocationHandle handle;
    CUresult result = api.mem_create(&handle, length, &prop, 0);
    if (result != CUDA_SUCCESS) return cudaDriverFailure("cuMemCreate", result);
    owner->allocation_handle_ = static_cast<uint64_t>(handle);
    owner->handle_owned_ = true;

    CUdeviceptr ptr = 0;
    result = api.mem_address_reserve(&ptr, length, va_alignment, 0, 0);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemAddressReserve", result);
        return rollback(status);
    }
    owner->base_ = reinterpret_cast<void*>(ptr);
    owner->address_reserved_ = true;

    result = api.mem_map(ptr, length, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemMap", result);
        return rollback(status);
    }
    owner->mapped_ = true;

    const auto access_start = std::chrono::steady_clock::now();
    auto observe_access = [&](bool success) {
        if (!options.access_observer) return;
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - access_start)
                .count();
        options.access_observer(
            static_cast<uint64_t>(std::max<int64_t>(elapsed, 0)), success);
    };

    auto grant_access = [&](CUmemLocationType location_type,
                            int location_id) -> Status {
        CUmemAccessDesc access = {};
        access.location.type = location_type;
        access.location.id = location_id;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        CUresult access_result = api.mem_set_access(ptr, length, &access, 1);
        if (access_result != CUDA_SUCCESS)
            return cudaDriverFailure("cuMemSetAccess", access_result);
        return Status::OK();
    };

    if (options.location_type == LocationType::HOST_NUMA) {
        status =
            grant_access(CU_MEM_LOCATION_TYPE_HOST_NUMA, options.location_id);
        if (!status.ok()) {
            observe_access(false);
            return rollback(status);
        }
    }

    int device_count = 0;
    result = api.device_get_count(&device_count);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuDeviceGetCount", result);
        observe_access(false);
        return rollback(status);
    }
    if (device_count <= 0) {
        observe_access(false);
        return rollback(Status::NotSupportedTransport(
            "VMM allocation requires at least one visible CUDA device"));
    }

    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
        CUdevice device;
        result = api.device_get(&device, ordinal);
        if (result != CUDA_SUCCESS) {
            status = cudaDriverFailure("cuDeviceGet", result);
            observe_access(false);
            return rollback(status);
        }
        status = grant_access(CU_MEM_LOCATION_TYPE_DEVICE, device);
        if (!status.ok()) {
            observe_access(false);
            return rollback(status);
        }
    }
    observe_access(true);

    result = api.mem_release(handle);
    if (result != CUDA_SUCCESS) {
        status = cudaDriverFailure("cuMemRelease", result);
        return rollback(status);
    }
    owner->handle_owned_ = false;
    owner->allocation_handle_ = 0;
    if (options.location_type == LocationType::HOST_NUMA &&
        options.fabric_exportable) {
        if (!RegisterOwnedRange(owner->base_, owner->length_)) {
            return rollback(Status::Memory(
                "HOST_NUMA VMM owned-range provenance registration failed"));
        }
        owner->owned_range_registered_ = true;
    }
    allocation = std::move(owner);
    return Status::OK();
}

#endif

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
static void submitBatchMemcpy(const std::vector<Slice*>& slices,
                              const std::vector<void*>& srcs,
                              const std::vector<void*>& dsts,
                              const std::vector<size_t>& sizes,
                              cudaStream_t stream) {
    if (slices.empty()) return;

    const size_t count = slices.size();
    cudaError_t err = cudaSuccess;

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
    err = cudaMemcpyBatchAsync(const_cast<const void**>(dsts.data()),
                               const_cast<const void**>(srcs.data()),
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
            slices[i]->local.cuda_stream = (void*)stream;
        }
    }
#elif CUDART_VERSION >= 12080
    err = cudaMemcpyBatchAsync(const_cast<void**>(dsts.data()),
                               const_cast<void**>(srcs.data()),
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
        // Copies [0, fail_idx) were submitted successfully → POSTED.
        // Copy [fail_idx] failed → FAILED.
        // Copies (fail_idx, count) were never submitted → FAILED.
        for (size_t i = 0; i < fail_idx; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void*)stream;
        }
        for (size_t i = fail_idx; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void*)stream;
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
        slices[i]->local.cuda_stream = (void*)stream;
    }
    return;  // Slice states already set above
#endif
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
#ifndef USE_CUDA
    return false;
#else
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

    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        cuDeviceGetAttribute(&device_support_fabric_mem,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             device_id);
        if (!device_support_fabric_mem) {
            return false;
        }
    }
    return true;
#endif
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
#if defined(USE_MNNVL) && defined(USE_CUDA)
    : use_fabric_mem_(supportFabricMem()),
      fabric_driver_api_(NvlinkVmmAllocation::ProductionDriverApi()),
      consumer_metrics_(std::make_unique<ConsumerMetrics>())
#else
    : use_fabric_mem_(supportFabricMem()),
      consumer_metrics_(std::make_unique<ConsumerMetrics>())
#endif
{
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
#if defined(USE_MNNVL) && defined(USE_CUDA)
    size_t cached_fabric_mapping_count = 0;
    for (const auto& entry : remap_entries_) {
        cached_fabric_mapping_count +=
            entry.second.kind == OpenedMappingKind::FABRIC;
    }
    try {
        quarantined_fabric_mappings_.reserve(
            quarantined_fabric_mappings_.size() + cached_fabric_mapping_count);
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: unable to reserve teardown Fabric "
                      "cleanup quarantine: "
                   << error.what();
    }
#endif
    for (auto& entry : remap_entries_) {
        if (entry.second.kind == OpenedMappingKind::FABRIC) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
            FabricMappingCleanup cleanup;
            cleanup.address =
                reinterpret_cast<CUdeviceptr>(entry.second.shm_addr);
            cleanup.length = entry.second.length;
            cleanup.mapped = true;
            cleanup.address_reserved = true;
            ReleaseOrQuarantineFabricMapping(
                cleanup, "releasing cached Fabric mapping during teardown");
#endif
        } else {
            cudaError_t result = cudaIpcCloseMemHandle(entry.second.shm_addr);
            if (result != cudaSuccess)
                LOG(ERROR) << "NvlinkTransport: cudaIpcCloseMemHandle failed "
                              "during teardown: "
                           << cudaGetErrorString(result);
        }
    }
    remap_entries_.clear();

#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (!RetryQuarantinedFabricMappings()) {
        const size_t pending_count = quarantined_fabric_mappings_.size();
        for (const auto& cleanup : quarantined_fabric_mappings_) {
            PreserveProcessLifetimeFabricCleanup(cleanup);
        }
        LOG(ERROR) << "NvlinkTransport: " << pending_count
                   << " Fabric mapping cleanup(s) remain live after teardown "
                      "retries; preserving ownership in the process-lifetime "
                      "quarantine";
        quarantined_fabric_mappings_.clear();
    }

    std::lock_guard<std::mutex> lock(register_mutex_);
    size_t retained_registration_count = 0;
    for (const auto& [_, registration] : local_registrations_) {
        retained_registration_count += registration.retained_handle_owned;
    }
    try {
        quarantined_retained_handles_.reserve(
            quarantined_retained_handles_.size() + retained_registration_count);
    } catch (const std::exception& error) {
        // ReleaseOrQuarantineRetainedHandle() falls back to the process-wide
        // quarantine if this transport-local vector cannot grow.
        LOG(ERROR) << "NvlinkTransport: unable to reserve teardown "
                      "retained-handle quarantine: "
                   << error.what();
    }
    for (auto& [_, registration] : local_registrations_) {
        if (registration.retained_handle_owned) {
            ReleaseOrQuarantineRetainedHandle(
                static_cast<CUmemGenericAllocationHandle>(
                    registration.retained_handle),
                "transport teardown");
        }
    }
    if (!RetryQuarantinedRetainedHandles()) {
        const size_t pending_count = quarantined_retained_handles_.size();
        for (uint64_t handle : quarantined_retained_handles_) {
            quarantineRetainedHandleForProcessLifetime(handle);
        }
        LOG(ERROR) << "NvlinkTransport: " << pending_count
                   << " retained registration handle(s) remain live after "
                      "teardown retries; preserving ownership in the "
                      "process-lifetime quarantine";
        quarantined_retained_handles_.clear();
    }
#endif
    local_registrations_.clear();
}

#if defined(USE_MNNVL) && defined(USE_CUDA)
bool NvlinkTransport::RetryQuarantinedRetainedHandles() {
    auto retained = quarantined_retained_handles_.begin();
    for (auto it = quarantined_retained_handles_.begin();
         it != quarantined_retained_handles_.end(); ++it) {
        const CUresult result = fabric_driver_api_.mem_release(
            static_cast<CUmemGenericAllocationHandle>(*it));
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: quarantined registration "
                          "cuMemRelease retry failed: "
                       << result;
            *retained++ = *it;
        }
    }
    quarantined_retained_handles_.erase(retained,
                                        quarantined_retained_handles_.end());
    return quarantined_retained_handles_.empty();
}

void NvlinkTransport::ReleaseOrQuarantineRetainedHandle(
    CUmemGenericAllocationHandle handle, const char* failure_stage) noexcept {
    const CUresult result = fabric_driver_api_.mem_release(handle);
    if (result == CUDA_SUCCESS) return;

    // registerLocalMemory() reserves one quarantine slot before retaining a
    // handle. Teardown also reserves for every live registration; if an
    // allocation still fails, retain the marker in a process-lifetime owner.
    try {
        quarantined_retained_handles_.push_back(static_cast<uint64_t>(handle));
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: local retained-handle quarantine "
                      "allocation failed: "
                   << error.what();
        quarantineRetainedHandleForProcessLifetime(
            static_cast<uint64_t>(handle));
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: unknown failure while quarantining "
                      "a retained registration handle";
        quarantineRetainedHandleForProcessLifetime(
            static_cast<uint64_t>(handle));
    }
    LOG(ERROR) << "NvlinkTransport: registration cleanup cuMemRelease failed "
                  "after "
               << failure_stage << ": " << result
               << "; retaining ownership for a later retry";
}
#endif

int NvlinkTransport::install(std::string& local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status NvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    const size_t first_task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(first_task_id + entries.size());
    std::vector<TransferTask*> tasks;
    tasks.reserve(entries.size());
    for (size_t index = 0; index < entries.size(); ++index) {
        auto& task = batch_desc.task_list[first_task_id + index];
        task.batch_id = batch_id;
        task.transport_ = this;
#ifdef USE_EVENT_DRIVEN_COMPLETION
        task.requires_periodic_status_polling = true;
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<TransferRequest*>(&entries[index]);
#else
        task.request = &entries[index];
#endif
        task.operation = entries[index].opcode;
        task.operation_initialized = true;
        task.total_bytes = entries[index].length;
        tasks.push_back(&task);
    }

    auto fail_submission = [&](Status status, bool copy_failure) {
        for (auto* task : tasks) {
            finalizeSubmissionFailure(*task, copy_failure);
        }
        return status;
    };

    // Resolve every remote range before allocating any Slice. A relocation
    // failure therefore has no partially staged Slice ownership to unwind.
    std::vector<uint64_t> resolved_addresses;
    resolved_addresses.reserve(entries.size());
    for (const auto& request : entries) {
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc != 0) {
                return fail_submission(
                    Status::Memory("NVLink remote address relocation failed"),
                    false);
            }
        }
        resolved_addresses.push_back(dest_addr);
    }

    // Get per-device transfer stream for the source buffer's device.
    CudaStreamEntry stream_entry =
        getStreamForRequest(entries.empty() ? nullptr : entries[0].source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return fail_submission(
            Status::Context("Failed to create NVLink CUDA stream"), true);

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
        return fail_submission(
            Status::Context("cudaEventRecord failed: " +
                            std::string(cudaGetErrorString(sync_err))),
            true);
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventSynchronize failed: " +
                            std::string(cudaGetErrorString(sync_err))),
            true);
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void*> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice*> slices;

    for (size_t index = 0; index < entries.size(); ++index) {
        const auto& request = entries[index];
        TransferTask& task = *tasks[index];
        const uint64_t dest_addr = resolved_addresses[index];
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->local.dest_addr = (char*)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void* src = (request.opcode == TransferRequest::READ)
                        ? (void*)slice->local.dest_addr
                        : (void*)slice->source_addr;
        void* dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void*)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);
    for (auto* task : tasks) {
        const uint64_t completed =
            task->success_slice_count + task->failed_slice_count;
        if (task->slice_count > 0 && completed == task->slice_count) {
            const bool success = task->failed_slice_count == 0;
            finalizeTransferResult(*task, success);
        }
    }

    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    if (__atomic_load_n(&task.submission_failed, __ATOMIC_ACQUIRE)) {
        status.transferred_bytes = task.transferred_bytes;
        status.s = TransferStatusEnum::FAILED;
        return Status::OK();
    }
    // Poll POSTED slices for async completion via cudaStreamQuery.
    // Cache the query result per stream to avoid redundant driver calls.
    // With SGLang-side torch.cuda.device(gpu_id), the calling thread's
    // active device matches the stream's device, so no device switching
    // is needed.
    std::unordered_map<cudaStream_t, cudaError_t> stream_status_cache;
    for (auto* slice : task.slice_list) {
        if (slice && slice->status == Slice::POSTED) {
            cudaStream_t stream = (cudaStream_t)slice->local.cuda_stream;
            auto it = stream_status_cache.find(stream);
            cudaError_t cuda_err;
            if (it == stream_status_cache.end()) {
                cuda_err = cudaStreamQuery(stream);
                stream_status_cache[stream] = cuda_err;
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
        const bool success = failed_slice_count == 0;
        finalizeTransferResult(task, success);
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
    const std::vector<TransferTask*>& task_list) {
    auto fail_submission = [&](Status status, bool copy_failure) {
        for (auto* task : task_list) {
            if (task == nullptr) continue;
            finalizeSubmissionFailure(*task, copy_failure);
        }
        return status;
    };

    std::vector<uint64_t> resolved_addresses;
    resolved_addresses.reserve(task_list.size());
    for (auto* task : task_list) {
        if (task == nullptr || task->request == nullptr) {
            return fail_submission(
                Status::InvalidArgument("NVLink transfer task is incomplete"),
                false);
        }
#ifdef USE_EVENT_DRIVEN_COMPLETION
        task->requires_periodic_status_polling = true;
#endif
        auto& request = *task->request;
        task->operation = request.opcode;
        task->operation_initialized = true;
        task->total_bytes = request.length;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc != 0) {
                return fail_submission(
                    Status::Memory("NVLink remote address relocation failed"),
                    false);
            }
        }
        resolved_addresses.push_back(dest_addr);
    }

    // Get per-device transfer stream. See submitTransfer() for rationale.
    CudaStreamEntry stream_entry = getStreamForRequest(
        task_list.empty() ? nullptr : task_list[0]->request->source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return fail_submission(
            Status::Context("Failed to create NVLink CUDA stream"), true);
    // Synchronize with caller's GPU work via cudaEventSynchronize.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventRecord failed: " +
                            std::string(cudaGetErrorString(sync_err))),
            true);
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventSynchronize failed: " +
                            std::string(cudaGetErrorString(sync_err))),
            true);
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void*> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice*> slices;

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        const uint64_t dest_addr = resolved_addresses[index];
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->local.dest_addr = (char*)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void* src = (request.opcode == TransferRequest::READ)
                        ? (void*)slice->local.dest_addr
                        : (void*)slice->source_addr;
        void* dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void*)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);
    for (auto* task : task_list) {
        const uint64_t completed =
            task->success_slice_count + task->failed_slice_count;
        if (task->slice_count > 0 && completed == task->slice_count) {
            const bool success = task->failed_slice_count == 0;
            finalizeTransferResult(*task, success);
        }
    }

    return Status::OK();
}

int NvlinkTransport::registerLocalMemory(void* addr, size_t length,
                                         const std::string& location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    if (addr == nullptr || length == 0 ||
        reinterpret_cast<uintptr_t>(addr) >
            std::numeric_limits<uintptr_t>::max() - length) {
        LOG(ERROR) << "NvlinkTransport: invalid registration range addr="
                   << addr << " length=" << length;
        return ERR_INVALID_ARGUMENT;
    }
    if (local_registrations_.count(addr) != 0) {
        LOG(ERROR) << "NvlinkTransport: address is already registered: "
                   << addr;
        return ERR_ADDRESS_OVERLAPPED;
    }

    LocalRegistration registration;
    registration.requested_addr = addr;
    registration.requested_length = length;
    registration.mapped_base = addr;
    registration.mapped_length = length;
    registration.remote_accessible = remote_accessible;

    // Local-only registrations deliberately accept ordinary HBM, VMM, and
    // legacy CPU memory. They are execution ownership records only and never
    // mutate remotely visible metadata.
    if (!remote_accessible) {
        local_registrations_.emplace(addr, registration);
        return 0;
    }

    BufferDesc desc;
    desc.name = location;

    if (!use_fabric_mem_) {
        cudaPointerAttributes attr;
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed: "
                       << cudaGetErrorString(err);
            return ERR_INVALID_ARGUMENT;
        }

        if (attr.type != cudaMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return ERR_INVALID_ARGUMENT;
        }

        cudaIpcMemHandle_t handle;
        err = cudaIpcGetMemHandle(&handle, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcGetMemHandle failed: "
                       << cudaGetErrorString(err);
            return ERR_MEMORY;
        }

        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.shm_name =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        registration.published = false;
        local_registrations_.emplace(addr, registration);
        return publishLocalRegistration(addr, desc, update_metadata);
    } else {
#if defined(USE_MNNVL) && defined(USE_CUDA)
        if (!fabric_driver_api_.mem_retain_allocation_handle ||
            !fabric_driver_api_.mem_get_allocation_properties_from_handle ||
            !fabric_driver_api_.mem_export_to_shareable_handle ||
            !fabric_driver_api_.mem_release) {
            LOG(ERROR) << "NvlinkTransport: incomplete Fabric driver adapter "
                          "for registration";
            return ERR_CONTEXT;
        }
        if (!RetryQuarantinedRetainedHandles()) {
            LOG(ERROR)
                << "NvlinkTransport: a retained registration handle "
                   "is still pending cleanup; refusing to retain another";
            return ERR_MEMORY;
        }
        try {
            quarantined_retained_handles_.reserve(
                quarantined_retained_handles_.size() + 1);
        } catch (const std::exception& error) {
            LOG(ERROR) << "NvlinkTransport: failed to reserve retained-handle "
                          "cleanup ownership: "
                       << error.what();
            return ERR_MEMORY;
        }
        Status capability =
            NvlinkVmmAllocation::CheckStrictFabricCapabilityWithDriverApi(
                fabric_driver_api_);
        if (!capability.ok()) {
            LOG(ERROR) << "NvlinkTransport: Fabric registration preflight "
                          "failed: "
                       << capability.ToString();
            return ERR_CONTEXT;
        }

        CUmemGenericAllocationHandle handle;
        auto result =
            fabric_driver_api_.mem_retain_allocation_handle(&handle, addr);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: remote Fabric registration "
                          "requires cuMemCreate memory; retain failed: "
                       << result;
            return ERR_INVALID_ARGUMENT;
        }
        registration.retained_handle_owned = true;
        registration.retained_handle = static_cast<uint64_t>(handle);
        RetainedHandleGuard retained_handle(*this, handle,
                                            "Fabric registration failure");

        CUmemAllocationProp allocation_prop = {};
        result = fabric_driver_api_.mem_get_allocation_properties_from_handle(
            &allocation_prop, handle);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: "
                          "cuMemGetAllocationPropertiesFromHandle failed: "
                       << result;
            return ERR_MEMORY;
        }
        const bool is_host_numa =
            allocation_prop.location.type == CU_MEM_LOCATION_TYPE_HOST_NUMA;
        const bool is_exact_owned_range =
            NvlinkVmmAllocation::IsExactOwnedRange(addr, length);

        CUdeviceptr real_address = 0;
        size_t real_size = 0;
        if (is_exact_owned_range) {
            if (!is_host_numa) {
                LOG(ERROR)
                    << "NvlinkTransport: Mooncake-owned HOST_NUMA provenance "
                       "does not match retained allocation properties";
                return ERR_INVALID_ARGUMENT;
            }

            // NvlinkVmmAllocation created and owns this exact base/length
            // mapping. cuMemRetainAllocationHandle above independently
            // verified that addr is still backed by a cuMemMap allocation, so
            // querying the range again would add only a current-context
            // dependency and no new range information.
            real_address = reinterpret_cast<CUdeviceptr>(addr);
            real_size = length;
        } else {
            if (!fabric_driver_api_.mem_get_address_range) {
                LOG(ERROR) << "NvlinkTransport: incomplete Fabric driver "
                              "adapter for external range registration";
                return ERR_CONTEXT;
            }
            result = fabric_driver_api_.mem_get_address_range(
                &real_address, &real_size, reinterpret_cast<CUdeviceptr>(addr));
            if (result != CUDA_SUCCESS) {
                LOG(ERROR) << "NvlinkTransport: cuMemGetAddressRange failed: "
                           << result;
                return ERR_MEMORY;
            }
            if (real_address == 0) {
                LOG(ERROR) << "NvlinkTransport: cuMemGetAddressRange returned "
                              "a null allocation base";
                return ERR_MEMORY;
            }
            if (is_host_numa &&
                NvlinkVmmAllocation::IsExactOwnedRange(
                    reinterpret_cast<void*>(real_address), real_size)) {
                LOG(ERROR) << "NvlinkTransport: Mooncake-owned HOST_NUMA VMM "
                              "registration requires its exact base and length";
                return ERR_INVALID_ARGUMENT;
            }
        }
        const uint64_t requested = reinterpret_cast<uint64_t>(addr);
        const uint64_t real = static_cast<uint64_t>(real_address);
        if (real_size == 0 || requested < real || length > real_size ||
            requested - real > real_size - length) {
            LOG(ERROR) << "NvlinkTransport: requested range is outside the "
                          "retained VMM mapping";
            return ERR_INVALID_ARGUMENT;
        }

        CUmemFabricHandle export_handle;
        result = fabric_driver_api_.mem_export_to_shareable_handle(
            &export_handle, handle, CU_MEM_HANDLE_TYPE_FABRIC, 0);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemExportToShareableHandle failed: "
                << result;
            return ERR_MEMORY;
        }

        desc.addr = real;
        desc.length = real_size;
        desc.shm_name =
            serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
        registration.mapped_base = reinterpret_cast<void*>(real_address);
        registration.mapped_length = real_size;
        local_registrations_.emplace(addr, registration);
        int rc = publishLocalRegistration(addr, desc, update_metadata);
        if (local_registrations_.count(addr) != 0) {
            retained_handle.TransferOwnership();
        }
        return rc;
#else
        LOG(ERROR) << "NvlinkTransport: Fabric registration requires CUDA";
        return ERR_CONTEXT;
#endif
    }
}

int NvlinkTransport::publishLocalRegistration(void* registration_addr,
                                              const BufferDesc& descriptor,
                                              bool update_metadata) {
    int rc = ERR_METADATA;
    bool publication_threw = false;
    try {
        rc = add_buffer_for_testing_
                 ? add_buffer_for_testing_(descriptor, update_metadata)
                 : metadata_->addLocalMemoryBuffer(descriptor, update_metadata);
    } catch (const std::exception& error) {
        // A storage backend can throw after committing the descriptor. Treat
        // the remote state as unknown and keep the tentative registration
        // fail-closed until an explicit unregister confirms deletion.
        LOG(ERROR) << "NvlinkTransport: metadata registration threw: "
                   << error.what();
        publication_threw = true;
        rc = ERR_MEMORY;
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: metadata registration threw an "
                      "unknown exception";
        publication_threw = true;
        rc = ERR_MEMORY;
    }
    if (publication_threw) {
        // Fabric's caller observes that the record remains present and moves
        // RetainedHandleGuard ownership into it. IPC follows the same
        // conservative metadata lifetime even though it has no CUDA handle.
        local_registrations_[registration_addr].published = true;
        return rc;
    }
    if (rc != 0) {
        int rollback_rc = ERR_METADATA;
        try {
            rollback_rc = remove_buffer_for_testing_
                              ? remove_buffer_for_testing_(
                                    reinterpret_cast<void*>(descriptor.addr),
                                    update_metadata)
                              : metadata_->removeLocalMemoryBuffer(
                                    reinterpret_cast<void*>(descriptor.addr),
                                    update_metadata);
        } catch (const std::exception& error) {
            LOG(ERROR) << "NvlinkTransport: metadata registration rollback "
                          "threw: "
                       << error.what();
            rollback_rc = ERR_METADATA;
        } catch (...) {
            LOG(ERROR) << "NvlinkTransport: metadata registration rollback "
                          "threw an unknown exception";
            rollback_rc = ERR_METADATA;
        }
        if (rollback_rc != 0 && rollback_rc != ERR_ADDRESS_NOT_REGISTERED) {
            LOG(ERROR) << "NvlinkTransport: metadata registration rollback "
                          "failed: "
                       << rollback_rc;
            // IPC and Fabric descriptors can both still be visible remotely.
            // Keep the record so the caller can retry unregistering the same
            // base; Fabric's caller also transfers the retained handle into it.
            local_registrations_[registration_addr].published = true;
            return rc;
        }
        local_registrations_.erase(registration_addr);
        return rc;
    }
    local_registrations_[registration_addr].published = true;
    return 0;
}

int NvlinkTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    auto it = local_registrations_.find(addr);
    if (it == local_registrations_.end()) {
        LOG(WARNING) << "NvlinkTransport: unbalanced unregister for " << addr;
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    LocalRegistration& registration = it->second;
    if (registration.published) {
        int rc = ERR_METADATA;
        try {
            rc = remove_buffer_for_testing_
                     ? remove_buffer_for_testing_(registration.mapped_base,
                                                  update_metadata)
                     : metadata_->removeLocalMemoryBuffer(
                           registration.mapped_base, update_metadata);
        } catch (const std::exception& error) {
            LOG(ERROR) << "NvlinkTransport: metadata unregistration threw; "
                          "retaining descriptor and handle ownership: "
                       << error.what();
            return ERR_MEMORY;
        } catch (...) {
            LOG(ERROR) << "NvlinkTransport: metadata unregistration threw an "
                          "unknown exception; retaining descriptor and handle "
                          "ownership";
            return ERR_MEMORY;
        }
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) return rc;
        if (rc == ERR_ADDRESS_NOT_REGISTERED) {
            LOG(WARNING) << "NvlinkTransport: published descriptor was "
                            "already absent for "
                         << registration.mapped_base;
        }
        registration.published = false;
    }

#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (registration.retained_handle_owned) {
        CUresult result = fabric_driver_api_.mem_release(
            static_cast<CUmemGenericAllocationHandle>(
                registration.retained_handle));
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: registration cuMemRelease failed: "
                       << result;
            return ERR_MEMORY;
        }
        registration.retained_handle_owned = false;
    }
#endif
    local_registrations_.erase(it);
    return 0;
}

int NvlinkTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    auto desc = get_segment_for_testing_
                    ? get_segment_for_testing_(target_id)
                    : metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        observeConsumerFailure(ConsumerFailureStage::IMPORT);
        LOG(ERROR) << "NvlinkTransport: target segment descriptor not found: "
                   << target_id;
        return ERR_METADATA;
    }
    if (length == 0) {
        observeConsumerFailure(ConsumerFailureStage::IMPORT);
        LOG(ERROR) << "NvlinkTransport: cannot relocate an empty range";
        return ERR_INVALID_ARGUMENT;
    }

    for (auto& entry : desc->buffers) {
        const bool range_matches =
            !entry.shm_name.empty() && entry.addr <= dest_addr &&
            length <= entry.length &&
            dest_addr - entry.addr <= entry.length - length;
        if (range_matches) {
            const auto cache_key = std::make_pair(target_id, entry.addr);
            remap_lock_.lockShared();
            auto cached = remap_entries_.find(cache_key);
            if (cached != remap_entries_.end()) {
                void* shm_addr = cached->second.shm_addr;
                remap_lock_.unlockShared();
                observeCacheLookup(true);
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            cached = remap_entries_.find(cache_key);
            if (cached == remap_entries_.end()) {
                observeCacheLookup(false);
                ScopedLatencyObservation observe_import_latency(
                    [this](uint64_t duration_us) {
                        observeLazyImportLatency(duration_us);
                    });
                std::vector<unsigned char> output_buffer;
                try {
                    deserializeBinaryData(entry.shm_name, output_buffer);
                } catch (const std::exception& error) {
                    observeConsumerFailure(ConsumerFailureStage::IMPORT);
                    LOG(ERROR) << "NvlinkTransport: invalid serialized remote "
                                  "handle: "
                               << error.what();
                    return ERR_INVALID_ARGUMENT;
                }

                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void* shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR)
                            << "NvlinkTransport: cudaIpcOpenMemHandle failed: "
                            << cudaGetErrorString(err);
                        return ERR_MEMORY;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    shm_entry.kind = OpenedMappingKind::IPC;
                    auto inserted =
                        remap_entries_.emplace(cache_key, shm_entry).second;
                    if (!inserted) {
                        cudaIpcCloseMemHandle(shm_addr);
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        return ERR_ADDRESS_OVERLAPPED;
                    }
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
                    if (entry.length == 0 ||
                        entry.length > std::numeric_limits<size_t>::max()) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR) << "NvlinkTransport: invalid Fabric mapping "
                                      "length "
                                   << entry.length;
                        return ERR_INVALID_ARGUMENT;
                    }
                    CUmemFabricHandle export_handle;
                    memcpy(&export_handle, output_buffer.data(),
                           sizeof(export_handle));
                    if (!fabric_driver_api_.mem_import_from_shareable_handle ||
                        !fabric_driver_api_.mem_address_reserve ||
                        !fabric_driver_api_.mem_map ||
                        !fabric_driver_api_.mem_set_access ||
                        !fabric_driver_api_.mem_unmap ||
                        !fabric_driver_api_.mem_address_free ||
                        !fabric_driver_api_.mem_release ||
                        !fabric_driver_api_.device_get_count ||
                        !fabric_driver_api_.device_get) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR) << "NvlinkTransport: incomplete Fabric "
                                      "driver adapter for lazy import";
                        return ERR_CONTEXT;
                    }
                    if (!RetryQuarantinedFabricMappings()) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR)
                            << "NvlinkTransport: a prior lazy Fabric import "
                               "is still pending staged cleanup; refusing to "
                               "import another handle";
                        return ERR_MEMORY;
                    }
                    try {
                        quarantined_fabric_mappings_.reserve(
                            quarantined_fabric_mappings_.size() + 1);
                    } catch (const std::exception& error) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR) << "NvlinkTransport: failed to reserve "
                                      "lazy Fabric cleanup ownership: "
                                   << error.what();
                        return ERR_MEMORY;
                    }
                    FabricMappingAttempt attempt(*this);
                    auto result =
                        fabric_driver_api_.mem_import_from_shareable_handle(
                            attempt.handleOut(), &export_handle,
                            CU_MEM_HANDLE_TYPE_FABRIC);
                    if (result != CUDA_SUCCESS) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR) << "NvlinkTransport: "
                                      "cuMemImportFromShareableHandle failed: "
                                   << result;
                        return ERR_MEMORY;
                    }
                    attempt.markHandleOwned();

                    result = fabric_driver_api_.mem_address_reserve(
                        attempt.addressOut(), static_cast<size_t>(entry.length),
                        0, 0, 0);
                    if (result != CUDA_SUCCESS) {
                        observeConsumerFailure(ConsumerFailureStage::RESERVE);
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemAddressReserve failed: "
                            << result;
                        return ERR_MEMORY;
                    }
                    attempt.markAddressReserved(
                        static_cast<size_t>(entry.length));

                    result = fabric_driver_api_.mem_map(attempt.address(),
                                                        entry.length, 0,
                                                        attempt.handle(), 0);
                    if (result != CUDA_SUCCESS) {
                        observeConsumerFailure(ConsumerFailureStage::MAP);
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return ERR_MEMORY;
                    }
                    attempt.markMapped();

                    int device_count = 0;
                    result = fabric_driver_api_.device_get_count(&device_count);
                    if (result != CUDA_SUCCESS || device_count <= 0) {
                        observeConsumerFailure(
                            ConsumerFailureStage::SET_ACCESS);
                        LOG(ERROR) << "NvlinkTransport: cuDeviceGetCount "
                                      "failed during lazy import: "
                                   << result;
                        return ERR_CONTEXT;
                    }

                    for (int ordinal = 0; ordinal < device_count; ++ordinal) {
                        CUdevice device;
                        result =
                            fabric_driver_api_.device_get(&device, ordinal);
                        if (result != CUDA_SUCCESS) {
                            observeConsumerFailure(
                                ConsumerFailureStage::SET_ACCESS);
                            LOG(ERROR) << "NvlinkTransport: cuDeviceGet failed "
                                          "during lazy import: "
                                       << result;
                            return ERR_CONTEXT;
                        }
                        CUmemAccessDesc access = {};
                        access.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
                        access.location.id = device;
                        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                        result = fabric_driver_api_.mem_set_access(
                            attempt.address(), entry.length, &access, 1);
                        if (result != CUDA_SUCCESS) {
                            observeConsumerFailure(
                                ConsumerFailureStage::SET_ACCESS);
                            LOG(ERROR)
                                << "NvlinkTransport: cuMemSetAccess failed for "
                                   "visible device "
                                << ordinal << ": " << result;
                            return ERR_MEMORY;
                        }
                    }

                    result = attempt.releaseHandle();
                    if (result != CUDA_SUCCESS) {
                        observeConsumerFailure(ConsumerFailureStage::IMPORT);
                        LOG(ERROR) << "NvlinkTransport: post-map cuMemRelease "
                                      "failed: "
                                   << result;
                        return ERR_MEMORY;
                    }

                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr =
                        reinterpret_cast<void*>(attempt.address());
                    shm_entry.length = entry.length;
                    shm_entry.kind = OpenedMappingKind::FABRIC;
                    auto inserted =
                        remap_entries_.emplace(cache_key, shm_entry).second;
                    if (!inserted) {
                        observeConsumerFailure(ConsumerFailureStage::MAP);
                        return ERR_ADDRESS_OVERLAPPED;
                    }
                    attempt.transferMappingOwnership();
#else
                    observeConsumerFailure(ConsumerFailureStage::IMPORT);
                    LOG(ERROR) << "NvlinkTransport: Fabric mapping requires "
                                  "CUDA/MNNVL support";
                    return ERR_CONTEXT;
#endif
                } else {
                    observeConsumerFailure(ConsumerFailureStage::IMPORT);
                    LOG(ERROR) << "NvlinkTransport: serialized handle size "
                                  "does not match active IPC/Fabric mode";
                    return ERR_INVALID_ARGUMENT;
                }
                cached = remap_entries_.find(cache_key);
            } else {
                observeCacheLookup(true);
            }
            if (cached == remap_entries_.end()) {
                observeConsumerFailure(ConsumerFailureStage::IMPORT);
                return ERR_MEMORY;
            }
            auto shm_addr = cached->second.shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
    }
    observeConsumerFailure(ConsumerFailureStage::IMPORT);
    LOG(ERROR) << "Requested address " << (void*)dest_addr << " to "
               << (void*)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    std::vector<void*> registered;
    registered.reserve(buffer_list.size());
    for (auto& buffer : buffer_list) {
        int rc = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                     false);
        if (rc != 0) {
            for (auto it = registered.rbegin(); it != registered.rend(); ++it)
                unregisterLocalMemory(*it, false);
            return rc;
        }
        registered.push_back(buffer.addr);
    }

    int rc = metadata_->updateLocalSegmentDesc();
    if (rc == 0) return 0;

    for (auto it = registered.rbegin(); it != registered.rend(); ++it)
        unregisterLocalMemory(*it, false);
    int rollback_rc = metadata_->updateLocalSegmentDesc();
    if (rollback_rc != 0) {
        LOG(ERROR) << "NvlinkTransport: batch registration metadata rollback "
                      "failed: "
                   << rollback_rc;
    }
    return rc;
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto& addr : addr_list) {
        int rc = unregisterLocalMemory(addr, false);
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) return rc;
    }
    return metadata_->updateLocalSegmentDesc();
}

#if defined(USE_MNNVL) && defined(USE_CUDA)
bool NvlinkTransport::TrackPinnedVmmAllocation(
    std::unique_ptr<NvlinkVmmAllocation> owner) {
    if (!owner || owner->base() == nullptr) return false;
    void* const ptr = owner->base();
    auto& registry = vmmAllocationOwnerRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    return registry.owners.emplace(ptr, std::move(owner)).second;
}

bool NvlinkTransport::ReleasePinnedVmmAllocation(void* ptr) {
    auto& registry = vmmAllocationOwnerRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.owners.find(ptr);
    if (it == registry.owners.end()) return false;

    try {
        Status status = it->second->Release();
        if (!status.ok()) {
            LOG(ERROR) << "NvlinkTransport: pinned VMM release failed; "
                          "retaining owner for repeated free: "
                       << status;
            return true;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: pinned VMM release threw; retaining "
                      "owner for repeated free: "
                   << error.what();
        return true;
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: pinned VMM release threw an unknown "
                      "exception; retaining owner for repeated free";
        return true;
    }

    registry.owners.erase(it);
    return true;
}
#endif

void* NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (!supportFabricMem()) {
        void* ptr = nullptr;
        cudaMalloc(&ptr, size);
        return ptr;
    }

    int cudaDev;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }

    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::DEVICE;
    options.location_id = cudaDev;
    options.requested_length = size;
    options.fabric_exportable = true;

    std::unique_ptr<NvlinkVmmAllocation> owner;
    Status status = NvlinkVmmAllocation::Create(options, owner);
    if (!status.ok()) {
        LOG(ERROR) << "NvlinkTransport: DEVICE Fabric VMM allocation failed: "
                   << status.ToString();
        return nullptr;
    }

    void* ptr = owner->base();
    if (!TrackPinnedVmmAllocation(std::move(owner))) {
        LOG(ERROR) << "NvlinkTransport: duplicate VMM allocation address "
                   << ptr;
        return nullptr;
    }
    return ptr;
#else
    void* ptr = nullptr;
    cudaMalloc(&ptr, size);
    return ptr;
#endif
}

void NvlinkTransport::freePinnedLocalMemory(void* ptr) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
    if (ptr == nullptr) return;

    if (ReleasePinnedVmmAllocation(ptr)) return;

    // Preserve compatibility with imported Fabric mappings until their
    // ownership moves to the registration/import RAII objects. Such mappings
    // are not present in the legacy allocation-owner map.
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        cudaGetLastError();
        cudaFree(ptr);
        return;
    }
    CUdeviceptr base = 0;
    result =
        cuMemGetAddressRange(&base, &size, reinterpret_cast<CUdeviceptr>(ptr));
    if (result == CUDA_SUCCESS) {
        cuMemUnmap(base, size);
        cuMemAddressFree(base, size);
    } else {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAddressRange failed: "
                   << result;
    }
    cuMemRelease(handle);
#else
    cudaFree(ptr);
#endif
}
}  // namespace mooncake
