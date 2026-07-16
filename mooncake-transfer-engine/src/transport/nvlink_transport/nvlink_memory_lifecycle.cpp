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

#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_metadata.h"

namespace mooncake {

#if defined(USE_MNNVL) && defined(USE_CUDA)
namespace {

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
            transport_.releaseOrQuarantineFabricMapping(
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
            transport_.releaseOrQuarantineRetainedHandle(handle_,
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

bool NvlinkTransport::cleanupFabricMapping(FabricMappingCleanup& cleanup,
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

bool NvlinkTransport::retryQuarantinedFabricMappings() noexcept {
    auto retained = quarantined_fabric_mappings_.begin();
    for (auto it = quarantined_fabric_mappings_.begin();
         it != quarantined_fabric_mappings_.end(); ++it) {
        if (!cleanupFabricMapping(*it, "retrying lazy Fabric cleanup")) {
            if (retained != it) *retained = *it;
            ++retained;
        }
    }
    quarantined_fabric_mappings_.erase(retained,
                                       quarantined_fabric_mappings_.end());
    return quarantined_fabric_mappings_.empty();
}

void NvlinkTransport::preserveProcessLifetimeFabricCleanup(
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

void NvlinkTransport::releaseOrQuarantineFabricMapping(
    FabricMappingCleanup cleanup, const char* failure_stage) noexcept {
    if (cleanupFabricMapping(cleanup, failure_stage)) return;

    try {
        // relocateSharedMemoryAddress() reserves this slot before importing a
        // handle. The catch remains as a destructor-safe last line of defense.
        quarantined_fabric_mappings_.push_back(cleanup);
        LOG(ERROR) << "NvlinkTransport: retaining failed Fabric cleanup for "
                      "a later retry";
    } catch (...) {
        preserveProcessLifetimeFabricCleanup(cleanup);
        LOG(ERROR) << "NvlinkTransport: preserving failed Fabric cleanup in "
                      "the process-lifetime quarantine";
    }
}
#endif

bool NvlinkTransport::supportsFabricMemory() {
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

NvlinkTransport::NvlinkTransport()
#if defined(USE_MNNVL) && defined(USE_CUDA)
    : use_fabric_mem_(supportsFabricMemory()),
      fabric_driver_api_(NvlinkVmmAllocation::ProductionDriverApi())
#else
    : use_fabric_mem_(supportsFabricMemory())
#endif
{
}

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
            releaseOrQuarantineFabricMapping(
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
    if (!retryQuarantinedFabricMappings()) {
        const size_t pending_count = quarantined_fabric_mappings_.size();
        for (const auto& cleanup : quarantined_fabric_mappings_) {
            preserveProcessLifetimeFabricCleanup(cleanup);
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
        // releaseOrQuarantineRetainedHandle() falls back to the process-wide
        // quarantine if this transport-local vector cannot grow.
        LOG(ERROR) << "NvlinkTransport: unable to reserve teardown "
                      "retained-handle quarantine: "
                   << error.what();
    }
    for (auto& [_, registration] : local_registrations_) {
        if (!registration.retained_handle_owned) continue;
        if (registration.published) {
            // A remotely visible (or ambiguously published) Fabric descriptor
            // must never outlive its CUDA handle. Keep the handle until process
            // exit instead of making a stale descriptor dereferenceable.
            quarantineRetainedHandleForProcessLifetime(
                registration.retained_handle);
            registration.retained_handle_owned = false;
            LOG(ERROR) << "NvlinkTransport: preserving a published Fabric "
                          "registration handle until process exit";
            continue;
        }
        releaseOrQuarantineRetainedHandle(
            static_cast<CUmemGenericAllocationHandle>(
                registration.retained_handle),
            "transport teardown");
    }
    if (!retryQuarantinedRetainedHandles()) {
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
bool NvlinkTransport::retryQuarantinedRetainedHandles() {
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

void NvlinkTransport::releaseOrQuarantineRetainedHandle(
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
        registration.descriptor = desc;
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
        if (!retryQuarantinedRetainedHandles()) {
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
        registration.descriptor = desc;
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
    try {
        rc = metadata_->addLocalMemoryBuffer(descriptor, update_metadata);
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: metadata registration threw: "
                   << error.what();
        rc = ERR_MEMORY;
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: metadata registration threw an "
                      "unknown exception";
        rc = ERR_MEMORY;
    }
    if (rc == 0) {
        local_registrations_[registration_addr].published = true;
        return 0;
    }

    // updateSegmentDesc() can fail after a backend has committed the new
    // descriptor. Re-stage the descriptor locally and publish its removal as
    // an explicit compensation. If compensation also fails, retain the exact
    // registration and CUDA handle; unregister retries by staging the same
    // descriptor again before removing it.
    int stage_rc = ensureLocalDescriptorPresent(descriptor);
    if (stage_rc != 0) {
        LOG(ERROR) << "NvlinkTransport: unable to stage metadata rollback: "
                   << stage_rc;
        local_registrations_[registration_addr].published = true;
        return rc;
    }

    int rollback_rc = ERR_METADATA;
    try {
        rollback_rc = metadata_->removeLocalMemoryBuffer(
            reinterpret_cast<void*>(descriptor.addr), update_metadata);
    } catch (const std::exception& error) {
        LOG(ERROR) << "NvlinkTransport: metadata registration rollback threw: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "NvlinkTransport: metadata registration rollback threw "
                      "an unknown exception";
    }
    if (rollback_rc == 0) {
        local_registrations_.erase(registration_addr);
        return rc;
    }
    LOG(ERROR) << "NvlinkTransport: metadata registration rollback failed: "
               << rollback_rc;
    local_registrations_[registration_addr].published = true;
    return rc;
}

int NvlinkTransport::ensureLocalDescriptorPresent(
    const BufferDesc& descriptor) {
    auto local = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!local) return ERR_METADATA;
    for (const auto& buffer : local->buffers) {
        if (buffer.addr != descriptor.addr) continue;
        if (buffer.length == descriptor.length &&
            buffer.name == descriptor.name &&
            buffer.shm_name == descriptor.shm_name) {
            return 0;
        }
        return ERR_ADDRESS_OVERLAPPED;
    }
    return metadata_->addLocalMemoryBuffer(descriptor, false);
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
        int stage_rc = ensureLocalDescriptorPresent(registration.descriptor);
        if (stage_rc != 0) {
            LOG(ERROR) << "NvlinkTransport: unable to stage metadata "
                          "unregistration: "
                       << stage_rc;
            return stage_rc;
        }
        int rc = ERR_METADATA;
        try {
            rc = metadata_->removeLocalMemoryBuffer(registration.mapped_base,
                                                    update_metadata);
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
        if (rc != 0) return rc;
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
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        LOG(ERROR) << "NvlinkTransport: target segment descriptor not found: "
                   << target_id;
        return ERR_METADATA;
    }
    if (length == 0) {
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
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();

            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            cached = remap_entries_.find(cache_key);
            if (cached == remap_entries_.end()) {
                std::vector<unsigned char> output_buffer;
                try {
                    deserializeBinaryData(entry.shm_name, output_buffer);
                } catch (const std::exception& error) {
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
                        return ERR_ADDRESS_OVERLAPPED;
                    }
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
#if defined(USE_MNNVL) && defined(USE_CUDA)
                    if (entry.length == 0 ||
                        entry.length > std::numeric_limits<size_t>::max()) {
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
                        LOG(ERROR) << "NvlinkTransport: incomplete Fabric "
                                      "driver adapter for lazy import";
                        return ERR_CONTEXT;
                    }
                    if (!retryQuarantinedFabricMappings()) {
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
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return ERR_MEMORY;
                    }
                    attempt.markMapped();

                    int device_count = 0;
                    result = fabric_driver_api_.device_get_count(&device_count);
                    if (result != CUDA_SUCCESS || device_count <= 0) {
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
                            LOG(ERROR)
                                << "NvlinkTransport: cuMemSetAccess failed for "
                                   "visible device "
                                << ordinal << ": " << result;
                            return ERR_MEMORY;
                        }
                    }

                    result = attempt.releaseHandle();
                    if (result != CUDA_SUCCESS) {
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
                        return ERR_ADDRESS_OVERLAPPED;
                    }
                    attempt.transferMappingOwnership();
#else
                    LOG(ERROR) << "NvlinkTransport: Fabric mapping requires "
                                  "CUDA/MNNVL support";
                    return ERR_CONTEXT;
#endif
                } else {
                    LOG(ERROR) << "NvlinkTransport: serialized handle size "
                                  "does not match active IPC/Fabric mode";
                    return ERR_INVALID_ARGUMENT;
                }
                cached = remap_entries_.find(cache_key);
            }
            if (cached == remap_entries_.end()) {
                return ERR_MEMORY;
            }
            auto shm_addr = cached->second.shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
    }
    LOG(ERROR) << "Requested address " << (void*)dest_addr << " to "
               << (void*)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

}  // namespace mooncake
