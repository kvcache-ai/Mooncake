#include "registered_pinned_memory.h"

#include <string>
#include <utility>

#include <glog/logging.h>

#if defined(USE_CUDA)
#include <cuda_runtime_api.h>
#elif defined(USE_XPU)
#include "device/accelerator_registry.h"
#endif

namespace mooncake {
namespace {

#if defined(USE_CUDA)
constexpr const char* kPinApi = "cudaHostRegister";
#elif defined(USE_XPU)
constexpr const char* kPinApi = "XPU prepare_for_device_copy";
#else
constexpr const char* kPinApi = "host memory registration";
#endif

void LogPinSkip(const std::string& owner, const char* reason, size_t size) {
    LOG(WARNING) << "Skip " << kPinApi << " for " << owner << ": " << reason
                 << ", size=" << size;
}

#if defined(USE_CUDA)
bool RegisterPinnedRegionWithCuda(void* addr, size_t size,
                                  std::string* error_message) {
    cudaError_t err = cudaHostRegister(addr, size, cudaHostRegisterPortable);
    if (err == cudaSuccess) return true;
    if (error_message) *error_message = cudaGetErrorString(err);
    cudaGetLastError();
    return false;
}

RegisteredPinnedMemoryManager::UnregisterResult UnregisterPinnedRegionWithCuda(
    void* addr, std::string* error_message) {
    cudaError_t err = cudaHostUnregister(addr);
    if (err == cudaSuccess) {
        return RegisteredPinnedMemoryManager::UnregisterResult::kSuccess;
    }
    if (error_message) *error_message = cudaGetErrorString(err);
    if (err == cudaErrorCudartUnloading) {
        return RegisteredPinnedMemoryManager::UnregisterResult::
            kRuntimeUnloading;
    }
    return RegisteredPinnedMemoryManager::UnregisterResult::kError;
}

#elif defined(USE_XPU)
// Store segments are plain host allocations; Level Zero treats them as
// pageable and stages every device copy through driver buffers. Importing them
// once lets local memcpy transfers DMA directly.
bool RegisterPinnedRegionWithXpu(void* addr, size_t size,
                                 std::string* error_message) {
    const auto* xpu = device::GetAcceleratorRegistry().GetDevice(
        device::AcceleratorVendor::kIntel);
    if (!xpu) {
        if (error_message) *error_message = "no XPU accelerator registered";
        return false;
    }
    std::string message;
    const bool ok = xpu->RegisterHostMemory(addr, size, message);
    if (!ok && error_message) *error_message = std::move(message);
    return ok;
}

RegisteredPinnedMemoryManager::UnregisterResult UnregisterPinnedRegionWithXpu(
    void* addr, std::string* error_message) {
    const auto* xpu = device::GetAcceleratorRegistry().GetDevice(
        device::AcceleratorVendor::kIntel);
    std::string message = "no XPU accelerator registered";
    if (xpu && xpu->UnregisterHostMemory(addr, message)) {
        return RegisteredPinnedMemoryManager::UnregisterResult::kSuccess;
    }
    if (error_message) *error_message = std::move(message);
    return RegisteredPinnedMemoryManager::UnregisterResult::kError;
}
#endif

RegisteredPinnedMemoryManager::PinOps DefaultPinOps() {
#if defined(USE_CUDA)
    return {RegisterPinnedRegionWithCuda, UnregisterPinnedRegionWithCuda};
#elif defined(USE_XPU)
    return {RegisterPinnedRegionWithXpu, UnregisterPinnedRegionWithXpu};
#else
    return {};
#endif
}

}  // namespace

RegisteredPinnedRegion::~RegisteredPinnedRegion() { release(); }

bool RegisteredPinnedRegion::release() {
    if (!manager_) return release_succeeded_;
    auto* manager = manager_;
    manager_ = nullptr;
    release_succeeded_ = manager->release(this);
    return release_succeeded_;
}

RegisteredPinnedMemoryManager& RegisteredPinnedMemoryManager::instance() {
    static RegisteredPinnedMemoryManager* manager =
        new RegisteredPinnedMemoryManager();
    return *manager;
}

RegisteredPinnedMemoryManager::RegisteredPinnedMemoryManager()
    : RegisteredPinnedMemoryManager(
          RegisteredPinnedMemoryConfig::FromEnvironment(), DefaultPinOps()) {}

RegisteredPinnedMemoryManager::RegisteredPinnedMemoryManager(
    RegisteredPinnedMemoryConfig config, PinOps pin_ops)
    : limit_bytes_(config.max_bytes), pin_ops_(pin_ops) {
#if defined(USE_CUDA) || defined(USE_XPU)
    LOG(INFO) << "Store segment pinned memory (" << kPinApi << ") is "
              << (limit_bytes_ != 0 ? "enabled" : "disabled")
              << ", max_bytes=" << limit_bytes_;
#else
    if (limit_bytes_ != 0) {
        LOG(INFO) << "Store segment pinning requested but this build has no "
                     "CUDA runtime support";
    }
#endif
}

std::shared_ptr<RegisteredPinnedRegion> RegisteredPinnedMemoryManager::try_pin(
    void* addr, size_t size, const std::string& owner) {
    if (!addr || size == 0 || limit_bytes_ == 0) return nullptr;
    if (!pin_ops_.register_region || !pin_ops_.unregister_region) {
        return nullptr;
    }

    const auto start = reinterpret_cast<uintptr_t>(addr);
    const auto end = start + size;
    if (end < start) {
        LogPinSkip(owner, "address range overflow", size);
        return nullptr;
    }

    std::shared_ptr<RegisteredPinnedRegion> region;
    try {
        region.reset(new RegisteredPinnedRegion(this, addr, size));
    } catch (...) {
        LogPinSkip(owner, "failed to allocate pin tracking", size);
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : regions_) {
            const auto region_start = reinterpret_cast<uintptr_t>(entry.addr);
            const auto region_end = region_start + entry.size;
            const bool overlaps = start < region_end && end > region_start;
            if (overlaps) {
                LogPinSkip(owner, "overlaps an active pinned region", size);
                return nullptr;
            }
        }

        if (size > limit_bytes_ || pinned_bytes_ > limit_bytes_ - size) {
            LOG(WARNING) << "Skip " << kPinApi << " for " << owner
                         << ": quota exceeded, requested=" << size
                         << ", pinned=" << pinned_bytes_
                         << ", limit=" << limit_bytes_;
            return nullptr;
        }

        try {
            regions_.push_back({addr, size, nullptr});
        } catch (...) {
            LogPinSkip(owner, "failed to allocate pin tracking", size);
            return nullptr;
        }
        pinned_bytes_ += size;
    }

    std::string error_message;
    const bool registered =
        pin_ops_.register_region(addr, size, &error_message);
    if (!registered) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            remove_inactive_region_locked(addr, size);
        }
        LOG(WARNING) << kPinApi << " failed for " << owner << ", size=" << size
                     << ", error=" << error_message
                     << ". Continue with pageable host memory.";
        return nullptr;
    }

    uint64_t pinned_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : regions_) {
            if (entry.addr == addr && entry.size == size && !entry.region) {
                entry.region = region.get();
                pinned_bytes = pinned_bytes_;
                break;
            }
        }
    }

    LOG(INFO) << kPinApi << " succeeded for " << owner << ", size=" << size
              << ", pinned=" << pinned_bytes << ", limit=" << limit_bytes_;
    return region;
}

bool RegisteredPinnedMemoryManager::release(RegisteredPinnedRegion* region) {
    if (!region || !region->addr_ || region->size_ == 0) return true;

    bool should_unregister = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : regions_) {
            if (entry.addr == region->addr_ && entry.size == region->size_ &&
                entry.region == region) {
                entry.region = nullptr;
                should_unregister = true;
                break;
            }
        }
    }
    if (!should_unregister) return true;

    std::string error_message;
    auto unregister_result =
        pin_ops_.unregister_region(region->addr_, &error_message);
    if (unregister_result != UnregisterResult::kSuccess) {
        if (unregister_result == UnregisterResult::kRuntimeUnloading) {
            LOG(WARNING) << "Skip cudaHostUnregister because CUDA runtime "
                            "is unloading, size="
                         << region->size_;
        } else {
            LOG(ERROR) << "cudaHostUnregister failed, size=" << region->size_
                       << ", error=" << error_message
                       << ". Keep the range reserved; backing memory must not "
                          "be freed.";
            return false;
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    remove_inactive_region_locked(region->addr_, region->size_);
    return true;
}

void RegisteredPinnedMemoryManager::remove_inactive_region_locked(void* addr,
                                                                  size_t size) {
    for (auto it = regions_.begin(); it != regions_.end(); ++it) {
        if (it->addr == addr && it->size == size && !it->region) {
            regions_.erase(it);
            pinned_bytes_ = pinned_bytes_ >= size ? pinned_bytes_ - size : 0;
            return;
        }
    }
}

}  // namespace mooncake
