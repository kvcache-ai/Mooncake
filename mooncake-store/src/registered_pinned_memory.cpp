#include "registered_pinned_memory.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <optional>
#include <string>
#include <system_error>

#include <glog/logging.h>

#if defined(USE_CUDA)
#include <cuda_runtime_api.h>
#endif

namespace mooncake {
namespace {

std::pair<const char*, const char*> TrimAsciiWhitespace(const char* value) {
    const char* begin = value;
    while (std::isspace(static_cast<unsigned char>(*begin))) {
        ++begin;
    }

    const char* end = begin;
    while (*end != '\0') {
        ++end;
    }
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(*(end - 1)))) {
        --end;
    }

    return {begin, end};
}

bool ParsePinnedMemoryEnabled() {
    const char* value = std::getenv("MC_STORE_PIN_MEMORY");
    if (!value) return true;

    auto [begin, end] = TrimAsciiWhitespace(value);
    std::string normalized(begin, end);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return !(normalized == "0" || normalized == "false" ||
             normalized == "off" || normalized == "no");
}

std::optional<uint64_t> ParsePinnedMemoryLimit() {
    const char* value = std::getenv("MC_STORE_PIN_MEMORY_MAX_BYTES");
    if (!value || value[0] == '\0') return 0;

    auto [number, end] = TrimAsciiWhitespace(value);
    if (*number == '-') {
        LOG(WARNING) << "Invalid MC_STORE_PIN_MEMORY_MAX_BYTES='" << value
                     << "', disabling Store segment pinning";
        return std::nullopt;
    }

    uint64_t parsed = 0;
    auto [ptr, ec] = std::from_chars(number, end, parsed);
    if (ec != std::errc{} || ptr != end) {
        LOG(WARNING) << "Invalid MC_STORE_PIN_MEMORY_MAX_BYTES='" << value
                     << "', disabling Store segment pinning";
        return std::nullopt;
    }
    return parsed;
}

std::pair<bool, uint64_t> ParsePinnedMemoryConfig() {
    if (!ParsePinnedMemoryEnabled()) return {false, 0};

    auto limit = ParsePinnedMemoryLimit();
    if (!limit.has_value() || *limit == 0) {
        return {false, 0};
    }
    return {true, *limit};
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

#endif

RegisteredPinnedMemoryManager::PinOps DefaultPinOps() {
#if defined(USE_CUDA)
    return {RegisterPinnedRegionWithCuda, UnregisterPinnedRegionWithCuda};
#else
    return {};
#endif
}

}  // namespace

RegisteredPinnedRegion::~RegisteredPinnedRegion() {
    if (manager_) manager_->release(this);
}

RegisteredPinnedMemoryManager& RegisteredPinnedMemoryManager::instance() {
    static RegisteredPinnedMemoryManager* manager =
        new RegisteredPinnedMemoryManager();
    return *manager;
}

RegisteredPinnedMemoryManager::RegisteredPinnedMemoryManager()
    : RegisteredPinnedMemoryManager(ParsePinnedMemoryConfig()) {}

RegisteredPinnedMemoryManager::RegisteredPinnedMemoryManager(
    std::pair<bool, uint64_t> config)
    : RegisteredPinnedMemoryManager(config, DefaultPinOps()) {}

RegisteredPinnedMemoryManager::RegisteredPinnedMemoryManager(
    std::pair<bool, uint64_t> config, PinOps pin_ops)
    : enabled_(config.first), limit_bytes_(config.second), pin_ops_(pin_ops) {
#if defined(USE_CUDA)
    LOG(INFO) << "Store segment pinned memory is "
              << (enabled_ ? "enabled" : "disabled")
              << ", max_bytes=" << limit_bytes_;
#else
    if (enabled_) {
        LOG(INFO) << "Store segment pinning requested but this build has no "
                     "CUDA runtime support";
    }
#endif
}

std::shared_ptr<RegisteredPinnedRegion> RegisteredPinnedMemoryManager::try_pin(
    void* addr, size_t size, const std::string& owner) {
    if (!addr || size == 0 || !enabled_) return nullptr;
    if (!pin_ops_.register_region || !pin_ops_.unregister_region) {
        return nullptr;
    }

    RegionKey key{addr, size};
    const auto start = reinterpret_cast<uintptr_t>(addr);
    const auto end = start + size;
    if (end < start) {
        LOG(WARNING) << "Skip cudaHostRegister for " << owner
                     << ": address range overflow, size=" << size;
        return nullptr;
    }

    std::shared_ptr<RegisteredPinnedRegion> region;
    try {
        region.reset(new RegisteredPinnedRegion(this, addr, size, owner));
    } catch (...) {
        LOG(WARNING) << "Skip cudaHostRegister for " << owner
                     << ": failed to allocate pin tracking, size=" << size;
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : regions_) {
            const auto region_start =
                reinterpret_cast<uintptr_t>(entry.first.addr);
            const auto region_end = region_start + entry.first.size;
            if (region_end < region_start) {
                LOG(WARNING) << "Skip cudaHostRegister for " << owner
                             << ": existing active range overflow, size="
                             << entry.first.size;
                return nullptr;
            }

            const bool overlaps = start < region_end && end > region_start;
            if (overlaps) {
                LOG(WARNING)
                    << "Skip cudaHostRegister for " << owner
                    << ": overlaps an active pinned region, size=" << size;
                return nullptr;
            }
        }

        if (size > limit_bytes_ || pinned_bytes_ > limit_bytes_ - size) {
            LOG(WARNING) << "Skip cudaHostRegister for " << owner
                         << ": quota exceeded, requested=" << size
                         << ", pinned=" << pinned_bytes_
                         << ", limit=" << limit_bytes_;
            return nullptr;
        }

        try {
            auto [_, inserted] = regions_.emplace(key, nullptr);
            if (!inserted) {
                LOG(WARNING) << "Skip cudaHostRegister for " << owner
                             << ": active region already exists, size=" << size;
                return nullptr;
            }
        } catch (...) {
            LOG(WARNING) << "Skip cudaHostRegister for " << owner
                         << ": failed to allocate pin tracking, size=" << size;
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
            drop_reservation_locked(key, size);
        }
        LOG(WARNING) << "cudaHostRegister failed for " << owner
                     << ", size=" << size << ", error=" << error_message
                     << ". Continue with pageable host memory.";
        return nullptr;
    }

    bool tracking_ready = false;
    uint64_t pinned_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = regions_.find(key);
        if (it != regions_.end() && it->second == nullptr) {
            it->second = region.get();
            pinned_bytes = pinned_bytes_;
            tracking_ready = true;
        }
    }
    if (!tracking_ready) {
        error_message.clear();
        auto unregister_result =
            pin_ops_.unregister_region(addr, &error_message);
        if (unregister_result != UnregisterResult::kSuccess) {
            LOG(ERROR) << "cudaHostUnregister failed after active range "
                          "tracking mismatch for "
                       << owner << ", size=" << size
                       << ", error=" << error_message
                       << ". Continue with best-effort cleanup.";
        }
        std::lock_guard<std::mutex> lock(mutex_);
        drop_reservation_locked(key, size);
        return nullptr;
    }

    LOG(INFO) << "cudaHostRegister succeeded for " << owner << ", size=" << size
              << ", pinned=" << pinned_bytes << ", limit=" << limit_bytes_;
    return region;
}

void RegisteredPinnedMemoryManager::release(RegisteredPinnedRegion* region) {
    if (!region || !region->addr_ || region->size_ == 0) return;

    RegionKey key{region->addr_, region->size_};
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto region_it = regions_.find(key);
        if (region_it == regions_.end() || region_it->second != region) {
            return;
        }
        region_it->second = nullptr;
    }

    std::string error_message;
    if (!pin_ops_.unregister_region) return;
    auto unregister_result =
        pin_ops_.unregister_region(region->addr_, &error_message);
    // Treat CUDA unregistration as best-effort cleanup: drop manager state so
    // stale raw tracking pointers do not outlive the Store segment owner.
    if (unregister_result != UnregisterResult::kSuccess) {
        if (unregister_result == UnregisterResult::kRuntimeUnloading) {
            LOG(WARNING) << "Skip cudaHostUnregister for " << region->owner_
                         << " because CUDA runtime is unloading, size="
                         << region->size_;
        } else {
            LOG(ERROR) << "cudaHostUnregister failed for " << region->owner_
                       << ", size=" << region->size_
                       << ", error=" << error_message
                       << ". Continue with best-effort cleanup.";
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = regions_.find(key);
    if (it == regions_.end() || it->second != nullptr) return;
    regions_.erase(it);
    if (pinned_bytes_ >= region->size_) {
        pinned_bytes_ -= region->size_;
    } else {
        pinned_bytes_ = 0;
    }
}

void RegisteredPinnedMemoryManager::drop_reservation_locked(
    const RegionKey& key, size_t size) {
    auto it = regions_.find(key);
    if (it != regions_.end() && it->second == nullptr) {
        regions_.erase(it);
        pinned_bytes_ = pinned_bytes_ >= size ? pinned_bytes_ - size : 0;
    }
}

}  // namespace mooncake
