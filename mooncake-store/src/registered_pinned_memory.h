#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace mooncake {

class RegisteredPinnedMemoryManager;

class RegisteredPinnedRegion {
   public:
    RegisteredPinnedRegion(const RegisteredPinnedRegion&) = delete;
    RegisteredPinnedRegion& operator=(const RegisteredPinnedRegion&) = delete;
    ~RegisteredPinnedRegion();

   private:
    friend class RegisteredPinnedMemoryManager;

    RegisteredPinnedRegion(RegisteredPinnedMemoryManager* manager, void* addr,
                           size_t size)
        : manager_(manager), addr_(addr), size_(size) {}

    RegisteredPinnedMemoryManager* manager_ = nullptr;
    void* addr_ = nullptr;
    size_t size_ = 0;
};

class RegisteredPinnedMemoryManager {
   public:
    enum class UnregisterResult { kSuccess, kRuntimeUnloading, kError };

    struct PinOps {
        bool (*register_region)(void* addr, size_t size,
                                std::string* error_message) = nullptr;
        UnregisterResult (*unregister_region)(
            void* addr, std::string* error_message) = nullptr;
    };

    static RegisteredPinnedMemoryManager& instance();

    std::shared_ptr<RegisteredPinnedRegion> try_pin(void* addr, size_t size,
                                                    const std::string& owner);

   private:
    friend class RegisteredPinnedRegion;

    struct ActiveRegion {
        void* addr;
        size_t size;
        RegisteredPinnedRegion* region;
    };

    RegisteredPinnedMemoryManager();
#if defined(MOONCAKE_STORE_TEST)
   public:
#endif
    RegisteredPinnedMemoryManager(std::pair<bool, uint64_t> config,
                                  PinOps pin_ops);
#if defined(MOONCAKE_STORE_TEST)
   private:
#endif

    void release(RegisteredPinnedRegion* region);
    void remove_inactive_region_locked(void* addr, size_t size);

    const bool enabled_;
    const uint64_t limit_bytes_;
    const PinOps pin_ops_;

    mutable std::mutex mutex_;
    uint64_t pinned_bytes_ = 0;
    std::vector<ActiveRegion> regions_;
};

}  // namespace mooncake
