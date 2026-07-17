#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

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
                           size_t size, std::string owner)
        : manager_(manager),
          addr_(addr),
          size_(size),
          owner_(std::move(owner)) {}

    RegisteredPinnedMemoryManager* manager_ = nullptr;
    void* addr_ = nullptr;
    size_t size_ = 0;
    std::string owner_;
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

    struct RegionKey {
        void* addr = nullptr;
        size_t size = 0;

        bool operator==(const RegionKey& other) const {
            return addr == other.addr && size == other.size;
        }
    };

    struct RegionKeyHash {
        size_t operator()(const RegionKey& key) const {
            const size_t addr_hash = std::hash<void*>{}(key.addr);
            return addr_hash ^
                   (std::hash<size_t>{}(key.size) + 0x9e3779b97f4a7c15ULL +
                    (addr_hash << 6) + (addr_hash >> 2));
        }
    };

    RegisteredPinnedMemoryManager();
    explicit RegisteredPinnedMemoryManager(std::pair<bool, uint64_t> config);
#if defined(MOONCAKE_STORE_TEST)
   public:
#endif
    RegisteredPinnedMemoryManager(std::pair<bool, uint64_t> config,
                                  PinOps pin_ops);
#if defined(MOONCAKE_STORE_TEST)
   private:
#endif

    void release(RegisteredPinnedRegion* region);
    void drop_reservation_locked(const RegionKey& key, size_t size);

    const bool enabled_;
    const uint64_t limit_bytes_;
    const PinOps pin_ops_;

    mutable std::mutex mutex_;
    uint64_t pinned_bytes_ = 0;
    std::unordered_map<RegionKey, RegisteredPinnedRegion*, RegionKeyHash>
        regions_;
};

}  // namespace mooncake
