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

    RegisteredPinnedRegion(void* addr, size_t size, std::string owner)
        : addr_(addr), size_(size), owner_(std::move(owner)) {}

    void* addr_ = nullptr;
    size_t size_ = 0;
    std::string owner_;
};

class RegisteredPinnedMemoryManager {
   public:
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

    void release(RegisteredPinnedRegion* region);
    void drop_reservation_locked(const RegionKey& key, size_t size);

    const bool enabled_;
    const uint64_t limit_bytes_;

    mutable std::mutex mutex_;
    uint64_t pinned_bytes_ = 0;
    std::unordered_map<RegionKey, RegisteredPinnedRegion*, RegionKeyHash>
        regions_;
};

}  // namespace mooncake
