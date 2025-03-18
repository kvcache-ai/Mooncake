#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "types.h"

namespace mooncake {

// Forward declarations for composite key types
struct AllocatorKey {
    std::string segment_name;
    uint64_t buffer;

    AllocatorKey() = default;
    AllocatorKey(const std::string& name, uint64_t buf)
        : segment_name(name), buffer(buf) {}

    bool operator==(const AllocatorKey& other) const {
        return segment_name == other.segment_name && buffer == other.buffer;
    }
};

// Hash function for AllocatorKey
struct AllocatorKeyHash {
    std::size_t operator()(const AllocatorKey& key) const {
        // Combine hashes of segment_name and buffer
        std::size_t h1 = std::hash<std::string>{}(key.segment_name);
        std::size_t h2 = std::hash<uint64_t>{}(key.buffer);
        return h1 ^ (h2 << 1);  // Simple hash combination
    }
};

/**
 * @brief Abstract interface for allocation strategy, responsible for choosing
 *        among multiple BufferAllocators.
 */
class AllocationStrategy {
   public:
    virtual ~AllocationStrategy() = default;

    /**
     * @brief Given all mounted BufferAllocators and required object size,
     *        the strategy can freely choose a suitable BufferAllocator.
     * @param allocators Container of mounted allocators, key is AllocatorKey
     * (segment_name, buffer), value is the corresponding allocator
     * @param objectSize Size of object to be allocated
     * @return Selected allocator; returns nullptr if allocation is not possible
     *         or no suitable allocator is found
     */
    virtual std::shared_ptr<BufHandle> Allocate(
        const std::unordered_map<AllocatorKey, std::shared_ptr<BufferAllocator>,
                                 AllocatorKeyHash>& allocators,
        size_t objectSize) = 0;
};

class RandomAllocationStrategy : public AllocationStrategy {
   public:
    RandomAllocationStrategy() : rng_(std::random_device{}()) {}

    std::shared_ptr<BufHandle> Allocate(
        const std::unordered_map<AllocatorKey, std::shared_ptr<BufferAllocator>,
                                 AllocatorKeyHash>& allocators,
        size_t objectSize) override {
        // Because there is only one allocator, we can directly allocate from it
        if (allocators.size() == 1) {
            auto& allocator = allocators.begin()->second;
            return allocator->allocate(objectSize);
        }
        // First, collect all eligible allocators
        std::vector<std::shared_ptr<BufferAllocator>> eligible;

        for (const auto& kv : allocators) {
            auto& allocator = kv.second;
            size_t capacity = allocator->capacity();
            size_t used = allocator->size();
            size_t available = capacity > used ? (capacity - used) : 0;

            if (available >= objectSize) {
                eligible.push_back(allocator);
            }
        }

        // If no eligible allocators found, return nullptr
        if (eligible.empty()) {
            return nullptr;
        }

        // Randomly select one from eligible allocators
        std::uniform_int_distribution<size_t> dist(0, eligible.size() - 1);
        size_t randomIndex = dist(rng_);
        const size_t max_try = 10;
        size_t try_count = 0;
        // Due to allocator fragmentation, we may fail to allocate memory even
        while (try_count < max_try) {
            auto& allocator = eligible[randomIndex];
            auto bufHandle = allocator->allocate(objectSize);
            if (bufHandle) {
                return bufHandle;
            }
            try_count++;
        }
        return nullptr;
    }

   private:
    std::mt19937 rng_;  // Mersenne Twister random number generator
};

}  // namespace mooncake
