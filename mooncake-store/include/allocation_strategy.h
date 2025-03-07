#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "types.h"

namespace mooncake {

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
     * @param allocators Container of mounted allocators, key is segment_name,
     *                  value is the corresponding allocator
     * @param objectSize Size of object to be allocated
     * @return Selected allocator; returns nullptr if allocation is not possible
     *         or no suitable allocator is found
     */
    virtual std::shared_ptr<BufHandle> Allocate(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize) = 0;
};

class RandomAllocationStrategy : public AllocationStrategy {
   public:
    RandomAllocationStrategy() : rng_(std::random_device{}()) {}

    std::shared_ptr<BufHandle> Allocate(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
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
