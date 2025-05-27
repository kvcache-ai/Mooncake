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
     * @param config Replica configuration
     * @return Selected allocator; returns nullptr if allocation is not possible
     *         or no suitable allocator is found
     */
    virtual std::unique_ptr<AllocatedBuffer> Allocate(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize, const ReplicateConfig& config) = 0;
};

/**
 * @brief Random allocation strategy with local preference support.
 *
 * This strategy first attempts to allocate from a preferred segment if
 * specified, then falls back to random allocation among all available
 * allocators.
 */
class RandomAllocationStrategy : public AllocationStrategy {
   public:
    RandomAllocationStrategy() : rng_(std::random_device{}()) {}

    std::unique_ptr<AllocatedBuffer> Allocate(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize, const ReplicateConfig& config) override {
        // Fast path: single allocator case
        if (allocators.size() == 1) {
            return allocators.begin()->second->allocate(objectSize);
        }

        // Try preferred segment first if specified
        if (auto preferred_buffer =
                TryPreferredAllocation(allocators, objectSize, config)) {
            return preferred_buffer;
        }

        // Fall back to random allocation among all eligible allocators
        return RandomAllocateFromEligible(allocators, objectSize);
    }

   private:
    static constexpr size_t kMaxRetryLimit = 10;

    std::mt19937 rng_;  // Mersenne Twister random number generator

    /**
     * @brief Attempts allocation from preferred segment if available and
     * eligible
     */
    std::unique_ptr<AllocatedBuffer> TryPreferredAllocation(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize, const ReplicateConfig& config) {
        if (config.preferred_segment.empty()) {
            return nullptr;
        }

        auto preferred_it = allocators.find(config.preferred_segment);
        if (preferred_it == allocators.end()) {
            return nullptr;
        }

        auto& preferred_allocator = preferred_it->second;
        if (MayHasSufficientSpace(preferred_allocator, objectSize)) {
            return preferred_allocator->allocate(objectSize);
        }

        return nullptr;
    }

    /**
     * @brief Performs random allocation from eligible allocators with retry
     * logic
     */
    std::unique_ptr<AllocatedBuffer> RandomAllocateFromEligible(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize) {
        auto eligible = CollectEligibleAllocators(allocators, objectSize);
        if (eligible.empty()) {
            return nullptr;
        }

        return TryAllocateWithRetry(eligible, objectSize);
    }

    /**
     * @brief Collects all allocators with sufficient available space
     */
    std::vector<std::shared_ptr<BufferAllocator>> CollectEligibleAllocators(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
            allocators,
        size_t objectSize) {
        std::vector<std::shared_ptr<BufferAllocator>> eligible;
        eligible.reserve(allocators.size());

        for (const auto& [segment_name, allocator] : allocators) {
            if (MayHasSufficientSpace(allocator, objectSize)) {
                eligible.push_back(allocator);
            }
        }

        return eligible;
    }

    /**
     * @brief Attempts allocation with random selection and retry logic
     */
    std::unique_ptr<AllocatedBuffer> TryAllocateWithRetry(
        std::vector<std::shared_ptr<BufferAllocator>>& eligible,
        size_t objectSize) {
        const size_t max_tries = std::min(kMaxRetryLimit, eligible.size());

        for (size_t try_count = 0; try_count < max_tries; ++try_count) {
            // Randomly select an allocator
            std::uniform_int_distribution<size_t> dist(0, eligible.size() - 1);
            const size_t random_index = dist(rng_);

            auto& allocator = eligible[random_index];
            if (auto buffer = allocator->allocate(objectSize)) {
                return buffer;
            }

            // Remove failed allocator and continue with remaining ones
            RemoveAllocatorAtIndex(eligible, random_index);
        }

        return nullptr;
    }

    /**
     * @brief Checks if allocator has sufficient available space
     */
    static bool MayHasSufficientSpace(
        const std::shared_ptr<BufferAllocator>& allocator,
        size_t required_size) {
        const size_t capacity = allocator->capacity();
        const size_t used = allocator->size();
        const size_t available = capacity > used ? (capacity - used) : 0;
        return available >= required_size;
    }

    /**
     * @brief Efficiently removes allocator at given index using swap-and-pop
     */
    static void RemoveAllocatorAtIndex(
        std::vector<std::shared_ptr<BufferAllocator>>& allocators,
        size_t index) {
        if (index + 1 != allocators.size()) {
            std::swap(allocators[index], allocators.back());
        }
        allocators.pop_back();
    }
};

}  // namespace mooncake
