#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

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
     * @param allocators Container of mounted allocators
     * @param allocators_by_name Container of mounted allocators, key is
     * segment_name, value is the corresponding allocator
     * @param objectSize Size of object to be allocated
     * @param config Replica configuration
     * @return Selected allocator; returns nullptr if allocation is not possible
     *         or no suitable allocator is found
     */
    virtual std::unique_ptr<AllocatedBuffer> Allocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        size_t objectSize, const ReplicateConfig& config,
        const std::unordered_set<std::string>& excluded_segments) = 0;
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
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        size_t objectSize, const ReplicateConfig& config,
        const std::unordered_set<std::string>& excluded_segments = {}) override {
        // Fast path: single allocator case
        if (allocators.size() == 1) {
            return excluded_segments.contains(allocators[0]->getSegmentName()) 
                ? nullptr : allocators[0]->allocate(objectSize);
        }

        // Try preferred segment first if specified
        if (auto preferred_buffer =
                TryPreferredAllocate(allocators_by_name, objectSize, config, excluded_segments)) {
            return preferred_buffer;
        }

        // Fall back to random allocation among all eligible allocators
        return TryRandomAllocate(allocators, objectSize, excluded_segments);
    }

   private:
    static constexpr size_t kMaxRetryLimit = 10;

    std::mt19937 rng_;  // Mersenne Twister random number generator

    /**
     * @brief Attempts allocation from preferred segment if available and
     * eligible
     */
    std::unique_ptr<AllocatedBuffer> TryPreferredAllocate(
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators,
        size_t objectSize, const ReplicateConfig& config,
        const std::unordered_set<std::string>& excluded_segments) {
        if (config.preferred_segment.empty()) {
            return nullptr;
        }

        auto preferred_it = allocators.find(config.preferred_segment);
        if (preferred_it == allocators.end() || excluded_segments.contains(preferred_it->first)) {
            return nullptr;
        }

        auto& preferred_allocators = preferred_it->second;
        for (auto& allocator : preferred_allocators) {
            auto buffer = allocator->allocate(objectSize);
            if (buffer != nullptr) {
                return buffer;
            }
        }

        return nullptr;
    }

    /**
     * @brief Attempts allocation with random selection and retry logic
     */
    std::unique_ptr<AllocatedBuffer> TryRandomAllocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        size_t objectSize,
        const std::unordered_set<std::string>& excluded_segments) {
        std::vector<size_t> allocator_indices;
        allocator_indices.reserve(allocators.size());

        for (size_t index = 0; index < allocators.size(); ++index) {
            const auto& segment_name = allocators[index]->getSegmentName();
            if (!excluded_segments.contains(segment_name)) {
                allocator_indices.push_back(index);
            }
        }

        const size_t max_tries = std::min(kMaxRetryLimit, allocator_indices.size());
        for (size_t try_count = 0; try_count < max_tries; ++try_count) {
            // Randomly select an allocator
            std::uniform_int_distribution<size_t> dist(
                0, allocator_indices.size() - 1);
            const size_t random_index = allocator_indices[dist(rng_)];

            auto& allocator = allocators[random_index];
            if (auto buffer = allocator->allocate(objectSize)) {
                return buffer;
            }

            // Remove failed allocator and continue with remaining ones
            if (random_index + 1 != allocator_indices.size()) {
                std::swap(allocator_indices[random_index],
                          allocator_indices[allocator_indices.size() - 1]);
            }
            allocator_indices.pop_back();
        }
        return nullptr;
    }
};

}  // namespace mooncake
