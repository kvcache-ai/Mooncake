#pragma once

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <set>
#include <unordered_map>
#include <iterator>
#include <time.h>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief A container for managing valid allocators.
 *
 * @note Thread safety: External synchronization is required for thread-safe
 * usage of this class. In this codebase, thread safety is provided by
 * `SegmentManager`'s `segment_mutex_`.
 */
class AllocatorManager {
   public:
    AllocatorManager() = default;
    ~AllocatorManager() = default;

    // Copy-construct disallowed.
    AllocatorManager(const AllocatorManager&) = delete;
    AllocatorManager& operator=(const AllocatorManager&) = delete;

    // Move-construct allowed.
    AllocatorManager(AllocatorManager&&) = default;
    AllocatorManager& operator=(AllocatorManager&&) = default;

    /**
     * @brief Add an allocator of segment `name` into the manager.
     * @param name the name of the segment
     * @param allocator the buffer allocator to add for the segment
     */
    void addAllocator(const std::string& name,
                      const std::shared_ptr<BufferAllocatorBase>& allocator) {
        if (!allocators_.contains(name)) {
            names_.push_back(name);
        }
        allocators_[name].push_back(allocator);
    }

    /**
     * @brief Remove an allocator of segment `name` from the manager. This
     *        also removes the name if there are no allocators after the
     *        removal.
     * @param name the name of the segment
     * @param allocator the buffer allocator to remove from the segment
     * @return true if the allocator is removed, false if the allocator does
     *         not exist
     */
    bool removeAllocator(
        const std::string& name,
        const std::shared_ptr<BufferAllocatorBase>& allocator) {
        auto it = allocators_.find(name);
        if (it == allocators_.end()) {
            return false;
        }

        // Try removing the allocator.
        bool allocator_removed = false;
        auto alloc_it =
            std::find(it->second.begin(), it->second.end(), allocator);
        if (alloc_it != it->second.end()) {
            it->second.erase(alloc_it);
            allocator_removed = true;
        }

        if (it->second.empty()) {
            // If there is no allocator left, remove the name too.
            allocators_.erase(name);
            auto name_it = std::find(names_.begin(), names_.end(), name);
            if (name_it != names_.end()) {
                std::swap(*name_it, names_.back());
                names_.pop_back();
            }
        }

        return allocator_removed;
    }

    /**
     * @brief Get the names of all segments. This returns a vector of the
     *        names so that we can randomly pick a segment without traversing.
     * @return a vector of names of all mounted segments
     */
    const std::vector<std::string>& getNames() const { return names_; }

    /**
     * @brief Get allocators belongs to the given segment name.
     * @return a vector of allocators belongs to the given segment name
     */
    const std::vector<std::shared_ptr<BufferAllocatorBase>>* getAllocators(
        const std::string& name) const {
        auto it = allocators_.find(name);
        if (it != allocators_.end()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

   private:
    // Name array for randomly picking allocators.
    std::vector<std::string> names_;
    // Segment name to allocators mapping.
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_;
};

/**
 * @brief Abstract interface for allocation strategy, responsible for
 *        allocating a slice (with one or more replicas) using available
 *        BufferAllocators.
 *
 * The allocation strategy follows best-effort semantics: if the requested
 * number of replicas cannot be fully satisfied due to resource constraints,
 * it will allocate as many replicas as possible rather than failing entirely.
 * Only returns an error if no replicas can be allocated at all.
 */
class AllocationStrategy {
   public:
    virtual ~AllocationStrategy() = default;

    /**
     * @brief Allocates a slice across the requested number of replicas
     *        using best-effort semantics.
     *
     * The allocation follows best-effort semantics: if the full requested
     * replica count cannot be satisfied, the method will allocate as many
     * replicas as possible across different segments. For each slice, replicas
     * are guaranteed to be placed on different segments to ensure redundancy.
     *
     * @param allocator_manager The allocator manager that manages the
     *                          allocators to use
     * @param slice_length Length of the slice to be allocated
     * @param replica_num Number of replicas to allocate
     * @param preferred_segments Preferred segments to allocate buffers from
     * @param excluded_segments Excluded segments that should not allocate
     * buffers from
     * @return tl::expected<std::vector<Replica>, ErrorCode> containing
     *         allocated replicas.
     *         - On success: vector of allocated replicas (may be fewer than
     *           requested due to resource constraints, but at least 1)
     *         - On failure: ErrorCode::NO_AVAILABLE_HANDLE if no replicas can
     *           be allocated, ErrorCode::INVALID_PARAMS for invalid
     *           configuration
     */
    virtual tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>()) = 0;
};

/**
 * @brief Random batch allocation strategy with local preference and
 *        replication guarantees support using best-effort semantics.
 *
 * This strategy ensures that for each slice, its replicas are placed in
 * different segments. Different slices may use the same segments.
 *
 * Best-effort behavior:
 * - Attempts to allocate the requested number of replicas
 * - If insufficient segments are available, allocates as many replicas as
 *   possible (limited by the number of available segments)
 * - Only fails if no replicas can be allocated at all
 * - Preferred segment allocation is attempted first if specified
 */
class RandomAllocationStrategy : public AllocationStrategy {
   public:
    RandomAllocationStrategy() = default;
    virtual ~RandomAllocationStrategy() = default;

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>()) {
        // Validate input parameters
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Check available segments.
        const auto& names = allocator_manager.getNames();
        if (names.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        // Random number generator.
        static thread_local std::mt19937 generator(std::random_device{}());

        std::vector<Replica> replicas;
        replicas.reserve(replica_num);

        // Fast path: single segment case
        if (names.size() == 1) {
            if (excluded_segments.contains(names[0])) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }

            auto buffer = allocateSingle(allocator_manager, names[0],
                                         slice_length, generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                return replicas;
            }
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::set<std::string> used_segments;

        // Try preferred segments first if specified
        for (auto& preferred_segment : preferred_segments) {
            if (excluded_segments.contains(preferred_segment) ||
                used_segments.contains(preferred_segment)) {
                // Skip excluded and used segments
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, preferred_segment,
                                         slice_length, generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                if (replicas.size() == replica_num) {
                    return replicas;
                }

                // Add preferred segment to used_segments on allocation success
                used_segments.insert(preferred_segment);
            }
        }

        auto batch_buffers = allocateBatch(
            allocator_manager, slice_length, generator,
            replica_num - replicas.size(),
            [&excluded_segments, &used_segments](const std::string& name) {
                return excluded_segments.contains(name) ||
                       used_segments.contains(name);
            });
        for (auto& buffer : batch_buffers) {
            replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING);
        }

        // Return allocated replicas (may be fewer than requested)
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

    std::unique_ptr<AllocatedBuffer> allocateSingle(
        const AllocatorManager& allocator_manager, const std::string& name,
        const size_t slice_length, std::mt19937& generator) {
        const auto allocators = allocator_manager.getAllocators(name);
        if (allocators == nullptr || allocators->size() == 0) {
            return nullptr;
        }

        const auto num_segs = allocators->size();
        if (num_segs == 1) {
            // Fast path for single segment
            return (*allocators)[0]->allocate(slice_length);
        }

        // Randomly select a start point to distribute
        // allocations across all segments
        std::uniform_int_distribution<size_t> dist(0, num_segs - 1);
        size_t seg_offset = dist(generator);
        for (size_t i = 0; i < num_segs; i++) {
            auto& allocator = (*allocators)[(i + seg_offset) % num_segs];
            if (auto buffer = allocator->allocate(slice_length)) {
                return buffer;
            }
        }

        return nullptr;
    }

    virtual std::vector<std::unique_ptr<AllocatedBuffer>> allocateBatch(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        std::mt19937& generator, const size_t expect_num,
        std::function<bool(const std::string&)> exclude_func) {
        const auto& names = allocator_manager.getNames();
        // If replica_num is not satisfied, allocate the remaining replicas
        // randomly.
        std::uniform_int_distribution<size_t> distribution(0, names.size() - 1);
        size_t start_idx = distribution(generator);

        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;

        std::vector<std::unique_ptr<AllocatedBuffer>> batch_buffers;
        batch_buffers.reserve(expect_num);
        while (batch_buffers.size() < expect_num && try_count < max_retry) {
            auto index = start_idx % names.size();
            start_idx++;
            try_count++;

            // Skip excluded and used segments
            if (exclude_func && exclude_func(names[index])) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, names[index],
                                         slice_length, generator);
            if (buffer) {
                batch_buffers.emplace_back(std::move(buffer));
                // Nit: no need to insert names[index] into used_segments here
                // because we only traverse all names once, thus there is no
                // chance to try allocating from a segment for the second time.
            }
        }
        return batch_buffers;
    }

   protected:
    static constexpr size_t kMaxRetryLimit = 100;
};

/**
 * @brief Weighted random batch allocation strategy using best-effort semantics.
 *
 * This strategy allocates buffers based on the available capacity of each
 * allocator, favoring those with more free space. It ensures that for each
 * slice, its replicas are placed in different segments. Different slices
 * may use the same segments.
 *
 * Best-effort behavior:
 * - Attempts to allocate the requested number of replicas
 * - If insufficient segments are available, allocates as many replicas as
 *   possible (limited by the number of available segments)
 * - Only fails if no replicas can be allocated at all
 */
class WeightedRandomAllocationStrategy : public RandomAllocationStrategy {
   public:
    WeightedRandomAllocationStrategy() = default;
    virtual ~WeightedRandomAllocationStrategy() = default;

    virtual std::vector<std::unique_ptr<AllocatedBuffer>> allocateBatch(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        std::mt19937& generator, const size_t expect_num,
        std::function<bool(const std::string&)> exclude_func) {
        struct Candidate {
            std::shared_ptr<BufferAllocatorBase> allocator;
            uint64_t available_capacity;
            uint64_t cumulative_weights;
            bool selected;
        };

        const auto& names = allocator_manager.getNames();
        std::vector<Candidate> candidates;
        candidates.reserve(names.size());

        uint64_t total_weight = 0;
        for (const auto& name : names) {
            if (exclude_func && exclude_func(name)) continue;

            auto allocators = allocator_manager.getAllocators(name);
            if (!allocators || allocators->empty()) continue;

            for (const auto& alloc : *allocators) {
                if (!alloc) continue;

                uint64_t avail = alloc->capacity() - alloc->size();
                if (avail > slice_length) {
                    total_weight += avail;
                    candidates.push_back({alloc, avail, total_weight, false});
                }
            }
        }

        if (candidates.empty()) return {};

        std::vector<std::unique_ptr<AllocatedBuffer>> batch_buffers;
        batch_buffers.reserve(std::min(expect_num, candidates.size()));

        // Selection loop (without replacement)
        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;
        std::uniform_int_distribution<uint64_t> dist(0, total_weight - 1);
        while (batch_buffers.size() < expect_num && try_count < max_retry) {
            try_count++;
            uint64_t r = dist(generator);
            for (auto& c : candidates) {
                if (c.selected) {
                    // skip selected candidates and update r
                    r += c.available_capacity;
                    continue;
                }

                if (r < c.cumulative_weights) {
                    c.selected = true;
                    const auto& alloc = c.allocator;
                    if (auto buffer = alloc->allocate(slice_length)) {
                        batch_buffers.emplace_back(std::move(buffer));
                    }
                    break;
                }
            }
        }

        return batch_buffers;
    }
};

inline std::shared_ptr<AllocationStrategy> CreateAllocationStrategy(
    AllocationStrategyType type) {
    switch (type) {
        case AllocationStrategyType::RANDOM:
            return std::make_shared<RandomAllocationStrategy>();
        case AllocationStrategyType::WEIGHTED_RANDOM:
            return std::make_shared<WeightedRandomAllocationStrategy>();

        default:  // default to random strategy
            return std::make_shared<RandomAllocationStrategy>();
    }
}

}  // namespace mooncake
