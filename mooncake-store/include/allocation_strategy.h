#pragma once

#include <algorithm>
#include <memory>
#include <numeric>
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

    /**
     * @brief Allocate one replica from the specified segment.
     *
     * @param allocator_manager The allocator manager that manages the
     *                          allocators to use
     * @param slice_length Length of the slice to be allocated
     * @param segment_name The name of the segment to allocate the replica from
     * @return tl::expected<Replica, ErrorCode> containing the allocated replica
     *         on success, or ErrorCode specifying the failure reason:
     *         - ErrorCode::SEGMENT_NOT_FOUND if the segment does not exist
     *         - ErrorCode::NO_AVAILABLE_HANDLE if the segment does not have
     *           enough space
     *         - ErrorCode::INVALID_PARAMS if configuration invalid
     */
    virtual tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) = 0;
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

        // If replica_num is not satisfied, allocate the remaining replicas
        // randomly.
        std::uniform_int_distribution<size_t> distribution(0, names.size() - 1);
        size_t start_idx = distribution(generator);

        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;

        while (replicas.size() < replica_num && try_count < max_retry) {
            auto index = start_idx % names.size();
            start_idx++;
            try_count++;

            // Skip excluded and used segments
            if (excluded_segments.contains(names[index]) ||
                used_segments.contains(names[index])) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, names[index],
                                         slice_length, generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                // Nit: no need to insert names[index] into used_segments here
                // because we only traverse all names once, thus there is no
                // chance to try allocating from a segment for the second time.
            }
        }

        // Return allocated replicas (may be fewer than requested)
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) {
        // Random number generator.
        static thread_local std::mt19937 generator(std::random_device{}());

        // Validate input parameters
        if (slice_length == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Check segment existence
        if (allocator_manager.getAllocators(segment_name) == nullptr) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }

        auto buffer = allocateSingle(allocator_manager, segment_name,
                                     slice_length, generator);
        if (buffer == nullptr) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        return Replica{std::move(buffer), ReplicaStatus::PROCESSING};
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

   private:
    static constexpr size_t kMaxRetryLimit = 100;
};

/**
 * @brief Best-of-N sampling allocation strategy.
 *
 * For each allocation of N replicas:
 * 1. Randomly sample min(2N, total) candidate segments from the eligible pool
 * 2. Query each candidate's free space, sort descending, pick the top N
 * 3. Try to allocate from these top-N segments
 * 4. If insufficient replicas are allocated, fallback to the base Random
 *    strategy for the remaining replicas
 *
 * This achieves near-optimal load balancing with low overhead:
 * - Sampling 2N is O(N), sorting 2N is O(N log N) — both small since N
 *   (replica count) is typically 1–3.
 * - New empty segments naturally win the comparison, getting filled quickly.
 * - Thread-safe: uses thread_local state for sampling, no shared mutable data.
 */
class BestOfNAllocationStrategy : public RandomAllocationStrategy {
   public:
    BestOfNAllocationStrategy() = default;

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>()) override {
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const auto& names = allocator_manager.getNames();
        if (names.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        static thread_local std::mt19937 generator(std::random_device{}());

        std::vector<Replica> replicas;
        replicas.reserve(replica_num);
        std::set<std::string> used_segments;

        // --- Handle preferred segments first (same as Random) ---
        for (const auto& preferred_segment : preferred_segments) {
            if (excluded_segments.contains(preferred_segment) ||
                used_segments.contains(preferred_segment)) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, preferred_segment,
                                         slice_length, generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                used_segments.insert(preferred_segment);
                if (replicas.size() == replica_num) {
                    return replicas;
                }
            }
        }

        const size_t remaining = replica_num - replicas.size();

        // --- Best-of-N: sample candidates from all segments, check eligibility
        // when selecting ---
        const size_t min_candidate_size = 6;
        size_t sample_count = std::max(2 * remaining, min_candidate_size);
        sample_count = std::min(sample_count, names.size());

        // Thread-local indices for Fisher-Yates sampling (no shared mutable
        // state, each thread has its own copy)
        static thread_local std::vector<size_t> tl_indices;
        if (tl_indices.size() != names.size()) {
            tl_indices.resize(names.size());
            std::iota(tl_indices.begin(), tl_indices.end(), 0);
        }

        // Compute free space ratio for sampled candidates
        struct Candidate {
            size_t name_idx;
            double free_ratio;  // free_bytes / capacity
        };
        std::vector<Candidate> candidates;
        candidates.reserve(sample_count);

        // Fisher-Yates partial shuffle using thread-local indices
        for (size_t i = 0; i < sample_count; ++i) {
            std::uniform_int_distribution<size_t> dist(i, names.size() - 1);
            size_t j = dist(generator);
            std::swap(tl_indices[i], tl_indices[j]);

            double free_ratio =
                getSegmentFreeRatio(allocator_manager, names[tl_indices[i]]);
            candidates.push_back({tl_indices[i], free_ratio});
        }

        // Sort by free space ratio descending
        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& a, const Candidate& b) {
                      return a.free_ratio > b.free_ratio;
                  });

        // Try to allocate from top candidates, skip excluded/used segments
        for (const auto& candidate : candidates) {
            if (replicas.size() >= replica_num) {
                break;
            }

            const auto& name = names[candidate.name_idx];

            // Skip excluded and used segments
            if (excluded_segments.contains(name) ||
                used_segments.contains(name)) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, name, slice_length,
                                         generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                used_segments.insert(name);
            }
        }

        if (replicas.size() >= replica_num) {
            return replicas;
        }

        // --- Fallback: Random allocation for any remaining replicas ---
        std::uniform_int_distribution<size_t> distribution(0, names.size() - 1);
        size_t start_idx = distribution(generator);
        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;

        while (replicas.size() < replica_num && try_count < max_retry) {
            auto index = start_idx % names.size();
            start_idx++;
            try_count++;

            // Skip excluded and used segments
            if (excluded_segments.contains(names[index]) ||
                used_segments.contains(names[index])) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, names[index],
                                         slice_length, generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING);
                used_segments.insert(names[index]);
            }
        }

        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 100;

    double getSegmentFreeRatio(const AllocatorManager& allocator_manager,
                               const std::string& name) {
        auto allocators = allocator_manager.getAllocators(name);
        if (!allocators || allocators->empty()) return 0.0;

        uint64_t total_capacity = 0;
        uint64_t total_free = 0;
        for (const auto& alloc : *allocators) {
            if (!alloc) continue;
            auto cap = static_cast<uint64_t>(alloc->capacity());
            total_capacity += cap;
            total_free += cap - static_cast<uint64_t>(alloc->size());
        }

        if (total_capacity == 0) return 0.0;
        return static_cast<double>(total_free) /
               static_cast<double>(total_capacity);
    }
};

class CxlAllocationStrategy : public AllocationStrategy {
   public:
    CxlAllocationStrategy() = default;
    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>()) {
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (preferred_segments.empty()) {
            LOG(ERROR) << "Preferred_segments is empty.";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const std::string& cxl_segment_name = preferred_segments[0];

        VLOG(1) << "Do cxl allocate, overwritten segment=" << cxl_segment_name;

        const auto cxl_allocators =
            allocator_manager.getAllocators(cxl_segment_name);

        if (cxl_allocators == nullptr || cxl_allocators->size() == 0) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        std::shared_ptr<BufferAllocatorBase> cxl_allocator =
            (*cxl_allocators)[0];
        if (!cxl_allocator) {
            LOG(ERROR) << "No CXL allocator in preferred_segment";
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::vector<Replica> replicas;
        replicas.reserve(replica_num);

        auto buffer = cxl_allocator->allocate(slice_length);
        if (!buffer) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        buffer->change_to_cxl(cxl_segment_name);
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING);

        VLOG(1) << "Successfully allocated " << replicas.size()
                << " CXL replica.";
        return replicas;
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
};

/**
 * @brief Factory function to create allocation strategy based on type
 */
inline std::shared_ptr<AllocationStrategy> CreateAllocationStrategy(
    AllocationStrategyType type) {
    switch (type) {
        case AllocationStrategyType::RANDOM:
            return std::make_shared<RandomAllocationStrategy>();
        case AllocationStrategyType::BEST_OF_N:
            return std::make_shared<BestOfNAllocationStrategy>();
        case AllocationStrategyType::CXL:
            return std::make_shared<CxlAllocationStrategy>();
        default:
            return std::make_shared<RandomAllocationStrategy>();
    }
}

}  // namespace mooncake
