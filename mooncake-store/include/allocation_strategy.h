#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <iterator>
#include <time.h>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "replica.h"
#include "types.h"

namespace mooncake {

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
     * @param allocators Container of mounted allocators
     * @param allocators_by_name Container of mounted allocators, key is
     *                          segment_name, value is the corresponding
     *                          allocators
     * @param slice_length Length of the slice to be allocated
     * @param config Replica configuration containing number of replicas and
     *               placement constraints
     * @return tl::expected<std::vector<Replica>, ErrorCode> containing
     *         allocated replicas.
     *         - On success: vector of allocated replicas (may be fewer than
     *           requested due to resource constraints, but at least 1)
     *         - On failure: ErrorCode::NO_AVAILABLE_HANDLE if no replicas can
     *           be allocated, ErrorCode::INVALID_PARAMS for invalid
     *           configuration
     */
    virtual tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const size_t slice_length,
        const ReplicateConfig& config) = 0;
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
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const size_t slice_length, const ReplicateConfig& config) {
        // Validate input parameters
        if (slice_length == 0 || config.replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Fast path: single allocator case
        if (allocators.size() == 1) {
            if (auto buffer = allocators[0]->allocate(slice_length)) {
                std::vector<Replica> result;
                result.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING);
                return result;
            }
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::vector<Replica> replicas;
        replicas.reserve(config.replica_num);

        // Try preferred segment first if specified
        if (!config.preferred_segment.empty()) {
            auto preferred_it =
                allocators_by_name.find(config.preferred_segment);
            if (preferred_it != allocators_by_name.end()) {
                for (auto& allocator : preferred_it->second) {
                    if (auto buffer = allocator->allocate(slice_length)) {
                        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING);
                        break;
                    }
                }
            }
        }

        if (replicas.size() == config.replica_num) {
            return replicas;
        }

        // If replica_num is not satisfied, allocate the remaining replicas randomly
        // Randomly select a starting point from allocators_by_name
        if (allocators_by_name.empty()) {
            if (replicas.empty()) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            return replicas;
        }

        static thread_local std::mt19937 generator(clock());
        std::uniform_int_distribution<size_t> distribution(0, allocators_by_name.size() - 1);
        size_t start_idx = distribution(generator);
        
        // Get iterator to the starting point
        auto start_it = allocators_by_name.begin();
        std::advance(start_it, start_idx);
        
        auto it = start_it;
        size_t max_retry = std::min(kMaxRetryLimit, allocators_by_name.size());
        size_t retry_count = 0;
        
        // Try to allocate remaining replicas, starting from random position
        // TODO: Change the segment data structure to avoid traversing the entire map every time
        while (replicas.size() < config.replica_num && retry_count < max_retry) {
            // Skip preferred segment if it was already allocated
            if (it->first != config.preferred_segment) {
                // Try each allocator in this segment
                bool allocated = false;
                for (auto& allocator : it->second) {
                    if (auto buffer = allocator->allocate(slice_length)) {
                        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING);
                        // Allocate at most one replica per segment
                        allocated = true;
                        break;
                    }
                }
                if (!allocated) {
                    ++retry_count;
                }
            }
            // Move to next segment (circular)
            ++it;
            if (it == allocators_by_name.end()) {
                it = allocators_by_name.begin();
            }

            // If we have cycled through all segments, break
            if (it == start_it) {
                break;
            }
        }
        
        // Return allocated replicas (may be fewer than requested)
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 10;
};

}  // namespace mooncake