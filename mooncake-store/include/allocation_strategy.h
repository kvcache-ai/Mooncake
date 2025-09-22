#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Abstract interface for allocation strategy, responsible for
 *        allocating multiple slices across multiple replicas using available
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
     * @brief Allocates multiple slices across the requested number of replicas
     *        using best-effort semantics. Each replica will contain all
     *        requested slices.
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
     * @param slice_sizes Sizes of slices to be allocated in each replica
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
        const std::vector<size_t>& slice_sizes,
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
        const std::vector<size_t>& slice_sizes, const ReplicateConfig& config) {
        if (auto validation_error =
                validateInput(slice_sizes, config.replica_num)) {
            return tl::make_unexpected(*validation_error);
        }

        std::vector<std::vector<std::unique_ptr<AllocatedBuffer>>>
            replica_buffers(config.replica_num);
        for (auto& replica_buffer : replica_buffers) {
            replica_buffer.reserve(slice_sizes.size());
        }

        // Track the actual number of replicas we can allocate
        size_t actual_replica_count = config.replica_num;

        // Allocate each slice across replicas
        for (size_t slice_idx = 0; slice_idx < slice_sizes.size();
             ++slice_idx) {
            auto slice_replicas = allocateSlice(allocators, allocators_by_name,
                                                slice_sizes[slice_idx],
                                                actual_replica_count, config);

            if (slice_replicas.empty()) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }

            if (slice_replicas.size() < actual_replica_count) {
                actual_replica_count = slice_replicas.size();
                // NOTE: replica allocation is best effort
                LOG(WARNING)
                    << "Failed to allocate all replicas for slice " << slice_idx
                    << ", reducing replica count to " << actual_replica_count;

                // Resize replica_buffers to match the new count
                replica_buffers.resize(actual_replica_count);
            }

            for (size_t replica_idx = 0; replica_idx < actual_replica_count;
                 ++replica_idx) {
                replica_buffers[replica_idx].push_back(
                    std::move(slice_replicas[replica_idx]));
            }
        }

        std::vector<Replica> replicas;
        replicas.reserve(actual_replica_count);
        for (size_t replica_idx = 0; replica_idx < actual_replica_count;
             ++replica_idx) {
            replicas.emplace_back(std::move(replica_buffers[replica_idx]),
                                  ReplicaStatus::PROCESSING);
        }

        return replicas;
    }

    std::optional<ErrorCode> validateInput(
        const std::vector<size_t>& slice_sizes, size_t replica_num) const {
        if (replica_num == 0 || slice_sizes.empty() ||
            std::count(slice_sizes.begin(), slice_sizes.end(), 0) > 0) {
            return ErrorCode::INVALID_PARAMS;
        }

        return std::nullopt;
    }

    /**
     * @brief Allocates replicas for a single slice across different segments
     */
    std::vector<std::unique_ptr<AllocatedBuffer>> allocateSlice(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        size_t slice_size, size_t replica_num, const ReplicateConfig& config,
        std::unordered_set<std::string>& used_segments) {
        std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
        buffers.reserve(replica_num);

        for (size_t i = 0; i < replica_num; ++i) {
            auto buffer =
                allocateSingleBuffer(allocators, allocators_by_name, slice_size,
                                     config, used_segments);

            if (!buffer) {
                break;
            }

            used_segments.insert(buffer->getSegmentName());
            buffers.push_back(std::move(buffer));
        }

        return buffers;
    }

    std::vector<std::unique_ptr<AllocatedBuffer>> allocateSlice(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        size_t slice_size, size_t replica_num, const ReplicateConfig& config) {
        std::unordered_set<std::string> empty_segments;
        return allocateSlice(allocators, allocators_by_name, slice_size,
                             replica_num, config, empty_segments);
    }

    /**
     * @brief Allocates a single buffer respecting preferences and exclusions
     */
    std::unique_ptr<AllocatedBuffer> allocateSingleBuffer(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        size_t size, const ReplicateConfig& config,
        const std::unordered_set<std::string>& excluded_segments) {
        // Try preferred segment first
        if (!config.preferred_segment.empty() &&
            !excluded_segments.contains(config.preferred_segment)) {
            auto preferred_it =
                allocators_by_name.find(config.preferred_segment);
            if (preferred_it != allocators_by_name.end()) {
                for (auto& allocator : preferred_it->second) {
                    if (auto buffer = allocator->allocate(size)) {
                        return buffer;
                    }
                }
            }
        }

        return tryRandomAllocate(allocators, size, excluded_segments);
    }

    /**
     * @brief Attempts allocation with random selection from allocators that can
     * fit the size
     */
    std::unique_ptr<AllocatedBuffer> tryRandomAllocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        size_t size, const std::unordered_set<std::string>& excluded_segments) {
        std::vector<size_t> eligible_indices;
        eligible_indices.reserve(allocators.size());
        for (size_t i = 0; i < allocators.size(); ++i) {
            if (!excluded_segments.contains(allocators[i]->getSegmentName()) &&
                allocators[i]->getLargestFreeRegion() >= size) {
                eligible_indices.push_back(i);
            }
        }

        if (eligible_indices.empty()) {
            return nullptr;
        }

        // Thread-local random number generator for thread safety
        thread_local std::mt19937 rng(std::random_device{}());
        std::shuffle(eligible_indices.begin(), eligible_indices.end(), rng);

        const size_t max_tries =
            std::min(kMaxRetryLimit, eligible_indices.size());
        for (size_t i = 0; i < max_tries; ++i) {
            auto& allocator = allocators[eligible_indices[i]];
            if (auto buffer = allocator->allocate(size)) {
                return buffer;
            }
            retry_counter_.fetch_add(1);  // Track allocation attempts
        }

        return nullptr;
    }

    /**
     * @brief Get the number of allocation retry attempts
     */
    uint64_t getRetryCount() const { return retry_counter_.load(); }

    /**
     * @brief Reset the retry counter
     */
    void resetRetryCount() { retry_counter_.store(0); }

   private:
    static constexpr size_t kMaxRetryLimit = 10;
    // Observer for allocation retries
    std::atomic_uint64_t retry_counter_{0};
};

}  // namespace mooncake
