#pragma once

#include <algorithm>
#include <fmt/format.h>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "types.h"

namespace mooncake {

/**
 * @brief Result of batch allocation operation
 */
struct AllocationResult {
    enum class Status {
        SUCCESS,        // All replicas allocated successfully
        FAILURE,        // No replicas allocated
        INVALID_PARAMS  // Invalid configuration provided
    };

    Status status{Status::FAILURE};
    std::vector<Replica> replicas;
    std::string error_message;

    // Helper methods
    [[nodiscard]] bool isSuccess() const noexcept {
        return status == Status::SUCCESS;
    }
    [[nodiscard]] size_t allocatedReplicasCounts() const noexcept {
        return replicas.size();
    }
};

/**
 * @brief Abstract interface for allocation strategy, responsible for
 *        allocating multiple slices across multiple replicas using available
 *        BufferAllocators.
 */
class AllocationStrategy {
   public:
    virtual ~AllocationStrategy() = default;

    /**
     * @brief Allocates multiple slices across the requested number of replicas.
     *        Each replica will contain all requested slices.
     *
     * @param allocators Container of mounted allocators
     * @param allocators_by_name Container of mounted allocators, key is
     *                          segment_name, value is the corresponding
     * allocators
     * @param slice_sizes Sizes of slices to be allocated in each replica
     * @param config Replica configuration containing number of replicas and
     *               placement constraints
     * @return AllocationResult containing allocated replicas and status.
     *         - SUCCESS: All requested replicas with all slices allocated
     *         - PARTIAL_SUCCESS: Some replicas allocated, check replicas vector
     *         - FAILURE: No allocation possible
     *         - INSUFFICIENT_SPACE: Not enough space for requested allocation
     */
    virtual AllocationResult Allocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const std::vector<size_t>& slice_sizes,
        const ReplicateConfig& config) = 0;

    /**
     * @brief Tries to allocate additional replicas for existing batch.
     *        Useful for increasing replication factor.
     *
     * @param allocators Container of mounted allocators
     * @param allocators_by_name Container of mounted allocators by segment name
     * @param slice_sizes Sizes of slices to be allocated in each new replica
     * @param existing_replicas Already allocated replicas to consider for
     *                         placement constraints
     * @param additional_count Number of additional replicas to allocate
     * @param config Replica configuration
     * @return AllocationResult with newly allocated replicas
     */
    virtual AllocationResult AllocateAdditionalReplicas(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const std::vector<size_t>& slice_sizes,
        const std::vector<Replica>& existing_replicas, size_t additional_count,
        const ReplicateConfig& config) = 0;
};

/**
 * @brief Random batch allocation strategy with local preference and
 *        replication guarantees support.
 *
 * This strategy ensures that for each slice, its replicas are placed in
 * different segments. Different slices may use the same segments.
 */
class RandomAllocationStrategy : public AllocationStrategy {
   public:
    RandomAllocationStrategy() : rng_(std::random_device{}()) {}

    AllocationResult Allocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const std::vector<size_t>& slice_sizes,
        const ReplicateConfig& config) override {
        AllocationResult result;

        if (!validateInput(slice_sizes, config.replica_num, result)) {
            return result;
        }

        std::vector<std::vector<std::unique_ptr<AllocatedBuffer>>>
            replica_buffers(config.replica_num);
        for (auto& replica_buffer : replica_buffers) {
            replica_buffer.reserve(slice_sizes.size());
        }

        // Allocate each slice across replicas
        for (size_t slice_idx = 0; slice_idx < slice_sizes.size();
             ++slice_idx) {
            auto slice_replicas = allocateSlice(allocators, allocators_by_name,
                                                slice_sizes[slice_idx],
                                                config.replica_num, config);

            if (slice_replicas.size() != config.replica_num) {
                result.status = AllocationResult::Status::FAILURE;
                result.error_message = fmt::format(
                    "Failed to allocate {} replicas for slice {} (size: {}). "
                    "Only {} replicas can be allocated.",
                    config.replica_num, slice_idx, slice_sizes[slice_idx],
                    slice_replicas.size());
                return result;
            }

            for (size_t replica_idx = 0; replica_idx < config.replica_num;
                 ++replica_idx) {
                replica_buffers[replica_idx].push_back(
                    std::move(slice_replicas[replica_idx]));
            }
        }

        result.replicas.reserve(config.replica_num);
        for (size_t replica_idx = 0; replica_idx < config.replica_num;
             ++replica_idx) {
            result.replicas.emplace_back(
                std::move(replica_buffers[replica_idx]),
                ReplicaStatus::PROCESSING);
        }
        result.status = AllocationResult::Status::SUCCESS;

        return result;
    }

    AllocationResult AllocateAdditionalReplicas(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<
            std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>&
            allocators_by_name,
        const std::vector<size_t>& slice_sizes,
        const std::vector<Replica>& existing_replicas, size_t additional_count,
        const ReplicateConfig& config) override {
        AllocationResult result;

        if (!validateInput(slice_sizes, additional_count, result)) {
            return result;
        }

        // Extract used segments for each slice from existing replicas
        std::vector<std::unordered_set<std::string>> slice_used_segments =
            extractUsedSegmentsPerSlice(existing_replicas, slice_sizes.size());

        std::vector<std::vector<std::unique_ptr<AllocatedBuffer>>>
            replica_buffers(additional_count);
        for (auto& replica_buffer : replica_buffers) {
            replica_buffer.reserve(slice_sizes.size());
        }

        for (size_t slice_idx = 0; slice_idx < slice_sizes.size();
             ++slice_idx) {
            auto slice_replicas = allocateSlice(
                allocators, allocators_by_name, slice_sizes[slice_idx],
                additional_count, config, slice_used_segments[slice_idx]);

            if (slice_replicas.size() != additional_count) {
                result.status = AllocationResult::Status::FAILURE;
                result.error_message = fmt::format(
                    "Could only allocate {} out of {} additional replicas for "
                    "slice {}",
                    slice_replicas.size(), additional_count, slice_idx);
                return result;
            }

            for (size_t replica_idx = 0; replica_idx < additional_count;
                 ++replica_idx) {
                replica_buffers[replica_idx].push_back(
                    std::move(slice_replicas[replica_idx]));
            }
        }

        result.replicas.reserve(additional_count);
        for (size_t replica_idx = 0; replica_idx < additional_count;
             ++replica_idx) {
            result.replicas.emplace_back(
                std::move(replica_buffers[replica_idx]),
                ReplicaStatus::PROCESSING);
        }

        if (result.replicas.size() == additional_count) {
            result.status = AllocationResult::Status::SUCCESS;
        } else {
            result.status = AllocationResult::Status::FAILURE;
            result.error_message = "Failed to allocate additional replicas";
        }

        return result;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 10;
    std::mt19937 rng_;

    bool validateInput(const std::vector<size_t>& slice_sizes,
                       size_t replica_num, AllocationResult& result) const {
        if (replica_num == 0) {
            result.status = AllocationResult::Status::INVALID_PARAMS;
            result.error_message = "Invalid replica count: 0";
            return false;
        }

        if (slice_sizes.empty()) {
            result.status = AllocationResult::Status::INVALID_PARAMS;
            result.error_message = "Empty slice sizes";
            return false;
        }

        if (std::count(slice_sizes.begin(), slice_sizes.end(), 0) > 0) {
            result.status = AllocationResult::Status::INVALID_PARAMS;
            result.error_message = "Invalid slice size: 0";
            return false;
        }

        return true;
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
     * @brief Attempts allocation with random selection
     */
    std::unique_ptr<AllocatedBuffer> tryRandomAllocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        size_t size, const std::unordered_set<std::string>& excluded_segments) {
        std::vector<size_t> eligible_indices;
        eligible_indices.reserve(allocators.size());
        for (size_t i = 0; i < allocators.size(); ++i) {
            if (!excluded_segments.contains(allocators[i]->getSegmentName())) {
                eligible_indices.push_back(i);
            }
        }

        if (eligible_indices.empty()) {
            return nullptr;
        }

        std::shuffle(eligible_indices.begin(), eligible_indices.end(), rng_);

        const size_t max_tries =
            std::min(kMaxRetryLimit, eligible_indices.size());
        for (size_t i = 0; i < max_tries; ++i) {
            auto& allocator = allocators[eligible_indices[i]];
            if (auto buffer = allocator->allocate(size)) {
                return buffer;
            }
        }

        return nullptr;
    }

    /**
     * @brief Extracts used segments per slice from existing replicas
     */
    std::vector<std::unordered_set<std::string>> extractUsedSegmentsPerSlice(
        const std::vector<Replica>& existing_replicas,
        size_t slice_count) const {
        std::vector<std::unordered_set<std::string>> slice_segments(
            slice_count);

        for (auto& segment_set : slice_segments) {
            segment_set.reserve(existing_replicas.size());
        }

        for (const auto& replica : existing_replicas) {
            auto segment_names = replica.get_segment_names();
            for (size_t i = 0; i < std::min(slice_count, segment_names.size());
                 ++i) {
                if (segment_names[i].has_value()) {
                    slice_segments[i].emplace(
                        std::move(segment_names[i].value()));
                }
            }
        }

        return slice_segments;
    }
};

}  // namespace mooncake
