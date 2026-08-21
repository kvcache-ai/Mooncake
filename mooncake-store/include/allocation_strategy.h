#pragma once

#include <algorithm>
#include <memory>
#include <random>
#include <stdexcept>
#include <set>
#include <string>
#include <unordered_map>
#include <iterator>
#include <time.h>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"  // Contains BufferAllocator declaration
#include "replica.h"
#include "types.h"
#include "random.h"

namespace mooncake {

class LocalSsdManager;
class ScopedAllocatorAccess;

struct SegmentAllocator {
    explicit SegmentAllocator(
        std::shared_ptr<BufferAllocatorBase> buffer_allocator,
        std::shared_ptr<ClientLivenessRecord> client_liveness)
        : allocator(std::move(buffer_allocator)),
          client_liveness(std::move(client_liveness)) {
        if (!this->client_liveness) {
            throw std::invalid_argument(
                "SegmentAllocator requires Client liveness");
        }
    }

    std::shared_ptr<BufferAllocatorBase> allocator;
    SegmentLifetime lifetime;

    [[nodiscard]] std::shared_ptr<ClientLivenessRecord> GetClientLiveness()
        const {
        return std::atomic_load_explicit(&client_liveness,
                                         std::memory_order_acquire);
    }

    [[nodiscard]] bool IsServing() const {
        const auto record = GetClientLiveness();
        return allocator && record && record->IsServing() &&
               lifetime.isAvailable();
    }

    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> record) {
        if (!record) {
            throw std::invalid_argument(
                "SegmentAllocator requires Client liveness");
        }
        std::atomic_store_explicit(&client_liveness, std::move(record),
                                   std::memory_order_release);
    }

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) const {
        if (!allocator || !lifetime.isAvailable()) {
            return nullptr;
        }
        const auto record = GetClientLiveness();
        if (!record) {
            return nullptr;
        }
        auto serving_guard = record->TryAcquireServingGuard();
        if (!serving_guard) {
            return nullptr;
        }
        auto buffer = allocator->allocate(size);
        if (!buffer) {
            return nullptr;
        }
        buffer->bindSegmentLifetime(lifetime);
        buffer->bindClientLiveness(record);
        if (!lifetime.isAvailable()) {
            return nullptr;
        }
        return buffer;
    }

   private:
    // ReMount may replace a restore-time gate while descriptor reads and
    // allocation attempts are concurrent.
    std::shared_ptr<ClientLivenessRecord> client_liveness;
};

using SegmentAllocatorRegistration = std::shared_ptr<SegmentAllocator>;

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
    SegmentAllocatorRegistration addAllocator(
        const std::string& name,
        const std::shared_ptr<BufferAllocatorBase>& allocator,
        std::shared_ptr<ClientLivenessRecord> client_liveness) {
        auto registration = std::make_shared<SegmentAllocator>(
            allocator, std::move(client_liveness));
        addRegistration(name, registration);
        return registration;
    }

    // NoF availability is managed by its own heartbeat state machine. Keep
    // that independent path explicit instead of using nullptr to mean Active.
    SegmentAllocatorRegistration addIndependentAllocator(
        const std::string& name,
        const std::shared_ptr<BufferAllocatorBase>& allocator) {
        return addAllocator(
            name, allocator,
            std::make_shared<ClientLivenessRecord>(
                ClientLivenessRecord::Clock::now()));
    }

    // Snapshot decoding rebuilds allocator registrations before MasterService
    // can reconstruct the canonical per-Client registry. The provisional
    // Active record is replaced by RebuildClientLivenessAfterRestore().
    SegmentAllocatorRegistration addRestoredAllocator(
        const std::string& name,
        const std::shared_ptr<BufferAllocatorBase>& allocator) {
        return addAllocator(
            name, allocator,
            std::make_shared<ClientLivenessRecord>(
                ClientLivenessRecord::Clock::now()));
    }

    void addRegistration(
        const std::string& name,
        const SegmentAllocatorRegistration& registration) {
        if (!registration) {
            throw std::invalid_argument(
                "Cannot add an empty Segment allocator registration");
        }
        if (!allocators_.contains(name)) {
            names_.push_back(name);
        }
        allocators_[name].push_back(registration);
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
        const SegmentAllocatorRegistration& registration) {
        auto it = allocators_.find(name);
        if (it == allocators_.end()) {
            return false;
        }

        // Try removing the allocator.
        bool allocator_removed = false;
        auto alloc_it =
            std::find(it->second.begin(), it->second.end(), registration);
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

    struct Replacement {
        std::string name;
        std::shared_ptr<BufferAllocatorBase> expected;
        std::shared_ptr<BufferAllocatorBase> replacement;
    };

    bool replaceAllocators(const std::vector<Replacement>& replacements) {
        std::vector<decltype(allocators_)::mapped_type::iterator> targets;
        targets.reserve(replacements.size());
        for (const auto& replacement : replacements) {
            auto it = allocators_.find(replacement.name);
            if (it == allocators_.end() || !replacement.replacement) {
                return false;
            }
            // Match on the registration's wrapped allocator: a replacement
            // swaps the underlying buffer allocator in place, preserving the
            // registration identity (SegmentLifetime and Client liveness
            // binding stay attached to the same SegmentAllocator).
            auto target = std::find_if(
                it->second.begin(), it->second.end(),
                [&replacement](const SegmentAllocatorRegistration& reg) {
                    return reg && reg->allocator == replacement.expected;
                });
            if (target == it->second.end()) {
                return false;
            }
            targets.push_back(target);
        }
        for (size_t i = 0; i < replacements.size(); ++i) {
            (*targets[i])->allocator = replacements[i].replacement;
        }
        return true;
    }

    /**
     * @brief Get the names of all segments. This returns a vector of the
     *        names so that we can randomly pick a segment without traversing.
     * @return a vector of names of all mounted segments
     */
    const std::vector<std::string>& getNames() const { return names_; }

    std::vector<std::string> getServingNames() const {
        std::vector<std::string> serving_names;
        serving_names.reserve(names_.size());
        for (const auto& name : names_) {
            const auto* registrations = getAllocators(name);
            if (registrations != nullptr &&
                std::any_of(registrations->begin(), registrations->end(),
                            [](const auto& registration) {
                                return registration &&
                                       registration->IsServing();
                            })) {
                serving_names.push_back(name);
            }
        }
        return serving_names;
    }

    /**
     * @brief Get allocators belongs to the given segment name.
     * @return a vector of allocators belongs to the given segment name
     */
    const std::vector<SegmentAllocatorRegistration>* getAllocators(
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
                       std::vector<SegmentAllocatorRegistration>>
        allocators_;
    friend class SegmentSerializer;  // for fork serialize
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
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY) = 0;

    virtual tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const ScopedAllocatorAccess& placement, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY);

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
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY) override {
        // Validate input parameters
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Check available segments.
        const auto names = allocator_manager.getServingNames();
        if (names.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::vector<Replica> replicas;
        replicas.reserve(replica_num);

        // Fast path: single segment case
        if (names.size() == 1) {
            if (excluded_segments.contains(names[0])) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }

            auto buffer =
                allocateSingle(allocator_manager, names[0], slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
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
                                         slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
                if (replicas.size() == replica_num) {
                    return replicas;
                }

                // Add preferred segment to used_segments on allocation success
                used_segments.insert(preferred_segment);
            }
        }

        // If replica_num is not satisfied, allocate the remaining replicas
        // randomly.
        size_t start_idx = randomIndex(names.size());

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

            auto buffer =
                allocateSingle(allocator_manager, names[index], slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
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
        // Validate input parameters
        if (slice_length == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Check segment existence
        if (allocator_manager.getAllocators(segment_name) == nullptr) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }

        auto buffer =
            allocateSingle(allocator_manager, segment_name, slice_length);
        if (buffer == nullptr) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        return Replica{std::move(buffer), ReplicaStatus::PROCESSING};
    }

    std::unique_ptr<AllocatedBuffer> allocateSingle(
        const AllocatorManager& allocator_manager, const std::string& name,
        const size_t slice_length) {
        const auto allocators = allocator_manager.getAllocators(name);
        if (allocators == nullptr || allocators->size() == 0) {
            return nullptr;
        }

        const auto num_segs = allocators->size();
        if (num_segs == 1) {
            // Fast path for single segment
            const auto& registration = (*allocators)[0];
            return registration ? registration->allocate(slice_length)
                                : nullptr;
        }

        // Randomly select a start point to distribute
        // allocations across all segments
        // Select a start segment to place the replica.
        size_t seg_offset = randomIndex(num_segs);
        for (size_t i = 0; i < num_segs; i++) {  // only allocate one replica
            const auto& registration =
                (*allocators)[(i + seg_offset) % num_segs];
            if (registration) {
                if (auto buffer = registration->allocate(slice_length)) {
                    return buffer;
                }
            }
        }

        return nullptr;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 100;
};

/**
 * @brief Shared sampled-and-ranked placement algorithm.
 *
 * Derived strategies provide only the score for each candidate segment.
 */
class RankedAllocationStrategy : public RandomAllocationStrategy {
   protected:
    template <typename ScoreFn>
    tl::expected<std::vector<Replica>, ErrorCode> AllocateRanked(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type, ScoreFn&& score) {
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const auto names = allocator_manager.getServingNames();
        if (names.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::vector<Replica> replicas;
        replicas.reserve(replica_num);
        std::set<std::string> used_segments;

        for (const auto& preferred_segment : preferred_segments) {
            if (excluded_segments.contains(preferred_segment) ||
                used_segments.contains(preferred_segment)) {
                continue;
            }
            auto buffer = allocateSingle(allocator_manager, preferred_segment,
                                         slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(preferred_segment);
                if (replicas.size() == replica_num) {
                    return replicas;
                }
            }
        }

        const size_t remaining = replica_num - replicas.size();
        const size_t sample_count =
            std::min(kCandidateMultiplier * remaining, names.size());
        const size_t start_idx = randomIndex(names.size());

        struct Candidate {
            size_t name_idx;
            double score;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(sample_count);
        for (size_t i = 0; i < sample_count; ++i) {
            const size_t idx = (start_idx + i) % names.size();
            const auto& name = names[idx];
            if (excluded_segments.contains(name) ||
                used_segments.contains(name)) {
                continue;
            }
            candidates.push_back({idx, score(name)});
        }

        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& lhs, const Candidate& rhs) {
                      return lhs.score > rhs.score;
                  });
        for (const auto& candidate : candidates) {
            if (replicas.size() >= replica_num) {
                break;
            }
            const auto& name = names[candidate.name_idx];
            auto buffer = allocateSingle(allocator_manager, name, slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(name);
            }
        }

        if (replicas.size() >= replica_num) {
            return replicas;
        }

        size_t fallback_idx = randomIndex(names.size());
        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;
        while (replicas.size() < replica_num && try_count < max_retry) {
            const size_t index = fallback_idx % names.size();
            ++fallback_idx;
            ++try_count;
            const auto& name = names[index];
            if (excluded_segments.contains(name) ||
                used_segments.contains(name)) {
                continue;
            }
            auto buffer = allocateSingle(allocator_manager, name, slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(name);
            }
        }

        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 100;
    static constexpr size_t kCandidateMultiplier = 6;
};

class FreeRatioFirstAllocationStrategy final : public RankedAllocationStrategy {
   public:
    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY) override {
        return AllocateRanked(
            allocator_manager, slice_length, replica_num, preferred_segments,
            excluded_segments, replica_type, [&](const std::string& name) {
                return GetSegmentFreeRatio(allocator_manager, name);
            });
    }

   private:
    static double GetSegmentFreeRatio(const AllocatorManager& allocator_manager,
                                      const std::string& name) {
        const auto* allocators = allocator_manager.getAllocators(name);
        if (!allocators || allocators->empty()) {
            return 0.0;
        }

        uint64_t total_capacity = 0;
        uint64_t total_free = 0;
        for (const auto& allocator : *allocators) {
            if (!allocator || !allocator->IsServing()) {
                continue;
            }
            const auto capacity =
                static_cast<uint64_t>(allocator->allocator->capacity());
            total_capacity += capacity;
            total_free += capacity -
                          static_cast<uint64_t>(allocator->allocator->size());
        }
        if (total_capacity == 0) {
            return 0.0;
        }
        return static_cast<double>(total_free) /
               static_cast<double>(total_capacity);
    }
};

class SsdFreeRatioFirstAllocationStrategy final
    : public RankedAllocationStrategy {
   public:
    explicit SsdFreeRatioFirstAllocationStrategy(
        const LocalSsdManager& local_ssd)
        : local_ssd_(local_ssd) {}

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const ScopedAllocatorAccess& placement, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY) override;

    using RandomAllocationStrategy::Allocate;

   private:
    const LocalSsdManager& local_ssd_;
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
            std::set<std::string>(),
        const ReplicaType replica_type = ReplicaType::MEMORY) override {
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
        std::vector<Replica> replicas;
        replicas.reserve(replica_num);

        std::unique_ptr<AllocatedBuffer> buffer;
        for (const auto& registration : *cxl_allocators) {
            if (registration && registration->IsServing()) {
                buffer = registration->allocate(slice_length);
                if (buffer) {
                    break;
                }
            }
        }
        if (!buffer) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        buffer->change_to_cxl(cxl_segment_name);
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                              replica_type);

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
    AllocationStrategyType type, const LocalSsdManager& local_ssd) {
    switch (type) {
        case AllocationStrategyType::RANDOM:
            return std::make_shared<RandomAllocationStrategy>();
        case AllocationStrategyType::FREE_RATIO_FIRST:
            return std::make_shared<FreeRatioFirstAllocationStrategy>();
        case AllocationStrategyType::CXL:
            return std::make_shared<CxlAllocationStrategy>();
        case AllocationStrategyType::SSD_FREE_RATIO_FIRST:
            return std::make_shared<SsdFreeRatioFirstAllocationStrategy>(
                local_ssd);
        case AllocationStrategyType::LOCAL_FIRST:
            return std::make_shared<RandomAllocationStrategy>();
        default:
            return std::make_shared<RandomAllocationStrategy>();
    }
}

}  // namespace mooncake
