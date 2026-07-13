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

class SegmentAllocator {
   public:
    explicit SegmentAllocator(
        std::shared_ptr<BufferAllocatorBase> buffer_allocator)
        : allocator_(std::move(buffer_allocator)) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) const {
        if (!allocator_) {
            return nullptr;
        }
        auto buffer = allocator_->allocate(size);
        if (buffer) {
            buffer->bindSegmentLifetime(lifetime_);
        }
        return buffer;
    }

    [[nodiscard]] size_t size() const {
        return allocator_ ? allocator_->size() : 0;
    }

    [[nodiscard]] size_t capacity() const {
        return allocator_ ? allocator_->capacity() : 0;
    }

    [[nodiscard]] size_t getTotalFreeSpace() const {
        return allocator_ ? allocator_->getTotalFreeSpace()
                          : kAllocatorUnknownFreeSpace;
    }

    [[nodiscard]] size_t getLargestFreeRegion() const {
        return allocator_ ? allocator_->getLargestFreeRegion()
                          : kAllocatorUnknownFreeSpace;
    }

    [[nodiscard]] bool isAvailable() const { return lifetime_.isAvailable(); }

    void setAvailable(bool available) const {
        lifetime_.setAvailable(available);
    }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime lifetime_;
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
        const std::shared_ptr<BufferAllocatorBase>& allocator) {
        return addRegistration(name,
                               std::make_shared<SegmentAllocator>(allocator));
    }

    SegmentAllocatorRegistration addRegistration(
        const std::string& name, SegmentAllocatorRegistration registration) {
        if (!registration) {
            return nullptr;
        }
        if (!registrations_.contains(name)) {
            names_.push_back(name);
        }
        registrations_[name].push_back(registration);
        return registration;
    }

    /**
     * @brief Remove an allocator registration of segment `name` from the
     *        manager. This also removes the name if there are no registrations
     *        after the removal.
     * @param name the name of the segment
     * @param registration the registration to remove from the segment
     * @param invalidate whether buffers allocated by this registration should
     *                   become unavailable
     * @return true if the registration is removed, false if it does not exist
     */
    bool removeRegistration(const std::string& name,
                            const SegmentAllocatorRegistration& registration,
                            bool invalidate = true) {
        if (!registration) {
            return false;
        }
        if (invalidate) {
            registration->setAvailable(false);
        }

        auto registrations_it = registrations_.find(name);
        if (registrations_it == registrations_.end()) {
            return false;
        }

        auto registration_it =
            std::find(registrations_it->second.begin(),
                      registrations_it->second.end(), registration);
        if (registration_it == registrations_it->second.end()) {
            return false;
        }

        registrations_it->second.erase(registration_it);
        if (registrations_it->second.empty()) {
            registrations_.erase(registrations_it);
            auto name_it = std::find(names_.begin(), names_.end(), name);
            if (name_it != names_.end()) {
                std::swap(*name_it, names_.back());
                names_.pop_back();
            }
        }
        return true;
    }

    /**
     * @brief Get the names of all segments. This returns a vector of the
     *        names so that we can randomly pick a segment without traversing.
     * @return a vector of names of all mounted segments
     */
    const std::vector<std::string>& getNames() const { return names_; }

    const std::vector<SegmentAllocatorRegistration>* getRegistrations(
        const std::string& name) const {
        auto it = registrations_.find(name);
        return it == registrations_.end() ? nullptr : &it->second;
    }

   private:
    // Name array for randomly picking allocator registrations.
    std::vector<std::string> names_;
    // Segment name to allocator registrations mapping.
    std::unordered_map<std::string, std::vector<SegmentAllocatorRegistration>>
        registrations_;
    friend class SegmentSerializer;  // for fork serialize
};

class SsdMetricsProvider {
   public:
    virtual ~SsdMetricsProvider() = default;
    virtual int64_t getSsdTotalCapacity(
        const std::string& segment_name) const = 0;
    virtual int64_t getSsdUsedBytes(const std::string& segment_name) const = 0;
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
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type,
        const SsdMetricsProvider* ssd_provider) {
        (void)ssd_provider;
        return Allocate(allocator_manager, slice_length, replica_num,
                        preferred_segments, excluded_segments, replica_type);
    }

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
                                         slice_length, generator);
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
        // Random number generator.
        static thread_local std::mt19937 generator(std::random_device{}());

        // Validate input parameters
        if (slice_length == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Check segment existence
        if (allocator_manager.getRegistrations(segment_name) == nullptr) {
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
        const auto registrations = allocator_manager.getRegistrations(name);
        if (registrations == nullptr || registrations->empty()) {
            return nullptr;
        }

        const auto num_segs = registrations->size();
        if (num_segs == 1) {
            // Fast path for single segment
            const auto& registration = (*registrations)[0];
            return registration->allocate(slice_length);
        }

        // Randomly select a start point to distribute
        // allocations across all segments
        std::uniform_int_distribution<size_t> dist(0, num_segs - 1);
        size_t seg_offset =
            dist(generator);  // select a start segment to place replica
        for (size_t i = 0; i < num_segs; i++) {  // only allocate one replica
            const auto& registration =
                (*registrations)[(i + seg_offset) % num_segs];
            if (auto buffer = registration->allocate(slice_length)) {
                return buffer;
            }
        }

        return nullptr;
    }

   private:
    static constexpr size_t kMaxRetryLimit = 100;
};

/**
 * @brief Free-ratio-first allocation strategy.
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
class FreeRatioFirstAllocationStrategy : public RandomAllocationStrategy {
   public:
    FreeRatioFirstAllocationStrategy() = default;

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
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(preferred_segment);
                if (replicas.size() == replica_num) {
                    return replicas;
                }
            }
        }

        const size_t remaining = replica_num - replicas.size();

        // --- Sample candidates: pick a random start, take 6*remaining
        // consecutive segments, then sort by free space ---
        size_t sample_count =
            std::min(kCandidateMultiplier * remaining, names.size());

        std::uniform_int_distribution<size_t> start_dist(0, names.size() - 1);
        size_t start_idx = start_dist(generator);

        struct Candidate {
            size_t name_idx;
            double free_ratio;  // free_bytes / capacity
        };
        std::vector<Candidate> candidates;
        candidates.reserve(sample_count);

        for (size_t i = 0; i < sample_count; ++i) {
            size_t idx = (start_idx + i) % names.size();
            double free_ratio =
                getSegmentFreeRatio(allocator_manager, names[idx]);
            candidates.push_back({idx, free_ratio});
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
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(name);
            }
        }

        if (replicas.size() >= replica_num) {
            return replicas;
        }

        // --- Fallback: Random allocation for any remaining replicas ---
        std::uniform_int_distribution<size_t> distribution(0, names.size() - 1);
        size_t fallback_idx = distribution(generator);
        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;

        while (replicas.size() < replica_num && try_count < max_retry) {
            auto index = fallback_idx % names.size();
            fallback_idx++;
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
                                      ReplicaStatus::PROCESSING, replica_type);
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
    static constexpr size_t kCandidateMultiplier = 6;

    double getSegmentFreeRatio(const AllocatorManager& allocator_manager,
                               const std::string& name) {
        auto registrations = allocator_manager.getRegistrations(name);
        if (!registrations || registrations->empty()) return 0.0;

        uint64_t total_capacity = 0;
        uint64_t total_free = 0;
        for (const auto& registration : *registrations) {
            if (!registration) continue;
            auto cap = static_cast<uint64_t>(registration->capacity());
            total_capacity += cap;
            total_free += cap - static_cast<uint64_t>(registration->size());
        }

        if (total_capacity == 0) return 0.0;
        return static_cast<double>(total_free) /
               static_cast<double>(total_capacity);
    }
};

class SsdFreeRatioFirstAllocationStrategy : public RandomAllocationStrategy {
   public:
    SsdFreeRatioFirstAllocationStrategy() = default;

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type,
        const SsdMetricsProvider* ssd_provider) override {
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

        // Handle preferred segments first
        for (const auto& preferred_segment : preferred_segments) {
            if (excluded_segments.contains(preferred_segment) ||
                used_segments.contains(preferred_segment)) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, preferred_segment,
                                         slice_length, generator);
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

        // Sample candidates and sort by SSD free ratio
        size_t sample_count =
            std::min(kCandidateMultiplier * remaining, names.size());

        std::uniform_int_distribution<size_t> start_dist(0, names.size() - 1);
        size_t start_idx = start_dist(generator);

        struct Candidate {
            size_t name_idx;
            double ssd_free_ratio;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(sample_count);

        for (size_t i = 0; i < sample_count; ++i) {
            size_t idx = (start_idx + i) % names.size();
            const auto& name = names[idx];

            if (excluded_segments.contains(name) ||
                used_segments.contains(name)) {
                continue;
            }

            double ssd_free_ratio = getSegmentSsdFreeRatio(name, ssd_provider);
            candidates.push_back({idx, ssd_free_ratio});
        }

        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& a, const Candidate& b) {
                      return a.ssd_free_ratio > b.ssd_free_ratio;
                  });

        for (const auto& candidate : candidates) {
            if (replicas.size() >= replica_num) {
                break;
            }

            const auto& name = names[candidate.name_idx];
            auto buffer = allocateSingle(allocator_manager, name, slice_length,
                                         generator);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
                used_segments.insert(name);
            }
        }

        if (replicas.size() >= replica_num) {
            return replicas;
        }

        // Fallback: Random allocation for remaining replicas
        std::uniform_int_distribution<size_t> distribution(0, names.size() - 1);
        size_t fallback_idx = distribution(generator);
        const size_t max_retry = std::min(kMaxRetryLimit, names.size());
        size_t try_count = 0;

        while (replicas.size() < replica_num && try_count < max_retry) {
            auto index = fallback_idx % names.size();
            fallback_idx++;
            try_count++;

            const auto& name = names[index];

            if (excluded_segments.contains(name) ||
                used_segments.contains(name)) {
                continue;
            }

            auto buffer = allocateSingle(allocator_manager, name, slice_length,
                                         generator);
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

    using AllocationStrategy::Allocate;

   private:
    static constexpr size_t kMaxRetryLimit = 100;
    static constexpr size_t kCandidateMultiplier = 6;

    double getSegmentSsdFreeRatio(
        const std::string& name, const SsdMetricsProvider* ssd_provider) const {
        if (!ssd_provider) return 1.0;
        int64_t total = ssd_provider->getSsdTotalCapacity(name);
        if (total <= 0) return 1.0;
        int64_t used = ssd_provider->getSsdUsedBytes(name);
        used = std::clamp<int64_t>(used, 0, total);
        int64_t free_bytes = total - used;
        return static_cast<double>(free_bytes) / static_cast<double>(total);
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

        const auto cxl_registrations =
            allocator_manager.getRegistrations(cxl_segment_name);

        if (cxl_registrations == nullptr || cxl_registrations->empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        std::vector<Replica> replicas;
        replicas.reserve(replica_num);

        auto buffer = (*cxl_registrations)[0]->allocate(slice_length);
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
    AllocationStrategyType type) {
    switch (type) {
        case AllocationStrategyType::RANDOM:
            return std::make_shared<RandomAllocationStrategy>();
        case AllocationStrategyType::FREE_RATIO_FIRST:
            return std::make_shared<FreeRatioFirstAllocationStrategy>();
        case AllocationStrategyType::CXL:
            return std::make_shared<CxlAllocationStrategy>();
        case AllocationStrategyType::SSD_FREE_RATIO_FIRST:
            return std::make_shared<SsdFreeRatioFirstAllocationStrategy>();
        case AllocationStrategyType::LOCAL_FIRST:
            return std::make_shared<RandomAllocationStrategy>();
        default:
            return std::make_shared<RandomAllocationStrategy>();
    }
}

}  // namespace mooncake
