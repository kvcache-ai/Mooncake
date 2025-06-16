#pragma once

#include <ostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <shared_mutex>
#include <string_view>
#include <variant>
#include <boost/functional/hash.hpp>

#include "types.h"
#include "allocator.h"
#include "allocation_strategy.h"

namespace mooncake {
/**
 * @brief Status of a mounted segment in master
 */
enum class SegmentStatus {
    UNDEFINED = 0,  // Uninitialized
    OK,             // Segment is mounted and available for allocation
    UNMOUNTING,     // Segment is under unmounting
};

/**
 * @brief Stream operator for SegmentStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const SegmentStatus& status) noexcept {
    static const std::unordered_map<SegmentStatus, std::string_view>
        status_strings{{SegmentStatus::UNDEFINED, "UNDEFINED"},
                       {SegmentStatus::OK, "OK"},
                       {SegmentStatus::UNMOUNTING, "UNMOUNTING"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

struct MountedSegment {
    Segment segment;
    SegmentStatus status;
    std::shared_ptr<BufferAllocator> buf_allocator;
};

// Forward declarations
class SegmentManager;

/**
 * @brief RAII-style access to segment mutex for thread-safe segment operations
 */
class ScopedSegmentAccess {
   public:
    /**
     * @brief Acquires a lock on the segment mutex
     * @param mutex Reference to the segment mutex
     */
    explicit ScopedSegmentAccess(SegmentManager* segment_manager, std::shared_mutex& mutex)
        : segment_manager_(segment_manager), lock_(mutex) {}

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);

    ErrorCode ReMountSegment(const std::vector<Segment>& segments, const UUID& client_id);

    ErrorCode PrepareUnmountSegment(const UUID& segment_id, const UUID& client_id, size_t& metrics_dec_capacity);

    ErrorCode CommitUnmountSegment(const UUID& segment_id, const size_t& metrics_dec_capacity);

    ErrorCode GetClientSegments(const UUID& client_id, std::vector<UUID>& segments) const;

    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);

    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);

   private:
    SegmentManager* segment_manager_;
    std::unique_lock<std::shared_mutex> lock_;
};

/**
 * @brief RAII-style access to allocators for thread-safe allocator usage
 */
class ScopedAllocatorAccess {
   public:
    explicit ScopedAllocatorAccess(std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocator>>>&
        allocators_by_name,
        std::vector<std::shared_ptr<BufferAllocator>>&
        allocators,
        std::shared_mutex& mutex)
        : allocators_by_name_(allocators_by_name), 
          allocators_(allocators),
          lock_(mutex) {}

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocator>>>& getAllocatorsByName() {
        return allocators_by_name_;
    }

    std::vector<std::shared_ptr<BufferAllocator>>& getAllocators() {
        return allocators_;
    }

   private:
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocator>>>&
        allocators_by_name_;
    std::vector<std::shared_ptr<BufferAllocator>>& allocators_;
    std::shared_lock<std::shared_mutex> lock_;
};

class SegmentManager {
   public:
    /**
     * @brief Get RAII-style access to segment management operations
     * @return ScopedSegmentAccess object that holds the lock
     */
    ScopedSegmentAccess getSegmentAccess() {
        return ScopedSegmentAccess(this, segment_mutex_);
    }

    /**
     * @brief Get RAII-style access to use allocators
     * @return ScopedAllocatorAccess object that holds the lock
     */
    ScopedAllocatorAccess getAllocatorAccess() {
        return ScopedAllocatorAccess(allocators_by_name_, allocators_, segment_mutex_);
    }

   private:

    mutable std::shared_mutex segment_mutex_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocator>>>
        allocators_by_name_;  // segment name -> allocators
    std::vector<std::shared_ptr<BufferAllocator>>
        allocators_;  // allocators
    std::unordered_map<UUID, MountedSegment, boost::hash<UUID>>
        mounted_segments_; // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_;  // client_id -> segment_ids

    friend class ScopedSegmentAccess;
};

}  // namespace mooncake