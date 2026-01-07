#pragma once

#include <boost/functional/hash.hpp>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

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
    std::shared_ptr<BufferAllocatorBase> buf_allocator;
};

struct LocalDiskSegment {
    mutable Mutex offloading_mutex_;
    bool enable_offloading;
    std::unordered_map<std::string, int64_t> GUARDED_BY(offloading_mutex_)
        offloading_objects;
    explicit LocalDiskSegment(bool enable_offloading)
        : enable_offloading(enable_offloading) {}

    LocalDiskSegment(const LocalDiskSegment&) = delete;
    LocalDiskSegment& operator=(const LocalDiskSegment&) = delete;

    LocalDiskSegment(LocalDiskSegment&&) = delete;
    LocalDiskSegment& operator=(LocalDiskSegment&&) = delete;
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
    explicit ScopedSegmentAccess(SegmentManager* segment_manager,
                                 std::shared_mutex& mutex)
        : segment_manager_(segment_manager), lock_(mutex) {}

    /**
     * @brief Mount a segment
     */
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);

    ErrorCode MountLocalDiskSegment(const UUID& client_id,
                                    bool enable_offloading);

    /**
     * @brief Re-mount a segment. To avoid infinite remount trying, only the
     * errors that may be solved by subsequent remount tryings are considered as
     * errors. When encounters unsolvable errors, the segment will not be
     * mounted while the return value will be OK.
     */
    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id);

    /**
     * @brief Prepare to unmount a segment by deleting its allocator
     */
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    size_t& metrics_dec_capacity);

    /**
     * @brief Deleting the segment to complete the unmounting operation
     */
    ErrorCode CommitUnmountSegment(const UUID& segment_id,
                                   const UUID& client_id,
                                   const size_t& metrics_dec_capacity);

    /**
     * @brief Get all the segments of a client
     */
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;

    /**
     * @brief Get the names of all the segments
     */
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);

    /**
     * @brief Get the segment by name. If there are multiple segments with the
     * same name, return the first one.
     */
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);

    /**
     * @brief Get the client id by segment name.
     */
    ErrorCode GetClientIdBySegmentName(const std::string& segment_name,
                                       UUID& client_id) const;

    /**
     * @brief Check if a segment name exists
     */
    bool ExistsSegmentName(const std::string& segment_name) const;

   private:
    SegmentManager* segment_manager_;
    std::unique_lock<std::shared_mutex> lock_;
};

/**
 * @brief RAII-style access to allocators for thread-safe allocator usage
 */
class ScopedAllocatorAccess {
   public:
    explicit ScopedAllocatorAccess(const AllocatorManager& allocator_manager,
                                   std::shared_mutex& mutex)
        : allocator_manager_(allocator_manager), lock_(mutex) {}

    const AllocatorManager& getAllocatorManager() { return allocator_manager_; }

   private:
    const AllocatorManager& allocator_manager_;
    std::shared_lock<std::shared_mutex> lock_;
};

/**
 * @brief RAII-style access to LocalDiskOffloadingQueues for thread-safe
 * LocalDiskOffloadingQueue usage
 */
class ScopedLocalDiskSegmentAccess {
   public:
    explicit ScopedLocalDiskSegmentAccess(
        std::unordered_map<std::string, UUID>& client_by_name,
        std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                           boost::hash<UUID>>& client_local_disk_segment,
        std::shared_mutex& mutex)
        : client_by_name_(client_by_name),
          client_local_disk_segment_(client_local_disk_segment),
          lock_(mutex) {}

    const std::unordered_map<std::string, UUID>& getClientByName() {
        return client_by_name_;
    }

    std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                       boost::hash<UUID>>&
    getClientLocalDiskSegment() {
        return client_local_disk_segment_;
    }

   private:
    const std::unordered_map<std::string, UUID>&
        client_by_name_;  // segment name -> client_id
    std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                       boost::hash<UUID>>& client_local_disk_segment_;
    std::shared_lock<std::shared_mutex> lock_;
};

class SegmentManager {
   public:
    /**
     * @brief Constructor for SegmentManager
     * @param memory_allocator Type of buffer allocator to use for new segments
     */
    explicit SegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB)
        : memory_allocator_(memory_allocator) {}

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
        return ScopedAllocatorAccess(allocator_manager_, segment_mutex_);
    }

    ScopedLocalDiskSegmentAccess getLocalDiskSegmentAccess() {
        return ScopedLocalDiskSegmentAccess(
            client_by_name_, client_local_disk_segment_, segment_mutex_);
    }

   private:
    mutable std::shared_mutex segment_mutex_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;
    const BufferAllocatorType
        memory_allocator_;  // Type of buffer allocator to use
    // allocator_manager_ only contains allocators whose segment status is OK.
    AllocatorManager allocator_manager_;
    std::unordered_map<UUID, MountedSegment, boost::hash<UUID>>
        mounted_segments_;  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_;  // client_id -> segment_ids
    std::unordered_map<std::string, UUID>
        client_by_name_;  // segment name -> client_id
    std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                       boost::hash<UUID>>
        client_local_disk_segment_;  // client_id -> local_disk_segment

    friend class ScopedSegmentAccess;
    friend class SegmentTest;  // for unit tests
};

}  // namespace mooncake
