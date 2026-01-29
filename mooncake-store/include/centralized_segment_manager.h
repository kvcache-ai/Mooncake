#pragma once

#include "allocation_strategy.h"
#include "allocator.h"
#include <boost/functional/hash.hpp>

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

struct CentralizedSegment : public Segment {
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

class CentralizedSegmentManager {
   public:
    /**
     * @brief Constructor for CentralizedSegmentManager
     * @param memory_allocator Type of buffer allocator to use for new segments
     */
    explicit CentralizedSegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET)
        : allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
          memory_allocator_(memory_allocator) {}

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id,
                           std::function<ErrorCode()>& pre_func);

    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id,
                             std::function<ErrorCode()>& pre_func);

    ErrorCode UnmountSegment(const UUID& segment_id, const UUID& client_id);
    ErrorCode BatchUnmountSegments(
        const std::vector<UUID>& unmount_segments,
        const std::vector<UUID>& client_ids,
        const std::vector<std::string>& segment_names);

    /**
     * @brief Get all the segments of a client
     */
    ErrorCode GetClientSegments(
        const UUID& client_id,
        std::vector<std::shared_ptr<Segment>>& segments) const;

    /**
     * @brief Prepare to unmount a segment by deleting its allocator
     */
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    size_t& metrics_dec_capacity,
                                    std::string& segment_name);
    ErrorCode BatchPrepareUnmountClientSegments(
        const std::vector<UUID>& clients, std::vector<UUID>& unmount_segments,
        std::vector<size_t>& dec_capacities, std::vector<UUID>& client_ids,
        std::vector<std::string>& segment_names);

    ErrorCode MountLocalDiskSegment(const UUID& client_id,
                                    bool enable_offloading);
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    ErrorCode PushOffloadingQueue(const std::string& key, const int64_t size,
                                  const std::string& segment_name);
    /**
     * @brief Get the segment by name. If there are multiple segments with the
     * same name, return the first one.
     */
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);
    ErrorCode QueryIp(const UUID& client_id, std::vector<std::string>& result);

   public:
    inline tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const uint64_t slice_length, const size_t replica_num,
        const std::vector<std::string>& preferred_segments) {
        return allocation_strategy_->Allocate(allocator_manager_, slice_length,
                                              replica_num, preferred_segments);
    }

   protected:
    ErrorCode InnerCheckMountSegment(const Segment& segment,
                                     const UUID& client_id);
    virtual ErrorCode InnerMountSegment(const Segment& segment,
                                        const UUID& client_id);
    ErrorCode InnerPrepareUnmountSegment(CentralizedSegment& mounted_segment);

    ErrorCode InnerUnmountSegment(const UUID& segment_id,
                                  const UUID& client_id);
    ErrorCode InnerGetClientSegments(
        const UUID& client_id,
        std::vector<std::shared_ptr<Segment>>& segments) const;

   private:
    static constexpr size_t OFFLOADING_QUEUE_LIMIT = 50000;

    mutable SharedMutex segment_mutex_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;
    const BufferAllocatorType
        memory_allocator_;  // Type of buffer allocator to use
    // allocator_manager_ only contains allocators whose segment status is OK.
    AllocatorManager allocator_manager_;
    std::unordered_map<std::string, UUID> client_by_name_
        GUARDED_BY(segment_mutex_);  // segment name -> client_id
    std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                       boost::hash<UUID>>
        client_local_disk_segment_
            GUARDED_BY(segment_mutex_);  // client_id -> local_disk_segment

    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_
            GUARDED_BY(segment_mutex_);  // client_id -> segment_ids

    friend class SegmentTest;  // for unit tests
};

}  // namespace mooncake