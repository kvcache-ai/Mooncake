#pragma once

#include "allocator.h"
#include "segment_manager.h"
#include <boost/functional/hash.hpp>

namespace mooncake {

// Although MountedCentralizedSegments are managed locally,
// buf_allocator of them are also registered to the upper-level
// CentralizedClientManager's global_allocator_manager_ via
// AllocatorChangeCallback, so that the Allocate() of segment can work globally.
struct MountedCentralizedSegment : public Segment {
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

class CentralizedSegmentManager : public SegmentManager {
   public:
    /**
     * @brief Constructor for CentralizedSegmentManager
     * @param memory_allocator Type of buffer allocator to use for new segments
     */
    explicit CentralizedSegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET)
        : memory_allocator_(memory_allocator) {}

    /**
     * @brief Callback for allocator changes (add/remove).
     * Registered by CentralizedClientManager to sync allocators with its
     * global_allocator_manager_, enabling global Allocate() without
     * iterating per-client segment managers.
     * @param segment_name Name of the segment
     * @param allocator The allocator being added or removed
     * @param is_add true if allocator is being added, false if removed
     * @return ErrorCode::OK on success
     */
    using AllocatorChangeCallback = std::function<tl::expected<void, ErrorCode>(
        const std::string& segment_name,
        const std::shared_ptr<BufferAllocatorBase>& allocator, bool is_add)>;

    void SetAllocatorChangeCallback(AllocatorChangeCallback cb);

    // Set the global visibility of all segments managed by this manager.
    // When visible is true, all segments are added to the global allocator
    // manager. When visible is false, all segments are removed from the
    // global allocator manager.
    tl::expected<void, ErrorCode> SetGlobalVisibility(bool visible);

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> override;

    auto MountLocalDiskSegment(bool enable_offloading)
        -> tl::expected<void, ErrorCode>;
    auto OffloadObjectHeartbeat(bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    auto PushOffloadingQueue(const std::string& key, const int64_t size,
                             const std::string& segment_name)
        -> tl::expected<void, ErrorCode>;

    auto QueryIp() -> tl::expected<std::vector<std::string>, ErrorCode>;

   protected:
    ErrorCode InnerCheckMountSegment(const Segment& segment);
    tl::expected<void, ErrorCode> InnerMountSegment(
        const Segment& segment) override;
    auto OnUnmountSegment(const std::shared_ptr<Segment>& segment)
        -> tl::expected<void, ErrorCode> override;

   private:
    static constexpr size_t OFFLOADING_QUEUE_LIMIT = 50000;

    const BufferAllocatorType
        memory_allocator_;  // Type of buffer allocator to use
    AllocatorChangeCallback allocator_change_cb_;
    std::shared_ptr<LocalDiskSegment> local_disk_segment_
        GUARDED_BY(segment_mutex_);

    friend class SegmentTest;  // for unit tests
};

}  // namespace mooncake