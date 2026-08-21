#pragma once

#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "allocator.h"
#include "segment/region.h"
#include "types.h"

namespace mooncake {

enum class SegmentStatus {
    UNDEFINED = 0,
    OK,
    DRAINING,
    DRAINED,
    GRACEFULLY_UNMOUNTING,
    UNMOUNTING,
};

constexpr int SegmentStatusAvailabilityRank(SegmentStatus status) noexcept {
    switch (status) {
        case SegmentStatus::OK:
            return 0;
        case SegmentStatus::DRAINING:
            return 1;
        case SegmentStatus::DRAINED:
            return 2;
        case SegmentStatus::GRACEFULLY_UNMOUNTING:
            return 3;
        case SegmentStatus::UNMOUNTING:
            return 4;
        case SegmentStatus::UNDEFINED:
            return 5;
    }
    return 5;
}

inline std::ostream& operator<<(std::ostream& os,
                                const SegmentStatus& status) noexcept {
    static const std::unordered_map<SegmentStatus, std::string_view>
        status_strings{
            {SegmentStatus::UNDEFINED, "UNDEFINED"},
            {SegmentStatus::OK, "OK"},
            {SegmentStatus::DRAINING, "DRAINING"},
            {SegmentStatus::DRAINED, "DRAINED"},
            {SegmentStatus::GRACEFULLY_UNMOUNTING, "GRACEFULLY_UNMOUNTING"},
            {SegmentStatus::UNMOUNTING, "UNMOUNTING"}};
    os << (status_strings.contains(status) ? status_strings.at(status)
                                           : "UNKNOWN");
    return os;
}

struct MountedRegion {
    Segment segment;
    UUID client_id{0, 0};
    SegmentStatus status{SegmentStatus::UNDEFINED};
    RegionKind kind{RegionKind::HOST_MEMORY};
};

struct SegmentAllocationRequest final {
    size_t size{0};
    size_t replica_count{1};
    std::string_view preferred_group;
    std::span<const std::string> preferred_groups;
    std::span<const std::string> excluded_groups;
    ReplicaType replica_type{ReplicaType::MEMORY};
    std::string_view writer_host_id;
    std::string_view object_key;
};

struct AllocationDiagnostics final {
    bool has_enough_groups{false};
};

}  // namespace mooncake
