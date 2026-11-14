#pragma once

#include <ostream>

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
                                SegmentStatus status) noexcept {
    switch (status) {
        case SegmentStatus::UNDEFINED:
            return os << "UNDEFINED";
        case SegmentStatus::OK:
            return os << "OK";
        case SegmentStatus::DRAINING:
            return os << "DRAINING";
        case SegmentStatus::DRAINED:
            return os << "DRAINED";
        case SegmentStatus::GRACEFULLY_UNMOUNTING:
            return os << "GRACEFULLY_UNMOUNTING";
        case SegmentStatus::UNMOUNTING:
            return os << "UNMOUNTING";
    }
    return os << "UNKNOWN";
}

}  // namespace mooncake
