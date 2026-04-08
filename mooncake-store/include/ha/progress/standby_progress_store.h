#pragma once

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

namespace standby_progress_store_detail {

constexpr auto kStandbyProgressFreshness = std::chrono::seconds(10);
constexpr auto kStandbyProgressRedisTtl = std::chrono::seconds(30);

inline std::string SerializeStandbyProgressValue(
    const StandbyProgressRecord& record) {
    return std::to_string(static_cast<int>(record.mode)) + "|" +
           std::to_string(record.view_version) + "|" +
           std::to_string(record.applied_seq_id) + "|" +
           std::to_string(record.required_seq_id) + "|" +
           std::to_string(record.updated_at_ms);
}

inline tl::expected<StandbyProgressRecord, ErrorCode>
DeserializeStandbyProgressValue(std::string_view standby_id,
                                std::string_view value) {
    StandbyProgressRecord record;
    record.standby_id = std::string(standby_id);

    std::string parts[5];
    size_t begin = 0;
    size_t index = 0;
    while (begin <= value.size() && index < 5) {
        const auto end = value.find('|', begin);
        if (end == std::string_view::npos) {
            parts[index++] = std::string(value.substr(begin));
            break;
        }
        parts[index++] = std::string(value.substr(begin, end - begin));
        begin = end + 1;
    }
    if (index != 5) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    try {
        const auto mode = std::stoi(parts[0]);
        if (mode < static_cast<int>(StandbyProgressMode::kSnapshotOnly) ||
            mode > static_cast<int>(StandbyProgressMode::kOplogFollowing)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        record.mode = static_cast<StandbyProgressMode>(mode);
        record.view_version = static_cast<ViewVersionId>(std::stoull(parts[1]));
        record.applied_seq_id =
            static_cast<OpLogSequenceId>(std::stoull(parts[2]));
        record.required_seq_id =
            static_cast<OpLogSequenceId>(std::stoull(parts[3]));
        record.updated_at_ms = std::stoll(parts[4]);
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return record;
}

inline bool IsStandbyProgressFresh(const StandbyProgressRecord& record,
                                   int64_t now_ms) {
    if (record.updated_at_ms <= 0) {
        return false;
    }
    const auto freshness_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kStandbyProgressFreshness)
            .count();
    return now_ms - record.updated_at_ms <= freshness_ms;
}

}  // namespace standby_progress_store_detail

class StandbyProgressStore {
   public:
    virtual ~StandbyProgressStore() = default;

    virtual ErrorCode Publish(const StandbyProgressRecord& record) = 0;

    virtual tl::expected<std::vector<StandbyProgressRecord>, ErrorCode>
    List() = 0;

    virtual ErrorCode Delete(std::string_view standby_id) = 0;
};

}  // namespace ha
}  // namespace mooncake
