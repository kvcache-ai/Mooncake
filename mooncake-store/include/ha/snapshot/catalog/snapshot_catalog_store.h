#pragma once

#include <charconv>
#include <cctype>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

namespace snapshot_catalog_store_detail {

constexpr std::string_view kSnapshotRoot = "mooncake_master_snapshot/";
constexpr std::string_view kSnapshotLatest = "latest.txt";
constexpr std::string_view kSnapshotManifest = "manifest.txt";
constexpr std::string_view kSnapshotDescriptor = "descriptor.txt";

inline bool IsAsciiDigit(char ch) {
    return std::isdigit(static_cast<unsigned char>(ch)) != 0;
}

inline bool IsValidSnapshotId(std::string_view snapshot_id) {
    if (snapshot_id.size() != 19) {
        return false;
    }

    for (size_t i = 0; i < snapshot_id.size(); ++i) {
        if (i == 8 || i == 15) {
            if (snapshot_id[i] != '_') {
                return false;
            }
            continue;
        }

        if (!IsAsciiDigit(snapshot_id[i])) {
            return false;
        }
    }

    return true;
}

inline std::string TrimAsciiWhitespace(std::string value) {
    constexpr std::string_view kAsciiWhitespace = " \t\n\r\f\v";
    const auto first = value.find_first_not_of(kAsciiWhitespace);
    if (first == std::string::npos) {
        return "";
    }

    const auto last = value.find_last_not_of(kAsciiWhitespace);
    return value.substr(first, last - first + 1);
}

inline std::string BuildSnapshotPrefix(const SnapshotId& snapshot_id) {
    return std::string(kSnapshotRoot) + snapshot_id + "/";
}

inline std::string BuildManifestKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotManifest);
}

inline std::string BuildDescriptorKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotDescriptor);
}

inline std::string BuildLatestKey() {
    return std::string(kSnapshotRoot) + std::string(kSnapshotLatest);
}

inline SnapshotDescriptor MakeSnapshotDescriptor(
    const SnapshotId& snapshot_id) {
    SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = BuildManifestKey(snapshot_id);
    descriptor.object_prefix = BuildSnapshotPrefix(snapshot_id);
    return descriptor;
}

template <typename Integer>
inline bool ParseDecimal(std::string_view text, Integer& value) {
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc() && result.ptr == end;
}

inline std::string SerializeSnapshotDescriptor(
    const SnapshotDescriptor& descriptor) {
    return std::to_string(descriptor.last_included_seq) + "|" +
           std::to_string(descriptor.producer_view_version) + "|" +
           std::to_string(descriptor.created_at_ms);
}

inline tl::expected<SnapshotDescriptor, ErrorCode>
DeserializeSnapshotDescriptor(const SnapshotId& snapshot_id,
                              std::string_view payload) {
    const auto first = payload.find('|');
    const auto second = first == std::string_view::npos
                            ? std::string_view::npos
                            : payload.find('|', first + 1);
    if (first == std::string_view::npos || second == std::string_view::npos) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    SnapshotDescriptor descriptor = MakeSnapshotDescriptor(snapshot_id);
    if (!ParseDecimal(payload.substr(0, first), descriptor.last_included_seq) ||
        !ParseDecimal(payload.substr(first + 1, second - first - 1),
                      descriptor.producer_view_version) ||
        !ParseDecimal(payload.substr(second + 1), descriptor.created_at_ms)) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    return descriptor;
}

}  // namespace snapshot_catalog_store_detail

class SnapshotCatalogStore {
   public:
    virtual ~SnapshotCatalogStore() = default;

    virtual ErrorCode Publish(const SnapshotDescriptor& snapshot) = 0;

    virtual tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
    GetLatest() = 0;

    virtual tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) = 0;

    virtual ErrorCode Delete(const SnapshotId& snapshot_id) = 0;
};

}  // namespace ha
}  // namespace mooncake
