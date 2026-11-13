#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake::object_storage_http {

struct ContentRange {
    uint64_t first;
    uint64_t last;
    std::optional<uint64_t> total;
};

inline std::optional<uint64_t> ParseRangeNumber(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    uint64_t number = 0;
    const auto result =
        std::from_chars(value.data(), value.data() + value.size(), number);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
        return std::nullopt;
    }
    return number;
}

inline std::optional<ContentRange> ParseContentRange(std::string_view value) {
    constexpr std::string_view kPrefix = "bytes ";
    if (!value.starts_with(kPrefix)) {
        return std::nullopt;
    }
    value.remove_prefix(kPrefix.size());
    const auto dash = value.find('-');
    if (dash == std::string_view::npos) {
        return std::nullopt;
    }
    const auto first = ParseRangeNumber(value.substr(0, dash));
    if (!first) {
        return std::nullopt;
    }
    value.remove_prefix(dash + 1);
    const auto slash = value.find('/');
    if (slash == std::string_view::npos) {
        return std::nullopt;
    }
    const auto last = ParseRangeNumber(value.substr(0, slash));
    if (!last) {
        return std::nullopt;
    }
    if (*last < *first) {
        return std::nullopt;
    }
    value.remove_prefix(slash + 1);
    if (value == "*") {
        return ContentRange{*first, *last, std::nullopt};
    }
    const auto total = ParseRangeNumber(value);
    if (!total) {
        return std::nullopt;
    }
    if (*total <= *last) {
        return std::nullopt;
    }
    return ContentRange{*first, *last, total};
}

// Validate a single HTTP byte range, including its declared total if known.
inline bool MatchesContentRange(
    const std::map<std::string, std::string>& headers, uint64_t first,
    size_t length) {
    const auto it = headers.find("content-range");
    if (it == headers.end()) {
        return false;
    }
    if (length == 0) {
        return false;
    }
    const auto range = ParseContentRange(it->second);
    if (!range) {
        return false;
    }
    return range->first == first && range->last - range->first == length - 1;
}

}  // namespace mooncake::object_storage_http
