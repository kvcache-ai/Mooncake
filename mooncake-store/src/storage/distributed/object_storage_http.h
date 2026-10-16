#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

namespace mooncake::object_storage_http {

// Validate a single HTTP byte range, including its declared total if known.
inline bool MatchesContentRange(
    const std::map<std::string, std::string>& headers, uint64_t first,
    size_t length) {
    const auto it = headers.find("content-range");
    if (it == headers.end() || !length) return false;
    std::string_view value(it->second);
    if (!value.starts_with("bytes ")) return false;
    value.remove_prefix(6);
    auto number = [&](uint64_t& out, char delimiter) {
        const auto result =
            std::from_chars(value.data(), value.data() + value.size(), out);
        if (result.ec != std::errc{} ||
            result.ptr == value.data() + value.size() ||
            *result.ptr != delimiter)
            return false;
        value.remove_prefix(result.ptr - value.data() + 1);
        return true;
    };
    uint64_t begin = 0, end = 0, total = 0;
    if (!number(begin, '-') || !number(end, '/') || begin != first ||
        end < begin || end - begin != length - 1)
        return false;
    if (value == "*") return true;
    const auto result =
        std::from_chars(value.data(), value.data() + value.size(), total);
    return result.ec == std::errc{} &&
           result.ptr == value.data() + value.size() && total > end;
}

}  // namespace mooncake::object_storage_http
