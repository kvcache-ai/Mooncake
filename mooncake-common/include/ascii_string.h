#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace mooncake {

constexpr bool IsAsciiWhitespace(char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' ||
           ch == '\v';
}

inline std::string_view TrimAsciiWhitespace(std::string_view value) {
    while (!value.empty() && IsAsciiWhitespace(value.front())) {
        value.remove_prefix(1);
    }
    while (!value.empty() && IsAsciiWhitespace(value.back())) {
        value.remove_suffix(1);
    }
    return value;
}

// Split a comma-separated list, trimming whitespace around each entry and
// dropping empty ones. Used to expand multi-disk configuration values such as
// FileStorageConfig::storage_filepath.
inline std::vector<std::string> SplitCommaList(std::string_view value) {
    std::vector<std::string> entries;
    while (true) {
        const size_t comma = value.find(',');
        const std::string_view token =
            TrimAsciiWhitespace(value.substr(0, comma));
        if (!token.empty()) {
            entries.emplace_back(token);
        }
        if (comma == std::string_view::npos) {
            break;
        }
        value.remove_prefix(comma + 1);
    }
    return entries;
}

constexpr char AsciiToLower(char ch) {
    return ch >= 'A' && ch <= 'Z' ? static_cast<char>(ch + ('a' - 'A')) : ch;
}

inline std::string AsciiToLower(std::string_view value) {
    std::string normalized;
    normalized.reserve(value.size());
    for (char ch : value) {
        normalized.push_back(AsciiToLower(ch));
    }
    return normalized;
}

inline bool AsciiCaseInsensitiveEquals(std::string_view lhs,
                                       std::string_view rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (AsciiToLower(lhs[i]) != AsciiToLower(rhs[i])) {
            return false;
        }
    }
    return true;
}

}  // namespace mooncake
