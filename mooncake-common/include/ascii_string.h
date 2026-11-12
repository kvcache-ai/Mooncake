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

// Split a `delimiter`-separated list, trimming ASCII whitespace around each
// entry. Empty entries are dropped unless `keep_empty` is set, which a
// positionally aligned list needs so that a missing entry can be rejected
// instead of shifting every later entry onto its neighbour.
// The returned views alias `value`, so they outlive the call only as long as
// the buffer behind `value` does: never pass a temporary std::string here.
inline std::vector<std::string_view> SplitAsciiList(std::string_view value,
                                                    char delimiter,
                                                    bool keep_empty = false) {
    std::vector<std::string_view> entries;
    while (true) {
        const size_t pos = value.find(delimiter);
        const std::string_view token =
            TrimAsciiWhitespace(value.substr(0, pos));
        if (keep_empty || !token.empty()) {
            entries.push_back(token);
        }
        if (pos == std::string_view::npos) {
            break;
        }
        value.remove_prefix(pos + 1);
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
