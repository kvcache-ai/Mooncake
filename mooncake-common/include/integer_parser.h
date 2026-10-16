#pragma once

#include <charconv>
#include <optional>
#include <string_view>
#include <system_error>
#include <type_traits>

#include "ascii_string.h"

namespace mooncake {

struct IntegerParseOptions {
    bool trim_ascii_whitespace{false};
    bool allow_leading_plus{false};
};

template <typename Integer>
std::optional<Integer> TryParseInteger(std::string_view value,
                                       IntegerParseOptions options = {}) {
    static_assert(std::is_integral_v<Integer> &&
                      !std::is_same_v<std::remove_cv_t<Integer>, bool>,
                  "TryParseInteger requires a non-bool integral type");

    if (options.trim_ascii_whitespace) {
        value = TrimAsciiWhitespace(value);
    }
    if (options.allow_leading_plus && !value.empty() && value.front() == '+') {
        value.remove_prefix(1);
        if (!value.empty() && (value.front() == '+' || value.front() == '-')) {
            return std::nullopt;
        }
    }
    if (value.empty()) {
        return std::nullopt;
    }

    Integer parsed{};
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

}  // namespace mooncake
