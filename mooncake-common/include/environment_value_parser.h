#pragma once

#include <cerrno>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "bool_parser.h"
#include "integer_parser.h"

namespace mooncake {

struct EnvironmentDoubleParseOptions {
    bool allow_trailing_characters{false};
    bool allow_non_finite{false};
};

inline std::optional<double> TryParseEnvironmentDouble(
    std::string_view value, EnvironmentDoubleParseOptions options = {}) {
    if (value.empty()) {
        return std::nullopt;
    }

    const std::string text(value);
    char* end = nullptr;
    const int saved_errno = errno;
    errno = 0;
    const double parsed = std::strtod(text.c_str(), &end);
    const int parse_errno = errno;
    errno = saved_errno;
    const bool consumed_value = end != text.c_str() && end != nullptr;
    while (end != nullptr && std::isspace(static_cast<unsigned char>(*end))) {
        ++end;
    }
    const bool consumed_required_input =
        options.allow_trailing_characters || (consumed_value && *end == '\0');
    const bool accepted_finiteness =
        options.allow_non_finite || std::isfinite(parsed);
    if (consumed_value && consumed_required_input && parse_errno != ERANGE &&
        accepted_finiteness) {
        return parsed;
    }
    return std::nullopt;
}

template <typename>
inline constexpr bool kUnsupportedEnvironmentValueType = false;

template <typename T>
std::optional<T> TryParseEnvironmentValue(std::string_view value) {
    if constexpr (std::is_same_v<T, std::string>) {
        return std::string(value);
    } else if constexpr (std::is_same_v<T, bool>) {
        return TryParseBool(value);
    } else if constexpr (std::is_integral_v<T>) {
        return TryParseInteger<T>(
            value, {.trim_ascii_whitespace = true, .allow_leading_plus = true});
    } else if constexpr (std::is_same_v<T, double>) {
        return TryParseEnvironmentDouble(value);
    } else {
        static_assert(kUnsupportedEnvironmentValueType<T>,
                      "unsupported environment value type");
    }
}

}  // namespace mooncake
