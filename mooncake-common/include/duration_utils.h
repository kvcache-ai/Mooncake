#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "ascii_string.h"
#include "integer_parser.h"

namespace mooncake {

inline bool ParseDurationMs(std::string_view value, uint64_t* result,
                            std::string* error = nullptr) {
    auto set_error = [&](std::string message) {
        if (error != nullptr) {
            *error = std::move(message);
        }
        return false;
    };

    if (result == nullptr) {
        return set_error("duration output pointer is null");
    }

    std::string_view trimmed = TrimAsciiWhitespace(value);
    if (trimmed.empty()) {
        return set_error(
            "duration is empty; expected a non-negative integer optionally "
            "followed by ms, s, m, or h");
    }

    size_t number_end = 0;
    while (number_end < trimmed.size() && trimmed[number_end] >= '0' &&
           trimmed[number_end] <= '9') {
        ++number_end;
    }

    if (number_end == 0) {
        return set_error(
            "duration must start with a non-negative integer and may use ms, "
            "s, m, or h as the unit suffix");
    }

    const auto numeric_value =
        TryParseInteger<uint64_t>(trimmed.substr(0, number_end));
    if (!numeric_value.has_value()) {
        return set_error("duration value is too large");
    }

    std::string_view suffix = TrimAsciiWhitespace(trimmed.substr(number_end));
    const std::string normalized_suffix = AsciiToLower(suffix);

    uint64_t multiplier = 1;
    if (normalized_suffix.empty() || normalized_suffix == "ms") {
        multiplier = 1;
    } else if (normalized_suffix == "s") {
        multiplier = 1000;
    } else if (normalized_suffix == "m") {
        multiplier = 60 * 1000;
    } else if (normalized_suffix == "h") {
        multiplier = 60 * 60 * 1000;
    } else {
        return set_error("unsupported duration unit '" + normalized_suffix +
                         "'; supported units are ms, s, m, and h");
    }

    if (*numeric_value > std::numeric_limits<uint64_t>::max() / multiplier) {
        return set_error("duration value is too large after unit conversion");
    }

    *result = *numeric_value * multiplier;
    return true;
}

}  // namespace mooncake
