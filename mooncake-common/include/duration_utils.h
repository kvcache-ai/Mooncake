#pragma once

#include <cctype>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace mooncake {

inline std::string_view TrimAsciiWhitespace(std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

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
    while (number_end < trimmed.size() &&
           std::isdigit(static_cast<unsigned char>(trimmed[number_end]))) {
        ++number_end;
    }

    if (number_end == 0) {
        return set_error(
            "duration must start with a non-negative integer and may use ms, "
            "s, m, or h as the unit suffix");
    }

    uint64_t numeric_value = 0;
    for (size_t i = 0; i < number_end; ++i) {
        const uint64_t digit = static_cast<uint64_t>(trimmed[i] - '0');
        if (numeric_value >
            (std::numeric_limits<uint64_t>::max() - digit) / 10) {
            return set_error("duration value is too large");
        }
        numeric_value = numeric_value * 10 + digit;
    }

    std::string_view suffix = TrimAsciiWhitespace(trimmed.substr(number_end));
    std::string normalized_suffix;
    normalized_suffix.reserve(suffix.size());
    for (char ch : suffix) {
        normalized_suffix.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }

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

    if (numeric_value > std::numeric_limits<uint64_t>::max() / multiplier) {
        return set_error("duration value is too large after unit conversion");
    }

    *result = numeric_value * multiplier;
    return true;
}

}  // namespace mooncake
