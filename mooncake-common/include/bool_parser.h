#pragma once

#include <optional>
#include <string_view>

#include "ascii_string.h"

namespace mooncake {

enum class BoolTokenSet {
    kTrueFalse,
    kCanonical,
};

struct BoolParseOptions {
    BoolTokenSet token_set{BoolTokenSet::kCanonical};
    bool trim_ascii_whitespace{true};
};

inline std::optional<bool> TryParseBool(std::string_view value,
                                        BoolParseOptions options = {}) {
    if (options.trim_ascii_whitespace) {
        value = TrimAsciiWhitespace(value);
    }
    if (value.empty()) {
        return std::nullopt;
    }

    if (value == "1") {
        return true;
    }
    if (value == "0") {
        return false;
    }
    if (AsciiCaseInsensitiveEquals(value, "true")) {
        return true;
    }
    if (AsciiCaseInsensitiveEquals(value, "false")) {
        return false;
    }
    if (options.token_set == BoolTokenSet::kTrueFalse) {
        return std::nullopt;
    }

    if (AsciiCaseInsensitiveEquals(value, "yes")) {
        return true;
    }
    if (AsciiCaseInsensitiveEquals(value, "no")) {
        return false;
    }
    if (AsciiCaseInsensitiveEquals(value, "on") ||
        AsciiCaseInsensitiveEquals(value, "enable")) {
        return true;
    }
    if (AsciiCaseInsensitiveEquals(value, "off") ||
        AsciiCaseInsensitiveEquals(value, "disable")) {
        return false;
    }
    return std::nullopt;
}

}  // namespace mooncake
