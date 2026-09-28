#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <string>

namespace mooncake {

[[nodiscard]] inline std::string byte_size_to_string(uint64_t bytes) {
    constexpr double kKiB = 1024.0;
    constexpr double kMiB = kKiB * 1024.0;
    constexpr double kGiB = kMiB * 1024.0;
    constexpr double kTiB = kGiB * 1024.0;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (static_cast<int64_t>(bytes) == std::numeric_limits<int64_t>::max()) {
        oss << "infinite";
    } else if (bytes >= static_cast<uint64_t>(kTiB)) {
        oss << bytes / kTiB << " TB";
    } else if (bytes >= static_cast<uint64_t>(kGiB)) {
        oss << bytes / kGiB << " GB";
    } else if (bytes >= static_cast<uint64_t>(kMiB)) {
        oss << bytes / kMiB << " MB";
    } else if (bytes >= static_cast<uint64_t>(kKiB)) {
        oss << bytes / kKiB << " KB";
    } else {
        oss.unsetf(std::ios::fixed);
        oss << bytes << " B";
    }
    return oss.str();
}

[[nodiscard]] inline std::optional<uint64_t> try_string_to_byte_size(
    const std::string& str) {
    if (str.empty()) {
        return std::nullopt;
    }

    std::string value_string = str;
    value_string.erase(0, value_string.find_first_not_of(" \t\r\n"));
    value_string.erase(value_string.find_last_not_of(" \t\r\n") + 1);
    if (value_string.empty()) {
        return std::nullopt;
    }
    if (value_string == "infinite") {
        return std::numeric_limits<uint64_t>::max();
    }

    size_t unit_offset = 0;
    double value = 0;
    try {
        value = std::stod(value_string, &unit_offset);
    } catch (const std::exception&) {
        return std::nullopt;
    }
    if (value < 0) {
        return std::nullopt;
    }
    if (unit_offset >= value_string.length()) {
        return static_cast<uint64_t>(value);
    }

    std::string unit = value_string.substr(unit_offset);
    unit.erase(0, unit.find_first_not_of(" \t\r\n"));
    std::transform(unit.begin(), unit.end(), unit.begin(),
                   [](unsigned char c) -> char { return std::toupper(c); });

    constexpr double kKiB = 1024.0;
    constexpr double kMiB = kKiB * 1024.0;
    constexpr double kGiB = kMiB * 1024.0;
    constexpr double kTiB = kGiB * 1024.0;
    if (unit == "KB" || unit == "K") {
        return static_cast<uint64_t>(value * kKiB);
    }
    if (unit == "MB" || unit == "M") {
        return static_cast<uint64_t>(value * kMiB);
    }
    if (unit == "GB" || unit == "G") {
        return static_cast<uint64_t>(value * kGiB);
    }
    if (unit == "TB" || unit == "T") {
        return static_cast<uint64_t>(value * kTiB);
    }
    if (unit == "B" || unit.empty()) {
        return static_cast<uint64_t>(value);
    }
    return std::nullopt;
}

[[nodiscard]] inline uint64_t string_to_byte_size(const std::string& str) {
    return try_string_to_byte_size(str).value_or(0);
}

}  // namespace mooncake
