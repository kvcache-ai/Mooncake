#pragma once

#include <cmath>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <string>

#include "ascii_string.h"

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

namespace detail {
enum class ByteSizeUnitSet { kAll, kLegacyOnly };

inline std::optional<uint64_t> try_string_to_byte_size_impl(
    const std::string& str, ByteSizeUnitSet unit_set) {
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

    std::string unit = value_string.substr(unit_offset);
    unit.erase(0, unit.find_first_not_of(" \t\r\n"));
    // Locale-independent folding: std::toupper('i') stays 'i' under tr_TR,
    // which would silently reject every IEC spelling there.
    unit = AsciiToLower(unit);

    constexpr double kKiB = 1024.0;
    constexpr double kMiB = kKiB * 1024.0;
    constexpr double kGiB = kMiB * 1024.0;
    constexpr double kTiB = kGiB * 1024.0;
    const bool is_iec_unit =
        unit == "kib" || unit == "mib" || unit == "gib" || unit == "tib";
    if (is_iec_unit && unit_set == ByteSizeUnitSet::kLegacyOnly) {
        return std::nullopt;
    }
    double multiplier = 1.0;
    if (unit == "kib" || unit == "kb" || unit == "k") {
        multiplier = kKiB;
    } else if (unit == "mib" || unit == "mb" || unit == "m") {
        multiplier = kMiB;
    } else if (unit == "gib" || unit == "gb" || unit == "g") {
        multiplier = kGiB;
    } else if (unit == "tib" || unit == "tb" || unit == "t") {
        multiplier = kTiB;
    } else if (unit != "b" && !unit.empty()) {
        return std::nullopt;
    }
    // UINT64_MAX itself rounds up as a double, so check an exclusive limit
    // instead; the negated form also rejects NaN. Casting a product at or
    // beyond 2^64 to uint64_t is undefined behavior.
    const double limit =
        std::ldexp(1.0, std::numeric_limits<uint64_t>::digits) / multiplier;
    if (!(value >= 0 && value < limit)) {
        return std::nullopt;
    }
    return static_cast<uint64_t>(value * multiplier);
}
}  // namespace detail

[[nodiscard]] inline std::optional<uint64_t> try_string_to_byte_size(
    const std::string& str) {
    return detail::try_string_to_byte_size_impl(str,
                                                detail::ByteSizeUnitSet::kAll);
}

// Rejects the IEC spellings (KiB/MiB/GiB/TiB) while keeping every legacy
// spelling (KB/MB/... and single letters). For knobs that predate IEC support
// and must keep rejecting it, e.g. the second-based SSD prefetch options.
[[nodiscard]] inline std::optional<uint64_t>
try_string_to_byte_size_legacy_units(const std::string& str) {
    return detail::try_string_to_byte_size_impl(
        str, detail::ByteSizeUnitSet::kLegacyOnly);
}

[[nodiscard]] inline uint64_t string_to_byte_size(const std::string& str) {
    return try_string_to_byte_size(str).value_or(0);
}

}  // namespace mooncake
