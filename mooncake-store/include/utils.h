#pragma once

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <glog/logging.h>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_writer.h>
#include <ylt/util/tl/expected.hpp>

#include "common_types.h"

namespace mooncake {

// Merge process-start cluster labels with caller-provided metric labels.
// A configured cluster_id takes precedence over a caller-provided value.
std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels);

// String utility functions

/**
 * @brief Convert a byte size to a human-readable string
 * @param bytes Number of bytes
 * @return std::string Human-readable string representation of size
 */
[[nodiscard]] inline std::string byte_size_to_string(uint64_t bytes) {
    const double KB = 1024.0;
    const double MB = KB * 1024.0;
    const double GB = MB * 1024.0;
    const double TB = GB * 1024.0;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (static_cast<int64_t>(bytes) == std::numeric_limits<int64_t>::max()) {
        oss << "infinite";
    } else if (bytes >= static_cast<uint64_t>(TB)) {
        oss << bytes / TB << " TB";
    } else if (bytes >= static_cast<uint64_t>(GB)) {
        oss << bytes / GB << " GB";
    } else if (bytes >= static_cast<uint64_t>(MB)) {
        oss << bytes / MB << " MB";
    } else if (bytes >= static_cast<uint64_t>(KB)) {
        oss << bytes / KB << " KB";
    } else {
        // Less than 1 KB, don't use fixed point
        oss.unsetf(std::ios::fixed);
        oss << bytes << " B";
        return oss.str();
    }

    return oss.str();
}

/**
 * @brief Convert a string representation of size to bytes
 * @param str String representation of size (e.g., "1.5 GB", "1024 MB",
 * "1048576")
 * @return uint64_t Number of bytes, or 0 if parsing fails
 */
[[nodiscard]] inline uint64_t string_to_byte_size(const std::string& str) {
    if (str.empty()) {
        return 0;
    }

    // Create a copy for manipulation
    std::string s = str;

    // Remove leading/trailing whitespace
    s.erase(0, s.find_first_not_of(" \t\r\n"));
    s.erase(s.find_last_not_of(" \t\r\n") + 1);

    if (s.empty()) {
        return 0;
    }

    // Handle special case for "infinite"
    if (s == "infinite") {
        return std::numeric_limits<uint64_t>::max();
    }

    // Parse the numeric part
    size_t pos = 0;
    double value = 0;

    try {
        value = std::stod(s, &pos);
    } catch (const std::exception&) {
        return 0;  // Failed to parse number
    }

    if (pos >= s.length()) {
        // No unit specified, assume bytes
        return static_cast<uint64_t>(value);
    }

    // Extract unit (remaining part of the string)
    std::string unit = s.substr(pos);
    // Remove leading whitespace from unit
    unit.erase(0, unit.find_first_not_of(" \t\r\n"));

    // Convert to uppercase for comparison
    std::transform(unit.begin(), unit.end(), unit.begin(), ::toupper);

    // Apply unit multiplier
    const double KB = 1024.0;
    const double MB = KB * 1024.0;
    const double GB = MB * 1024.0;
    const double TB = GB * 1024.0;

    if (unit == "KB" || unit == "K") {
        return static_cast<uint64_t>(value * KB);
    } else if (unit == "MB" || unit == "M") {
        return static_cast<uint64_t>(value * MB);
    } else if (unit == "GB" || unit == "G") {
        return static_cast<uint64_t>(value * GB);
    } else if (unit == "TB" || unit == "T") {
        return static_cast<uint64_t>(value * TB);
    } else if (unit == "B" || unit.empty()) {
        return static_cast<uint64_t>(value);
    } else {
        // Unknown unit
        return 0;
    }
}

/**
 * @brief Split a string by delimiter into a vector of strings
 * @param str The string to split
 * @param delimiter The delimiter to split by (default is comma)
 * @param trim_spaces Whether to trim leading/trailing spaces from each token
 * @param keep_empty Whether to keep empty tokens in the result
 * @return Vector of split strings
 */
std::vector<std::string> splitString(const std::string& str,
                                     char delimiter = ',',
                                     bool trim_spaces = true,
                                     bool keep_empty = false);

/// @brief Transparent hasher enabling heterogeneous string_view lookup
/// in unordered containers keyed by std::string.
/// std::string implicitly converts to string_view, so one overload suffices.
struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
};

/** @brief Returns true if the UUID is the all-zero sentinel value. */
[[nodiscard]] inline bool IsZeroUUID(const UUID& uuid) noexcept {
    return uuid.first == 0 && uuid.second == 0;
}

// Helper: Get integer from environment variable, fallback to default
template <typename T>
T GetEnvOr(const char* name, T default_value) {
    const char* env_val = std::getenv(name);
    if (!env_val || std::string(env_val).empty()) {
        return default_value;
    }
    try {
        long long value = std::stoll(env_val);
        // Check range for unsigned types
        if constexpr (std::is_same_v<T, uint32_t>) {
            if (value < 0 || value > UINT32_MAX) throw std::out_of_range("");
        }
        return static_cast<T>(value);
    } catch (...) {
        return default_value;
    }
}

std::string GetEnvStringOr(const char* name, const std::string& default_value);
}  // namespace mooncake
