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

#include "common/byte_size.h"
#include "common_types.h"

namespace mooncake {

// Merge process-start cluster labels with caller-provided metric labels.
// A configured cluster_id takes precedence over a caller-provided value.
std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels);

// String utility functions

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
