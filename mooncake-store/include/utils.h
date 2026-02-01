#pragma once

#include <cstddef>
#include <cstdlib>
#include <linux/memfd.h>
#include <linux/mman.h>
#include <string>
#include <limits>
#include <ylt/util/tl/expected.hpp>

#include "rpc_types.h"
#include "types.h"

namespace mooncake {

// Convert ErrorCode to integer for Python bindings
template <class T>
constexpr bool is_supported_return_type_v =
    std::is_void_v<T> || std::is_integral_v<T>;

template <class T>
requires is_supported_return_type_v<T> int64_t
to_py_ret(const tl::expected<T, ErrorCode>& exp)
noexcept {
    if (!exp) {
        return static_cast<int64_t>(toInt(exp.error()));
    }

    if constexpr (std::is_void_v<T>) {
        return 0;
    } else if constexpr (std::is_integral_v<T>) {
        return static_cast<int64_t>(exp.value());
    } else {
        static_assert(!sizeof(T), "Unsupported payload type in to_py_ret()");
    }
}

// Forward declarations
template <typename T>
void to_stream(std::ostream& os, const T& value);

template <typename T>
void to_stream(std::ostream& os, const std::vector<T>& vec);

template <typename T1, typename T2>
void to_stream(std::ostream& os, const std::pair<T1, T2>& p);

// Implementation of the base template
template <typename T>
void to_stream(std::ostream& os, const T& value) {
    if constexpr (std::is_same_v<T, bool>) {
        os << (value ? "true" : "false");
    } else if constexpr (std::is_arithmetic_v<T>) {
        os << value;
    } else if constexpr (std::is_convertible_v<T, std::string_view>) {
        os << "\"" << value << "\"";
    } else if constexpr (ylt::reflection::is_ylt_refl_v<T>) {
        std::string str;
        struct_json::to_json(value, str);
        os << str;
    } else {
        os << value;
    }
}

// Specialization for std::vector
template <typename T>
void to_stream(std::ostream& os, const std::vector<T>& vec) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        to_stream(os, vec[i]);
        if (i < vec.size() - 1) {
            os << ",";
        }
    }
    os << "]";
}

template <typename K, typename V>
void to_stream(std::ostream& os, const std::unordered_map<K, V>& map) {
    to_stream<K, V, std::hash<K>>(os, map);
}

template <typename K, typename V, typename H>
void to_stream(std::ostream& os, const std::unordered_map<K, V, H>& map) {
    os << "{";
    auto it = map.begin();
    while (it != map.end()) {
        to_stream(os, it->first);
        os << ": ";
        to_stream(os, it->second);
        ++it;
        if (it != map.end()) {
            os << ", ";
        }
    }
    os << "}";
}

// Specialization for std::pair
template <typename T1, typename T2>
void to_stream(std::ostream& os, const std::pair<T1, T2>& p) {
    os << "{\"first\":";
    to_stream(os, p.first);
    os << ",\"second\":";
    to_stream(os, p.second);
    os << "}";
}

// Specialization for std::optional
template <typename T>
void to_stream(std::ostream& os, const std::optional<T>& opt) {
    if (opt.has_value()) {
        to_stream(os, opt.value());
    } else {
        os << "nullopt";
    }
}

template <typename T>
std::string expected_to_str(const tl::expected<T, ErrorCode>& expected) {
    std::ostringstream oss;
    if (expected.has_value()) {
        oss << "status=success, value=";
        if constexpr (std::is_same_v<T, void>) {
            oss << "void";
        } else {
            to_stream(oss, expected.value());
        }
    } else {
        oss << "status=failed, error=" << toString(expected.error());
    }
    return oss.str();
}

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

// Buffer allocator functions

constexpr size_t SZ_2MB = 2 * 1024 * 1024;
constexpr size_t SZ_1GB = 1024 * 1024 * 1024;

/**
 * @brief Allocates memory for the `BufferAllocator` class.
 * @param total_size The total size of the memory to allocate.
 * @return A pointer to the allocated memory.
 */
void* allocate_buffer_allocator_memory(
    size_t total_size, const std::string& protocol = "",
    size_t alignment = facebook::cachelib::Slab::kSize);

inline size_t align_up(size_t size, size_t alignment) {
    if (alignment == 0) {
        return size;
    }
    return ((size + alignment - 1) / alignment) * alignment;
}

/**
 * @brief Get hugepage size from env and optionally set the corresponding memfd
 * flags.
 * * @param out_flags Optional pointer to an int. If provided,
 * MAP_HUGETLB and MAP_HUGE_2MB/1GB will be OR-ed into it.
 * @return size_t Hugepage size in bytes, or 0 if disabled.
 */
[[nodiscard]] inline size_t get_hugepage_size_from_env(
    unsigned int* out_flags = nullptr, bool use_memfd = false) {
    const char* use_hp_env = std::getenv("MC_STORE_USE_HUGEPAGE");
    if (use_hp_env == nullptr) {
        return 0;
    }

    constexpr size_t SZ_2MB = 2 * 1024 * 1024;
    constexpr size_t SZ_1GB = 1024 * 1024 * 1024;

    size_t size = SZ_2MB;  // Default to 2MB

    const char* size_env = std::getenv("MC_STORE_HUGEPAGE_SIZE");
    if (size_env != nullptr) {
        size_t parsed_size = string_to_byte_size(size_env);

        if (parsed_size == SZ_2MB || parsed_size == SZ_1GB) {
            size = parsed_size;
        } else {
            LOG(WARNING) << "Invalid MC_STORE_HUGEPAGE_SIZE='" << size_env
                         << "'. Supported: 2MB, 1GB. Fallback to 2MB.";
            size = SZ_2MB;
        }
    }

    if (out_flags != nullptr) {
        if (use_memfd) {
            *out_flags |= MFD_HUGETLB;
        } else {
            *out_flags |= MAP_HUGETLB;
        }

        // Add size specific flag
        if (size == SZ_2MB) {
            if (use_memfd) {
                *out_flags |= MFD_HUGE_2MB;
            } else {
                *out_flags |= MAP_HUGE_2MB;
            }
        } else if (size == SZ_1GB) {
            if (use_memfd) {
                *out_flags |= MFD_HUGE_1GB;
            } else {
                *out_flags |= MAP_HUGE_1GB;
            }
        }
        LOG(INFO) << "Using hugepage size: "
                  << (size == SZ_2MB ? "2MB" : "1GB");
    }

    return size;
}

// Hugepage-backed allocation helpers (MAP_HUGETLB + MADV_HUGEPAGE)
void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment);
void free_buffer_mmap_memory(void* ptr, size_t total_size);

void free_memory(const std::string& protocol, void* ptr);

// Network utility functions

/**
 * @brief Check if a TCP port is available for binding
 * @param port The port number to check
 * @return true if port is available, false otherwise
 */
bool isPortAvailable(int port);

// Simple RAII class for automatically binding to an available port
// The socket is bound during construction and released during destruction
class AutoPortBinder {
   public:
    // Constructs binder and attempts to bind to an available port in range
    // [min_port, max_port] After successful construction, the port is bound and
    // reserved until destruction
    AutoPortBinder(int min_port = 12300, int max_port = 14300);
    ~AutoPortBinder();

    // Non-copyable, non-movable
    AutoPortBinder(const AutoPortBinder&) = delete;
    AutoPortBinder& operator=(const AutoPortBinder&) = delete;
    AutoPortBinder(AutoPortBinder&&) = delete;
    AutoPortBinder& operator=(AutoPortBinder&&) = delete;

    int getPort() const { return port_; }

   private:
    int socket_fd_;
    int port_;
};

// HTTP utility: simple GET, returns body on 200, otherwise error code
tl::expected<std::string, int> httpGet(const std::string& url);

// Network utility: obtain an available TCP port on loopback by binding to 0
int getFreeTcpPort();

int64_t time_gen();

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
