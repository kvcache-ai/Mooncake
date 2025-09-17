#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

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

/*
    @brief Allocates memory for the `BufferAllocator` class.
    @param total_size The total size of the memory to allocate.
    @return A pointer to the allocated memory.
*/
void* allocate_buffer_allocator_memory(
    size_t total_size, const std::string& protocol = "",
    size_t alignment = facebook::cachelib::Slab::kSize);

void free_memory(const std::string& protocol, void* ptr);

[[nodiscard]] inline std::string byte_size_to_string(uint64_t bytes) {
    const double KB = 1024.0;
    const double MB = KB * 1024.0;
    const double GB = MB * 1024.0;
    const double TB = GB * 1024.0;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    if (bytes >= static_cast<uint64_t>(TB)) {
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

}  // namespace mooncake
