#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>

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
void* allocate_buffer_allocator_memory(size_t total_size);

void** rdma_args(const std::string& device_name);

}  // namespace mooncake