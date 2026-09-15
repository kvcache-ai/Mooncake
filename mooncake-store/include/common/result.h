#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "rpc_types.h"
#include "types.h"

namespace mooncake {

template <class T>
constexpr bool is_supported_return_type_v =
    std::is_void_v<T> || std::is_integral_v<T>;

template <class T>
    requires is_supported_return_type_v<T>
int64_t to_py_ret(const tl::expected<T, ErrorCode>& exp) noexcept {
    if (!exp) {
        return static_cast<int64_t>(toInt(exp.error()));
    }
    if constexpr (std::is_void_v<T>) {
        return 0;
    } else {
        return static_cast<int64_t>(exp.value());
    }
}

template <typename T>
void to_stream(std::ostream& os, const T& value);

template <typename T>
void to_stream(std::ostream& os, const std::vector<T>& vec);

template <typename T1, typename T2>
void to_stream(std::ostream& os, const std::pair<T1, T2>& p);

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

template <typename T>
void to_stream(std::ostream& os, const std::vector<T>& vec) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        to_stream(os, vec[i]);
        if (i + 1 < vec.size()) {
            os << ",";
        }
    }
    os << "]";
}

template <typename K, typename V, typename H>
void to_stream(std::ostream& os, const std::unordered_map<K, V, H>& map) {
    os << "{";
    auto it = map.begin();
    while (it != map.end()) {
        to_stream(os, it->first);
        os << ": ";
        to_stream(os, it->second);
        if (++it != map.end()) {
            os << ", ";
        }
    }
    os << "}";
}

template <typename K, typename V>
void to_stream(std::ostream& os, const std::unordered_map<K, V>& map) {
    to_stream<K, V, std::hash<K>>(os, map);
}

template <typename T1, typename T2>
void to_stream(std::ostream& os, const std::pair<T1, T2>& p) {
    os << "{\"first\":";
    to_stream(os, p.first);
    os << ",\"second\":";
    to_stream(os, p.second);
    os << "}";
}

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

}  // namespace mooncake
