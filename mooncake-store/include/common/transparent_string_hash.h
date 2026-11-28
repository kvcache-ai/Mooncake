#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace mooncake {

struct PrehashedStringView final {
    std::string_view value;
    size_t hash;

    friend bool operator==(PrehashedStringView lhs,
                           std::string_view rhs) noexcept {
        return lhs.value == rhs;
    }
};

struct TransparentStringHash final {
    using is_transparent = void;

    size_t operator()(std::string_view value) const noexcept {
        return std::hash<std::string_view>{}(value);
    }
    size_t operator()(const std::string& value) const noexcept {
        return (*this)(std::string_view(value));
    }
    size_t operator()(const char* value) const noexcept {
        return (*this)(std::string_view(value));
    }
    size_t operator()(const PrehashedStringView& value) const noexcept {
        return value.hash;
    }
};

}  // namespace mooncake
