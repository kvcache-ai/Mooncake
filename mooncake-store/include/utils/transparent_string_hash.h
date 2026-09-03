#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace mooncake {

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
};

}  // namespace mooncake
