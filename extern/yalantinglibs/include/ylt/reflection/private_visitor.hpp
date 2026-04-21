#pragma once
#include <tuple>

#include "internal/common_macro.hpp"

namespace ylt::reflection {
template <typename T, auto... field>
struct private_visitor {
  friend inline constexpr auto get_private_ptrs(
      const ylt::reflection::identity<T>&) {
    constexpr auto tp = std::make_tuple(field...);
    return tp;
  }
};
}  // namespace ylt::reflection
