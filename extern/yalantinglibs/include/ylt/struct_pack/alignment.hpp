/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>

#include "reflection.hpp"
#include "util.h"

namespace struct_pack::detail {
namespace align {

template <typename T>
constexpr std::size_t alignment_impl();

template <typename T>
constexpr std::size_t alignment_v = alignment_impl<T>();

template <typename type, std::size_t... I>
constexpr std::size_t default_alignment_helper(std::index_sequence<I...>) {
  return (std::max)(
      {(is_compatible_v<remove_cvref_t<std::tuple_element_t<I, type>>>
            ? std::size_t{0}
            : align::alignment_v<
                  remove_cvref_t<std::tuple_element_t<I, type>>>)...});
}

template <typename T>
constexpr std::size_t default_alignment() {
  if constexpr (!is_trivial_serializable<T>::value && !is_trivial_view_v<T>) {
    using type = decltype(get_types<T>());
    return default_alignment_helper<type>(
        std::make_index_sequence<std::tuple_size_v<type>>());
  }
  else if constexpr (is_trivial_view_v<T>) {
    return std::alignment_of_v<typename T::value_type>;
  }
  else {
    return std::alignment_of_v<T>;
  }
}
template <typename T>
constexpr std::size_t default_alignment_v = default_alignment<T>();

template <typename T>
constexpr std::size_t alignment_impl();

template <typename type, std::size_t... I>
constexpr std::size_t pack_alignment_impl_helper(std::index_sequence<I...>) {
  return (std::max)(
      {(is_compatible_v<remove_cvref_t<std::tuple_element_t<I, type>>>
            ? std::size_t{0}
            : align::alignment_v<
                  remove_cvref_t<std::tuple_element_t<I, type>>>)...});
}

template <typename T>
constexpr std::size_t pack_alignment_impl() {
  static_assert(std::is_class_v<T>);
  static_assert(!is_trivial_view_v<T>);
  constexpr auto ret = struct_pack::pack_alignment_v<T>;
  static_assert(ret == 0 || ret == 1 || ret == 2 || ret == 4 || ret == 8 ||
                ret == 16);
  if constexpr (ret == 0) {
    using type = decltype(get_types<T>());
    return pack_alignment_impl_helper<type>(
        std::make_index_sequence<std::tuple_size_v<type>>());
  }
  else {
    return ret;
  }
}

template <typename T>
constexpr std::size_t pack_alignment_v = pack_alignment_impl<T>();

template <typename T>
constexpr std::size_t alignment_impl() {
  if constexpr (struct_pack::detail::is_compatible_v<T>) {
    return 0;
  }
  else if constexpr (struct_pack::alignment_v<T> == 0 &&
                     struct_pack::pack_alignment_v<T> == 0) {
    return default_alignment_v<T>;
  }
  else if constexpr (struct_pack::alignment_v<T> != 0) {
    if constexpr (is_trivial_serializable<T>::value) {
      static_assert(default_alignment_v<T> == alignment_v<T>);
    }
    constexpr auto ret = struct_pack::alignment_v<T>;
    static_assert(
        [](std::size_t align) constexpr {
          while (align % 2 == 0) {
            align /= 2;
          }
          return align == 1;
        }(ret),
        "alignment should be power of 2");
    return ret;
  }
  else {
    if constexpr (is_trivial_serializable<T>::value) {
      return default_alignment_v<T>;
    }
    else {
      return pack_alignment_v<T>;
    }
  }
}
template <typename P, typename T, std::size_t I>
struct calculate_trival_obj_size {
  constexpr void operator()(std::size_t &total);
};

template <typename P, typename T, std::size_t I>
struct calculate_padding_size_impl {
  constexpr void operator()(
      std::size_t &offset,
      std::array<std::size_t, struct_pack::members_count<P> + 1>
          &padding_size) {
    if constexpr (is_compatible_v<T>) {
      padding_size[I] = 0;
    }
    else if constexpr (is_trivial_view_v<T>) {
      calculate_padding_size_impl<P, typename T::value_type, I>{}(offset,
                                                                  padding_size);
    }
    else {
      if constexpr (align::alignment_v<T>) {
        if (offset % align::alignment_v<T>) {
          padding_size[I] = (std::min)(
              align::pack_alignment_v<P> - 1,
              align::alignment_v<T> - offset % align::alignment_v<T>);
        }
        else {
          padding_size[I] = 0;
        }
      }
      else {
        padding_size[I] = 0;
      }
      offset += padding_size[I];
      if constexpr (is_trivial_serializable<T>::value)
        offset += sizeof(T);
      else {
        for_each<T, calculate_trival_obj_size>(offset);
        static_assert(is_trivial_serializable<T, true>::value);
      }
    }
  }
};

template <typename T>
constexpr auto calculate_padding_size() {
  std::array<std::size_t, struct_pack::members_count<T> + 1> padding_size{};
  std::size_t offset = 0;
  for_each<T, calculate_padding_size_impl>(offset, padding_size);
  if constexpr (align::alignment_v<T> > 0) {
    if (offset % align::alignment_v<T>) {
      padding_size[struct_pack::members_count<T>] =
          align::alignment_v<T> - offset % align::alignment_v<T>;
      return padding_size;
    }
  }
  padding_size[struct_pack::members_count<T>] = 0;
  return padding_size;
}
template <typename T>
constexpr std::array<std::size_t, struct_pack::members_count<T> + 1>
    padding_size = calculate_padding_size<T>();
template <typename T>
constexpr std::size_t get_total_padding_size() {
  std::size_t sum = 0;
  for (auto &e : padding_size<T>) {
    sum += e;
  }
  return sum;
};
template <typename T>
constexpr std::size_t total_padding_size = get_total_padding_size<T>();

template <typename P, typename T, std::size_t I>
using calculate_trival_obj_size_wrapper = calculate_trival_obj_size<P, T, I>;

template <typename P, typename T, std::size_t I>
constexpr void calculate_trival_obj_size<P, T, I>::operator()(
    std::size_t &total) {
  if constexpr (I == 0) {
    total += total_padding_size<P>;
  }
  if constexpr (!is_compatible_v<T>) {
    if constexpr (is_trivial_serializable<T>::value) {
      total += sizeof(T);
    }
    else if constexpr (is_trivial_view_v<T>) {
      total += sizeof(typename T::value_type);
    }
    else {
      static_assert(is_trivial_serializable<T, true>::value);
      std::size_t offset = 0;
      for_each<T, calculate_trival_obj_size_wrapper>(offset);
      total += offset;
    }
  }
}

}  // namespace align
}  // namespace struct_pack::detail