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
#include <cassert>
#include <cstdint>
#include <memory>
#include <tuple>

#include "error_code.hpp"
#include "marco.h"
#include "size_info.hpp"
#include "util.h"
namespace struct_pack {

template <typename... Args>
STRUCT_PACK_INLINE constexpr std::uint32_t get_type_code();
namespace detail {
struct MD5_pair {
  uint32_t md5;
  uint32_t index;
  constexpr friend bool operator<(const MD5_pair &l, const MD5_pair &r) {
    return l.md5 < r.md5;
  }
  constexpr friend bool operator>(const MD5_pair &l, const MD5_pair &r) {
    return l.md5 > r.md5;
  }
  constexpr friend bool operator==(const MD5_pair &l, const MD5_pair &r) {
    return l.md5 == r.md5;
  }
};

template <typename T>
constexpr uint32_t get_types_code();

template <typename DerivedClasses>
struct MD5_set {
  static constexpr int size = std::tuple_size_v<DerivedClasses>;
  static_assert(size <= 256);

 private:
  template <std::size_t... Index>
  static constexpr std::array<MD5_pair, size> calculate_md5(
      std::index_sequence<Index...>) {
    std::array<MD5_pair, size> md5{};
    ((md5[Index] =
          MD5_pair{
              get_types_code<std::tuple_element_t<Index, DerivedClasses>>(),
              Index}),
     ...);
    compile_time_sort(md5);
    return md5;
  }
  static constexpr std::size_t has_hash_collision_impl() {
    for (std::size_t i = 1; i < size; ++i) {
      if (value[i - 1] == value[i]) {
        return value[i].index;
      }
    }
    return 0;
  }

 public:
  static constexpr std::array<MD5_pair, size> value =
      calculate_md5(std::make_index_sequence<size>());
  static constexpr std::size_t has_hash_collision = has_hash_collision_impl();
};

template <typename BaseClass, typename DerivedClasses>
struct public_base_class_checker {
  static_assert(std::tuple_size_v<DerivedClasses> <= 256);

 private:
  template <std::size_t... Index>
  static constexpr bool calculate_md5(std::index_sequence<Index...>) {
    return (std::is_base_of_v<BaseClass,
                              std::tuple_element_t<Index, DerivedClasses>> &&
            ...);
  }

 public:
  static constexpr bool value = public_base_class_checker::calculate_md5(
      std::make_index_sequence<std::tuple_size_v<DerivedClasses>>());
};

template <typename DerivedClasses>
struct deserialize_derived_class_helper {
  template <size_t index, typename BaseClass, typename unpack>
  static STRUCT_PACK_INLINE constexpr struct_pack::err_code run(
      std::unique_ptr<BaseClass> &base, unpack &unpacker) {
    if constexpr (index >= std::tuple_size_v<DerivedClasses>) {
      unreachable();
    }
    else {
      using derived_class = std::tuple_element_t<index, DerivedClasses>;
      base = std::make_unique<derived_class>();
      return unpacker.deserialize(*(derived_class *)base.get());
    }
  }
};

template <typename T, uint64_t parent_tag = 0>
constexpr size_info inline calculate_one_size(const T &item);

template <typename DerivedClasses>
struct calculate_one_size_derived_class_helper {
  template <size_t index, typename BaseClass>
  static STRUCT_PACK_INLINE constexpr size_info run(BaseClass *base) {
    if constexpr (index >= std::tuple_size_v<DerivedClasses>) {
      unreachable();
    }
    else {
      using derived_class = std::tuple_element_t<index, DerivedClasses>;
#ifdef STRUCT_PACK_RTTI_ENABLED
      assert(dynamic_cast<derived_class *>(base));
#endif
      return calculate_one_size(*(derived_class *)base);
    }
  }
};

template <typename DerivedClasses, typename size_type, typename version>
struct serialize_one_derived_class_helper {
  template <size_t index, typename packer, typename BaseClass>
  static STRUCT_PACK_INLINE constexpr void run(packer *self, BaseClass *base) {
    if constexpr (index >= std::tuple_size_v<DerivedClasses>) {
      unreachable();
    }
    else {
      using derived_class = std::tuple_element_t<index, DerivedClasses>;
#ifdef STRUCT_PACK_RTTI_ENABLED
      assert(dynamic_cast<derived_class *>(base));
#endif
      self->template serialize_one<size_type::value, version::value>(
          *(derived_class *)base);
    }
  }
};

template <typename DerivedClasses, typename size_type, typename version,
          typename NotSkip>
struct deserialize_one_derived_class_helper {
  template <size_t index, typename unpacker, typename Pointer>
  static STRUCT_PACK_INLINE constexpr struct_pack::err_code run(unpacker *self,
                                                                Pointer &base) {
    if constexpr (index >= std::tuple_size_v<DerivedClasses>) {
      unreachable();
    }
    else {
      using derived_class = std::tuple_element_t<index, DerivedClasses>;
      base = std::make_unique<derived_class>();
      return self->template deserialize_one<size_type::value, version::value,
                                            NotSkip::value>(
          *(derived_class *)base.get());
    }
  }
};

template <typename T>
void struct_pack_derived_decl(const T *) = delete;

template <typename Base>
using derived_class_set_t = decltype(struct_pack_derived_decl((Base *)nullptr));

template <typename Base>
constexpr std::size_t search_type_by_md5(uint32_t id, bool &ok) {
  constexpr auto &MD5s = MD5_set<derived_class_set_t<Base>>::value;
  auto result = std::lower_bound(MD5s.begin(), MD5s.end(), MD5_pair{id, 0});
  ok = (result != MD5s.end() && result->md5 == id);
  if (!ok)
    return MD5s.size();
  else
    return result->index;
}

template <typename T>
std::uint32_t get_struct_pack_id_impl() {
  if constexpr (std::is_abstract_v<T>) {
    struct_pack::detail::unreachable();
  }
  else {
    return struct_pack::get_type_code<T>();
  }
}

template <typename Base, typename... Derives>
constexpr auto derived_decl_impl() {
  if constexpr (std::is_abstract_v<Base>) {
    return struct_pack::detail::declval<std::tuple<Derives...>>();
  }
  else {
    return struct_pack::detail::declval<std::tuple<Base, Derives...>>();
  }
}

template <typename Base, typename... Derives>
constexpr bool check_ID_collision() {
  if constexpr (std::is_abstract_v<Base>) {
    constexpr auto has_collision = struct_pack::detail::MD5_set<
        std::tuple<Derives...>>::has_hash_collision;
    if constexpr (has_collision != 0) {
      static_assert(
          !sizeof(std::tuple_element_t<has_collision, std::tuple<Derives...>>),
          "ID collision happened, consider add member `static "
          "constexpr uint64_t struct_pack_id` for collision type. ");
    }
  }
  else {
    constexpr auto has_collision = struct_pack::detail::MD5_set<
        std::tuple<Base, Derives...>>::has_hash_collision;
    if constexpr (has_collision != 0) {
      static_assert(!sizeof(std::tuple_element_t<has_collision,
                                                 std::tuple<Base, Derives...>>),
                    "ID collision happened, consider add member `static "
                    "constexpr uint64_t struct_pack_id` for collision type. ");
    }
  }
  return true;
}
}  // namespace detail
}  // namespace struct_pack