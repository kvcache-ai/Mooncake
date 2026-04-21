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

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "struct_pack/alignment.hpp"
#include "struct_pack/calculate_size.hpp"
#include "struct_pack/compatible.hpp"
#include "struct_pack/derived_helper.hpp"
#include "struct_pack/derived_marco.hpp"
#include "struct_pack/error_code.hpp"
#include "struct_pack/md5_constexpr.hpp"
#include "struct_pack/packer.hpp"
#include "struct_pack/reflection.hpp"
#include "struct_pack/trivial_view.hpp"
#include "struct_pack/type_calculate.hpp"
#include "struct_pack/type_id.hpp"
#include "struct_pack/type_trait.hpp"
#include "struct_pack/unpacker.hpp"
#include "struct_pack/user_helper.hpp"
#include "struct_pack/varint.hpp"

#if __has_include(<expected>) && __cplusplus > 202002L
#include <expected>
#if __cpp_lib_expected >= 202202L
#else
#include "util/expected.hpp"
#endif
#else
#include "util/expected.hpp"
#endif

namespace struct_pack {
inline std::error_code make_error_code(struct_pack::errc err) {
  return std::error_code(static_cast<int>(err),
                         struct_pack::detail::category());
}

/*!
 * \defgroup struct_pack struct_pack
 *
 * \brief yaLanTingLibs struct_pack
 *
 *
 *
 */

/*!
 * \ingroup struct_pack
 * Get the error message corresponding to the error code `err`.
 * @param err error code.
 * @return error message.
 */
inline std::string_view error_message(struct_pack::errc err) noexcept {
  return struct_pack::detail::make_error_message(err);
}

template <typename... Args>
constexpr std::uint32_t get_type_code() {
  static_assert(sizeof...(Args) > 0);
  std::uint32_t ret = 0;
  if constexpr (sizeof...(Args) == 1) {
    if constexpr (std::is_abstract_v<Args...>) {
      struct_pack::detail::unreachable();
    }
    else {
      ret = detail::get_types_code<Args...>();
    }
  }
  else {
    ret = detail::get_types_code<std::tuple<detail::remove_cvref_t<Args>...>>();
  }
  ret = ret - ret % 2;
  return ret;
}

template <typename... Args>
constexpr decltype(auto) get_type_literal() {
  static_assert(sizeof...(Args) > 0);
#ifdef STRUCT_PACK_DISABLE_OUTER_BRACE
  if constexpr (sizeof...(Args) == 1) {
    using Types = decltype(detail::get_types<Args...>());
    return detail::get_types_literal_without_outer_brace<Args..., Types>(
        std::make_index_sequence<std::tuple_size_v<Types>>());
  }
  else {
    return detail::get_types_literal_without_outer_brace<
        std::tuple<detail::remove_cvref_t<Args>...>,
        detail::remove_cvref_t<Args>...>();
  }
#else
  if constexpr (sizeof...(Args) == 1) {
    using Types = decltype(detail::get_types<Args...>());
    return detail::get_types_literal<Args..., Types>(
        std::make_index_sequence<std::tuple_size_v<Types>>());
  }
  else {
    return detail::get_types_literal<
        std::tuple<detail::remove_cvref_t<Args>...>,
        detail::remove_cvref_t<Args>...>();
  }
#endif
}

/*!
 * \ingroup struct_pack
 * Get the byte size of the packing objects.
 * TODO: add doc
 * @tparam Args the types of packing objects.
 * @param args the packing objects.
 * @return byte size.
 */

template <uint64_t conf = sp_config::DEFAULT, typename... Args>
[[nodiscard]] constexpr struct_pack::serialize_buffer_size get_needed_size(
    const Args &...args) {
  return detail::get_serialize_runtime_info<conf>(args...);
}

template <uint64_t conf = sp_config::DEFAULT, typename Writer, typename... Args>
void serialize_to(Writer &writer, const Args &...args) {
  static_assert(sizeof...(args) > 0);
  if constexpr (struct_pack::writer_t<Writer>) {
    auto info = detail::get_serialize_runtime_info<conf>(args...);
    struct_pack::detail::serialize_to<conf>(writer, info, args...);
  }
  else if constexpr (detail::struct_pack_buffer<Writer>) {
    static_assert(sizeof...(args) > 0);
    auto data_offset = writer.size();
    auto info = detail::get_serialize_runtime_info<conf>(args...);
    auto total = data_offset + info.size();
    detail::resize(writer, total);
    auto real_writer =
        struct_pack::detail::memory_writer{(char *)writer.data() + data_offset};
    struct_pack::detail::serialize_to<conf>(real_writer, info, args...);
  }
  else {
    static_assert(!sizeof(Writer),
                  "The Writer is not satisfied struct_pack::writer_t or "
                  "struct_pack_buffer requirement!");
  }
}

template <uint64_t conf = sp_config::DEFAULT, typename... Args>
void serialize_to(char *buffer, serialize_buffer_size info,
                  const Args &...args) {
  static_assert(sizeof...(args) > 0);
  auto writer = struct_pack::detail::memory_writer{(char *)buffer};
  struct_pack::detail::serialize_to<conf>(writer, info, args...);
}

template <uint64_t conf = sp_config::DEFAULT,
#if __cpp_concepts >= 201907L
          detail::struct_pack_buffer Buffer,
#else
          typename Buffer,
#endif
          typename... Args>
void serialize_to_with_offset(Buffer &buffer, std::size_t offset,
                              const Args &...args) {
#if __cpp_concepts < 201907L
  static_assert(detail::struct_pack_buffer<Buffer>,
                "The buffer is not satisfied struct_pack_buffer requirement!");
#endif
  static_assert(sizeof...(args) > 0);
  auto info = detail::get_serialize_runtime_info<conf>(args...);
  auto old_size = buffer.size();
  detail::resize(buffer, old_size + offset + info.size());
  auto writer = struct_pack::detail::memory_writer{(char *)buffer.data() +
                                                   old_size + offset};
  struct_pack::detail::serialize_to<conf>(writer, info, args...);
}

template <
#if __cpp_concepts >= 201907L
    detail::struct_pack_buffer Buffer = std::vector<char>,
#else
    typename Buffer = std::vector<char>,
#endif
    typename... Args>
[[nodiscard]] Buffer serialize(const Args &...args) {
#if __cpp_concepts < 201907L
  static_assert(detail::struct_pack_buffer<Buffer>,
                "The buffer is not satisfied struct_pack_buffer requirement!");
#endif
  static_assert(sizeof...(args) > 0);
  Buffer buffer;
  serialize_to(buffer, args...);
  return buffer;
}

template <
#if __cpp_concepts >= 201907L
    detail::struct_pack_buffer Buffer = std::vector<char>,
#else
    typename Buffer = std::vector<char>,
#endif
    typename... Args>
[[nodiscard]] Buffer serialize_with_offset(std::size_t offset,
                                           const Args &...args) {
#if __cpp_concepts < 201907L
  static_assert(detail::struct_pack_buffer<Buffer>,
                "The buffer is not satisfied struct_pack_buffer requirement!");
#endif
  static_assert(sizeof...(args) > 0);
  Buffer buffer;
  serialize_to_with_offset(buffer, offset, args...);
  return buffer;
}

template <uint64_t conf,
#if __cpp_concepts >= 201907L
          detail::struct_pack_buffer Buffer = std::vector<char>,
#else
          typename Buffer = std::vector<char>,
#endif
          typename... Args>
[[nodiscard]] Buffer serialize(const Args &...args) {
#if __cpp_concepts < 201907L
  static_assert(detail::struct_pack_buffer<Buffer>,
                "The buffer is not satisfied struct_pack_buffer requirement!");
#endif
  static_assert(sizeof...(args) > 0);
  Buffer buffer;
  serialize_to<conf>(buffer, args...);
  return buffer;
}

template <uint64_t conf,
#if __cpp_concepts >= 201907L
          detail::struct_pack_buffer Buffer = std::vector<char>,
#else
          typename Buffer = std::vector<char>,
#endif
          typename... Args>
[[nodiscard]] Buffer serialize_with_offset(std::size_t offset,
                                           const Args &...args) {
#if __cpp_concepts < 201907L
  static_assert(detail::struct_pack_buffer<Buffer>,
                "The buffer is not satisfied struct_pack_buffer requirement!");
#endif
  static_assert(sizeof...(args) > 0);
  Buffer buffer;
  serialize_to_with_offset<conf>(buffer, offset, args...);
  return buffer;
}

#if __cpp_concepts >= 201907L
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::detail::deserialize_view View>
#else
template <
    uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
    typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const View &v,
                                                   Args &...args) {
  detail::memory_reader reader{(const char *)v.data(),
                               (const char *)v.data() + v.size()};
  detail::unpacker<detail::memory_reader, conf> in(reader);
  return in.deserialize(t, args...);
}

template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args>
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const char *data,
                                                   size_t size, Args &...args) {
  detail::memory_reader reader{data, data + size};
  detail::unpacker<detail::memory_reader, conf> in(reader);
  return in.deserialize(t, args...);
}

#if __cpp_concepts >= 201907L
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::reader_t Reader>
#else
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, Reader &reader,
                                                   Args &...args) {
  detail::unpacker<Reader, conf> in(reader);
  std::size_t consume_len;
  auto old_pos = reader.tellg();
  auto ret = in.deserialize_with_len(consume_len, t, args...);
  std::size_t delta = reader.tellg() - old_pos;
  if SP_LIKELY (consume_len > 0) {
    if SP_UNLIKELY (delta > consume_len) {
      // TODO test this branch
      ret = struct_pack::errc::invalid_buffer;
    }
    else {
      reader.ignore(consume_len - delta);
    }
  }

  return ret;
}
#if __cpp_concepts >= 201907L
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::detail::deserialize_view View>
#else
template <
    uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
    typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const View &v,
                                                   size_t &consume_len,
                                                   Args &...args) {
  detail::memory_reader reader{(const char *)v.data(),
                               (const char *)v.data() + v.size()};
  detail::unpacker<detail::memory_reader, conf> in(reader);
  auto ret = in.deserialize_with_len(consume_len, t, args...);
  if SP_LIKELY (!ret) {
    consume_len = (std::max)((size_t)(reader.now - v.data()), consume_len);
  }
  else {
    consume_len = 0;
  }
  return ret;
}

template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args>
[[nodiscard]] struct_pack::err_code deserialize_to(T &t, const char *data,
                                                   size_t size,
                                                   size_t &consume_len,
                                                   Args &...args) {
  detail::memory_reader reader{data, data + size};
  detail::unpacker<detail::memory_reader, conf> in(reader);
  auto ret = in.deserialize_with_len(consume_len, t, args...);
  if SP_LIKELY (!ret) {
    consume_len = (std::max)((size_t)(reader.now - data), consume_len);
  }
  else {
    consume_len = 0;
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          struct_pack::detail::deserialize_view View>
#else
template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args,
          typename View>
#endif
[[nodiscard]] struct_pack::err_code deserialize_to_with_offset(T &t,
                                                               const View &v,
                                                               size_t &offset,
                                                               Args &...args) {
  size_t sz;
  auto ret =
      deserialize_to(t, v.data() + offset, v.size() - offset, sz, args...);
  offset += sz;
  return ret;
}

template <uint64_t conf = sp_config::DEFAULT, typename T, typename... Args>
[[nodiscard]] struct_pack::err_code deserialize_to_with_offset(
    T &t, const char *data, size_t size, size_t &offset, Args &...args) {
  size_t sz;
  auto ret = deserialize_to(t, data + offset, size - offset, sz, args...);
  offset += sz;
  return ret;
}

#if __cpp_concepts >= 201907L
template <typename... Args, struct_pack::detail::deserialize_view View>
#else
template <
    typename... Args, typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] auto deserialize(const View &v) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to(ret.value(), v);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

template <typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  if (auto errc = deserialize_to(ret.value(), data, size); errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}
#if __cpp_concepts >= 201907L
template <typename... Args, struct_pack::reader_t Reader>
#else
template <typename... Args, typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] auto deserialize(Reader &v) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to(ret.value(), v);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <typename... Args, struct_pack::detail::deserialize_view View>
#else
template <typename... Args, typename View>
#endif
[[nodiscard]] auto deserialize(const View &v, size_t &consume_len) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to(ret.value(), v, consume_len);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

template <typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size,
                               size_t &consume_len) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to(ret.value(), data, size, consume_len);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <uint64_t conf, typename... Args,
          struct_pack::detail::deserialize_view View>
#else
template <
    uint64_t conf, typename... Args, typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] auto deserialize(const View &v) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to<conf>(ret.value(), v);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

template <uint64_t conf, typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size) {
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  if (auto errc = deserialize_to<conf>(ret.value(), data, size); errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <uint64_t conf, typename... Args, struct_pack::reader_t Reader>
#else
template <uint64_t conf, typename... Args, typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] auto deserialize(Reader &v) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to<conf>(ret.value(), v);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <uint64_t conf, typename... Args,
          struct_pack::detail::deserialize_view View>
#else
template <uint64_t conf, typename... Args, typename View>
#endif
[[nodiscard]] auto deserialize(const View &v, size_t &consume_len) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to<conf>(ret.value(), v, consume_len);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

template <uint64_t conf, typename... Args>
[[nodiscard]] auto deserialize(const char *data, size_t size,
                               size_t &consume_len) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to<conf>(ret.value(), data, size, consume_len);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <typename... Args, struct_pack::detail::deserialize_view View>
#else
template <typename... Args, typename View>
#endif
[[nodiscard]] auto deserialize_with_offset(const View &v, size_t &offset) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to_with_offset(ret.value(), v, offset);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

template <typename... Args>
[[nodiscard]] auto deserialize_with_offset(const char *data, size_t size,
                                           size_t &offset) {
  static_assert(sizeof...(Args) > 0,
                "the correct code is struct_pack::deserialize<Type...>();");
  expected<detail::get_args_type<Args...>, struct_pack::err_code> ret;
  auto errc = deserialize_to_with_offset(ret.value(), data, size, offset);
  if SP_UNLIKELY (errc) {
    ret = unexpected<struct_pack::err_code>{errc};
  }
  return ret;
}

#if __cpp_concepts >= 201907L
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field, struct_pack::detail::deserialize_view View>
#else
template <
    typename T, size_t I, uint64_t conf = sp_config::DEFAULT, typename Field,
    typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, const View &v) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  static_assert(std::is_same_v<Field, T_Field>,
                "The dst's type is not correct. It should be as same as the "
                "T's Ith field's type");
  detail::memory_reader reader((const char *)v.data(),
                               (const char *)v.data() + v.size());
  detail::unpacker<detail::memory_reader, conf> in(reader);
  return in.template get_field<T, I>(dst);
}

template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field>
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, const char *data,
                                                 size_t size) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  static_assert(std::is_same_v<Field, T_Field>,
                "The dst's type is not correct. It should be as same as the "
                "T's Ith field's type");
  detail::memory_reader reader{data, data + size};
  detail::unpacker<detail::memory_reader, conf> in(reader);
  return in.template get_field<T, I>(dst);
}

#if __cpp_concepts >= 201907L
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field, struct_pack::reader_t Reader>
#else
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Field, typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] struct_pack::err_code get_field_to(Field &dst, Reader &reader) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  static_assert(std::is_same_v<Field, T_Field>,
                "The dst's type is not correct. It should be as same as the "
                "T's Ith field's type");
  detail::unpacker<Reader, conf> in(reader);
  return in.template get_field<T, I>(dst);
}

#if __cpp_concepts >= 201907L
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          struct_pack::detail::deserialize_view View>
#else
template <
    typename T, size_t I, uint64_t conf = sp_config::DEFAULT, typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] auto get_field(const View &v) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  expected<T_Field, struct_pack::err_code> ret;
  auto ec = get_field_to<T, I, conf>(ret.value(), v);
  if SP_UNLIKELY (ec) {
    ret = unexpected<struct_pack::err_code>{ec};
  }
  return ret;
}

template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT>
[[nodiscard]] auto get_field(const char *data, size_t size) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  expected<T_Field, struct_pack::err_code> ret;
  auto ec = get_field_to<T, I, conf>(ret.value(), data, size);
  if SP_UNLIKELY (ec) {
    ret = unexpected<struct_pack::err_code>{ec};
  }
  return ret;
}
#if __cpp_concepts >= 201907L
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          struct_pack::reader_t Reader>
#else
template <typename T, size_t I, uint64_t conf = sp_config::DEFAULT,
          typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] auto get_field(Reader &reader) {
  using T_Field = std::tuple_element_t<I, decltype(detail::get_types<T>())>;
  expected<T_Field, struct_pack::err_code> ret;
  auto ec = get_field_to<T, I, conf>(ret.value(), reader);
  if SP_UNLIKELY (ec) {
    ret = unexpected<struct_pack::err_code>{ec};
  }
  return ret;
}
#if __cpp_concepts >= 201907L
template <typename BaseClass, typename... DerivedClasses,
          struct_pack::reader_t Reader>
#else
template <typename BaseClass, typename... DerivedClasses, typename Reader,
          typename = std::enable_if_t<struct_pack::reader_t<Reader>>>
#endif
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(Reader &reader) {
  static_assert(sizeof...(DerivedClasses) > 0,
                "There must have a least one derived class");
  static_assert(
      struct_pack::detail::public_base_class_checker<
          BaseClass, std::tuple<DerivedClasses...>>::value,
      "the First type should be the base class of all derived class ");
  constexpr auto has_hash_collision = struct_pack::detail::MD5_set<
      std::tuple<DerivedClasses...>>::has_hash_collision;
  if constexpr (has_hash_collision != 0) {
    static_assert(!sizeof(std::tuple_element_t<has_hash_collision,
                                               std::tuple<DerivedClasses...>>),
                  "ID collision happened, consider add member `static "
                  "constexpr uint64_t struct_pack_id` for collision type. ");
  }
  else {
    struct_pack::expected<std::unique_ptr<BaseClass>, struct_pack::err_code>
        ret;
    auto ec = struct_pack::detail::deserialize_derived_class<BaseClass,
                                                             DerivedClasses...>(
        ret.value(), reader);
    if SP_UNLIKELY (ec) {
      ret = unexpected<struct_pack::err_code>{ec};
    }
    return ret;
  }
}
#if __cpp_concepts >= 201907L
template <typename BaseClass, typename... DerivedClasses,
          struct_pack::detail::deserialize_view View>
#else
template <
    typename BaseClass, typename... DerivedClasses, typename View,
    typename = std::enable_if_t<struct_pack::detail::deserialize_view<View>>>
#endif
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(const View &v) {
  detail::memory_reader reader{v.data(), v.data() + v.size()};
  if constexpr (std::is_abstract_v<BaseClass>) {
    return deserialize_derived_class<BaseClass, DerivedClasses...>(reader);
  }
  else {
    return deserialize_derived_class<BaseClass, BaseClass, DerivedClasses...>(
        reader);
  }
}
template <typename BaseClass, typename... DerivedClasses>
[[nodiscard]] struct_pack::expected<std::unique_ptr<BaseClass>,
                                    struct_pack::err_code>
deserialize_derived_class(const char *data, size_t size) {
  detail::memory_reader reader{data, data + size};
  if constexpr (std::is_abstract_v<BaseClass>) {
    return deserialize_derived_class<BaseClass, DerivedClasses...>(reader);
  }
  else {
    return deserialize_derived_class<BaseClass, BaseClass, DerivedClasses...>(
        reader);
  }
}

}  // namespace struct_pack