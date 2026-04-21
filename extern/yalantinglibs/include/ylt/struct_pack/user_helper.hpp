#pragma once
#include <cstdint>

#include "ylt/struct_pack/calculate_size.hpp"
#include "ylt/struct_pack/endian_wrapper.hpp"
#include "ylt/struct_pack/packer.hpp"
#include "ylt/struct_pack/reflection.hpp"
#include "ylt/struct_pack/unpacker.hpp"
namespace struct_pack {
template <std::size_t size_width = sizeof(uint64_t), typename Writer,
          typename T>
STRUCT_PACK_INLINE void write(Writer& writer, const T& t) {
  struct_pack::detail::packer<Writer, T, true> packer{writer};
  packer.template serialize_one<size_width, UINT64_MAX, 0>(t);
};
template <std::size_t size_width = sizeof(uint64_t), typename Writer,
          typename T>
STRUCT_PACK_INLINE void write(Writer& writer, const T* t, std::size_t len) {
  if constexpr (detail::is_trivial_serializable<T>::value &&
                detail::is_little_endian_copyable<sizeof(T)>) {
    detail::write_bytes_array(writer, (char*)t, sizeof(T) * len);
  }
  else {
    struct_pack::detail::packer<Writer, T, true> packer{writer};
    for (std::size_t i = 0; i < len; ++i) {
      packer.template serialize_one<size_width, UINT64_MAX, 0>(t[i]);
    }
  }
};
template <std::size_t size_width = sizeof(uint64_t), bool ifSkip = false,
          typename Reader, typename T>
STRUCT_PACK_INLINE struct_pack::err_code read(Reader& reader, T& t) {
  struct_pack::detail::unpacker<Reader, sp_config::DEFAULT, true> unpacker{
      reader};
  return unpacker.template deserialize_one<size_width, UINT64_MAX, !ifSkip>(t);
};
template <std::size_t size_width = sizeof(uint64_t), bool ifSkip = false,
          typename Reader, typename T>
STRUCT_PACK_INLINE struct_pack::err_code read(Reader& reader, T* t,
                                              std::size_t len) {
  if constexpr (detail::is_trivial_serializable<T>::value &&
                detail::is_little_endian_copyable<sizeof(T)>) {
    if constexpr (!ifSkip) {
      if SP_UNLIKELY (!detail::read_bytes_array(reader, (char*)t,
                                                sizeof(T) * len)) {
        return struct_pack::errc::no_buffer_space;
      }
    }
    else {
      return reader.ignore(sizeof(T) * len) ? errc{} : errc::no_buffer_space;
    }
  }
  else {
    struct_pack::detail::unpacker<Reader, sp_config::DEFAULT, true> unpacker{
        reader};
    for (std::size_t i = 0; i < len; ++i) {
      auto code =
          unpacker.template deserialize_one<size_width, UINT64_MAX, !ifSkip>(
              t[i]);
      if SP_UNLIKELY (code) {
        return code;
      }
    }
  }
  return {};
};
template <std::size_t size_width = sizeof(uint64_t), typename T>
STRUCT_PACK_INLINE constexpr std::size_t get_write_size(const T& t) {
  auto sz = struct_pack::detail::calculate_one_size(t);
  return sz.size_cnt * size_width + sz.total;
};
template <std::size_t size_width = sizeof(uint64_t), typename T>
STRUCT_PACK_INLINE constexpr std::size_t get_write_size(const T* t,
                                                        std::size_t len) {
  if constexpr (detail::is_trivial_serializable<T>::value) {
    return sizeof(T) * len;
  }
  else {
    detail::size_info sz{};
    for (std::size_t i = 0; i < len; ++i) {
      sz += struct_pack::detail::calculate_one_size(t[i]);
    }
    return sz.size_cnt * size_width + sz.total;
  }
};
};  // namespace struct_pack