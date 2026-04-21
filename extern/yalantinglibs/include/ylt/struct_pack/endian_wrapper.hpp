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
#include <bit>
#include <cstdint>
#include <type_traits>

#include "reflection.hpp"
#include "ylt/struct_pack/error_code.hpp"
#include "ylt/struct_pack/marco.h"
#include "ylt/struct_pack/util.h"

namespace struct_pack {
namespace detail {
#if __cpp_lib_endian >= 201907L
constexpr inline bool is_system_little_endian =
    (std::endian::little == std::endian::native);
static_assert(std::endian::native == std::endian::little ||
                  std::endian::native == std::endian::big,
              "struct_pack don't support middle-endian");
#else
#define BYTEORDER_LITTLE_ENDIAN 0  // Little endian machine.
#define BYTEORDER_BIG_ENDIAN 1     // Big endian machine.

//#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN

#ifndef BYTEORDER_ENDIAN
// Detect with GCC 4.6's macro.
#if defined(__BYTE_ORDER__)
#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
#elif (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
#else
#error \
    "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
#endif
// Detect with GLIBC's endian.h.
#elif defined(__GLIBC__)
#include <endian.h>
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
#elif (__BYTE_ORDER == __BIG_ENDIAN)
#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
#else
#error \
    "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
#endif
// Detect with _LITTLE_ENDIAN and _BIG_ENDIAN macro.
#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)
#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)
#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
// Detect with architecture macros.
#elif defined(__sparc) || defined(__sparc__) || defined(_POWER) || \
    defined(__powerpc__) || defined(__ppc__) || defined(__hpux) || \
    defined(__hppa) || defined(_MIPSEB) || defined(_POWER) ||      \
    defined(__s390__)
#define BYTEORDER_ENDIAN BYTEORDER_BIG_ENDIAN
#elif defined(__i386__) || defined(__alpha__) || defined(__ia64) ||  \
    defined(__ia64__) || defined(_M_IX86) || defined(_M_IA64) ||     \
    defined(_M_ALPHA) || defined(__amd64) || defined(__amd64__) ||   \
    defined(_M_AMD64) || defined(__x86_64) || defined(__x86_64__) || \
    defined(_M_X64) || defined(__bfin__)
#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
#elif defined(_MSC_VER) && (defined(_M_ARM) || defined(_M_ARM64))
#define BYTEORDER_ENDIAN BYTEORDER_LITTLE_ENDIAN
#else
#error \
    "Unknown machine byteorder endianness detected. User needs to define BYTEORDER_ENDIAN."
#endif
#endif
constexpr inline bool is_system_little_endian =
    (BYTEORDER_ENDIAN == BYTEORDER_LITTLE_ENDIAN);
#endif

template <std::size_t block_size>
constexpr inline bool is_little_endian_copyable =
    is_system_little_endian || block_size == 1;

template <typename T>
T swap_endian(T u) {
  union {
    T u;
    unsigned char u8[sizeof(T)];
  } source, dest;
  source.u = u;
  for (size_t k = 0; k < sizeof(T); k++)
    dest.u8[k] = source.u8[sizeof(T) - k - 1];
  return dest.u;
}

inline uint16_t bswap16(uint16_t raw) {
#ifdef _MSC_VER
  return _byteswap_ushort(raw);
#elif defined(__clang__) || defined(__GNUC__)
  return __builtin_bswap16(raw);
#else
  return swap_endian(raw);
#endif
};

inline uint32_t bswap32(uint32_t raw) {
#ifdef _MSC_VER
  return _byteswap_ulong(raw);
#elif defined(__clang__) || defined(__GNUC__)
  return __builtin_bswap32(raw);
#else
  return swap_endian(raw);
#endif
};

inline uint64_t bswap64(uint64_t raw) {
#ifdef _MSC_VER
  return _byteswap_uint64(raw);
#elif defined(__clang__) || defined(__GNUC__)
  return __builtin_bswap64(raw);
#else
  return swap_endian(raw);
#endif
};

template <std::size_t block_size, typename writer_t>
STRUCT_PACK_INLINE void write_wrapper(writer_t& writer,
                                      const char* SP_RESTRICT data) {
  if constexpr (is_system_little_endian || block_size == 1) {
    writer.write(data, block_size);
  }
  else if constexpr (block_size == 2) {
    auto tmp = bswap16(*(uint16_t*)data);
    writer.write((char*)&tmp, block_size);
  }
  else if constexpr (block_size == 4) {
    auto tmp = bswap32(*(uint32_t*)data);
    writer.write((char*)&tmp, block_size);
  }
  else if constexpr (block_size == 8) {
    auto tmp = bswap64(*(uint64_t*)data);
    writer.write((char*)&tmp, block_size);
  }
  else if constexpr (block_size == 16) {
    auto tmp1 = bswap64(*(uint64_t*)data),
         tmp2 = bswap64(*(uint64_t*)(data + 8));
    writer.write((char*)&tmp2, block_size);
    writer.write((char*)&tmp1, block_size);
  }
  else {
    static_assert(!sizeof(writer), "illegal block size(should be 1,2,4,8,16)");
  }
}
template <typename writer_t>
STRUCT_PACK_INLINE void write_bytes_array(writer_t& writer, const char* data,
                                          std::size_t length) {
  if SP_UNLIKELY (length >= PTRDIFF_MAX)
    unreachable();
  else
    writer.write(data, length);
}
template <std::size_t block_size, typename writer_t, typename T>
STRUCT_PACK_INLINE void low_bytes_write_wrapper(writer_t& writer,
                                                const T& elem) {
  static_assert(sizeof(T) >= block_size);
  if constexpr (is_system_little_endian) {
    const char* data = (const char*)&elem;
    writer.write(data, block_size);
  }
  else if constexpr (block_size == sizeof(T)) {
    write_wrapper<block_size>(writer, (const char*)&elem);
  }
  else {
    const char* data = sizeof(T) - block_size + (const char*)&elem;
    if constexpr (block_size == 1) {
      writer.write(data, block_size);
    }
    else if constexpr (block_size == 2) {
      auto tmp = bswap16(*(uint16_t*)data);
      writer.write((char*)&tmp, block_size);
    }
    else if constexpr (block_size == 4) {
      auto tmp = bswap32(*(uint32_t*)data);
      writer.write((char*)&tmp, block_size);
    }
    else {
      static_assert(!sizeof(writer), "illegal block size(should be 1,2,4)");
    }
  }
}
template <std::size_t block_size, typename reader_t>
STRUCT_PACK_INLINE bool read_wrapper(reader_t& reader, char* SP_RESTRICT data) {
  if constexpr (is_system_little_endian || block_size == 1) {
    return static_cast<bool>(reader.read(data, block_size));
  }
  else {
    std::array<char, block_size> tmp;
    bool res = static_cast<bool>(reader.read((char*)&tmp, block_size));
    if SP_UNLIKELY (!res) {
      return res;
    }
    if constexpr (block_size == 2) {
      *(uint16_t*)data = bswap16(*(uint16_t*)&tmp);
    }
    else if constexpr (block_size == 4) {
      *(uint32_t*)data = bswap32(*(uint32_t*)&tmp);
    }
    else if constexpr (block_size == 8) {
      *(uint64_t*)data = bswap64(*(uint64_t*)&tmp);
    }
    else if constexpr (block_size == 16) {
      *(uint64_t*)(data + 8) = bswap64(*(uint64_t*)&tmp);
      *(uint64_t*)data = bswap64(*(uint64_t*)(&tmp + 8));
    }
    else {
      static_assert(!sizeof(reader),
                    "illegal block size(should be 1,2,4,8,16)");
    }
    return true;
  }
}
template <typename reader_t>
STRUCT_PACK_INLINE bool read_bytes_array(reader_t& reader,
                                         char* SP_RESTRICT data,
                                         std::size_t length) {
  return static_cast<bool>(reader.read(data, length));
}
template <std::size_t block_size, typename reader_t, typename T>
STRUCT_PACK_INLINE bool low_bytes_read_wrapper(reader_t& reader, T& elem) {
  static_assert(sizeof(T) >= block_size);
  if constexpr (is_system_little_endian) {
    char* data = (char*)&elem;
    return static_cast<bool>(reader.read(data, block_size));
  }
  else if constexpr (block_size == sizeof(T)) {
    return read_wrapper<block_size>(reader, (char*)&elem);
  }
  else {
    char* data = (char*)&elem + sizeof(T) - block_size;
    if constexpr (block_size > 1) {
      char tmp[block_size];
      bool res = static_cast<bool>(reader.read(tmp, block_size));
      if SP_UNLIKELY (!res) {
        return res;
      }
      if constexpr (block_size == 2) {
        *(uint16_t*)data = bswap16(*(uint16_t*)tmp);
      }
      else if constexpr (block_size == 4) {
        *(uint32_t*)data = bswap32(*(uint32_t*)tmp);
      }
      else {
        static_assert(!sizeof(reader), "illegal block size(should be 1,2,4)");
      }
      return true;
    }
    else {
      return static_cast<bool>(reader.read(data, block_size));
    }
  }
}
}  // namespace detail
};  // namespace struct_pack