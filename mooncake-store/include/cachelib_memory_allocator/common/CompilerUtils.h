/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#define CACHELIB_PACKED_ATTR __attribute__((__packed__))
#define CACHELIB_INLINE __attribute__((__always_inline__))

namespace facebook {
namespace cachelib {

/**
 * If the value is set to true for some type T, then T is safe to persistently
 * store in shared memory or nvm.
 */
template <class T>
struct IsShmSafe {
  static constexpr bool value = false;
};

namespace detail {
template <size_t>
struct CheckSize {};
} // namespace detail

/**
 * Certifies that the type named by 'Name' is of the given size and is
 * safe to store in shared memory or nvm.
 */
#define CACHELIB_SHM_CERTIFY(Name, size)                        \
  template <>                                                   \
  struct facebook::cachelib::IsShmSafe<Name> {                  \
    static constexpr bool value = true;                         \
  };                                                            \
  constexpr facebook::cachelib::detail::CheckSize<sizeof(Name)> \
      FB_ANONYMOUS_VARIABLE(checkSize) =                        \
          facebook::cachelib::detail::CheckSize<size>();        \
  static_assert(std::is_standard_layout<Name>::value,           \
                #Name "must be standard layout")

// convenience struct for getting the number of bits in a byte.
template <typename T>
struct NumBits {
  static constexpr unsigned int kBitsPerByte = 8;
  static constexpr uint8_t value =
      static_cast<uint8_t>(sizeof(T) * kBitsPerByte);
  static_assert(sizeof(T) * kBitsPerByte <= UINT8_MAX,
                "number of bits in this structure larger than max uint8_t");
};
} // namespace cachelib
} // namespace facebook
