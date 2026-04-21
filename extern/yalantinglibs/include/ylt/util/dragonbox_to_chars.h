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
// Copyright 2020-2022 Junekey Jeon
//
// The contents of this file may be used under the terms of
// the Apache License v2.0 with LLVM Exceptions.
//
//    (See accompanying file LICENSE-Apache or copy at
//     https://llvm.org/foundation/relicensing/LICENSE.txt)
//
// Alternatively, the contents of this file may be used under the terms of
// the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE-Boost or copy at
//     https://www.boost.org/LICENSE_1_0.txt)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

#ifndef JKJ_HEADER_DRAGONBOX_TO_CHARS
#define JKJ_HEADER_DRAGONBOX_TO_CHARS

#include "dragonbox.h"

#if defined(__GNUC__) || defined(__clang__)
#define JKJ_FORCEINLINE inline __attribute__((always_inline))
#elif defined(_MSC_VER)
#define JKJ_FORCEINLINE __forceinline
#else
#define JKJ_FORCEINLINE inline
#endif

namespace jkj::dragonbox {
namespace to_chars_detail {
template <class Float, class FloatTraits>
extern char *to_chars(typename FloatTraits::carrier_uint significand,
                      int exponent, char *buffer) noexcept;

// These "//"'s are to prevent clang-format to ruin this nice alignment.
// Thanks to reddit user u/mcmcc:
// https://www.reddit.com/r/cpp/comments/so3wx9/dragonbox_110_is_released_a_fast_floattostring/hw8z26r/?context=3
static constexpr char radix_100_table[] = {
    '0', '0', '0', '1', '0', '2', '0', '3', '0', '4',  //
    '0', '5', '0', '6', '0', '7', '0', '8', '0', '9',  //
    '1', '0', '1', '1', '1', '2', '1', '3', '1', '4',  //
    '1', '5', '1', '6', '1', '7', '1', '8', '1', '9',  //
    '2', '0', '2', '1', '2', '2', '2', '3', '2', '4',  //
    '2', '5', '2', '6', '2', '7', '2', '8', '2', '9',  //
    '3', '0', '3', '1', '3', '2', '3', '3', '3', '4',  //
    '3', '5', '3', '6', '3', '7', '3', '8', '3', '9',  //
    '4', '0', '4', '1', '4', '2', '4', '3', '4', '4',  //
    '4', '5', '4', '6', '4', '7', '4', '8', '4', '9',  //
    '5', '0', '5', '1', '5', '2', '5', '3', '5', '4',  //
    '5', '5', '5', '6', '5', '7', '5', '8', '5', '9',  //
    '6', '0', '6', '1', '6', '2', '6', '3', '6', '4',  //
    '6', '5', '6', '6', '6', '7', '6', '8', '6', '9',  //
    '7', '0', '7', '1', '7', '2', '7', '3', '7', '4',  //
    '7', '5', '7', '6', '7', '7', '7', '8', '7', '9',  //
    '8', '0', '8', '1', '8', '2', '8', '3', '8', '4',  //
    '8', '5', '8', '6', '8', '7', '8', '8', '8', '9',  //
    '9', '0', '9', '1', '9', '2', '9', '3', '9', '4',  //
    '9', '5', '9', '6', '9', '7', '9', '8', '9', '9'   //
};
static constexpr char radix_100_head_table[] = {
    '0', '.', '1', '.', '2', '.', '3', '.', '4', '.',  //
    '5', '.', '6', '.', '7', '.', '8', '.', '9', '.',  //
    '1', '.', '1', '.', '1', '.', '1', '.', '1', '.',  //
    '1', '.', '1', '.', '1', '.', '1', '.', '1', '.',  //
    '2', '.', '2', '.', '2', '.', '2', '.', '2', '.',  //
    '2', '.', '2', '.', '2', '.', '2', '.', '2', '.',  //
    '3', '.', '3', '.', '3', '.', '3', '.', '3', '.',  //
    '3', '.', '3', '.', '3', '.', '3', '.', '3', '.',  //
    '4', '.', '4', '.', '4', '.', '4', '.', '4', '.',  //
    '4', '.', '4', '.', '4', '.', '4', '.', '4', '.',  //
    '5', '.', '5', '.', '5', '.', '5', '.', '5', '.',  //
    '5', '.', '5', '.', '5', '.', '5', '.', '5', '.',  //
    '6', '.', '6', '.', '6', '.', '6', '.', '6', '.',  //
    '6', '.', '6', '.', '6', '.', '6', '.', '6', '.',  //
    '7', '.', '7', '.', '7', '.', '7', '.', '7', '.',  //
    '7', '.', '7', '.', '7', '.', '7', '.', '7', '.',  //
    '8', '.', '8', '.', '8', '.', '8', '.', '8', '.',  //
    '8', '.', '8', '.', '8', '.', '8', '.', '8', '.',  //
    '9', '.', '9', '.', '9', '.', '9', '.', '9', '.',  //
    '9', '.', '9', '.', '9', '.', '9', '.', '9', '.'   //
};

// These digit generation routines are inspired by James Anhalt's itoa
// algorithm: https://github.com/jeaiii/itoa The main idea is for given n, find
// y such that floor(10^k * y / 2^32) = n holds, where k is an appropriate
// integer depending on the length of n. For example, if n = 1234567, we set k
// = 6. In this case, we have floor(y / 2^32) = 1, floor(10^2 * ((10^0 * y) mod
// 2^32) / 2^32) = 23, floor(10^2 * ((10^2 * y) mod 2^32) / 2^32) = 45, and
// floor(10^2 * ((10^4 * y) mod 2^32) / 2^32) = 67.
// See https://jk-jeon.github.io/posts/2022/02/jeaiii-algorithm/ for more
// explanation.

JKJ_FORCEINLINE static void print_9_digits(std::uint32_t s32, int &exponent,
                                           char *&buffer) noexcept {
  // -- IEEE-754 binary32
  // Since we do not cut trailing zeros in advance, s32 must be of 6~9 digits
  // unless the original input was subnormal.
  // In particular, when it is of 9 digits it shouldn't have any trailing zeros.
  // -- IEEE-754 binary64
  // In this case, s32 must be of 7~9 digits unless the input is subnormal,
  // and it shouldn't have any trailing zeros if it is of 9 digits.
  if (s32 >= 1'0000'0000) {
    // 9 digits.
    // 1441151882 = ceil(2^57 / 1'0000'0000) + 1
    auto prod = s32 * std::uint64_t(1441151882);
    prod >>= 25;
    std::memcpy(buffer, radix_100_head_table + std::uint32_t(prod >> 32) * 2,
                2);

    prod = std::uint32_t(prod) * std::uint64_t(100);
    std::memcpy(buffer + 2, radix_100_table + std::uint32_t(prod >> 32) * 2, 2);
    prod = std::uint32_t(prod) * std::uint64_t(100);
    std::memcpy(buffer + 4, radix_100_table + std::uint32_t(prod >> 32) * 2, 2);
    prod = std::uint32_t(prod) * std::uint64_t(100);
    std::memcpy(buffer + 6, radix_100_table + std::uint32_t(prod >> 32) * 2, 2);
    prod = std::uint32_t(prod) * std::uint64_t(100);
    std::memcpy(buffer + 8, radix_100_table + std::uint32_t(prod >> 32) * 2, 2);

    exponent += 8;
    buffer += 10;
  }
  else if (s32 >= 100'0000) {
    // 7 or 8 digits.
    // 281474978 = ceil(2^48 / 100'0000) + 1
    auto prod = s32 * std::uint64_t(281474978);
    prod >>= 16;
    auto two_digits = std::uint32_t(prod >> 32);
    // If s32 is of 8 digits, increase the exponent by 7.
    // Otherwise, increase it by 6.
    exponent += (6 + unsigned(two_digits >= 10));

    // Write the first digit and the decimal point.
    std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
    // This third character may be overwritten later but we don't care.
    buffer[2] = radix_100_table[two_digits * 2 + 1];

    // Remaining 6 digits are all zero?
    if (std::uint32_t(prod) <=
        std::uint32_t((std::uint64_t(1) << 32) / 100'0000)) {
      // The number of characters actually written is:
      //   1, if only the first digit is nonzero, which means that either s32 is
      //   of 7 digits or it is of 8 digits but the second digit is zero, or 3,
      //   otherwise.
      // Note that buffer[2] is never zero if s32 is of 7 digits, because the
      // input is never zero.
      buffer +=
          (1 + (unsigned(two_digits >= 10) & unsigned(buffer[2] > '0')) * 2);
    }
    else {
      // At least one of the remaining 6 digits are nonzero.
      // After this adjustment, now the first destination becomes buffer + 2.
      buffer += unsigned(two_digits >= 10);

      // Obtain the next two digits.
      prod = std::uint32_t(prod) * std::uint64_t(100);
      two_digits = std::uint32_t(prod >> 32);
      std::memcpy(buffer + 2, radix_100_table + two_digits * 2, 2);

      // Remaining 4 digits are all zero?
      if (std::uint32_t(prod) <=
          std::uint32_t((std::uint64_t(1) << 32) / 1'0000)) {
        buffer += (3 + unsigned(buffer[3] > '0'));
      }
      else {
        // At least one of the remaining 4 digits are nonzero.

        // Obtain the next two digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        two_digits = std::uint32_t(prod >> 32);
        std::memcpy(buffer + 4, radix_100_table + two_digits * 2, 2);

        // Remaining 2 digits are all zero?
        if (std::uint32_t(prod) <=
            std::uint32_t((std::uint64_t(1) << 32) / 100)) {
          buffer += (5 + unsigned(buffer[5] > '0'));
        }
        else {
          // Obtain the last two digits.
          prod = std::uint32_t(prod) * std::uint64_t(100);
          two_digits = std::uint32_t(prod >> 32);
          std::memcpy(buffer + 6, radix_100_table + two_digits * 2, 2);

          buffer += (7 + unsigned(buffer[7] > '0'));
        }
      }
    }
  }
  else if (s32 >= 1'0000) {
    // 5 or 6 digits.
    // 429497 = ceil(2^32 / 1'0000)
    auto prod = s32 * std::uint64_t(429497);
    auto two_digits = std::uint32_t(prod >> 32);

    // If s32 is of 6 digits, increase the exponent by 5.
    // Otherwise, increase it by 4.
    exponent += (4 + unsigned(two_digits >= 10));

    // Write the first digit and the decimal point.
    std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
    // This third character may be overwritten later but we don't care.
    buffer[2] = radix_100_table[two_digits * 2 + 1];

    // Remaining 4 digits are all zero?
    if (std::uint32_t(prod) <=
        std::uint32_t((std::uint64_t(1) << 32) / 1'0000)) {
      // The number of characters actually written is 1 or 3, similarly to the
      // case of 7 or 8 digits.
      buffer +=
          (1 + (unsigned(two_digits >= 10) & unsigned(buffer[2] > '0')) * 2);
    }
    else {
      // At least one of the remaining 4 digits are nonzero.
      // After this adjustment, now the first destination becomes buffer + 2.
      buffer += unsigned(two_digits >= 10);

      // Obtain the next two digits.
      prod = std::uint32_t(prod) * std::uint64_t(100);
      two_digits = std::uint32_t(prod >> 32);
      std::memcpy(buffer + 2, radix_100_table + two_digits * 2, 2);

      // Remaining 2 digits are all zero?
      if (std::uint32_t(prod) <=
          std::uint32_t((std::uint64_t(1) << 32) / 100)) {
        buffer += (3 + unsigned(buffer[3] > '0'));
      }
      else {
        // Obtain the last two digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        two_digits = std::uint32_t(prod >> 32);
        std::memcpy(buffer + 4, radix_100_table + two_digits * 2, 2);

        buffer += (5 + unsigned(buffer[5] > '0'));
      }
    }
  }
  else if (s32 >= 100) {
    // 3 or 4 digits.
    // 42949673 = ceil(2^32 / 100)
    auto prod = s32 * std::uint64_t(42949673);
    auto two_digits = std::uint32_t(prod >> 32);

    // If s32 is of 4 digits, increase the exponent by 3.
    // Otherwise, increase it by 2.
    exponent += (2 + int(two_digits >= 10));

    // Write the first digit and the decimal point.
    std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
    // This third character may be overwritten later but we don't care.
    buffer[2] = radix_100_table[two_digits * 2 + 1];

    // Remaining 2 digits are all zero?
    if (std::uint32_t(prod) <= std::uint32_t((std::uint64_t(1) << 32) / 100)) {
      // The number of characters actually written is 1 or 3, similarly to the
      // case of 7 or 8 digits.
      buffer +=
          (1 + (unsigned(two_digits >= 10) & unsigned(buffer[2] > '0')) * 2);
    }
    else {
      // At least one of the remaining 2 digits are nonzero.
      // After this adjustment, now the first destination becomes buffer + 2.
      buffer += unsigned(two_digits >= 10);

      // Obtain the last two digits.
      prod = std::uint32_t(prod) * std::uint64_t(100);
      two_digits = std::uint32_t(prod >> 32);
      std::memcpy(buffer + 2, radix_100_table + two_digits * 2, 2);

      buffer += (3 + unsigned(buffer[3] > '0'));
    }
  }
  else {
    // 1 or 2 digits.
    // If s32 is of 2 digits, increase the exponent by 1.
    exponent += int(s32 >= 10);

    // Write the first digit and the decimal point.
    std::memcpy(buffer, radix_100_head_table + s32 * 2, 2);
    // This third character may be overwritten later but we don't care.
    buffer[2] = radix_100_table[s32 * 2 + 1];

    // The number of characters actually written is 1 or 3, similarly to the
    // case of 7 or 8 digits.
    buffer += (1 + (unsigned(s32 >= 10) & unsigned(buffer[2] > '0')) * 2);
  }
}

template <>
inline char *to_chars<float, default_float_traits<float>>(
    std::uint32_t s32, int exponent, char *buffer) noexcept {
  // Print significand.
  print_9_digits(s32, exponent, buffer);

  // Print exponent and return
  if (exponent < 0) {
    std::memcpy(buffer, "E-", 2);
    buffer += 2;
    exponent = -exponent;
  }
  else {
    buffer[0] = 'E';
    buffer += 1;
  }

  if (exponent >= 10) {
    std::memcpy(buffer, &radix_100_table[exponent * 2], 2);
    buffer += 2;
  }
  else {
    buffer[0] = char('0' + exponent);
    buffer += 1;
  }

  return buffer;
}

template <>
inline char *to_chars<double, default_float_traits<double>>(
    std::uint64_t const significand, int exponent, char *buffer) noexcept {
  // Print significand by decomposing it into a 9-digit block and a 8-digit
  // block.
  std::uint32_t first_block, second_block;
  bool no_second_block;

  if (significand >= 1'0000'0000) {
    first_block = std::uint32_t(significand / 1'0000'0000);
    second_block = std::uint32_t(significand) - first_block * 1'0000'0000;
    exponent += 8;
    no_second_block = (second_block == 0);
  }
  else {
    first_block = std::uint32_t(significand);
    no_second_block = true;
  }

  if (no_second_block) {
    print_9_digits(first_block, exponent, buffer);
  }
  else {
    // We proceed similarly to print_9_digits(), but since we do not need to
    // remove trailing zeros, the procedure is a bit simpler.
    if (first_block >= 1'0000'0000) {
      // The input is of 17 digits, thus there should be no trailing zero at
      // all. The first block is of 9 digits. 1441151882 = ceil(2^57 /
      // 1'0000'0000) + 1
      auto prod = first_block * std::uint64_t(1441151882);
      prod >>= 25;
      std::memcpy(buffer, radix_100_head_table + std::uint32_t(prod >> 32) * 2,
                  2);

      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 2, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 4, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 6, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 8, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);

      // The second block is of 8 digits.
      // 281474978 = ceil(2^48 / 100'0000) + 1
      prod = second_block * std::uint64_t(281474978);
      prod >>= 16;
      prod += 1;
      std::memcpy(buffer + 10, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 12, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 14, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);
      prod = std::uint32_t(prod) * std::uint64_t(100);
      std::memcpy(buffer + 16, radix_100_table + std::uint32_t(prod >> 32) * 2,
                  2);

      exponent += 8;
      buffer += 18;
    }
    else {
      if (first_block >= 100'0000) {
        // 7 or 8 digits.
        // 281474978 = ceil(2^48 / 100'0000) + 1
        auto prod = first_block * std::uint64_t(281474978);
        prod >>= 16;
        auto two_digits = std::uint32_t(prod >> 32);

        std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
        buffer[2] = radix_100_table[two_digits * 2 + 1];

        exponent += (6 + unsigned(two_digits >= 10));
        buffer += unsigned(two_digits >= 10);

        // Print remaining 6 digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 2, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 4, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 6, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);

        buffer += 8;
      }
      else if (first_block >= 1'0000) {
        // 5 or 6 digits.
        // 429497 = ceil(2^32 / 1'0000)
        auto prod = first_block * std::uint64_t(429497);
        auto two_digits = std::uint32_t(prod >> 32);

        std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
        buffer[2] = radix_100_table[two_digits * 2 + 1];

        exponent += (4 + unsigned(two_digits >= 10));
        buffer += unsigned(two_digits >= 10);

        // Print remaining 4 digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 2, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 4, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);

        buffer += 6;
      }
      else if (first_block >= 100) {
        // 3 or 4 digits.
        // 42949673 = ceil(2^32 / 100)
        auto prod = first_block * std::uint64_t(42949673);
        auto two_digits = std::uint32_t(prod >> 32);

        std::memcpy(buffer, radix_100_head_table + two_digits * 2, 2);
        buffer[2] = radix_100_table[two_digits * 2 + 1];

        exponent += (2 + unsigned(two_digits >= 10));
        buffer += unsigned(two_digits >= 10);

        // Print remaining 2 digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        std::memcpy(buffer + 2, radix_100_table + std::uint32_t(prod >> 32) * 2,
                    2);

        buffer += 4;
      }
      else {
        // 1 or 2 digits.
        std::memcpy(buffer, radix_100_head_table + first_block * 2, 2);
        buffer[2] = radix_100_table[first_block * 2 + 1];

        exponent += unsigned(first_block >= 10);
        buffer += (2 + unsigned(first_block >= 10));
      }

      // Next, print the second block.
      // The second block is of 8 digits, but we may have trailing zeros.
      // 281474978 = ceil(2^48 / 100'0000) + 1
      auto prod = second_block * std::uint64_t(281474978);
      prod >>= 16;
      prod += 1;
      auto two_digits = std::uint32_t(prod >> 32);
      std::memcpy(buffer, radix_100_table + two_digits * 2, 2);

      // Remaining 6 digits are all zero?
      if (std::uint32_t(prod) <=
          std::uint32_t((std::uint64_t(1) << 32) / 100'0000)) {
        buffer += (1 + unsigned(buffer[1] > '0'));
      }
      else {
        // Obtain the next two digits.
        prod = std::uint32_t(prod) * std::uint64_t(100);
        two_digits = std::uint32_t(prod >> 32);
        std::memcpy(buffer + 2, radix_100_table + two_digits * 2, 2);

        // Remaining 4 digits are all zero?
        if (std::uint32_t(prod) <=
            std::uint32_t((std::uint64_t(1) << 32) / 1'0000)) {
          buffer += (3 + unsigned(buffer[3] > '0'));
        }
        else {
          // Obtain the next two digits.
          prod = std::uint32_t(prod) * std::uint64_t(100);
          two_digits = std::uint32_t(prod >> 32);
          std::memcpy(buffer + 4, radix_100_table + two_digits * 2, 2);

          // Remaining 2 digits are all zero?
          if (std::uint32_t(prod) <=
              std::uint32_t((std::uint64_t(1) << 32) / 100)) {
            buffer += (5 + unsigned(buffer[5] > '0'));
          }
          else {
            // Obtain the last two digits.
            prod = std::uint32_t(prod) * std::uint64_t(100);
            two_digits = std::uint32_t(prod >> 32);
            std::memcpy(buffer + 6, radix_100_table + two_digits * 2, 2);
            buffer += (7 + unsigned(buffer[7] > '0'));
          }
        }
      }
    }
  }

  // Print exponent and return
  if (exponent < 0) {
    std::memcpy(buffer, "E-", 2);
    buffer += 2;
    exponent = -exponent;
  }
  else {
    buffer[0] = 'E';
    buffer += 1;
  }

  if (exponent >= 100) {
    // d1 = exponent / 10; d2 = exponent % 10;
    // 6554 = ceil(2^16 / 10)
    auto prod = std::uint32_t(exponent) * std::uint32_t(6554);
    auto d1 = prod >> 16;
    prod = std::uint16_t(prod) * std::uint32_t(5);  // * 10
    auto d2 = prod >> 15;                           // >> 16
    std::memcpy(buffer, &radix_100_table[d1 * 2], 2);
    buffer[2] = char('0' + d2);
    buffer += 3;
  }
  else if (exponent >= 10) {
    std::memcpy(buffer, &radix_100_table[exponent * 2], 2);
    buffer += 2;
  }
  else {
    buffer[0] = char('0' + exponent);
    buffer += 1;
  }

  return buffer;
}

// Avoid needless ABI overhead incurred by tag dispatch.
template <class PolicyHolder, class Float, class FloatTraits>
inline char *to_chars_n_impl(float_bits<Float, FloatTraits> br,
                             char *buffer) noexcept {
  auto const exponent_bits = br.extract_exponent_bits();
  auto const s = br.remove_exponent_bits(exponent_bits);

  if (br.is_finite(exponent_bits)) {
    if (s.is_negative()) {
      *buffer = '-';
      ++buffer;
    }
    if (br.is_nonzero()) {
      auto result = to_decimal<Float, FloatTraits>(
          s, exponent_bits, policy::sign::ignore, policy::trailing_zero::ignore,
          typename PolicyHolder::decimal_to_binary_rounding_policy{},
          typename PolicyHolder::binary_to_decimal_rounding_policy{},
          typename PolicyHolder::cache_policy{});
      return to_chars_detail::to_chars<Float, FloatTraits>(
          result.significand, result.exponent, buffer);
    }
    else {
      std::memcpy(buffer, "0E0", 3);
      return buffer + 3;
    }
  }
  else {
    if (s.has_all_zero_significand_bits()) {
      if (s.is_negative()) {
        *buffer = '-';
        ++buffer;
      }
      std::memcpy(buffer, "Infinity", 8);
      return buffer + 8;
    }
    else {
      std::memcpy(buffer, "NaN", 3);
      return buffer + 3;
    }
  }
}
}  // namespace to_chars_detail

// Returns the next-to-end position
template <class Float, class FloatTraits = default_float_traits<Float>,
          class... Policies>
inline char *to_chars_n(Float x, char *buffer, Policies... policies) noexcept {
  using namespace jkj::dragonbox::detail::policy_impl;
  using policy_holder = decltype(make_policy_holder(
      base_default_pair_list<
          base_default_pair<decimal_to_binary_rounding::base,
                            decimal_to_binary_rounding::nearest_to_even>,
          base_default_pair<binary_to_decimal_rounding::base,
                            binary_to_decimal_rounding::to_even>,
          base_default_pair<cache::base, cache::full>>{},
      policies...));

  return to_chars_detail::to_chars_n_impl<policy_holder>(
      float_bits<Float, FloatTraits>(x), buffer);
}

// Null-terminate and bypass the return value of fp_to_chars_n
template <class Float, class FloatTraits = default_float_traits<Float>,
          class... Policies>
inline char *to_chars(Float x, char *buffer, Policies... policies) noexcept {
  auto ptr = to_chars_n<Float, FloatTraits>(x, buffer, policies...);
  *ptr = '\0';
  return ptr;
}

// Maximum required buffer size (excluding null-terminator)
template <class FloatFormat>
inline constexpr std::size_t max_output_string_length =
    std::is_same_v<FloatFormat, ieee754_binary32>
        ?
        // sign(1) + significand(9) + decimal_point(1) + exp_marker(1) +
        // exp_sign(1) + exp(2)
        (1 + 9 + 1 + 1 + 1 + 2)
        :
        // format == ieee754_format::binary64
        // sign(1) + significand(17) + decimal_point(1) + exp_marker(1) +
        // exp_sign(1) + exp(3)
        (1 + 17 + 1 + 1 + 1 + 3);
}  // namespace jkj::dragonbox

#endif
