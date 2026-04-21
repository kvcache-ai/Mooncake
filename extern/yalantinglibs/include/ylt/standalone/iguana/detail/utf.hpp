#pragma once

#include <cassert>
#include <stdexcept>

#include "iguana/define.h"

namespace iguana {
// https://github.com/Tencent/rapidjson/blob/master/include/rapidjson/reader.h
template <typename Ch = char, typename It>
inline unsigned parse_unicode_hex4(It &&it) {
  unsigned codepoint = 0;
  for (int i = 0; i < 4; i++) {
    Ch c = *it;
    codepoint <<= 4;
    codepoint += static_cast<unsigned>(c);
    if (c >= '0' && c <= '9')
      codepoint -= '0';
    else if (c >= 'A' && c <= 'F')
      codepoint -= 'A' - 10;
    else if (c >= 'a' && c <= 'f')
      codepoint -= 'a' - 10;
    else {
      throw std::runtime_error("Invalid Unicode Escape Hex");
    }
    ++it;
  }
  return codepoint;
}

// https://github.com/Tencent/rapidjson/blob/master/include/rapidjson/encodings.h
template <typename Ch = char, typename OutputStream>
inline void encode_utf8(OutputStream &os, unsigned codepoint) {
  if (codepoint <= 0x7F)
    os.push_back(static_cast<Ch>(codepoint & 0xFF));
  else if (codepoint <= 0x7FF) {
    os.push_back(static_cast<Ch>(0xC0 | ((codepoint >> 6) & 0xFF)));
    os.push_back(static_cast<Ch>(0x80 | ((codepoint & 0x3F))));
  }
  else if (codepoint <= 0xFFFF) {
    os.push_back(static_cast<Ch>(0xE0 | ((codepoint >> 12) & 0xFF)));
    os.push_back(static_cast<Ch>(0x80 | ((codepoint >> 6) & 0x3F)));
    os.push_back(static_cast<Ch>(0x80 | (codepoint & 0x3F)));
  }
  else {
    assert(codepoint <= 0x10FFFF);
    os.push_back(static_cast<Ch>(0xF0 | ((codepoint >> 18) & 0xFF)));
    os.push_back(static_cast<Ch>(0x80 | ((codepoint >> 12) & 0x3F)));
    os.push_back(static_cast<Ch>(0x80 | ((codepoint >> 6) & 0x3F)));
    os.push_back(static_cast<Ch>(0x80 | (codepoint & 0x3F)));
  }
}

// https://github.com/Tencent/rapidjson/blob/master/include/rapidjson/encodings.h
static inline unsigned char GetRange(unsigned char c) {
  static const unsigned char type[] = {
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
      0,    0,    0,    0,    0,    0,    0,    0,    0x10, 0x10, 0x10, 0x10,
      0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10,
      0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
      0x40, 0x40, 0x40, 0x40, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
      0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
      0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
      8,    8,    2,    2,    2,    2,    2,    2,    2,    2,    2,    2,
      2,    2,    2,    2,    2,    2,    2,    2,    2,    2,    2,    2,
      2,    2,    2,    2,    2,    2,    2,    2,    10,   3,    3,    3,
      3,    3,    3,    3,    3,    3,    3,    3,    3,    4,    3,    3,
      11,   6,    6,    6,    5,    8,    8,    8,    8,    8,    8,    8,
      8,    8,    8,    8,
  };
  return type[c];
}

// https://github.com/Tencent/rapidjson/blob/master/include/rapidjson/encodings.h
template <typename Ch = char, typename It>
inline bool decode_utf8(It &&it, unsigned &codepoint) {
  auto c = *(it++);
  bool result = true;
  auto copy = [&]() IGUANA__INLINE_LAMBDA {
    c = *(it++);
    codepoint = (codepoint << 6) | (static_cast<unsigned char>(c) & 0x3Fu);
  };
  auto trans = [&](unsigned mask) IGUANA__INLINE_LAMBDA {
    result &= ((GetRange(static_cast<unsigned char>(c)) & mask) != 0);
  };
  auto tail = [&]() IGUANA__INLINE_LAMBDA {
    copy();
    trans(0x70);
  };
  if (!(c & 0x80)) {
    codepoint = static_cast<unsigned char>(c);
    return true;
  }
  unsigned char type = GetRange(static_cast<unsigned char>(c));
  if (type >= 32) {
    codepoint = 0;
  }
  else {
    codepoint = (0xFFu >> type) & static_cast<unsigned char>(c);
  }
  switch (type) {
    case 2:
      tail();
      return result;
    case 3:
      tail();
      tail();
      return result;
    case 4:
      copy();
      trans(0x50);
      tail();
      return result;
    case 5:
      copy();
      trans(0x10);
      tail();
      tail();
      return result;
    case 6:
      tail();
      tail();
      tail();
      return result;
    case 10:
      copy();
      trans(0x20);
      tail();
      return result;
    case 11:
      copy();
      trans(0x60);
      tail();
      tail();
      return result;
    default:
      return false;
  }
}

}  // namespace iguana
