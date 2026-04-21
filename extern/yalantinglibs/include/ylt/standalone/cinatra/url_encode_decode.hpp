//
// Created by xmh on 18-3-16.
//

#ifndef CPPWEBSERVER_URL_ENCODE_DECODE_HPP
#define CPPWEBSERVER_URL_ENCODE_DECODE_HPP
#include <codecvt>
#include <locale>
#include <string>
#include <string_view>
namespace code_utils {
inline static std::string url_encode(const std::string &value) noexcept {
  static auto hex_chars = "0123456789ABCDEF";

  std::string result;
  result.reserve(value.size());  // Minimum size of result

  for (auto &chr : value) {
    if (!((chr >= '0' && chr <= '9') || (chr >= 'A' && chr <= 'Z') ||
          (chr >= 'a' && chr <= 'z') || chr == '-' || chr == '.' ||
          chr == '_' || chr == '~'))
      result += std::string("%") +
                hex_chars[static_cast<unsigned char>(chr) >> 4] +
                hex_chars[static_cast<unsigned char>(chr) & 15];
    else
      result += chr;
  }

  return result;
}

inline static std::string url_decode(std::string_view str) noexcept {
  std::string result;
  result.reserve(str.size());

  for (size_t i = 0; i < str.size(); ++i) {
    char ch = str[i];
    if (ch == '%') {
      constexpr char hex[] = "0123456789ABCDEF";

      if (++i == str.size()) {
        result.push_back('?');
        break;
      }

      int hi = (int)(std::find(hex, hex + 16, toupper(str[i])) - hex);

      if (++i == str.size()) {
        result.push_back('?');
        break;
      }

      int lo = (int)(std::find(hex, hex + 16, toupper(str[i])) - hex);

      if ((hi >= 16) || (lo >= 16)) {
        result.push_back('?');
        break;
      }

      result.push_back((char)((hi << 4) + lo));
    }
    else if (ch == '+')
      result.push_back(' ');
    else
      result.push_back(ch);
  }

  return result;
}

// from h2o
inline const char *MAP =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

inline const char *MAP_URL_ENCODED =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789-_";
inline size_t base64_encode(char *_dst, const void *_src, size_t len,
                            int url_encoded) {
  char *dst = _dst;
  const uint8_t *src = reinterpret_cast<const uint8_t *>(_src);
  const char *map = url_encoded ? MAP_URL_ENCODED : MAP;
  uint32_t quad;

  for (; len >= 3; src += 3, len -= 3) {
    quad = ((uint32_t)src[0] << 16) | ((uint32_t)src[1] << 8) | src[2];
    *dst++ = map[quad >> 18];
    *dst++ = map[(quad >> 12) & 63];
    *dst++ = map[(quad >> 6) & 63];
    *dst++ = map[quad & 63];
  }
  if (len != 0) {
    quad = (uint32_t)src[0] << 16;
    *dst++ = map[quad >> 18];
    if (len == 2) {
      quad |= (uint32_t)src[1] << 8;
      *dst++ = map[(quad >> 12) & 63];
      *dst++ = map[(quad >> 6) & 63];
      if (!url_encoded)
        *dst++ = '=';
    }
    else {
      *dst++ = map[(quad >> 12) & 63];
      if (!url_encoded) {
        *dst++ = '=';
        *dst++ = '=';
      }
    }
  }

  *dst = '\0';
  return dst - _dst;
}

inline static std::string get_string_by_urldecode(std::string_view content) {
  return url_decode(std::string(content.data(), content.size()));
}

}  // namespace code_utils
#endif  // CPPWEBSERVER_URL_ENCODE_DECODE_HPP