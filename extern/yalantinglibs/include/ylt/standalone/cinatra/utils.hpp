//
// Created by qiyu on 12/19/17.
//

#ifndef CINATRA_UTILS_HPP
#define CINATRA_UTILS_HPP

#pragma once
#include <algorithm>
#include <array>
#include <asio/buffer.hpp>
#include <cctype>
#include <charconv>
#include <cstddef>  //std::byte
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "define.h"
#include "response_cv.hpp"
#include "string_resize.hpp"

namespace cinatra {
struct ci_less {
  // case-independent (ci) compare_less binary function
  struct nocase_compare {
    bool operator()(const unsigned char &c1, const unsigned char &c2) const {
      return tolower(c1) < tolower(c2);
    }
  };

  bool operator()(std::string_view s1, std::string_view s2) const {
    return std::lexicographical_compare(s1.begin(), s1.end(),  // source range
                                        s2.begin(), s2.end(),  // dest range
                                        nocase_compare());     // comparison
  }
};

inline std::string get_content_type_str(req_content_type type) {
  std::string str;
  switch (type) {
    case req_content_type::html:
      str = "text/html; charset=UTF-8";
      break;
    case req_content_type::json:
      str = "application/json; charset=UTF-8";
      break;
    case req_content_type::text:
      str = "text/plain";
      break;
    case req_content_type::string:
      str = "text/html; charset=UTF-8";
      break;
    case req_content_type::multipart:
      str = "multipart/form-data; boundary=";
      break;
    case req_content_type::form_url_encode:
      str = "application/x-www-form-urlencoded";
      break;
    case req_content_type::octet_stream:
      str = "application/octet-stream";
      break;
    case req_content_type::xml:
      str = "application/xml";
      break;
    case req_content_type::none:
    default:
      break;
  }

  return str;
}

inline void replace_all(std::string &out, const std::string &from,
                        const std::string &to) {
  if (from.empty())
    return;
  size_t start_pos = 0;
  while ((start_pos = out.find(from, start_pos)) != std::string::npos) {
    out.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

inline std::string_view get_extension(std::string_view name) {
  size_t pos = name.rfind('.');
  if (pos == std::string_view::npos) {
    return {};
  }

  return name.substr(pos);
}

inline int64_t hex_to_int(std::string_view s) {
  if (s.empty())
    return -1;

  char *p;
  int64_t n = strtoll(s.data(), &p, 16);
  if (n == 0 && s.front() != '0') {
    return -1;
  }

  return n;
}

inline std::vector<std::string_view> split_sv(std::string_view s,
                                              std::string_view delimiter) {
  size_t start = 0;
  size_t end = s.find_first_of(delimiter);

  std::vector<std::string_view> output;

  while (end <= std::string_view::npos) {
    output.emplace_back(s.substr(start, end - start));

    if (end == std::string_view::npos)
      break;

    start = end + 1;
    end = s.find_first_of(delimiter, start);
  }

  return output;
}

inline std::string_view trim_sv(std::string_view v) {
  v.remove_prefix((std::min)(v.find_first_not_of(" "), v.size()));
  v.remove_suffix((std::min)(v.size() - v.find_last_not_of(" ") - 1, v.size()));
  return v;
}

inline void to_chunked_buffers(std::vector<asio::const_buffer> &buffers,
                               std::string &size_str,
                               std::string_view chunk_data, bool eof) {
  size_t length = chunk_data.size();
  if (length > 0) {
    // convert bytes transferred count to a hex string.
    detail::resize(size_str, 20);
    auto [ptr, ec] =
        std::to_chars(size_str.data(), size_str.data() + 20, length, 16);
    std::string_view chunk_size{size_str.data(),
                                size_t(std::distance(size_str.data(), ptr))};

    // Construct chunk based on rfc2616 section 3.6.1
    buffers.push_back(asio::buffer(chunk_size));
    buffers.push_back(asio::buffer(CRCF));
    buffers.push_back(asio::buffer(chunk_data, length));
    buffers.push_back(asio::buffer(CRCF));
  }

  // append last-chunk
  if (eof) {
    buffers.push_back(asio::buffer(LAST_CHUNK));
    buffers.push_back(asio::buffer(CRCF));
  }
}

static const std::string base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

inline std::string base64_encode(const std::string &str) {
  std::string ret;
  int i = 0;
  int j = 0;
  char char_array_3[3];
  char char_array_4[4];

  auto bytes_to_encode = str.data();
  size_t in_len = str.size();
  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] =
          ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] =
          ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (i = 0; (i < 4); i++) ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++) char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] =
        ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] =
        ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++) ret += base64_chars[char_array_4[j]];

    while ((i++ < 3)) ret += '=';
  }

  return ret;
}

// from h2o
// inline const char *MAP =
//    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
//    "abcdefghijklmnopqrstuvwxyz"
//    "0123456789+/";

inline bool is_valid_utf8(unsigned char *s, size_t length) {
  for (unsigned char *e = s + length; s != e;) {
    if (s + 4 <= e && ((*(uint32_t *)s) & 0x80808080) == 0) {
      s += 4;
    }
    else {
      while (!(*s & 0x80)) {
        if (++s == e) {
          return true;
        }
      }

      if ((s[0] & 0x60) == 0x40) {
        if (s + 1 >= e || (s[1] & 0xc0) != 0x80 || (s[0] & 0xfe) == 0xc0) {
          return false;
        }
        s += 2;
      }
      else if ((s[0] & 0xf0) == 0xe0) {
        if (s + 2 >= e || (s[1] & 0xc0) != 0x80 || (s[2] & 0xc0) != 0x80 ||
            (s[0] == 0xe0 && (s[1] & 0xe0) == 0x80) ||
            (s[0] == 0xed && (s[1] & 0xe0) == 0xa0)) {
          return false;
        }
        s += 3;
      }
      else if ((s[0] & 0xf8) == 0xf0) {
        if (s + 3 >= e || (s[1] & 0xc0) != 0x80 || (s[2] & 0xc0) != 0x80 ||
            (s[3] & 0xc0) != 0x80 || (s[0] == 0xf0 && (s[1] & 0xf0) == 0x80) ||
            (s[0] == 0xf4 && s[1] > 0x8f) || s[0] > 0xf4) {
          return false;
        }
        s += 4;
      }
      else {
        return false;
      }
    }
  }
  return true;
}

inline std::vector<std::string_view> split(std::string_view s,
                                           std::string_view delimiter) {
  size_t start = 0;
  size_t end = s.find_first_of(delimiter);

  std::vector<std::string_view> output;

  while (end <= std::string_view::npos) {
    output.emplace_back(s.substr(start, end - start));

    if (end == std::string_view::npos)
      break;

    start = end + 1;
    end = s.find_first_of(delimiter, start);
  }

  return output;
}

inline const std::unordered_map<std::string_view, std::string_view>
get_cookies_map(std::string_view cookies_str) {
  std::unordered_map<std::string_view, std::string_view> cookies;
  auto cookies_vec = split(cookies_str, "; ");
  for (auto iter : cookies_vec) {
    auto cookie_key_vlaue = split(iter, "=");
    if (cookie_key_vlaue.size() == 2) {
      cookies[cookie_key_vlaue[0]] = cookie_key_vlaue[1];
    }
  }
  return cookies;
};

template <bool is_first_time, bool is_last_time>
inline std::string_view get_chuncked_buffers(size_t length,
                                             std::array<char, 24> &buffer) {
  if constexpr (is_last_time) {
    return std::string_view{"\r\n0\r\n\r\n"};
  }
  else {
    auto [ptr, ec] = std::to_chars(
        buffer.data() + 2, buffer.data() + buffer.size() - 2, length, 16);
    *ptr++ = '\r';
    *ptr++ = '\n';
    if constexpr (is_first_time) {
      buffer[0] = '\r';
      buffer[1] = '\n';
      return std::string_view(buffer.data() + 2,
                              std::distance(buffer.data() + 2, ptr));
    }
    else {
      return std::string_view(buffer.data(), std::distance(buffer.data(), ptr));
    }
  }
}
}  // namespace cinatra

#endif  // CINATRA_UTILS_HPP
