// Glaze Library
// For the license information refer to glaze.hpp

#pragma once

#include "common.hpp"
#include "detail/string_stream.hpp"
#include "util.hpp"
#include "value.hpp"

namespace iguana {

class numeric_str {
 public:
  std::string_view &value() { return val_; }
  std::string_view value() const { return val_; }
  template <typename T>
  T convert() {
    static_assert(num_v<T>, "T must be numeric type");
    if (val_.empty())
      IGUANA_UNLIKELY { throw std::runtime_error("Failed to parse number"); }
    T res;
    detail::from_chars(val_.data(), val_.data() + val_.size(), res);
    return res;
  }

 private:
  std::string_view val_;
};

template <typename T>
constexpr inline bool numeric_str_v =
    std::is_same_v<numeric_str, std::remove_cvref_t<T>>;

template <typename It>
IGUANA_INLINE void skip_comment(It &&it, It &&end) {
  ++it;
  if (it == end)
    IGUANA_UNLIKELY {
      throw std::runtime_error("Unexpected end, expected comment");
    }
  else if (*it == '/') {
    while (++it != end && *it != '\n')
      ;
  }
  else if (*it == '*') {
    while (++it != end) {
      if (*it == '*')
        IGUANA_UNLIKELY {
          if (++it == end)
            IGUANA_UNLIKELY { break; }
          else if (*it == '/')
            IGUANA_LIKELY {
              ++it;
              break;
            }
        }
    }
  }
  else
    IGUANA_UNLIKELY throw std::runtime_error("Expected / or * after /");
}

template <typename It>
IGUANA_INLINE void skip_ws(It &&it, It &&end) {
  while (it != end) {
    // assuming ascii
    if (static_cast<uint8_t>(*it) < 33) {
      ++it;
    }
    else if (*it == '/') {
      skip_comment(it, end);
    }
    else {
      break;
    }
  }
}

template <typename It>
IGUANA_INLINE void skip_ws_no_comments(It &&it, It &&end) {
  while (it != end) {
    // assuming ascii
    if (static_cast<uint8_t>(*it) < 33)
      IGUANA_LIKELY { ++it; }
    else {
      break;
    }
  }
}

inline constexpr auto has_escape = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0101110001011100010111000101110001011100010111000101110001011100);
};

template <typename It>
IGUANA_INLINE void skip_till_escape_or_qoute(It &&it, It &&end) {
  static_assert(contiguous_iterator<std::decay_t<decltype(it)>>);
  if (std::distance(it, end) >= 7)
    IGUANA_LIKELY {
      const auto end_m7 = end - 7;
      for (; it < end_m7; it += 8) {
        const auto chunk = *reinterpret_cast<const uint64_t *>(&*it);
        uint64_t test = has_qoute(chunk) | has_escape(chunk);
        if (test != 0) {
          it += (countr_zero(test) >> 3);
          return;
        }
      }
    }

  // Tail end of buffer. Should be rare we even get here
  while (it < end) {
    switch (*it) {
      case '\\':
      case '"':
        return;
    }
    ++it;
  }
  throw std::runtime_error("Expected \"");
}

template <typename It>
IGUANA_INLINE void skip_till_qoute(It &&it, It &&end) {
  static_assert(contiguous_iterator<std::decay_t<decltype(it)>>);
  if (std::distance(it, end) >= 7)
    IGUANA_LIKELY {
      const auto end_m7 = end - 7;
      for (; it < end_m7; it += 8) {
        const auto chunk = *reinterpret_cast<const uint64_t *>(&*it);
        uint64_t test = has_qoute(chunk);
        if (test != 0) {
          it += (countr_zero(test) >> 3);
          return;
        }
      }
    }

  // Tail end of buffer. Should be rare we even get here
  while (it < end) {
    if (*it == '"')
      return;
    ++it;
  }
  throw std::runtime_error("Expected \"");
}

template <typename It>
IGUANA_INLINE void skip_string(It &&it, It &&end) noexcept {
  ++it;
  while (it < end) {
    if (*it == '"') {
      ++it;
      break;
    }
    else if (*it == '\\' && ++it == end)
      IGUANA_UNLIKELY
    break;
    ++it;
  }
}

template <char open, char close, typename It>
IGUANA_INLINE void skip_until_closed(It &&it, It &&end) {
  ++it;
  size_t open_count = 1;
  size_t close_count = 0;
  while (it < end && open_count > close_count) {
    switch (*it) {
      case '/':
        skip_comment(it, end);
        break;
      case '"':
        skip_string(it, end);
        break;
      case open:
        ++open_count;
        ++it;
        break;
      case close:
        ++close_count;
        ++it;
        break;
      default:
        ++it;
    }
  }
}

IGUANA_INLINE bool is_numeric(char c) noexcept {
  static constexpr int is_num[256] = {
      // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 0
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 1
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0,  // 2
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,  // 3
      0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 4
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 5
      0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 6
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 7
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 8
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 9
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // A
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // B
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // C
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // D
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // E
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0   // F
  };
  return static_cast<bool>(is_num[static_cast<unsigned int>(c)]);
}

// '\t' '\r' '\n'  '"' '}' ']' ',' ' '  '\0'
IGUANA_INLINE bool can_follow_number(char c) noexcept {
  static constexpr int can_follow_num[256] = {
      //  0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
      1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0,  // 0
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 1
      1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,  // 2
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 3
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 4
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,  // 5
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 6
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,  // 7
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 8
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 9
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // A
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // B
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // C
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // D
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // E
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0   // F
  };
  return static_cast<bool>(can_follow_num[static_cast<unsigned int>(c)]);
}

}  // namespace iguana
