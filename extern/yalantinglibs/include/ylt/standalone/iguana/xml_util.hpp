#pragma once
#include "common.hpp"
#include "detail/pb_type.hpp"
#include "util.hpp"

namespace iguana {
template <typename T>
struct iguana_required_struct;
#define REQUIRED_IMPL(STRUCT_NAME, N, ...)             \
  template <>                                          \
  struct iguana::iguana_required_struct<STRUCT_NAME> { \
    inline static constexpr auto requied_arr() {       \
      std::array<std::string_view, N> arr_required = { \
          WRAP_ARGS(CONCAT_NAME, 0, ##__VA_ARGS__)};   \
      return arr_required;                             \
    }                                                  \
  };

#define REQUIRED(STRUCT_NAME, ...) \
  REQUIRED_IMPL(STRUCT_NAME, YLT_ARG_COUNT(__VA_ARGS__), __VA_ARGS__)

template <class T, class = void>
struct has_iguana_required_arr : std::false_type {};

template <class T>
struct has_iguana_required_arr<
    T, std::void_t<decltype(iguana_required_struct<T>::requied_arr())>>
    : std::true_type {};

template <class T>
constexpr bool has_iguana_required_arr_v = has_iguana_required_arr<T>::value;

template <typename T,
          typename map_type = std::unordered_map<std::string, std::string>>
class xml_attr_t {
 public:
  T &value() { return val_; }
  map_type &attr() { return attr_; }
  const T &value() const { return val_; }
  const map_type &attr() const { return attr_; }
  using value_type = std::remove_cvref_t<T>;

 private:
  T val_;
  map_type attr_;
};

template <typename T>
using xml_attr_view_t =
    xml_attr_t<T, std::map<std::string_view, std::string_view>>;

template <typename T = std::string_view,
          std::enable_if_t<string_container_v<T>, int> = 0>
class xml_cdata_t {
 public:
  T &value() { return val_; }
  const T &value() const { return val_; }
  using value_type = std::remove_cvref_t<T>;

 private:
  T val_;
};

template <typename T>
constexpr inline bool is_attr_t_v = false;

template <typename T, typename map_type>
constexpr inline bool is_attr_t_v<xml_attr_t<T, map_type>> = true;

template <typename T>
constexpr inline bool attr_v = is_attr_t_v<std::remove_cvref_t<T>>;

template <typename>
struct is_cdata_t : std::false_type {};

template <typename T>
struct is_cdata_t<xml_cdata_t<T>> : std::true_type {};

template <std::size_t index, template <typename...> typename Condition,
          typename Tuple>
constexpr int element_index_helper() {
  if constexpr (index == std::tuple_size_v<Tuple>) {
    return index;
  }
  else {
    using item_type =
        ylt::reflection::remove_cvref_t<std::tuple_element_t<index, Tuple>>;

    return Condition<item_type>::value
               ? index
               : element_index_helper<index + 1, Condition, Tuple>();
  }
}

template <template <typename...> typename Condition, typename T>
constexpr int tuple_element_index() {
  using Tuple = decltype(ylt::reflection::object_to_tuple(std::declval<T>()));
  return element_index_helper<0, Condition, Tuple>();
}

template <template <typename...> typename Condition, typename T>
constexpr size_t get_type_index() {
  return tuple_element_index<Condition, T>();
}

template <typename T>
constexpr inline bool cdata_v = is_cdata_t<std::remove_cvref_t<T>>::value;

inline constexpr auto has_greater = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0011111000111110001111100011111000111110001111100011111000111110);
};

inline constexpr auto has_space = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0010000000100000001000000010000000100000001000000010000000100000);
};

inline constexpr auto has_smaller = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0011110000111100001111000011110000111100001111000011110000111100);
};

inline constexpr auto has_square_bracket =
    [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
      return has_zero(
          chunk ^
          0b0101110101011101010111010101110101011101010111010101110101011101);
    };

inline constexpr auto has_and = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0010011000100110001001100010011000100110001001100010011000100110);
};

inline constexpr auto has_equal = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0011110100111101001111010011110100111101001111010011110100111101);
};

inline constexpr auto has_apos = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0010011100100111001001110010011100100111001001110010011100100111);
};

template <typename It>
IGUANA_INLINE void skip_sapces_and_newline(It &&it, It &&end) {
  while (it != end && (static_cast<uint8_t>(*it) < 33)) {
    ++it;
  }
}

template <typename It>
IGUANA_INLINE void match_close_tag(It &&it, It &&end, std::string_view key) {
  if (it == end || (*it++) != '/')
    IGUANA_UNLIKELY {
      throw std::runtime_error("unclosed tag: " + std::string(key));
    }
  size_t size = key.size();
  if (static_cast<size_t>(std::distance(it, end)) <= size ||
      std::string_view{&*it, size} != key)
    IGUANA_UNLIKELY {
      throw std::runtime_error("unclosed tag: " + std::string(key));
    }
  it += size;
  match<'>'>(it, end);

  // skip_till<'>'>(it, end); // not check
  // ++it;
}

// returns true if the specified character 'c' is found, false otherwise.
template <char c, typename It>
IGUANA_INLINE bool advance_until_character(It &&it, It &&end) {
  static_assert(contiguous_iterator<std::decay_t<It>>);
  if (std::distance(it, end) >= 7)
    IGUANA_LIKELY {
      const auto end_m7 = end - 7;
      for (; it < end_m7; it += 8) {
        const auto chunk = *reinterpret_cast<const uint64_t *>(&*it);
        uint64_t test;
        if constexpr (c == '&')
          test = has_and(chunk);
        else
          static_assert(!c, "not support this character");
        if (test != 0) {
          it += (countr_zero(test) >> 3);
          return true;
        }
      }
    }
  // Tail end of buffer. Should be rare we even get here
  while (it < end) {
    if (*it == c)
      return true;
    ++it;
  }
  return false;
}

template <char c, typename It>
IGUANA_INLINE void skip_till(It &&it, It &&end) {
  static_assert(contiguous_iterator<std::decay_t<It>>);

  if (std::distance(it, end) >= 7)
    IGUANA_LIKELY {
      const auto end_m7 = end - 7;
      for (; it < end_m7; it += 8) {
        const auto chunk = *reinterpret_cast<const uint64_t *>(&*it);
        uint64_t test;
        if constexpr (c == '>')
          test = has_greater(chunk);
        else if constexpr (c == '<')
          test = has_smaller(chunk);
        else if constexpr (c == '"')
          test = has_qoute(chunk);
        else if constexpr (c == ' ')
          test = has_space(chunk);
        else if constexpr (c == ']')
          test = has_square_bracket(chunk);
        else if constexpr (c == '=')
          test = has_equal(chunk);
        else if constexpr (c == '\'')
          test = has_apos(chunk);
        else
          static_assert(!c, "not support this character");
        if (test != 0) {
          it += (countr_zero(test) >> 3);
          return;
        }
      }
    }

  // Tail end of buffer. Should be rare we even get here
  while (it < end) {
    if (*it == c)
      return;
    ++it;
  }
  static constexpr char b[] = {c, '\0'};
  std::string error = std::string("Expected: ").append(b);
  throw std::runtime_error(error);
}

// skip_till<'>', '<'>(it, end);
template <typename It>
IGUANA_INLINE void skip_till_greater_or_space(It &&it, It &&end) {
  static_assert(contiguous_iterator<std::decay_t<It>>);

  if (std::distance(it, end) >= 7)
    IGUANA_LIKELY {
      const auto end_m7 = end - 7;
      for (; it < end_m7; it += 8) {
        const auto chunk = *reinterpret_cast<const uint64_t *>(&*it);
        uint64_t test = has_greater(chunk) | has_space(chunk);
        if (test != 0) {
          it += (countr_zero(test) >> 3);
          return;
        }
      }
    }

  // Tail end of buffer. Should be rare we even get here
  while (it < end) {
    switch (*it) {
      case '>':
      case ' ':
        return;
    }
    ++it;
  }
  throw std::runtime_error("Expected > or space");
}

template <char c, typename It>
IGUANA_INLINE auto skip_pass(It &&it, It &&end) {
  skip_till<c>(it, end);
  auto res = it++ - 1;
  while (*res == ' ') {
    --res;
  }
  return res + 1;
}

template <char... C, typename It>
IGUANA_INLINE bool is_match(It &&it, const It &end) {
  const auto n = static_cast<size_t>(std::distance(it, end));
  if ((n < sizeof...(C)) || (... || (*it++ != C))) {
    return false;
  }
  return true;
}

template <typename U, typename It, std::enable_if_t<string_v<U>, int> = 0>
IGUANA_INLINE void parse_escape_xml(U &value, It &&it, It &&end) {
  static const unsigned char lookup_digits[256] = {
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   255, 255,
      255, 255, 255, 255, 255, 10,  11,  12,  13,  14,  15,  255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 10,  11,  12,  13,  14,  15,  255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255};
  switch (*(it + 1)) {
    // &amp; &apos;
    case 'a':
      if (is_match<'m', 'p', ';'>(it + 2, end)) {
        value.push_back('&');
        it += 5;
        return;
      }
      if (is_match<'p', 'o', 's', ';'>(it + 2, end)) {
        value.push_back('\'');
        it += 6;
      }
      break;
    // &quot;
    case 'q':
      if (is_match<'u', 'o', 't', ';'>(it + 2, end)) {
        value.push_back('\"');
        it += 6;
      }
      break;
    // &gt;
    case 'g':
      if (is_match<'t', ';'>(it + 2, end)) {
        value.push_back('>');
        it += 4;
      }
      break;
    // &lt;
    case 'l':
      if (is_match<'t', ';'>(it + 2, end)) {
        value.push_back('<');
        it += 4;
      }
      break;
    case '#':
      if (*(it + 2) == 'x') {
        // &#x
        unsigned long codepoint = 0;
        it += 3;
        while (true) {
          auto digit = lookup_digits[static_cast<unsigned char>(*it)];
          if (digit == 0xFF)
            break;
          codepoint = codepoint * 16 + digit;
          ++it;
        }
        encode_utf8(value, codepoint);
      }
      else {
        unsigned long codepoint = 0;
        it += 2;
        while (true) {
          auto digit = lookup_digits[static_cast<unsigned char>(*it)];
          if (digit == 0xFF)
            break;
          codepoint = codepoint * 10 + digit;
          ++it;
        }
        encode_utf8(value, codepoint);
      }
      match<';'>(it, end);
      break;
    default:
      // skip '&'
      // loose policy: allow '&'
      value.push_back(*(it++));
      break;
  }
}

}  // namespace iguana
