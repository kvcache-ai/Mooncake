#pragma once

#include <charconv>

#include "detail/charconv.h"
#include "detail/utf.hpp"
#include "yaml_util.hpp"

namespace iguana {

template <typename T, typename It,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void from_yaml(T &value, It &&it, It &&end,
                             size_t min_spaces = 0);

namespace detail {

template <typename U, typename It,
          std::enable_if_t<yaml_not_support_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  throw std::bad_function_call();
}

template <typename U, class It,
          std::enable_if_t<yaml_not_support_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  throw std::bad_function_call();
}

template <typename U, typename It, std::enable_if_t<string_v<U>, int> = 0>
IGUANA_INLINE void parse_escape_str(U &value, It &&it, It &&end) {
  auto start = it;
  value.clear();
  while (it != end) {
    if (*(it++) == '\\')
      IGUANA_UNLIKELY {
        if (*it == 'u') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          ++it;
          if (std::distance(it, end) < 4)
            IGUANA_UNLIKELY
          throw std::runtime_error(R"(Expected 4 hexadecimal digits)");
          auto code_point = parse_unicode_hex4(it);
          encode_utf8(value, code_point);
          start = it;
        }
        else if (*it == 'n') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          start = ++it;
          value.push_back('\n');
        }
        else if (*it == 't') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          start = ++it;
          value.push_back('\t');
        }
        else if (*it == 'r') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          start = ++it;
          value.push_back('\r');
        }
        else if (*it == 'b') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          start = ++it;
          value.push_back('\b');
        }
        else if (*it == 'f') {
          value.append(&*start,
                       static_cast<size_t>(std::distance(start, it) - 1));
          start = ++it;
          value.push_back('\f');
        }
      }
  }
  value.append(&*start, static_cast<size_t>(std::distance(start, end)));
}

// use '-' here to simply represent '>-'
template <char block_type, typename U, typename It,
          std::enable_if_t<string_v<U>, int> = 0>
IGUANA_INLINE void parse_block_str(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  auto spaces = skip_space_and_lines<false>(it, end, min_spaces);
  if (spaces < min_spaces)
    IGUANA_UNLIKELY {
      // back to the end of the previous line
      if constexpr (block_type == '|' || block_type == '>') {
        value.push_back('\n');
      }
      it -= spaces + 1;
      return;
    }
  while (it != end) {
    auto start = it;
    auto value_end = skip_till_newline(it, end);
    value.append(&*start, static_cast<size_t>(std::distance(start, value_end)));
    spaces = skip_space_and_lines<false>(it, end, min_spaces);
    if ((spaces < min_spaces) || (it == end))
      IGUANA_UNLIKELY {
        if constexpr (block_type == '|' || block_type == '>') {
          value.push_back('\n');
        }
        it -= spaces + 1;
        return;
      }
    if constexpr (block_type == '|') {
      value.push_back('\n');
    }
    else if constexpr (block_type == '-' || block_type == '>') {
      value.push_back(' ');
    }
  }
}

template <typename U, typename It,
          std::enable_if_t<map_container_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces);

template <typename U, class It, std::enable_if_t<num_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  if (value_begin == value_end)
    IGUANA_UNLIKELY { return; }
  auto size = std::distance(value_begin, value_end);
  const auto start = &*value_begin;
  detail::from_chars(start, start + size, value);
}

template <typename U, typename It, std::enable_if_t<is_pb_type_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  yaml_parse_value(value.val, value_begin, value_end);
}

// string_view should be used  for string with ' " ?
template <typename U, typename It,
          std::enable_if_t<string_container_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  using T = std::decay_t<U>;
  auto start = value_begin;
  auto end = value_end;
  if (*value_begin == '"') {
    ++start;
    --end;
    if (*end != '"')
      IGUANA_UNLIKELY {
        // TODO: improve
        auto it = start;
        while (*it != '"' && it != end) {
          ++it;
        }
        if (it == end || (*(it + 1) != '#'))
          IGUANA_UNLIKELY { throw std::runtime_error(R"(Expected ")"); }
        end = it;
      }
    if constexpr (string_v<T>) {
      parse_escape_str(value, start, end);
      return;
    }
  }
  else if (*value_begin == '\'') {
    ++start;
    --end;
    if (*end != '\'')
      IGUANA_UNLIKELY { throw std::runtime_error(R"(Expected ')"); }
  }
  value = T(&*start, static_cast<size_t>(std::distance(start, end)));
  if ((value == "~") || (value == "null")) {
    value = T{};
  }
}

template <typename U, typename It, std::enable_if_t<char_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  if (static_cast<size_t>(std::distance(value_begin, value_end)) != 1)
    IGUANA_UNLIKELY { throw std::runtime_error("Expected one character"); }
  value = *value_begin;
}

template <typename U, typename It, std::enable_if_t<enum_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &value, It &&value_begin,
                                    It &&value_end) {
  static constexpr auto str_to_enum = get_enum_map<true, std::decay_t<U>>();
  if constexpr (bool_v<decltype(str_to_enum)>) {
    // not defined a specialization template
    using T = std::underlying_type_t<std::decay_t<U>>;
    yaml_parse_value(reinterpret_cast<T &>(value), value_begin, value_end);
  }
  else {
    auto enum_names = std::string_view(
        &*value_begin,
        static_cast<size_t>(std::distance(value_begin, value_end)));
    auto it = str_to_enum.find(enum_names);
    if (it != str_to_enum.end())
      IGUANA_LIKELY { value = it->second; }
    else {
      throw std::runtime_error(std::string(enum_names) +
                               " missing corresponding value in enum_value");
    }
  }
}

template <typename U, typename It, std::enable_if_t<bool_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_value(U &&value, It &&value_begin,
                                    It &&value_end) {
  auto bool_v = std::string_view(
      &*value_begin,
      static_cast<size_t>(std::distance(value_begin, value_end)));
  if (bool_v == "true") {
    value = true;
  }
  else if (bool_v == "false") {
    value = false;
  }
  else
    IGUANA_UNLIKELY { throw std::runtime_error("Expected true or false"); }
}

template <typename U, typename It,
          std::enable_if_t<ylt_refletable_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  from_yaml(value, it, end, min_spaces);
}

template <typename U, typename It, std::enable_if_t<plain_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  using T = std::decay_t<U>;
  if constexpr (string_v<T>) {
    if (skip_space_till_end(it, end))
      IGUANA_UNLIKELY { return; }
    if (*it == '|') {
      ++it;
      parse_block_str<'|'>(value, it, end, min_spaces);
    }
    else if (*it == '>') {
      ++it;
      if (*it == '-') {
        ++it;
        parse_block_str<'-'>(value, it, end, min_spaces);
      }
      else {
        parse_block_str<'>'>(value, it, end, min_spaces);
      }
    }
    else {
      auto indent = skip_space_and_lines<false>(it, end, min_spaces);
      // in case of a string field it is valid for the field to have no content
      if (indent < min_spaces) {
        value = "";
      }
      else {
        auto start = it;
        auto value_end = skip_till_newline(it, end);
        yaml_parse_value(value, start, value_end);
      }
    }
  }
  else {
    skip_space_and_lines(it, end, min_spaces);
    auto start = it;
    auto value_end = skip_till_newline(it, end);
    yaml_parse_value(value, start, value_end);
  }
}

template <typename U, typename It, std::enable_if_t<is_pb_type_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  yaml_parse_item(value.val, it, end, min_spaces);
}

template <typename U, typename It, std::enable_if_t<optional_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces);

template <typename U, typename It, std::enable_if_t<smart_ptr_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces);

// minspaces : The minimum indentation
template <typename U, typename It,
          std::enable_if_t<sequence_container_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  value.clear();
  auto spaces = skip_space_and_lines(it, end, min_spaces);
  using value_type = typename std::remove_cv_t<U>::value_type;
  if (*it == '[') {
    ++it;
    skip_space_and_lines(it, end, min_spaces);
    while (it != end) {
      if (*it == ']')
        IGUANA_UNLIKELY {
          ++it;
          return;
        }
      if constexpr (plain_v<value_type>) {
        auto start = it;
        auto value_end = yaml_skip_till<',', ']'>(it, end);
        yaml_parse_value(value.emplace_back(), start, value_end);
        if (*(it - 1) == ']')
          IGUANA_UNLIKELY { return; }
      }
      else {
        yaml_parse_item(value.emplace_back(), it, end, spaces + 1);
        skip_space_and_lines(it, end, min_spaces);
        if (*it == ',')
          IGUANA_LIKELY { ++it; }
      }
      skip_space_and_lines(it, end, min_spaces);
    }
  }
  else if (*it == '-') {
    while (it != end) {
      auto subspaces = skip_space_and_lines<false>(it, end, spaces);
      if (subspaces < spaces)
        IGUANA_UNLIKELY {
          it -= subspaces + 1;
          return;
        }
      if (it == end)
        IGUANA_UNLIKELY
      return;
      match<'-'>(it, end);
      // space + 1 because of the skipped '-'
      subspaces = skip_space_and_lines(it, end, spaces + 1);
      if constexpr (string_v<value_type>) {
        // except string_v because of scalar blocks
        yaml_parse_item(value.emplace_back(), it, end, spaces + 1);
      }
      else if constexpr (plain_v<value_type>) {
        auto start = it;
        auto value_end = skip_till_newline(it, end);
        yaml_parse_value(value.emplace_back(), start, value_end);
      }
      else {
        yaml_parse_item(value.emplace_back(), it, end, subspaces);
      }
    }
  }
  else
    IGUANA_UNLIKELY { throw std::runtime_error("Expected ']' or '-'"); }
}

template <typename U, typename It, std::enable_if_t<tuple_v<U>, int> = 0>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  auto spaces = skip_space_and_lines(it, end, min_spaces);
  if (*it == '[') {
    ++it;
    skip_space_and_lines(it, end, min_spaces);
    for_each_tuple(
        [&](auto &v) IGUANA__INLINE_LAMBDA {
          using value_type = std::decay_t<decltype(v)>;
          skip_space_and_lines(it, end, min_spaces);
          if constexpr (plain_v<value_type>) {
            auto start = it;
            auto value_end = yaml_skip_till<',', ']'>(it, end);
            yaml_parse_value(v, start, value_end);
          }
          else {
            yaml_parse_item(v, it, end, spaces + 1);
            skip_space_and_lines(it, end, min_spaces);
            ++it;  // skip ,
          }
        },
        value);
  }
  else if (*it == '-') {
    for_each_tuple(
        [&](auto &v) IGUANA__INLINE_LAMBDA {
          // we don't need to determine when it will end
          // because the user decides how many items there are
          // the following is similar to sequence_t
          using value_type = std::decay_t<decltype(v)>;
          skip_space_and_lines(it, end, spaces);
          match<'-'>(it, end);
          [[maybe_unused]] auto subspaces =
              skip_space_and_lines(it, end, spaces + 1);
          if constexpr (string_v<value_type>) {
            yaml_parse_item(v, it, end, spaces + 1);
          }
          else if constexpr (plain_v<value_type>) {
            auto start = it;
            auto value_end = skip_till_newline(it, end);
            yaml_parse_value(v, start, value_end);
          }
          else {
            yaml_parse_item(v, it, end, subspaces);
          }
        },
        value);
  }
  else {
    throw std::runtime_error("Expected ']' or '-'");
  }
}

template <typename U, typename It, std::enable_if_t<map_container_v<U>, int>>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  using T = std::remove_reference_t<U>;
  using key_type = typename T::key_type;
  using value_type = typename T::mapped_type;
  [[maybe_unused]] auto spaces = skip_space_and_lines(it, end, min_spaces);
  if (*it == '{') {
    ++it;
    [[maybe_unused]] auto subspaces = skip_space_and_lines(it, end, min_spaces);
    while (it != end) {
      if (*it == '}')
        IGUANA_UNLIKELY {
          ++it;
          return;
        }
      auto start = it;
      auto value_end = yaml_skip_till<':'>(it, end);
      key_type key;
      yaml_parse_value(key, start, value_end);
      subspaces = skip_space_and_lines(it, end, min_spaces);
      if constexpr (plain_v<value_type>) {
        start = it;
        value_end = yaml_skip_till<',', '}'>(it, end);
        yaml_parse_value(value[key], start, value_end);
        if (*(it - 1) == '}')
          IGUANA_UNLIKELY { return; }
      }
      else {
        yaml_parse_item(value[key], it, end, min_spaces);
        subspaces = skip_space_and_lines(it, end, min_spaces);
        if (*it == ',')
          IGUANA_LIKELY { ++it; }
      }
      subspaces = skip_space_and_lines(it, end, min_spaces);
    }
  }
  else {
    auto subspaces = skip_space_and_lines<false>(it, end, min_spaces);
    while (it != end) {
      if (subspaces < min_spaces)
        IGUANA_UNLIKELY {
          it -= subspaces + 1;
          return;
        }
      auto start = it;
      auto value_end = yaml_skip_till<':'>(it, end);
      key_type key;
      yaml_parse_value(key, start, value_end);
      subspaces = skip_space_and_lines(it, end, min_spaces);
      if constexpr (plain_v<value_type> && !string_v<value_type>) {
        start = it;
        value_end = skip_till_newline(it, end);
        yaml_parse_value(value[key], start, value_end);
      }
      else {
        yaml_parse_item(value[key], it, end, spaces + 1);
      }
      subspaces = skip_space_and_lines<false>(it, end, min_spaces);
    }
  }
}

template <typename U, typename It, std::enable_if_t<smart_ptr_v<U>, int>>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  using T = std::remove_reference_t<U>;
  if constexpr (unique_ptr_v<T>) {
    value = std::make_unique<typename T::element_type>();
  }
  else {
    value = std::make_shared<typename T::element_type>();
  }
  static_assert(!string_v<typename T::element_type>,
                "smart_ptr<string> is not allowed");
  yaml_parse_item(*value, it, end, min_spaces);
}

template <typename U, typename It, std::enable_if_t<optional_v<U>, int>>
IGUANA_INLINE void yaml_parse_item(U &value, It &&it, It &&end,
                                   size_t min_spaces) {
  using T = std::remove_reference_t<U>;
  using value_type = typename T::value_type;
  auto spaces = skip_space_and_lines<false>(it, end, min_spaces);
  if (it == end)
    IGUANA_UNLIKELY { return; }
  if (spaces < min_spaces)
    IGUANA_UNLIKELY {
      it -= spaces + 1;
      return;
    }
  auto start = it;
  std::decay_t<It> value_end = end;
  if (*it == 'n') {
    value_end = skip_till_newline(it, end);
    auto opt_v = std::string_view(
        &*start, static_cast<size_t>(std::distance(start, value_end)));
    if (opt_v == "null") {
      return;
    }
  }
  if constexpr (string_v<value_type> || !plain_v<value_type>) {
    it = start;
    yaml_parse_item(value.emplace(), it, end, min_spaces);
  }
  else {
    if (value_end == end)
      IGUANA_LIKELY {
        // if value_end isn't touched
        value_end = skip_till_newline(it, end);
      }
    yaml_parse_value(value.emplace(), start, value_end);
  }
}

template <typename It>
IGUANA_INLINE void skip_object_value(It &&it, It &&end, size_t min_spaces) {
  size_t subspace = min_spaces;
  while (it != end) {
    while (it != end && *it != '\n') {
      ++it;
    }
    // here : it == '\n'
    subspace = 0;
    if (it != end)
      IGUANA_LIKELY { ++it; }
    while (it != end && *(it++) == ' ') {
      ++subspace;
    }
    if (subspace < min_spaces) {
      it -= subspace + 1;
      return;
    }
  }
}

}  // namespace detail

template <typename T, typename It, std::enable_if_t<ylt_refletable_v<T>, int>>
IGUANA_INLINE void from_yaml(T &value, It &&it, It &&end, size_t min_spaces) {
  auto spaces = skip_space_and_lines(it, end, min_spaces);
  while (it != end) {
    auto start = it;
    auto keyend = yaml_skip_till<':'>(it, end);
    std::string_view key = std::string_view{
        &*start, static_cast<size_t>(std::distance(start, keyend))};
    static auto frozen_map = ylt::reflection::get_variant_map<T>();

    if constexpr (frozen_map.size() > 0) {
      const auto &member_it = frozen_map.find(key);
      if (member_it != frozen_map.end())
        IGUANA_LIKELY {
          std::visit(
              [&](auto offset) IGUANA__INLINE_LAMBDA {
                using value_type = typename decltype(offset)::type;
                auto member_ptr =
                    (value_type *)((char *)(&value) + offset.value);
                detail::yaml_parse_item(*member_ptr, it, end, spaces + 1);
              },
              member_it->second);
        }
      else
        IGUANA_UNLIKELY {
#ifdef THROW_UNKNOWN_KEY
          throw std::runtime_error("Unknown key: " + std::string(key));
#else
          detail::skip_object_value(it, end, spaces + 1);
#endif
        }
    }
    auto subspaces = skip_space_and_lines<false>(it, end, min_spaces);
    if (subspaces < min_spaces)
      IGUANA_UNLIKELY {
        it -= subspaces + 1;
        return;
      }
  }
}

template <typename T, typename It,
          std::enable_if_t<non_ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void from_yaml(T &value, It &&it, It &&end) {
  detail::yaml_parse_item(value, it, end, 0);
}

template <typename T, typename It>
IGUANA_INLINE void from_yaml(T &value, It &&it, It &&end,
                             std::error_code &ec) noexcept {
  try {
    from_yaml(value, it, end);
    ec = {};
  } catch (std::runtime_error &e) {
    ec = iguana::make_error_code(e.what());
  }
}

template <typename T, typename View,
          std::enable_if_t<string_container_v<View>, int> = 0>
IGUANA_INLINE void from_yaml(T &value, const View &view) {
  from_yaml(value, std::begin(view), std::end(view));
}

template <typename T, typename View,
          std::enable_if_t<string_container_v<View>, int> = 0>
IGUANA_INLINE void from_yaml(T &value, const View &view,
                             std::error_code &ec) noexcept {
  try {
    from_yaml(value, std::begin(view), std::end(view));
    ec = {};
  } catch (std::runtime_error &e) {
    ec = iguana::make_error_code(e.what());
  }
}

template <typename T>
IGUANA_INLINE void from_yaml_adl(iguana_adl_t *p, T &t,
                                 std::string_view pb_str) {
  iguana::from_yaml(t, pb_str);
}

}  // namespace iguana
