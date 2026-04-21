#pragma once
#include <charconv>

#include "detail/charconv.h"
#include "detail/utf.hpp"
#include "xml_util.hpp"

namespace iguana {
namespace detail {

template <typename U, typename It, std::enable_if_t<optional_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name);

template <typename U, typename It, std::enable_if_t<smart_ptr_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name);

template <typename U, typename It,
          std::enable_if_t<ylt_refletable_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name);

template <typename U, typename It, std::enable_if_t<attr_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name);

template <typename U, typename It, std::enable_if_t<plain_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_value(U &&value, It &&begin, It &&end) {
  using T = std::decay_t<U>;
  if constexpr (string_container_v<T>) {
    if constexpr (string_view_v<T>) {
      value = T(&*begin, static_cast<size_t>(std::distance(begin, end)));
    }
    else {
      // TODO: When not parsing the value in the attribute, it is not necessary
      // to unescape'and "
      value.clear();
      auto pre = begin;
      while (advance_until_character<'&'>(begin, end)) {
        value.append(T(&*pre, static_cast<size_t>(std::distance(pre, begin))));
        parse_escape_xml(value, begin, end);
        pre = begin;
      }
      value.append(T(&*pre, static_cast<size_t>(std::distance(pre, begin))));
    }
  }
  else if constexpr (num_v<T>) {
    auto size = std::distance(begin, end);
    const auto start = &*begin;
    detail::from_chars(start, start + size, value);
  }
  else if constexpr (char_v<T>) {
    if (static_cast<size_t>(std::distance(begin, end)) != 1)
      IGUANA_UNLIKELY { throw std::runtime_error("Expected one character"); }
    value = *begin;
  }
  else if constexpr (bool_v<T>) {
    auto bool_v = std::string_view(
        &*begin, static_cast<size_t>(std::distance(begin, end)));
    if (bool_v == "true") {
      value = true;
    }
    else if (bool_v == "false") {
      value = false;
    }
    else
      IGUANA_UNLIKELY { throw std::runtime_error("Expected true or false"); }
  }
  else if constexpr (enum_v<T>) {
    static constexpr auto str_to_enum = get_enum_map<true, T>();
    if constexpr (bool_v<decltype(str_to_enum)>) {
      // not defined a specialization template
      xml_parse_value(reinterpret_cast<std::underlying_type_t<T> &>(value),
                      begin, end);
    }
    else {
      auto enum_names = std::string_view(
          &*begin, static_cast<size_t>(std::distance(begin, end)));
      auto it = str_to_enum.find(enum_names);
      if (it != str_to_enum.end())
        IGUANA_LIKELY { value = it->second; }
      else {
        throw std::runtime_error(std::string(enum_names) +
                                 " missing corresponding value in enum_value");
      }
    }
  }
}

template <typename U, typename It, std::enable_if_t<is_pb_type_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_value(U &&value, It &&begin, It &&end) {
  xml_parse_value(value.val, begin, end);
}

template <typename U, typename It,
          std::enable_if_t<map_container_v<U>, int> = 0>
IGUANA_INLINE void parse_attr(U &&value, It &&it, It &&end) {
  using key_type = typename std::remove_cvref_t<U>::key_type;
  using value_type = typename std::remove_cvref_t<U>::mapped_type;
  while (it != end) {
    skip_sapces_and_newline(it, end);
    if (*it == '>' || *it == '/')
      IGUANA_UNLIKELY { return; }
    auto key_begin = it;
    auto key_end = skip_pass<'='>(it, end);
    key_type key;
    xml_parse_value(key, key_begin, key_end);

    skip_sapces_and_newline(it, end);
    auto value_begin = it + 1;
    auto value_end = value_begin;
    if (*it == '"')
      IGUANA_LIKELY {
        ++it;
        value_end = skip_pass<'"'>(it, end);
      }
    else if (*it == '\'') {
      ++it;
      value_end = skip_pass<'\''>(it, end);
    }
    else
      IGUANA_UNLIKELY { throw std::runtime_error("expected quote or apos"); }
    value_type v;
    xml_parse_value(v, value_begin, value_end);
    value.emplace(std::move(key), std::move(v));
  }
}

template <typename U, typename It, std::enable_if_t<plain_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  skip_till<'>'>(it, end);
  ++it;
  skip_sapces_and_newline(it, end);
  auto value_begin = it;
  auto value_end = skip_pass<'<'>(it, end);
  xml_parse_value(value, value_begin, value_end);
  match_close_tag(it, end, name);
}

template <typename U, typename It, std::enable_if_t<is_pb_type_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  xml_parse_item(value.val, it, end, name);
}

template <typename U, typename It,
          std::enable_if_t<sequence_container_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  xml_parse_item(value.emplace_back(), it, end, name);
  skip_sapces_and_newline(it, end);
  while (it != end) {
    match<'<'>(it, end);
    if (*it == '?' || *it == '!')
      IGUANA_UNLIKELY {
        --it;
        return;
      }
    auto start = it;
    skip_till_greater_or_space(it, end);
    std::string_view key = std::string_view{
        &*start, static_cast<size_t>(std::distance(start, it))};
    if (key != name)
      IGUANA_UNLIKELY {
        it = start - 1;
        return;
      }
    xml_parse_item(value.emplace_back(), it, end, name);
    skip_sapces_and_newline(it, end);
  }
}

template <typename U, typename It,
          std::enable_if_t<map_container_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  throw std::bad_function_call();
}

template <typename U, typename It, std::enable_if_t<variant_v<U>, int> = 0>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  throw std::bad_function_call();
}

template <typename U, typename It, std::enable_if_t<optional_v<U>, int>>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  using value_type = typename std::remove_reference_t<U>::value_type;
  skip_till<'>'>(it, end);
  if (*(it - 1) == '/')
    IGUANA_LIKELY {
      ++it;
      return;
    }

  if constexpr (plain_v<value_type>) {
    // The following code is for option not to be emplaced
    // when parse  "...> <..."
    skip_till<'>'>(it, end);
    ++it;
    skip_sapces_and_newline(it, end);
    auto value_begin = it;
    auto value_end = skip_pass<'<'>(it, end);
    if (value_begin == value_end) {
      match_close_tag(it, end, name);
      return;
    }
    xml_parse_value(value.emplace(), value_begin, value_end);
    match_close_tag(it, end, name);
  }
  else {
    xml_parse_item(value.emplace(), it, end, name);
  }
}

template <typename U, typename It, std::enable_if_t<smart_ptr_v<U>, int>>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  if constexpr (unique_ptr_v<U>) {
    value = std::make_unique<typename std::remove_cvref_t<U>::element_type>();
  }
  else {
    value = std::make_shared<typename std::remove_cvref_t<U>::element_type>();
  }

  xml_parse_item(*value, it, end, name);
}

template <typename U, typename It, std::enable_if_t<attr_v<U>, int>>
IGUANA_INLINE void xml_parse_item(U &value, It &&it, It &&end,
                                  std::string_view name) {
  parse_attr(value.attr(), it, end);
  xml_parse_item(value.value(), it, end, name);
}

//  /> or skip <?>、 <!> and <tag></tag> until the </name>
//  loose inspection here
template <typename It>
IGUANA_INLINE void skip_object_value(It &&it, It &&end, std::string_view name) {
  skip_till<'>'>(it, end);
  ++it;
  if (*(it - 2) == '/') {
    // .../>
    return;
  }
  // </name>
  size_t size = name.size();
  while (it != end) {
    skip_till<'>'>(it, end);
    if (*(it - size - 2) == '<' && *(it - size - 1) == '/' &&
        (std::string_view{&*(it - size), size} == name)) {
      ++it;
      return;
    }
    ++it;
    skip_sapces_and_newline(it, end);
  }
  throw std::runtime_error("unclosed tag: " + std::string(name));
}

// skip <?...?>
template <typename It>
IGUANA_INLINE void skip_instructions(It &&it, It &&end) {
  while (*(it - 1) != '?') {
    ++it;
    skip_till<'>'>(it, end);
  }
  ++it;
}

template <typename It>
IGUANA_INLINE void skip_cdata(It &&it, It &&end) {
  ++it;
  skip_till<']'>(it, end);
  ++it;
  match<']', '>'>(it, end);
}

template <typename It>
IGUANA_INLINE void skip_comment(It &&it, It &&end) {
  while (*(it - 1) != '-' || *(it - 2) != '-') {
    ++it;
    skip_till<'>'>(it, end);
  }
  ++it;
}

// return true means reach the close tag
template <size_t cdata_idx, typename T, typename It,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE auto skip_till_close_tag(T &value, It &&it, It &&end) {
  while (true) {
    skip_sapces_and_newline(it, end);
    match<'<'>(it, end);
    if (*it == '/')
      IGUANA_UNLIKELY {
        // reach the close tag
        return true;
      }
    else if (*it == '?')
      IGUANA_UNLIKELY {
        skip_instructions(it, end);
        continue;
      }
    else if (*it == '!')
      IGUANA_UNLIKELY {
        ++it;
        if (*it == '[') {
          // <![
          if constexpr (cdata_idx == ylt::reflection::members_count_v<T>) {
            skip_cdata(it, end);
          }
          else {
            // if parse cdata
            ++it;
            match<'C', 'D', 'A', 'T', 'A', '['>(it, end);
            skip_sapces_and_newline(it, end);
            auto &cdata_value = ylt::reflection::get<cdata_idx>(value);
            using VT = typename std::decay_t<decltype(cdata_value)>::value_type;
            auto vb = it;
            auto ve = skip_pass<']'>(it, end);
            if constexpr (string_view_v<VT>) {
              cdata_value.value() =
                  VT(&*vb, static_cast<size_t>(std::distance(vb, ve)));
            }
            else {
              cdata_value.value().append(
                  &*vb, static_cast<size_t>(std::distance(vb, ve)));
            }
            match<']', '>'>(it, end);
          }
        }
        else if (*it == '-') {
          // <!-- -->
          skip_comment(it, end);
        }
        else {
          // <!D... >
          skip_till<'>'>(it, end);
          ++it;
        }
        continue;
      }
    return false;
  }
}

template <typename It>
IGUANA_INLINE void skip_till_first_key(It &&it, It &&end) {
  while (it != end) {
    skip_sapces_and_newline(it, end);
    match<'<'>(it, end);
    if (*it == '?')
      IGUANA_UNLIKELY {
        skip_instructions(it, end);
        continue;
      }
    else if (*it == '!')
      IGUANA_UNLIKELY {
        ++it;
        if (*it == '-') {
          // <!-- -->
          skip_comment(it, end);
        }
        else {
          // <!D... >
          skip_till<'>'>(it, end);
          ++it;
        }
        continue;
      }
    else {
      break;
    }
  }
}

template <typename T>
IGUANA_INLINE void check_required(std::string_view key_set) {
  if constexpr (iguana::has_iguana_required_arr_v<T>) {
    constexpr auto required_arr =
        iguana::iguana_required_struct<T>::requied_arr();
    for (auto &item : required_arr) {
      if (key_set.find(item) == std::string_view::npos) {
        std::string err = "required filed ";
        err.append(item).append(" not found!");
        throw std::invalid_argument(err);
      }
    }
  }
}

template <typename T, typename It, std::enable_if_t<ylt_refletable_v<T>, int>>
IGUANA_INLINE void xml_parse_item(T &value, It &&it, It &&end,
                                  std::string_view name) {
  using U = std::decay_t<T>;
  constexpr auto cdata_idx = get_type_index<is_cdata_t, U>();
  skip_till<'>'>(it, end);
  ++it;
  if (skip_till_close_tag<cdata_idx>(value, it, end)) {
    match_close_tag(it, end, name);
    return;
  }
  auto start = it;
  skip_till_greater_or_space(it, end);
  std::string_view key =
      std::string_view{&*start, static_cast<size_t>(std::distance(start, it))};

  [[maybe_unused]] std::string key_set;
  bool parse_done = false;
  // sequential parse
  ylt::reflection::for_each(value, [&](auto &field, auto st_key, auto index) {
#if defined(_MSC_VER) && _MSVC_LANG < 202002L
    // seems MVSC can't pass a constexpr value to lambda
    constexpr auto cdata_idx = get_type_index<is_cdata_t, U>();
#endif
    using item_type = std::remove_reference_t<decltype(field)>;
    if constexpr (cdata_v<item_type>) {
      return;
    }
    if (parse_done || key != st_key)
      IGUANA_UNLIKELY { return; }
    if constexpr (!cdata_v<item_type>) {
      xml_parse_item(field, it, end, key);
      if constexpr (iguana::has_iguana_required_arr_v<U>) {
        key_set.append(key).append(", ");
      }
    }
    if (skip_till_close_tag<cdata_idx>(value, it, end))
      IGUANA_UNLIKELY {
        match_close_tag(it, end, name);
        parse_done = true;
        return;
      }
    start = it;
    skip_till_greater_or_space(it, end);
    key = std::string_view{&*start,
                           static_cast<size_t>(std::distance(start, it))};
  });
  if (parse_done)
    IGUANA_UNLIKELY {
      check_required<U>(key_set);
      return;
    }
  // map parse
  while (true) {
    static auto frozen_map = ylt::reflection::get_variant_map<U>();
    const auto &member_it = frozen_map.find(key);
    if (member_it != frozen_map.end())
      IGUANA_LIKELY {
        std::visit(
            [&](auto offset) IGUANA__INLINE_LAMBDA {
              using value_type = typename decltype(offset)::type;
              if constexpr (!cdata_v<value_type>) {
                auto member_ptr =
                    (value_type *)((char *)(&value) + offset.value);
                xml_parse_item(*member_ptr, it, end, key);
                if constexpr (iguana::has_iguana_required_arr_v<U>) {
                  key_set.append(key).append(", ");
                }
              }
            },
            member_it->second);
      }
    else
      IGUANA_UNLIKELY {
#ifdef THROW_UNKNOWN_KEY
        throw std::runtime_error("Unknown key: " + std::string(key));
#else
        skip_object_value(it, end, key);
#endif
      }
    if (skip_till_close_tag<cdata_idx>(value, it, end)) {
      match_close_tag(it, end, name);
      check_required<U>(key_set);
      return;
    }
    start = it;
    skip_till_greater_or_space(it, end);
    key = std::string_view{&*start,
                           static_cast<size_t>(std::distance(start, it))};
  }
}

}  // namespace detail

template <typename It, typename U, std::enable_if_t<attr_v<U>, int> = 0>
IGUANA_INLINE void from_xml(U &value, It &&it, It &&end) {
  detail::skip_till_first_key(it, end);
  auto start = it;
  skip_till_greater_or_space(it, end);
  std::string_view key =
      std::string_view{&*start, static_cast<size_t>(std::distance(start, it))};
  detail::parse_attr(value.attr(), it, end);
  detail::xml_parse_item(value.value(), it, end, key);
}

template <typename It, typename U,
          std::enable_if_t<ylt_refletable_v<U>, int> = 0>
IGUANA_INLINE void from_xml(U &value, It &&it, It &&end) {
  detail::skip_till_first_key(it, end);
  auto start = it;
  skip_till_greater_or_space(it, end);
  std::string_view key =
      std::string_view{&*start, static_cast<size_t>(std::distance(start, it))};
  detail::xml_parse_item(value, it, end, key);
}

template <typename U, typename View,
          std::enable_if_t<string_container_v<View>, int> = 0>
IGUANA_INLINE void from_xml(U &value, const View &view) {
  from_xml(value, std::begin(view), std::end(view));
}

template <typename Num, std::enable_if_t<num_v<Num>, int> = 0>
IGUANA_INLINE Num get_number(std::string_view str) {
  Num num;
  detail::xml_parse_value(num, str.begin(), str.end());
  return num;
}

template <typename T>
IGUANA_INLINE void from_xml_adl(iguana_adl_t *p, T &t,
                                std::string_view pb_str) {
  iguana::from_xml(t, pb_str);
}

}  // namespace iguana