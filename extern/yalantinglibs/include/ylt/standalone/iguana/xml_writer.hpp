#pragma once

#include "detail/charconv.h"
#include "xml_util.hpp"

namespace iguana {

#ifdef XML_ATTR_USE_APOS
#define XML_ATTR_DELIMITER '\''
#else
#define XML_ATTR_DELIMITER '\"'
#endif

// TODO: improve by precaculate size
template <bool escape_quote_apos, typename Ch, typename SizeType,
          typename Stream>
IGUANA_INLINE void render_string_with_escape_xml(const Ch *it, SizeType length,
                                                 Stream &ss) {
  auto end = it;
  std::advance(end, length);
  while (it < end) {
#ifdef XML_ESCAPE_UNICODE
    if (static_cast<unsigned>(*it) >= 0x80)
      IGUANA_UNLIKELY {
        write_unicode_to_string<true>(it, ss);
        continue;
      }
#endif
    if constexpr (escape_quote_apos) {
      if constexpr (XML_ATTR_DELIMITER == '\"') {
        if (*it == '"')
          IGUANA_UNLIKELY {
            ss.append("&quot;");
            ++it;
            continue;
          }
      }
      else {
        if (*it == '\'')
          IGUANA_UNLIKELY {
            ss.append("&apos;");
            ++it;
            continue;
          }
      }
    }
    if (*it == '&')
      IGUANA_UNLIKELY { ss.append("&amp;"); }
    else if (*it == '>')
      IGUANA_UNLIKELY { ss.append("&gt;"); }
    else if (*it == '<')
      IGUANA_UNLIKELY { ss.append("&lt;"); }
    else {
      ss.push_back(*it);
    }
    ++it;
  }
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<sequence_container_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name);

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<map_container_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name);

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, T &&t, std::string_view name);

template <bool pretty, size_t spaces, typename Stream>
IGUANA_INLINE void render_tail(Stream &ss, std::string_view str) {
  if constexpr (pretty) {
    ss.append(spaces, '\t');
  }
  ss.push_back('<');
  ss.push_back('/');
  ss.append(str.data(), str.size());
  ss.push_back('>');
  if constexpr (pretty) {
    ss.push_back('\n');
  }
}

template <bool pretty, size_t spaces, typename Stream>
IGUANA_INLINE void render_head(Stream &ss, std::string_view str) {
  if constexpr (pretty) {
    ss.append(spaces, '\t');
  }
  ss.push_back('<');
  ss.append(str.data(), str.size());
  ss.push_back('>');
}

template <bool escape_quote_apos = false, typename Stream, typename T,
          std::enable_if_t<plain_v<T>, int> = 0>
IGUANA_INLINE void render_value(Stream &ss, const T &value) {
  if constexpr (string_container_v<T>) {
    render_string_with_escape_xml<escape_quote_apos>(value.data(), value.size(),
                                                     ss);
  }
  else if constexpr (num_v<T>) {
    char temp[65];
    auto p = detail::to_chars(temp, value);
    ss.append(temp, p - temp);
  }
  else if constexpr (char_v<T>) {
    ss.push_back(value);
  }
  else if constexpr (bool_v<T>) {
    ss.append(value ? "true" : "false");
  }
  else if constexpr (enum_v<T>) {
    static constexpr auto enum_to_str = get_enum_map<false, std::decay_t<T>>();
    if constexpr (bool_v<decltype(enum_to_str)>) {
      render_value(ss, static_cast<std::underlying_type_t<T>>(value));
    }
    else {
      auto it = enum_to_str.find(value);
      if (it != enum_to_str.end())
        IGUANA_LIKELY {
          auto str = it->second;
          render_value(ss, std::string_view(str.data(), str.size()));
        }
      else {
        throw std::runtime_error(
            std::to_string(static_cast<std::underlying_type_t<T>>(value)) +
            " is a missing value in enum_value");
      }
    }
  }
  else {
    static_assert(!sizeof(T), "type is not supported");
  }
}

template <bool escape_quote_apos = false, typename Stream, typename T,
          std::enable_if_t<is_pb_type_v<T>, int> = 0>
IGUANA_INLINE void render_value(Stream &ss, const T &value) {
  render_value<escape_quote_apos>(ss, value);
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<map_container_v<T>, int> = 0>
inline void render_xml_attr(Stream &ss, const T &value, std::string_view name) {
  if constexpr (pretty) {
    ss.append(spaces, '\t');
  }
  ss.push_back('<');
  ss.append(name.data(), name.size());
  for (auto [k, v] : value) {
    ss.push_back(' ');
    render_value(ss, k);
    ss.push_back('=');
    ss.push_back(XML_ATTR_DELIMITER);
    render_value<true>(ss, v);
    ss.push_back(XML_ATTR_DELIMITER);
  }
  ss.push_back('>');
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<plain_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  render_value(ss, value);
  render_tail<pretty, 0>(ss, name);
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<is_pb_type_v<std::decay_t<T>>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  render_xml_value<pretty, spaces>(ss, value.val, name);
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<variant_v<std::decay_t<T>>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  throw std::bad_function_call();
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<optional_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  if (value) {
    render_xml_value<pretty, spaces>(ss, *value, name);
  }
  else {
    render_tail<pretty, 0>(ss, name);
  }
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<smart_ptr_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  if (value) {
    render_xml_value<pretty, spaces>(ss, *value, name);
  }
  else {
    render_tail<pretty, 0>(ss, name);
  }
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<attr_v<T>, int> = 0>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  render_xml_attr<pretty, spaces>(ss, value.attr(), name);
  render_xml_value<pretty, spaces>(ss, value.value(), name);
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<sequence_container_v<T>, int>>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  using value_type =
      underline_type_t<typename std::remove_cvref_t<T>::value_type>;
  for (const auto &v : value) {
    if constexpr (attr_v<value_type>) {
      render_xml_value<pretty, spaces>(ss, v, name);
    }
    else {
      render_head<pretty, spaces>(ss, name);
      render_xml_value<pretty, spaces>(ss, v, name);
    }
  }
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<map_container_v<T>, int>>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  throw std::bad_function_call();
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<variant_v<T>, int>>
IGUANA_INLINE void render_xml_value(Stream &ss, const T &value,
                                    std::string_view name) {
  throw std::bad_function_call();
}

template <bool pretty, size_t spaces, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int>>
IGUANA_INLINE void render_xml_value(Stream &ss, T &&t, std::string_view name) {
  if constexpr (pretty) {
    ss.push_back('\n');
  }
  ylt::reflection::for_each(t, [&](auto &field, auto tag_name, auto index) {
    using value_type = underline_type_t<decltype(field)>;
    static_assert(!std::is_pointer_v<value_type>,
                  "don't use raw pointer, please use smart pointer");
    if constexpr (sequence_container_v<value_type>) {
      render_xml_value<pretty, spaces + 1>(ss, field, tag_name);
    }
    else if constexpr (attr_v<value_type>) {
      render_xml_value<pretty, spaces + 1>(ss, field, tag_name);
    }
    else if constexpr (cdata_v<value_type>) {
      if constexpr (pretty) {
        ss.append(spaces + 1, '\t');
        ss.append("<![CDATA[").append(field.value()).append("]]>\n");
      }
      else {
        ss.append("<![CDATA[").append(field.value()).append("]]>");
      }
    }
    else {
      render_head<pretty, spaces + 1>(ss, tag_name);
      render_xml_value<pretty, spaces + 1>(ss, field, tag_name);
    }
  });
  render_tail<pretty, spaces>(ss, name);
}

template <bool pretty = false, typename Stream, typename T,
          std::enable_if_t<attr_v<T>, int> = 0>
IGUANA_INLINE void to_xml(T &&t, Stream &s) {
  using value_type = typename std::decay_t<T>::value_type;
  static_assert(ylt_refletable_v<value_type>, "value_type must be refletable");
  constexpr std::string_view root_name =
      ylt::reflection::get_struct_name<value_type>();
  render_xml_value<pretty, 0>(s, std::forward<T>(t), root_name);
}

template <bool pretty = false, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void to_xml(T &&t, Stream &s) {
  constexpr std::string_view root_name =
      ylt::reflection::get_struct_name<ylt::reflection::remove_cvref_t<T>>();
  render_head<pretty, 0>(s, root_name);
  render_xml_value<pretty, 0>(s, std::forward<T>(t), root_name);
}

template <typename T>
IGUANA_INLINE void to_xml_adl(iguana_adl_t *p, T &t, std::string &pb_str) {
  to_xml(t, pb_str);
}

}  // namespace iguana
