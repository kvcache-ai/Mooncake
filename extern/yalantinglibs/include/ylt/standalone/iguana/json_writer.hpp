//
// Created by Qiyu on 17-6-5.
//

#ifndef SERIALIZE_JSON_HPP
#define SERIALIZE_JSON_HPP
#include "json_util.hpp"

namespace iguana {

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void to_json(T &&t, Stream &s);
namespace detail {
template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<optional_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &val);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<fixed_array_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &t);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<sequence_container_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<pair_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<smart_ptr_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<map_container_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &o);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<tuple_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &s, T &&t);

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<variant_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &s, T &&t);

template <typename Stream, typename InputIt, typename T, typename F>
IGUANA_INLINE void join(Stream &ss, InputIt first, InputIt last, const T &delim,
                        const F &f) {
  if (first == last)
    return;
  f(*first++);
  while (first != last) {
    ss.push_back(delim);
    f(*first++);
  }
}

template <bool Is_writing_escape, typename Stream>
IGUANA_INLINE void to_json_impl(Stream &ss, std::nullptr_t) {
  ss.append("null");
}

template <bool Is_writing_escape, typename Stream>
IGUANA_INLINE void to_json_impl(Stream &ss, bool b) {
  ss.append(b ? "true" : "false");
};

template <bool Is_writing_escape, typename Stream>
IGUANA_INLINE void to_json_impl(Stream &ss, char value) {
  ss.append("\"");
  ss.push_back(value);
  ss.append("\"");
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<num_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, T value) {
  char temp[65];
  auto p = detail::to_chars(temp, value);
  ss.append(temp, p - temp);
}

// template <bool Is_writing_escape, typename Stream, typename T,
//           std::enable_if_t<is_pb_type_v<T>, int> = 0>
// IGUANA_INLINE void to_json_impl(Stream &ss, T value) {
//   to_json_impl<Is_writing_escape>(ss, value.val);
// }

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<numeric_str_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, T v) {
  ss.append(v.value().data(), v.value().size());
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<string_container_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, T &&t) {
  ss.push_back('"');
  if constexpr (Is_writing_escape) {
    write_string_with_escape(t.data(), t.size(), ss);
  }
  else {
    ss.append(t.data(), t.size());
  }
  ss.push_back('"');
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<num_v<T>, int> = 0>
IGUANA_INLINE void render_key(Stream &ss, const T &t) {
  ss.push_back('"');
  to_json_impl<Is_writing_escape>(ss, t);
  ss.push_back('"');
}

// template <bool Is_writing_escape, typename Stream, typename T,
//           std::enable_if_t<is_pb_type_v<T>, int> = 0>
// IGUANA_INLINE void render_key(Stream &ss, const T &t) {
//   render_key<Is_writing_escape>(ss, t.val);
// }

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<string_container_v<T>, int> = 0>
IGUANA_INLINE void render_key(Stream &ss, T &&t) {
  to_json_impl<Is_writing_escape>(ss, std::forward<T>(t));
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, T &&t) {
  to_json(std::forward<T>(t), ss);
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<enum_v<T>, int> = 0>
IGUANA_INLINE void to_json_impl(Stream &ss, T val) {
  static constexpr auto enum_to_str = get_enum_map<false, std::decay_t<T>>();
  if constexpr (bool_v<decltype(enum_to_str)>) {
    to_json_impl<Is_writing_escape>(
        ss, static_cast<std::underlying_type_t<T>>(val));
  }
  else {
    auto it = enum_to_str.find(val);
    if (it != enum_to_str.end())
      IGUANA_LIKELY {
        auto str = it->second;
        to_json_impl<Is_writing_escape>(
            ss, std::string_view(str.data(), str.size()));
      }
    else {
      throw std::runtime_error(
          std::to_string(static_cast<std::underlying_type_t<T>>(val)) +
          " is a missing value in enum_value");
    }
  }
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<optional_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &val) {
  if (!val) {
    ss.append("null");
  }
  else {
    to_json_impl<Is_writing_escape>(ss, *val);
  }
}

template <bool Is_writing_escape, typename Stream, typename T>
IGUANA_INLINE void render_array(Stream &ss, const T &v) {
  ss.push_back('[');
  join(ss, std::begin(v), std::end(v), ',',
       [&ss](const auto &jsv) IGUANA__INLINE_LAMBDA {
         to_json_impl<Is_writing_escape>(ss, jsv);
       });
  ss.push_back(']');
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<fixed_array_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &t) {
  if constexpr (std::is_same_v<char, std::remove_reference_t<
                                         decltype(std::declval<T>()[0])>>) {
    constexpr size_t n = sizeof(T) / sizeof(decltype(std::declval<T>()[0]));
    ss.push_back('"');
    auto get_length = [&t](int n) constexpr {
      for (int i = 0; i < n; ++i) {
        if (t[i] == '\0')
          return i;
      }
      return n;
    };
    size_t len = get_length(n);
    ss.append(std::begin(t), len);
    ss.push_back('"');
  }
  else {
    render_array<Is_writing_escape>(ss, t);
  }
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<map_container_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &o) {
  ss.push_back('{');
  join(ss, o.cbegin(), o.cend(), ',',
       [&ss](const auto &jsv) IGUANA__INLINE_LAMBDA {
         render_key<Is_writing_escape>(ss, jsv.first);
         ss.push_back(':');
         to_json_impl<Is_writing_escape>(ss, jsv.second);
       });
  ss.push_back('}');
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<sequence_container_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v) {
  constexpr char pre = pair_v<typename T::value_type> ? '{' : '[';
  ss.push_back(pre);
  join(ss, v.cbegin(), v.cend(), ',',
       [&ss](const auto &jsv) IGUANA__INLINE_LAMBDA {
         to_json_impl<Is_writing_escape>(ss, jsv);
       });
  constexpr char post = pair_v<typename T::value_type> ? '}' : ']';
  ss.push_back(post);
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<pair_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v) {
  render_key<Is_writing_escape>(ss, v.first);
  ss.push_back(':');
  to_json_impl<Is_writing_escape>(ss, v.second);
}

constexpr auto write_json_key = [](auto &s, auto name) IGUANA__INLINE_LAMBDA {
  s.push_back('"');
  // will be replaced by string_view later
  s.append(name.data(), name.size());
  s.push_back('"');
};

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<smart_ptr_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &ss, const T &v) {
  if (v) {
    to_json_impl<Is_writing_escape>(ss, *v);
  }
  else {
    ss.append("null");
  }
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<tuple_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &s, T &&t) {
  using U = typename std::decay_t<T>;
  s.push_back('[');
  constexpr size_t size = std::tuple_size_v<U>;
  foreach_tuple(
      [&s, size](auto &v, auto i) IGUANA__INLINE_LAMBDA {
        to_json_impl<Is_writing_escape>(s, v);

        if (i != size - 1)
          IGUANA_LIKELY { s.push_back(','); }
      },
      std::forward<T>(t));
  s.push_back(']');
}

template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<variant_v<T>, int>>
IGUANA_INLINE void to_json_impl(Stream &s, T &&t) {
  static_assert(!has_duplicate_type_v<std::remove_reference_t<T>>,
                "don't allow same type in std::variant");
  std::visit(
      [&s](auto value) {
        to_json_impl<Is_writing_escape>(s, value);
      },
      t);
}
}  // namespace detail
template <bool Is_writing_escape, typename Stream, typename T,
          std::enable_if_t<ylt_refletable_v<T>, int>>
IGUANA_INLINE void to_json(T &&t, Stream &s) {
  using namespace detail;
  s.push_back('{');
  using U = ylt::reflection::remove_cvref_t<T>;
  constexpr auto Count = ylt::reflection::members_count_v<U>;
  if constexpr (Count > 0) {
    ylt::reflection::for_each(t, [&](auto &field, auto name, auto index) {
      static_assert(
          !std::is_pointer_v<std::remove_reference_t<decltype(field)>>,
          "don't use raw pointer, please use smart pointer");
      write_json_key(s, name);
      s.push_back(':');
      to_json_impl<Is_writing_escape>(s, field);
      if (index < Count - 1)
        IGUANA_LIKELY { s.push_back(','); }
    });
  }

  s.push_back('}');
}

template <bool Is_writing_escape = true, typename Stream, typename T,
          std::enable_if_t<non_ylt_refletable_v<T>, int> = 0>
IGUANA_INLINE void to_json(T &&t, Stream &s) {
  using namespace detail;
  to_json_impl<Is_writing_escape>(s, t);
}

template <typename T>
IGUANA_INLINE void to_json_adl(iguana_adl_t *p, T &t, std::string &pb_str) {
  to_json(t, pb_str);
}

}  // namespace iguana
#endif  // SERIALIZE_JSON_HPP
