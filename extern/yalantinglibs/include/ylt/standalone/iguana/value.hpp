#pragma once
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "error_code.h"

namespace iguana {

#define GCC_COMPILER (defined(__GNUC__) && !defined(__clang__))

#ifdef GCC_COMPILER
#include <map>
template <class Key, class T, typename... Args>
using json_map = std::map<Key, T, Args...>;
#else
template <class Key, class T, typename... Args>
using json_map = std::unordered_map<Key, T, Args...>;
#endif

enum dom_parse_error { ok, wrong_type };

template <typename CharT>
struct basic_json_value
    : std::variant<std::monostate, std::nullptr_t, bool, double, int,
                   std::basic_string<CharT>,
                   std::vector<basic_json_value<CharT>>,
                   json_map<std::basic_string<CharT>, basic_json_value<CharT>>,
                   std::basic_string_view<CharT>> {
  using string_type = std::basic_string<CharT>;
  using string_view_type = std::basic_string_view<CharT>;
  using array_type = std::vector<basic_json_value<CharT>>;
  using object_type = json_map<string_type, basic_json_value<CharT>>;

  using base_type =
      std::variant<std::monostate, std::nullptr_t, bool, double, int,
                   string_type, array_type, object_type, string_view_type>;

  using base_type::base_type;

  inline const static std::unordered_map<size_t, std::string> type_map_ = {
      {0, "undefined type"}, {1, "null type"},   {2, "bool type"},
      {3, "double type"},    {4, "int type"},    {5, "string type"},
      {6, "array type"},     {7, "object type"}, {8, "string_view type"}};

  basic_json_value() : base_type(std::in_place_type<std::monostate>) {}

  basic_json_value(CharT const *value)
      : base_type(std::in_place_type<string_type>, value) {}

  base_type &base() { return *this; }
  base_type const &base() const { return *this; }

  bool is_undefined() const {
    return std::holds_alternative<std::monostate>(*this);
  }
  bool is_null() const { return std::holds_alternative<std::nullptr_t>(*this); }
  bool is_bool() const { return std::holds_alternative<bool>(*this); }
  bool is_double() const { return std::holds_alternative<double>(*this); }
  bool is_int() const { return std::holds_alternative<int>(*this); }
  bool is_number() const { return is_double() || is_int(); }
  bool is_string() const { return std::holds_alternative<string_type>(*this); }
  bool is_array() const { return std::holds_alternative<array_type>(*this); }
  bool is_object() const { return std::holds_alternative<object_type>(*this); }
  bool is_string_view() const {
    return std::holds_alternative<string_view_type>(*this);
  }

  // if type is not match, will throw exception, if pass std::error_code, won't
  // throw exception
  template <typename T>
  T get() const {
    try {
      return std::get<T>(*this);
    } catch (std::exception &e) {
      auto it = type_map_.find(this->index());
      if (it == type_map_.end()) {
        throw std::invalid_argument("undefined type");
      }
      else {
        throw std::invalid_argument(it->second);
      }
    } catch (...) {
      throw std::invalid_argument("unknown exception");
    }
  }

  template <typename T>
  T get(std::error_code &ec) const {
    try {
      return get<T>();
    } catch (std::exception &e) {
      ec = iguana::make_error_code(iguana::dom_errc::wrong_type, e.what());
      return T{};
    }
  }

  template <typename T>
  std::error_code get_to(T &v) const {
    std::error_code ec;
    v = get<T>(ec);
    return ec;
  }
  template <typename T>
  T at(const std::string &key) {
    const auto &map = get<object_type>();
    auto it = map.find(key);
    if (it == map.end()) {
      throw std::invalid_argument("the key is unknown");
    }
    return it->second.template get<T>();
  }

  template <typename T>
  T at(const std::string &key, std::error_code &ec) {
    const auto &map = get<object_type>(ec);
    if (ec) {
      return T{};
    }

    auto it = map.find(key);
    if (it == map.end()) {
      ec = std::make_error_code(std::errc::invalid_argument);
      return T{};
    }
    return it->second.template get<T>(ec);
  }

  template <typename T>
  T at(size_t idx) {
    const auto &arr = get<array_type>();
    if (idx >= arr.size()) {
      throw std::out_of_range("idx is out of range");
    }
    return arr[idx].template get<T>();
  }

  template <typename T>
  T at(size_t idx, std::error_code &ec) {
    const auto &arr = get<array_type>(ec);
    if (ec) {
      return T{};
    }

    if (idx >= arr.size()) {
      ec = std::make_error_code(std::errc::result_out_of_range);
      return T{};
    }

    return arr[idx].template get<T>(ec);
  }

  object_type to_object() const { return get<object_type>(); }
  object_type to_object(std::error_code &ec) const {
    return get<object_type>(ec);
  }

  array_type to_array() const { return get<array_type>(); }
  array_type to_array(std::error_code &ec) const { return get<array_type>(ec); }

  double to_double() const { return get<double>(); }
  double to_double(std::error_code &ec) const { return get<double>(ec); }

  int to_int() const { return get<int>(); }
  int to_int(std::error_code &ec) const { return get<int>(ec); }

  bool to_bool() const { return get<bool>(); }
  bool to_bool(std::error_code &ec) const { return get<bool>(ec); }

  string_type to_string() const { return get<string_type>(); }
  string_type to_string(std::error_code &ec) const {
    return get<string_type>(ec);
  }

  string_view_type to_string_view() const { return get<string_view_type>(); }
  string_view_type to_string_view(std::error_code &ec) const {
    return get<string_view_type>(ec);
  }
};

template <typename CharT>
using basic_jarray = typename basic_json_value<CharT>::array_type;

template <typename CharT>
using basic_jobject = typename basic_json_value<CharT>::object_type;

template <typename CharT>
using basic_jpair = typename basic_jobject<CharT>::value_type;

using jvalue = basic_json_value<char>;
using jarray = basic_jarray<char>;
using jobject = basic_jobject<char>;
using jpair = basic_jpair<char>;

template <typename CharT>
void swap(basic_json_value<CharT> &lhs, basic_json_value<CharT> &rhs) noexcept {
  lhs.swap(rhs);
}

}  // namespace iguana
