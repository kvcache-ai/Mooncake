#pragma once
#include "detail/string_resize.hpp"
#include "pb_util.hpp"

namespace iguana {
template <typename T>
IGUANA_INLINE void from_pb(T& t, std::string_view pb_str);
namespace detail {

template <typename T>
IGUANA_INLINE void from_pb_impl(T& val, std::string_view& pb_str,
                                uint32_t field_no = 0);

template <typename T>
IGUANA_INLINE void decode_pair_value(T& val, std::string_view& pb_str) {
  size_t pos;
  uint32_t key = detail::decode_varint(pb_str, pos);
  pb_str = pb_str.substr(pos);
  WireType wire_type = static_cast<WireType>(key & 0b0111);
  if (wire_type != detail::get_wire_type<std::remove_reference_t<T>>()) {
    return;
  }
  from_pb_impl(val, pb_str);
}

template <typename T>
IGUANA_INLINE void from_pb_impl(T& val, std::string_view& pb_str,
                                uint32_t field_no) {
  size_t pos = 0;
  if constexpr (ylt_refletable_v<T>) {
    size_t pos;
    uint32_t size = detail::decode_varint(pb_str, pos);
    pb_str = pb_str.substr(pos);
    if (pb_str.size() < size)
      IGUANA_UNLIKELY {
        throw std::invalid_argument("Invalid fixed int value: too few bytes.");
      }
    if (size == 0) {
      return;
    }
    from_pb(val, pb_str.substr(0, size));
    pb_str = pb_str.substr(size);
  }
  else if constexpr (is_sequence_container<T>::value) {
    using item_type = typename T::value_type;
    if constexpr (is_lenprefix_v<item_type>) {
      // item_type non-packed
      while (!pb_str.empty()) {
        item_type item{};
        from_pb_impl(item, pb_str);
        val.push_back(std::move(item));
        if (pb_str.empty()) {
          break;
        }
        uint32_t key = detail::decode_varint(pb_str, pos);
        uint32_t field_number = key >> 3;
        if (field_number != field_no) {
          break;
        }
        else {
          pb_str = pb_str.substr(pos);
        }
      }
    }
    else {
      // item_type packed
      size_t pos;
      uint32_t size = detail::decode_varint(pb_str, pos);
      pb_str = pb_str.substr(pos);
      if (pb_str.size() < size)
        IGUANA_UNLIKELY {
          throw std::invalid_argument(
              "Invalid fixed int value: too few bytes.");
        }
      if constexpr (is_fixed_v<item_type>) {
        int num = size / sizeof(item_type);
        int old_size = val.size();
        detail::resize(val, old_size + num);
        std::memcpy(val.data() + old_size, pb_str.data(), size);
        pb_str = pb_str.substr(size);
      }
      else {
        size_t start = pb_str.size();

        while (!pb_str.empty()) {
          item_type item;
          from_pb_impl(item, pb_str);
          val.push_back(std::move(item));
          if (start - pb_str.size() == size) {
            break;
          }
        }
      }
    }
  }
  else if constexpr (is_map_container<T>::value) {
    using item_type = std::pair<typename T::key_type, typename T::mapped_type>;
    while (!pb_str.empty()) {
      size_t pos;
      uint32_t size = detail::decode_varint(pb_str, pos);
      pb_str = pb_str.substr(pos);
      if (pb_str.size() < size)
        IGUANA_UNLIKELY {
          throw std::invalid_argument(
              "Invalid fixed int value: too few bytes.");
        }
      item_type item = {};
      decode_pair_value(item.first, pb_str);
      decode_pair_value(item.second, pb_str);
      val.emplace(std::move(item));

      if (pb_str.empty()) {
        break;
      }
      uint32_t key = detail::decode_varint(pb_str, pos);
      uint32_t field_number = key >> 3;
      if (field_number != field_no) {
        break;
      }
      pb_str = pb_str.substr(pos);
    }
  }
  else if constexpr (std::is_integral_v<T>) {
    val = static_cast<T>(detail::decode_varint(pb_str, pos));
    pb_str = pb_str.substr(pos);
  }
  else if constexpr (detail::is_signed_varint_v<T>) {
    constexpr size_t len = sizeof(typename T::value_type);
    uint64_t temp = detail::decode_varint(pb_str, pos);
    if constexpr (len == 8) {
      val.val = detail::decode_zigzag(temp);
    }
    else {
      val.val = detail::decode_zigzag(static_cast<uint32_t>(temp));
    }
    pb_str = pb_str.substr(pos);
  }
  else if constexpr (detail::is_fixed_v<T>) {
    constexpr size_t size = sizeof(typename T::value_type);
    if (pb_str.size() < size)
      IGUANA_UNLIKELY {
        throw std::invalid_argument("Invalid fixed int value: too few bytes.");
      }
    memcpy(&(val.val), pb_str.data(), size);
    pb_str = pb_str.substr(size);
  }
  else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
    constexpr size_t size = sizeof(T);
    if (pb_str.size() < size)
      IGUANA_UNLIKELY {
        throw std::invalid_argument("Invalid fixed int value: too few bytes.");
      }
    memcpy(&(val), pb_str.data(), size);
    pb_str = pb_str.substr(size);
  }
  else if constexpr (std::is_same_v<T, std::string> ||
                     std::is_same_v<T, std::string_view>) {
    size_t size = detail::decode_varint(pb_str, pos);
    if (pb_str.size() < pos + size)
      IGUANA_UNLIKELY {
        throw std::invalid_argument("Invalid string value: too few bytes.");
      }
    if constexpr (std::is_same_v<T, std::string_view>) {
      val = std::string_view(pb_str.data() + pos, size);
    }
    else {
      detail::resize(val, size);
      memcpy(val.data(), pb_str.data() + pos, size);
    }
    pb_str = pb_str.substr(size + pos);
  }
  else if constexpr (std::is_enum_v<T>) {
    using U = std::underlying_type_t<T>;
    U value{};
    from_pb_impl(value, pb_str);
    val = static_cast<T>(value);
  }
  else if constexpr (optional_v<T>) {
    from_pb_impl(val.emplace(), pb_str);
  }
  else {
    static_assert(!sizeof(T), "err");
  }
}

template <typename T, typename Field>
IGUANA_INLINE void parse_oneof(T& t, const Field& f, std::string_view& pb_str) {
  using item_type = typename std::decay_t<Field>::sub_type;
  from_pb_impl(t.template emplace<item_type>(), pb_str, f.field_no);
}
}  // namespace detail

template <typename T>
IGUANA_INLINE void from_pb(T& t, std::string_view pb_str) {
  if (pb_str.empty())
    IGUANA_UNLIKELY { return; }
  size_t pos = 0;
  uint32_t key = detail::decode_varint(pb_str, pos);
  WireType wire_type = static_cast<WireType>(key & 0b0111);
  uint32_t field_number = key >> 3;
#ifdef SEQUENTIAL_PARSE
  static auto tp = detail::get_pb_members_tuple(t);
  constexpr size_t SIZE = std::tuple_size_v<std::decay_t<decltype(tp)>>;
  bool parse_done = false;
  auto ptr = &t;
  detail::for_each_n(
      [&, ptr](auto i) IGUANA__INLINE_LAMBDA {
        auto val = std::get<decltype(i)::value>(tp);
        using sub_type = typename std::decay_t<decltype(val)>::sub_type;
        using value_type = typename std::decay_t<decltype(val)>::value_type;
        constexpr bool is_variant_v = variant_v<value_type>;
        // sub_type is the element type when value_type is the variant type;
        // otherwise, they are the same.
        if (parse_done || field_number != val.field_no) {
          return;
        }
        pb_str = pb_str.substr(pos);
        if (wire_type != detail::get_wire_type<sub_type>())
          IGUANA_UNLIKELY { throw std::runtime_error("unmatched wire_type"); }

        auto member_ptr = (value_type*)((char*)(ptr) + val.offset);
        if constexpr (is_variant_v) {
          detail::parse_oneof(*member_ptr, val, pb_str);
        }
        else {
          detail::from_pb_impl(*member_ptr, pb_str, val.field_no);
        }
        if (pb_str.empty()) {
          parse_done = true;
          return;
        }
        key = detail::decode_varint(pb_str, pos);
        wire_type = static_cast<WireType>(key & 0b0111);
        field_number = key >> 3;
      },
      std::make_index_sequence<SIZE>{});
  if (parse_done)
    IGUANA_LIKELY { return; }
#endif
  while (true) {
    pb_str = pb_str.substr(pos);
    static auto map = detail::get_members(std::forward<T>(t));
    auto& member = map.at(field_number);
    std::visit(
        [&t, &pb_str, wire_type](auto& val) {
          using sub_type = typename std::decay_t<decltype(val)>::sub_type;
          using value_type = typename std::decay_t<decltype(val)>::value_type;
          if (wire_type != detail::get_wire_type<sub_type>()) {
            throw std::runtime_error("unmatched wire_type");
          }
          if constexpr (variant_v<value_type>) {
            detail::parse_oneof(val.value(t), val, pb_str);
          }
          else {
            detail::from_pb_impl(val.value(t), pb_str, val.field_no);
          }
        },
        member);
    if (!pb_str.empty())
      IGUANA_LIKELY {
        key = detail::decode_varint(pb_str, pos);
        wire_type = static_cast<WireType>(key & 0b0111);
        field_number = key >> 3;
      }
    else {
      return;
    }
  }
}

template <typename T>
IGUANA_INLINE void from_pb_adl(iguana_adl_t* p, T& t, std::string_view pb_str) {
  iguana::from_pb(t, pb_str);
}
}  // namespace iguana
