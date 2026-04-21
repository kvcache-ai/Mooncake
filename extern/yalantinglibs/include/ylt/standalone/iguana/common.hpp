#pragma once
#include <any>

#include "util.hpp"

namespace iguana {
struct iguana_adl_t {};

namespace detail {
template <typename T>
struct identity {};

struct field_info {
  size_t offset;
  std::string_view type_name;
};

struct base {
  virtual void to_pb(std::string& str) const {}
  virtual void from_pb(std::string_view str) {}
  virtual void to_xml(std::string& str) const {}
  virtual void from_xml(std::string_view str) {}
  virtual void to_json(std::string& str) const {}
  virtual void from_json(std::string_view str) {}
  virtual void to_yaml(std::string& str) const {}
  virtual void from_yaml(std::string_view str) {}
  virtual std::vector<std::string_view> get_fields_name() const { return {}; }
  virtual std::any get_field_any(std::string_view name) const { return {}; }
  virtual iguana::detail::field_info get_field_info(
      std::string_view name) const {
    return {};
  }

  template <typename T>
  T& get_field_value(std::string_view name) {
    auto info = get_field_info(name);
    check_field<T>(name, info);
    auto ptr = (((char*)this) + info.offset);
    return *((T*)ptr);
  }

  template <typename T, typename FiledType = T>
  void set_field_value(std::string_view name, T val) {
    auto info = get_field_info(name);
    check_field<FiledType>(name, info);

    auto ptr = (((char*)this) + info.offset);

    static_assert(std::is_constructible_v<FiledType, T>, "can not assign");

    *((FiledType*)ptr) = std::move(val);
  }
  virtual ~base() {}

 private:
  template <typename T>
  void check_field(std::string_view name, const field_info& info) {
    if (info.offset == 0) {
      throw std::invalid_argument(std::string(name) + " field not exist ");
    }

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
    if (info.type_name != iguana::type_string<T>()) {
      std::string str = "type is not match: can not assign ";
      str.append(iguana::type_string<T>());
      str.append(" to ").append(info.type_name);

      throw std::invalid_argument(str);
    }
#endif
  }
};

inline std::unordered_map<std::string_view,
                          std::function<std::shared_ptr<base>()>>
    g_pb_map;

template <typename T>
struct field_type_t;

template <typename... Args>
struct field_type_t<std::tuple<Args...>> {
  using value_type = std::variant<Args...>;
};

template <typename T>
constexpr size_t count_variant_size() {
  if constexpr (is_variant<T>::value) {
    return std::variant_size_v<T>;
  }
  else {
    return 1;
  }
}

template <typename T, size_t... I>
constexpr size_t tuple_type_count_impl(std::index_sequence<I...>) {
  return (
      (count_variant_size<member_value_type_t<std::tuple_element_t<I, T>>>() +
       ...));
}

template <typename T>
constexpr size_t tuple_type_count() {
  return tuple_type_count_impl<T>(
      std::make_index_sequence<std::tuple_size_v<T>>{});
}

template <typename T, size_t Size, typename Tuple, size_t... I>
constexpr auto inline get_members_impl(Tuple&& tp, std::index_sequence<I...>) {
  return frozen::unordered_map<uint32_t, T, sizeof...(I)>{
      {std::get<I>(tp).field_no,
       T{std::in_place_index<I>, std::move(std::get<I>(tp))}}...};
}

template <typename T, typename = void>
struct is_custom_reflection : std::false_type {};

template <typename T>
struct is_custom_reflection<
    T, std::void_t<decltype(get_members_impl(std::declval<T*>()))>>
    : std::true_type {};

template <typename T>
inline constexpr bool is_custom_reflection_v =
    is_custom_reflection<ylt::reflection::remove_cvref_t<T>>::value;

// owner_type: parant type, value_type: member value type, SubType: subtype from
// variant
template <typename Owner, typename Value, size_t FieldNo,
          typename ElementType = Value>
struct pb_field_t {
  using owner_type = ylt::reflection::remove_cvref_t<Owner>;
  using value_type = Value;
  using sub_type = ElementType;

  // constexpr pb_field_t() = default;
  auto& value(owner_type& value) const {
    auto member_ptr = (value_type*)((char*)(&value) + offset);
    return *member_ptr;
  }
  auto const& value(const owner_type& value) const {
    auto member_ptr = (value_type*)((char*)(&value) + offset);
    return *member_ptr;
  }

  size_t offset;
  std::string_view field_name;

  inline static constexpr uint32_t field_no = FieldNo;
};

template <size_t I, typename ValueType, typename Array>
constexpr inline auto get_field_no_impl(Array& arr, size_t& index) {
  arr[I] = index;
  if constexpr (is_variant<ValueType>::value) {
    constexpr size_t variant_size = std::variant_size_v<ValueType>;
    index += (variant_size);
  }
  else {
    index++;
    return;
  }
}

template <typename Tuple, size_t... I>
inline constexpr auto get_field_no(std::index_sequence<I...>) {
  std::array<size_t, sizeof...(I)> arr{};
  size_t index = 0;
  (get_field_no_impl<
       I, ylt::reflection::remove_cvref_t<std::tuple_element_t<I, Tuple>>>(
       arr, index),
   ...);
  return arr;
}

template <typename T, typename value_type, size_t field_no, size_t... I>
constexpr inline auto build_pb_variant_fields(size_t offset,
                                              std::string_view name,
                                              std::index_sequence<I...>) {
  return std::tuple(
      pb_field_t<T, value_type, field_no + I + 1,
                 std::variant_alternative_t<I, value_type>>{offset, name}...);
}

template <typename T, size_t field_no, typename ValueType>
constexpr inline auto build_pb_fields_impl(size_t offset,
                                           std::string_view name) {
  using value_type = ylt::reflection::remove_cvref_t<ValueType>;
  using U = std::remove_reference_t<T>;

  if constexpr (is_variant<value_type>::value) {
    constexpr uint32_t variant_size = std::variant_size_v<value_type>;
    return build_pb_variant_fields<U, value_type, field_no>(
        offset, name, std::make_index_sequence<variant_size>{});
  }
  else {
    return std::tuple(pb_field_t<U, value_type, field_no + 1>{offset, name});
  }
}

template <typename Tuple, typename T, typename Array, size_t... I>
inline auto build_pb_fields(const Array& offset_arr,
                            std::index_sequence<I...>) {
  constexpr auto arr = ylt::reflection::get_member_names<T>();
  constexpr std::array<size_t, sizeof...(I)> indexs =
      get_field_no<Tuple>(std::make_index_sequence<sizeof...(I)>{});
  return std::tuple_cat(
      build_pb_fields_impl<T, indexs[I], std::tuple_element_t<I, Tuple>>(
          offset_arr[I], arr[I])...);
}

template <typename T>
inline auto get_pb_members_tuple(T&& t) {
  using U = ylt::reflection::remove_cvref_t<T>;
  if constexpr (ylt_refletable_v<U>) {
    static auto& offset_arr = ylt::reflection::internal::get_member_offset_arr(
        ylt::reflection::internal::wrapper<U>::value);
    using Tuple = decltype(ylt::reflection::object_to_tuple(std::declval<U>()));
    return build_pb_fields<Tuple, T>(
        offset_arr, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
  }
  else if constexpr (is_custom_reflection_v<U>) {
    return get_members_impl((U*)nullptr);
  }
  else {
    static_assert(!sizeof(T), "not a reflectable type");
  }
}

template <typename T>
inline auto get_members(T&& t) {
  if constexpr (ylt_refletable_v<T> || is_custom_reflection_v<T>) {
    static auto tp = get_pb_members_tuple(std::forward<T>(t));
    using Tuple = std::decay_t<decltype(tp)>;
    using value_type = typename field_type_t<Tuple>::value_type;
    constexpr auto Size = tuple_type_count<Tuple>();
    return get_members_impl<value_type, Size>(tp,
                                              std::make_index_sequence<Size>{});
  }
  else {
    static_assert(!sizeof(T), "expected reflection or custom reflection");
  }
}
}  // namespace detail

template <typename T>
inline bool register_type() {
#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
  if constexpr (std::is_base_of_v<detail::base, T>) {
    auto it = detail::g_pb_map.emplace(type_string<T>(), [] {
      return std::make_shared<T>();
    });
    return it.second;
  }
  else {
    return true;
  }
#else
  return true;
#endif
}

template <typename T, typename U>
IGUANA_INLINE constexpr size_t member_offset(T* t, U T::*member) {
  return (char*)&(t->*member) - (char*)t;
}

template <auto ptr, size_t field_no>
IGUANA_INLINE auto build_pb_field(std::string_view name) {
  using owner =
      typename ylt::reflection::member_traits<decltype(ptr)>::owner_type;
  using value_type =
      typename ylt::reflection::member_traits<decltype(ptr)>::value_type;
  size_t offset = member_offset((owner*)nullptr, ptr);
  return iguana::detail::pb_field_t<owner, value_type, field_no>{offset, name};
}
}  // namespace iguana