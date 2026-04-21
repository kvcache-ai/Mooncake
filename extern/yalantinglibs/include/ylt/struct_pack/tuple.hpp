/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// https://github.com/codeinred/tuplet/blob/main/include/tuplet/tuple.hpp
// commit id: ce4ab635c4f70b63ef9d19728bc8d76d71ae8685
// Use of this source code is governed by a Boost Software License that can be
// found in the LICENSE file.

#ifndef TUPLET_TUPLET_HPP_IMPLEMENTATION
#define TUPLET_TUPLET_HPP_IMPLEMENTATION

#include <compare>
#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

#if (__has_cpp_attribute(no_unique_address))
#define TUPLET_NO_UNIQUE_ADDRESS [[no_unique_address]]
#elif (__has_cpp_attribute(msvc::no_unique_address)) || \
    ((defined _MSC_VER) && (!defined __clang__))
// Note __has_cpp_attribute(msvc::no_unique_address) itself doesn't work as
// of 19.30.30709, even though the attribute itself is supported. See
// https://github.com/llvm/llvm-project/issues/49358#issuecomment-981041089
#define TUPLET_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#else
// no_unique_address is not available.
#define TUPLET_NO_UNIQUE_ADDRESS
#endif

// tuplet concepts and traits
namespace tuplet {
template <class T>
using identity_t = T;

// Obtains T::type
template <class T>
using type_t = typename T::type;

template <size_t I>
using tag = std::integral_constant<size_t, I>;

template <size_t I>
constexpr tag<I> tag_v{};

template <size_t N>
using tag_range = std::make_index_sequence<N>;

template <class T, class U>
concept same_as = std::is_same_v<T, U> && std::is_same_v<U, T>;

template <class T, class U>
concept other_than = !std::is_same_v<std::decay_t<T>, U>;

template <class Tup>
using base_list_t = typename std::decay_t<Tup>::base_list;
template <class Tup>
using element_list_t = typename std::decay_t<Tup>::element_list;

template <class Tuple>
concept base_list_tuple = requires() {
  typename std::decay_t<Tuple>::base_list;
};

template <class T>
concept stateless = std::is_empty_v<std::decay_t<T>>;

template <class T>
concept indexable = stateless<T> || requires(T t) {
  t[tag<0>()];
};

template <size_t I, indexable Tup>
constexpr decltype(auto) get(Tup&& tup);

template <class T, class U>
concept other_than_tuple =
    !std::is_same_v<std::decay_t<T>, U> && (requires(U u) { get<U>(); });

template <class U, class T>
concept assignable_to = requires(U u, T t) {
  t = u;
};

template <class T>
concept ordered = requires(T const& t) {
  {t <=> t};
};
template <class T>
concept equality_comparable = requires(T const& t) {
  { t == t } -> same_as<bool>;
};
}  // namespace tuplet

// tuplet::type_list implementation
// tuplet::type_map implementation
// tuplet::tuple_elem implementation
// tuplet::deduce_elems
namespace tuplet {
template <class... T>
struct tuple;

template <class... T>
struct type_list {};

template <class... Ls, class... Rs>
constexpr auto operator+(type_list<Ls...>, type_list<Rs...>) {
  return type_list<Ls..., Rs...>{};
}

template <class... Bases>
struct type_map : Bases... {
  using base_list = type_list<Bases...>;
  using Bases::operator[]...;
  using Bases::decl_elem...;
  auto operator<=>(type_map const&) const = default;
  bool operator==(type_map const&) const = default;
};

template <size_t I, class T>
struct tuple_elem {
  // Like declval, but with the element
  static T decl_elem(tag<I>);
  using type = T;

  TUPLET_NO_UNIQUE_ADDRESS T value;

  constexpr decltype(auto) operator[](tag<I>) & { return (value); }
  constexpr decltype(auto) operator[](tag<I>) const& { return (value); }
  constexpr decltype(auto) operator[](tag<I>) && {
    return (std::move(*this).value);
  }
  auto operator<=>(tuple_elem const&) const = default;
  bool operator==(tuple_elem const&) const = default;
  // Implements comparison for tuples containing reference types
  constexpr auto operator<=>(tuple_elem const& other) const noexcept(noexcept(
      value <=> other.value)) requires(std::is_reference_v<T>&& ordered<T>) {
    return value <=> other.value;
  }
  constexpr bool operator==(tuple_elem const& other) const
      noexcept(noexcept(value == other.value)) requires(
          std::is_reference_v<T>&& equality_comparable<T>) {
    return value == other.value;
  }
};
template <class T>
using unwrap_ref_decay_t = typename std::unwrap_ref_decay<T>::type;
}  // namespace tuplet

// tuplet::detail::get_tuple_base implementation
// tuplet::detail::apply_impl
// tuplet::detail::size_t_from_digits
namespace tuplet::detail {
template <class A, class... T>
struct get_tuple_base;

template <size_t... I, class... T>
struct get_tuple_base<std::index_sequence<I...>, T...> {
  using type = type_map<tuple_elem<I, T>...>;
};

template <class F, class T, class... Bases>
constexpr decltype(auto) apply_impl(F&& f, T&& t, type_list<Bases...>) {
  return static_cast<F&&>(f)(static_cast<T&&>(t).identity_t<Bases>::value...);
}
template <char... D>
constexpr size_t size_t_from_digits() {
  static_assert((('0' <= D && D <= '9') && ...), "Must be integral literal");
  size_t num = 0;
  return ((num = num * 10 + (D - '0')), ..., num);
}
template <class First, class>
using first_t = First;

template <class T, class... Q>
constexpr auto repeat_type(type_list<Q...>) {
  return type_list<first_t<T, Q>...>{};
}
template <class... Outer>
constexpr auto get_outer_bases(type_list<Outer...>) {
  return (repeat_type<Outer>(base_list_t<type_t<Outer>>{}) + ...);
}
template <class... Outer>
constexpr auto get_inner_bases(type_list<Outer...>) {
  return (base_list_t<type_t<Outer>>{} + ...);
}

// This takes a forwarding tuple as a parameter. The forwarding tuple only
// contains references, so it should just be taken by value.
template <class T, class... Outer, class... Inner>
constexpr auto cat_impl(T tup, type_list<Outer...>, type_list<Inner...>)
    -> tuple<type_t<Inner>...> {
  return {static_cast<type_t<Outer>&&>(tup.identity_t<Outer>::value)
              .identity_t<Inner>::value...};
}
}  // namespace tuplet::detail

// tuplet::tuple implementation
namespace tuplet {
template <class... T>
using tuple_base_t =
    typename detail::get_tuple_base<tag_range<sizeof...(T)>, T...>::type;

template <class... T>
struct tuple : tuple_base_t<T...> {
  constexpr static size_t N = sizeof...(T);
  using super = tuple_base_t<T...>;
  using super::operator[];
  using base_list = typename super::base_list;
  using element_list = type_list<T...>;
  using super::decl_elem;

  template <other_than_tuple<tuple> U>  // Preserves default assignments
  constexpr auto& operator=(U&& tup) {
    using tuple2 = std::decay_t<U>;
    if constexpr (base_list_tuple<tuple2>) {
      eq_impl(static_cast<U&&>(tup), base_list(), typename tuple2::base_list());
    }
    else {
      eq_impl(static_cast<U&&>(tup), tag_range<N>());
    }
    return *this;
  }

  template <assignable_to<T>... U>
  constexpr auto& assign(U&&... values) {
    assign_impl(base_list(), static_cast<U&&>(values)...);
    return *this;
  }

  auto operator<=>(tuple const&) const = default;
  bool operator==(tuple const&) const = default;

  // Applies a function to every element of the tuple. The order is the
  // declaration order, so first the function will be applied to element 0,
  // then element 1, then element 2, and so on, where element N is identified
  // by get<N>
  template <class F>
  constexpr void for_each(F&& func) & {
    for_each_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr void for_each(F&& func) const& {
    for_each_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr void for_each(F&& func) && {
    static_cast<tuple&&>(*this).for_each_impl(base_list(),
                                              static_cast<F&&>(func));
  }

  // Applies a function to each element successively, until one returns a
  // truthy value. Returns true if any application returned a truthy value,
  // and false otherwise
  template <class F>
  constexpr bool any(F&& func) & {
    return any_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr bool any(F&& func) const& {
    return any_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr bool any(F&& func) && {
    return static_cast<tuple&&>(*this).any_impl(base_list(),
                                                static_cast<F&&>(func));
  }

  // Applies a function to each element successively, until one returns a
  // falsy value. Returns true if every application returned a truthy value,
  // and false otherwise
  template <class F>
  constexpr bool all(F&& func) & {
    return all_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr bool all(F&& func) const& {
    return all_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr bool all(F&& func) && {
    return static_cast<tuple&&>(*this).all_impl(base_list(),
                                                static_cast<F&&>(func));
  }

  // Map a function over every element in the tuple, using the values to
  // construct a new tuple
  template <class F>
  constexpr auto map(F&& func) & {
    return map_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr auto map(F&& func) const& {
    return map_impl(base_list(), static_cast<F&&>(func));
  }
  template <class F>
  constexpr auto map(F&& func) && {
    return static_cast<tuple&&>(*this).map_impl(base_list(),
                                                static_cast<F&&>(func));
  }

 private:
  template <class U, class... B1, class... B2>
  constexpr void eq_impl(U&& u, type_list<B1...>, type_list<B2...>) {
    // See:
    // https://developercommunity.visualstudio.com/t/fold-expressions-unreliable-in-171-with-c20/1676476
    (void(B1::value = static_cast<U&&>(u).B2::value), ...);
  }
  template <class U, size_t... I>
  constexpr void eq_impl(U&& u, std::index_sequence<I...>) {
    (void(tuple_elem<I, T>::value = get<I>(static_cast<U&&>(u))), ...);
  }
  template <class... U, class... B>
  constexpr void assign_impl(type_list<B...>, U&&... u) {
    (void(B::value = static_cast<U&&>(u)), ...);
  }

  template <class F, class... B>
  constexpr void for_each_impl(type_list<B...>, F&& func) & {
    (void(func(B::value)), ...);
  }
  template <class F, class... B>
  constexpr void for_each_impl(type_list<B...>, F&& func) const& {
    (void(func(B::value)), ...);
  }
  template <class F, class... B>
  constexpr void for_each_impl(type_list<B...>, F&& func) && {
    (void(func(static_cast<T&&>(B::value))), ...);
  }

  template <class F, class... B>
  constexpr bool any_impl(type_list<B...>, F&& func) & {
    return (bool(func(B::value)) || ...);
  }
  template <class F, class... B>
  constexpr bool any_impl(type_list<B...>, F&& func) const& {
    return (bool(func(B::value)) || ...);
  }
  template <class F, class... B>
  constexpr bool any_impl(type_list<B...>, F&& func) && {
    return (bool(func(static_cast<T&&>(B::value))) || ...);
  }

  template <class F, class... B>
  constexpr bool all_impl(type_list<B...>, F&& func) & {
    return (bool(func(B::value)) && ...);
  }
  template <class F, class... B>
  constexpr bool all_impl(type_list<B...>, F&& func) const& {
    return (bool(func(B::value)) && ...);
  }
  template <class F, class... B>
  constexpr bool all_impl(type_list<B...>, F&& func) && {
    return (bool(func(static_cast<T&&>(B::value))) && ...);
  }

  template <class F, class... B>
  constexpr auto map_impl(
      type_list<B...>,
      F&& func) & -> tuple<unwrap_ref_decay_t<decltype(func(B::value))>...> {
    return {func(B::value)...};
  }
  template <class F, class... B>
  constexpr auto map_impl(type_list<B...>, F&& func)
      const& -> tuple<unwrap_ref_decay_t<decltype(func(B::value))>...> {
    return {func(B::value)...};
  }
  template <class F, class... B>
  constexpr auto map_impl(type_list<B...>, F&& func) && -> tuple<
      unwrap_ref_decay_t<decltype(func(static_cast<T&&>(B::value)))>...> {
    return {func(static_cast<T&&>(B::value))...};
  }
};
template <>
struct tuple<> : tuple_base_t<> {
  constexpr static size_t N = 0;
  using super = tuple_base_t<>;
  using base_list = type_list<>;
  using element_list = type_list<>;

  template <other_than<tuple> U>  // Preserves default assignments
  requires stateless<U>           // Check that U is similarly stateless
  constexpr auto& operator=(U&&) noexcept { return *this; }

  constexpr auto& assign() noexcept { return *this; }
  auto operator<=>(tuple const&) const = default;
  bool operator==(tuple const&) const = default;

  // Applies a function to every element of the tuple. The order is the
  // declaration order, so first the function will be applied to element 0,
  // then element 1, then element 2, and so on, where element N is identified
  // by get<N>
  //
  // (Does nothing when invoked on empty tuple)
  template <class F>
  constexpr void for_each(F&&) const noexcept {}

  // Applies a function to each element successively, until one returns a
  // truthy value. Returns true if any application returned a truthy value,
  // and false otherwise
  //
  // (Returns false for empty tuple)
  template <class F>
  constexpr bool any(F&&) const noexcept {
    return false;
  }

  // Applies a function to each element successively, until one returns a
  // falsy value. Returns true if every application returned a truthy value,
  // and false otherwise
  //
  // (Returns true for empty tuple)
  template <class F>
  constexpr bool all(F&&) const noexcept {
    return true;
  }

  // Map a function over every element in the tuple, using the values to
  // construct a new tuple
  //
  // (Returns empty tuple when invoked on empty tuple)
  template <class F>
  constexpr auto map(F&&) const noexcept {
    return tuple{};
  }
};
template <class... Ts>
tuple(Ts...) -> tuple<unwrap_ref_decay_t<Ts>...>;
}  // namespace tuplet

// tuplet::pair implementation
namespace tuplet {
template <class First, class Second>
struct pair {
  constexpr static size_t N = 2;
  TUPLET_NO_UNIQUE_ADDRESS First first;
  TUPLET_NO_UNIQUE_ADDRESS Second second;

  constexpr decltype(auto) operator[](tag<0>) & { return (first); }
  constexpr decltype(auto) operator[](tag<0>) const& { return (first); }
  constexpr decltype(auto) operator[](tag<0>) && {
    return (std::move(*this).first);
  }
  constexpr decltype(auto) operator[](tag<1>) & { return (second); }
  constexpr decltype(auto) operator[](tag<1>) const& { return (second); }
  constexpr decltype(auto) operator[](tag<1>) && {
    return (std::move(*this).second);
  }

  template <other_than<pair> Type>  // Preserves default assignments
  constexpr auto& operator=(Type&& tup) {
    auto&& [a, b] = static_cast<Type&&>(tup);
    first = static_cast<decltype(a)&&>(a);
    second = static_cast<decltype(b)&&>(b);
    return *this;
  }

  template <assignable_to<First> F2, assignable_to<Second> S2>
  constexpr auto& assign(F2&& f, S2&& s) {
    first = static_cast<F2&&>(f);
    second = static_cast<S2&&>(s);
    return *this;
  }
  auto operator<=>(pair const&) const = default;
  bool operator==(pair const&) const = default;
};
template <class A, class B>
pair(A, B) -> pair<unwrap_ref_decay_t<A>, unwrap_ref_decay_t<B>>;
}  // namespace tuplet

// tuplet::convert implementation
namespace tuplet {
// Converts from one tuple type to any other tuple or U
template <class Tuple>
struct convert {
  using base_list = typename std::decay_t<Tuple>::base_list;
  Tuple tuple;
  template <class U>
  constexpr operator U() && {
    return convert_impl<U>(base_list{});
  }

 private:
  template <class U, class... Bases>
  constexpr U convert_impl(type_list<Bases...>) {
    return U{static_cast<Tuple&&>(tuple).identity_t<Bases>::value...};
  }
};
template <class Tuple>
convert(Tuple&) -> convert<Tuple&>;
template <class Tuple>
convert(Tuple const&) -> convert<Tuple const&>;
template <class Tuple>
convert(Tuple&&) -> convert<Tuple>;
}  // namespace tuplet

// tuplet::get implementation
// tuplet::tie implementation
// tuplet::apply implementation
namespace tuplet {
template <size_t I, indexable Tup>
constexpr decltype(auto) get(Tup&& tup) {
  return static_cast<Tup&&>(tup)[tag<I>()];
}

template <class... T>
constexpr tuple<T&...> tie(T&... t) {
  return {t...};
}

template <class F, base_list_tuple Tup>
constexpr decltype(auto) apply(F&& func, Tup&& tup) {
  return detail::apply_impl(static_cast<F&&>(func), static_cast<Tup&&>(tup),
                            typename std::decay_t<Tup>::base_list());
}
template <class F, class A, class B>
constexpr decltype(auto) apply(F&& func, tuplet::pair<A, B>& pair) {
  return static_cast<F&&>(func)(pair.first, pair.second);
}
template <class F, class A, class B>
constexpr decltype(auto) apply(F&& func, tuplet::pair<A, B> const& pair) {
  return static_cast<F&&>(func)(pair.first, pair.second);
}
template <class F, class A, class B>
constexpr decltype(auto) apply(F&& func, tuplet::pair<A, B>&& pair) {
  return static_cast<F&&>(func)(std::move(pair).first, std::move(pair).second);
}
}  // namespace tuplet

// tuplet::tuple_cat implementation
// tuplet::make_tuple implementation
// tuplet::forward_as_tuple implementation
namespace tuplet {
template <base_list_tuple... T>
constexpr auto tuple_cat(T&&... ts) {
  if constexpr (sizeof...(T) == 0) {
    return tuple<>();
  }
  else {
/**
 * It appears that Clang produces better assembly when
 * TUPLET_CAT_BY_FORWARDING_TUPLE == 0, while GCC produces better assembly when
 * TUPLET_CAT_BY_FORWARDING_TUPLE == 1. MSVC always produces terrible assembly
 * in either case. This will set TUPLET_CAT_BY_FORWARDING_TUPLE to the correct
 * value (0 for clang, 1 for everyone else)
 *
 * See: https://github.com/codeinred/tuplet/discussions/14
 */
#if !defined(TUPLET_CAT_BY_FORWARDING_TUPLE)
#if defined(__clang__)
#define TUPLET_CAT_BY_FORWARDING_TUPLE 0
#else
#define TUPLET_CAT_BY_FORWARDING_TUPLE 1
#endif
#endif
#if TUPLET_CAT_BY_FORWARDING_TUPLE
    using big_tuple = tuple<T&&...>;
#else
    using big_tuple = tuple<std::decay_t<T>...>;
#endif
    using outer_bases = base_list_t<big_tuple>;
    constexpr auto outer = detail::get_outer_bases(outer_bases{});
    constexpr auto inner = detail::get_inner_bases(outer_bases{});
    return detail::cat_impl(big_tuple{static_cast<T&&>(ts)...}, outer, inner);
  }
}

template <typename... Ts>
constexpr auto make_tuple(Ts&&... args) {
  return tuple<unwrap_ref_decay_t<Ts>...>{static_cast<Ts&&>(args)...};
}

template <typename... T>
constexpr auto forward_as_tuple(T&&... a) noexcept {
  return tuple<T&&...>{static_cast<T&&>(a)...};
}
}  // namespace tuplet

// tuplet literals
namespace tuplet::literals {
template <char... D>
constexpr auto operator""_tag() noexcept
    -> tag<detail::size_t_from_digits<D...>()> {
  return {};
}
}  // namespace tuplet::literals

// std::tuple_size specialization
// std::tuple_element specialization
namespace std {
template <class... T>
struct tuple_size<tuplet::tuple<T...>>
    : std::integral_constant<size_t, sizeof...(T)> {};

template <size_t I, class... T>
struct tuple_element<I, tuplet::tuple<T...>> {
  using type = decltype(tuplet::tuple<T...>::decl_elem(tuplet::tag<I>()));
};
template <class A, class B>
struct tuple_size<tuplet::pair<A, B>> : std::integral_constant<size_t, 2> {};

template <size_t I, class A, class B>
struct tuple_element<I, tuplet::pair<A, B>> {
  static_assert(I < 2, "tuplet::pair only has 2 elements");
  using type = std::conditional_t<I == 0, A, B>;
};
}  // namespace std

namespace tuplet {
template <class T>
constexpr size_t tuple_size_v = std::tuple_size<T>::value;

template <size_t I, class T>
using tuple_element_t = typename std::tuple_element<I, T>::type;
}  // namespace tuplet
#endif
