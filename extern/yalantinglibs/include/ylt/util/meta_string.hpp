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
 *
 * Author's Email: metabeyond@outlook.com
 * Author's Github: https://github.com/refvalue/
 * Description: this source file contains an implementation of a compile-time
 * meta-string that can be utilized as a non-type parameter of a template.
 */
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#if __has_include(<span>)
#include <compare>
#include <concepts>
#include <span>
#endif
#include <string_view>
#include <utility>

namespace refvalue {
template <std::size_t N>
struct meta_string {
  std::array<char, N + 1> elements_;

  constexpr meta_string() noexcept : elements_{} {}

  constexpr meta_string(const char (&data)[N + 1]) noexcept {
    for (size_t i = 0; i < N + 1; i++) elements_[i] = data[i];
  }

#if __has_include(<span>)
  template <std::size_t... Ns>
  constexpr meta_string(std::span<const char, Ns>... data) noexcept
      : elements_{} {
    auto iter = elements_.begin();

    ((iter = std::copy(data.begin(), data.end(), iter)), ...);
  }
#endif

  template <std::size_t... Ns>
  constexpr meta_string(const meta_string<Ns>&... data) noexcept : elements_{} {
    auto iter = elements_.begin();

    ((iter = std::copy(data.begin(), data.end(), iter)), ...);
  }

#if __has_include(<span>)
  template <std::same_as<char>... Ts>
  constexpr meta_string(Ts... chars) noexcept requires(sizeof...(Ts) == N)
      : elements_{chars...} {}
#endif

  constexpr char& operator[](std::size_t index) noexcept {
    return elements_[index];
  }

  constexpr const char& operator[](std::size_t index) const noexcept {
    return elements_[index];
  }

  constexpr operator std::string_view() const noexcept {
    return std::string_view{elements_.data(), size()};
  }

  constexpr bool empty() const noexcept { return size() == 0; }

  constexpr std::size_t size() const noexcept { return N; }

  constexpr char& front() noexcept { return elements_.front(); }

  constexpr const char& front() const noexcept { return elements_.front(); }

  constexpr char& back() noexcept { return elements_[size() - 1]; }

  constexpr const char& back() const noexcept { return elements_[size() - 1]; }

  constexpr auto begin() noexcept { return elements_.begin(); }

  constexpr auto begin() const noexcept { return elements_.begin(); }

  constexpr auto end() noexcept { return elements_.begin() + size(); }

  constexpr auto end() const noexcept { return elements_.begin() + size(); }

  constexpr char* data() noexcept { return elements_.data(); }

  constexpr const char* data() const noexcept { return elements_.data(); };

  constexpr const char* c_str() const noexcept { return elements_.data(); }

  constexpr bool contains(char c) const noexcept {
    return std::find(begin(), end(), c) != end();
  }

  constexpr bool contains(std::string_view str) const noexcept {
    return str.size() <= size()
               ? std::search(begin(), end(), str.begin(), str.end()) != end()
               : false;
  }

  static constexpr size_t substr_len(size_t pos, size_t count) {
    if (pos >= N) {
      return 0;
    }
    else if (count == std::string_view::npos || pos + count > N) {
      return N - pos;
    }
    else {
      return count;
    }
  }

  template <size_t pos, size_t count = std::string_view::npos>
  constexpr meta_string<substr_len(pos, count)> substr() const noexcept {
    constexpr size_t n = substr_len(pos, count);

    meta_string<n> result;
    for (std::size_t i = 0; i < n; ++i) {
      result[i] = elements_[pos + i];
    }
    return result;
  }

  constexpr size_t rfind(char c) const noexcept {
    return std::string_view(*this).rfind(c);
  }

  constexpr size_t find(char c) const noexcept {
    return std::string_view(*this).find(c);
  }
};

template <std::size_t N>
meta_string(const char (&)[N]) -> meta_string<N - 1>;

#if __has_include(<span>)
template <std::size_t... Ns>
meta_string(std::span<const char, Ns>...) -> meta_string<(Ns + ...)>;
#endif

template <std::size_t... Ns>
meta_string(const meta_string<Ns>&...) -> meta_string<(Ns + ...)>;

#if __has_include(<span>)
template <std::same_as<char>... Ts>
meta_string(Ts...) -> meta_string<sizeof...(Ts)>;
#endif

#if __has_include(<span>)
template <std::size_t M, std::size_t N>
constexpr auto operator<=>(const meta_string<M>& left,
                           const meta_string<N>& right) noexcept {
  return static_cast<std::string_view>(left).compare(
             static_cast<std::string_view>(right)) <=> 0;
}
#endif

template <std::size_t M, std::size_t N>
constexpr bool operator==(const meta_string<M>& left,
                          const meta_string<N>& right) noexcept {
  return static_cast<std::string_view>(left) ==
         static_cast<std::string_view>(right);
}

template <std::size_t M, std::size_t N>
constexpr bool operator==(const meta_string<M>& left,
                          const char (&right)[N]) noexcept {
  return static_cast<std::string_view>(left) ==
         static_cast<std::string_view>(meta_string{right});
}

template <std::size_t M, std::size_t N>
constexpr auto operator+(const meta_string<M>& left,
                         const meta_string<N>& right) noexcept {
  return meta_string{left, right};
}

template <std::size_t M, std::size_t N>
constexpr auto operator+(const meta_string<M>& left,
                         const char (&right)[N]) noexcept {
  meta_string<M + N - 1> s;
  for (size_t i = 0; i < M; ++i) s[i] = left[i];
  for (size_t i = 0; i < N; ++i) s[M + i] = right[i];
  return s;
}

template <std::size_t M, std::size_t N>
constexpr auto operator+(const char (&left)[M],
                         const meta_string<N>& right) noexcept {
  meta_string<M + N - 1> s;
  for (size_t i = 0; i < M - 1; ++i) s[i] = left[i];
  for (size_t i = 0; i < N; ++i) s[M + i - 1] = right[i];
  return s;
}

#if __has_include(<span>)
template <meta_string S, meta_string Delim>
struct split_of {
  static constexpr auto value = [] {
    constexpr std::string_view view{S};
    constexpr auto group_count = std::count_if(S.begin(), S.end(),
                                               [](char c) {
                                                 return Delim.contains(c);
                                               }) +
                                 1;
    std::array<std::string_view, group_count> result{};

    auto iter = result.begin();

    for (std::size_t start_index = 0, end_index = view.find_first_of(Delim);;
         start_index = end_index + 1,
                     end_index = view.find_first_of(Delim, start_index)) {
      *(iter++) = view.substr(start_index, end_index - start_index);

      if (end_index == std::string_view::npos) {
        break;
      }
    }

    return result;
  }();
};

template <meta_string S, meta_string Delim>
inline constexpr auto&& split_of_v = split_of<S, Delim>::value;

template <meta_string S, meta_string Delim>
struct split {
  static constexpr std::string_view view{S};
  static constexpr auto value = [] {
    constexpr auto group_count = [] {
      std::size_t count{};
      std::size_t index{};

      while ((index = view.find(Delim, index)) != std::string_view::npos) {
        count++;
        index += Delim.size();
      }

      return count + 1;
    }();
    std::array<std::string_view, group_count> result{};

    auto iter = result.begin();

    for (std::size_t start_index = 0, end_index = view.find(Delim);;
         start_index = end_index + Delim.size(),
                     end_index = view.find(Delim, start_index)) {
      *(iter++) = view.substr(start_index, end_index - start_index);

      if (end_index == std::string_view::npos) {
        break;
      }
    }

    return result;
  }();
};

template <meta_string S, meta_string Delim>
inline constexpr auto&& split_v = split<S, Delim>::value;

template <meta_string S, char C>
struct remove_char {
  static constexpr auto value = [] {
    struct removal_metadata {
      decltype(S) result;
      std::size_t actual_size;
    };

    constexpr auto metadata = [] {
      auto result = S;
      auto removal_end = std::remove(result.begin(), result.end(), C);

      return removal_metadata{
          .result{std::move(result)},
          .actual_size{static_cast<std::size_t>(removal_end - result.begin())}};
    }();

    meta_string<metadata.actual_size> result;

    std::copy(metadata.result.begin(),
              metadata.result.begin() + metadata.actual_size, result.begin());

    return result;
  }();
};

template <meta_string S, char C>
inline constexpr auto&& remove_char_v = remove_char<S, C>::value;

template <meta_string S, meta_string Removal>
struct remove {
  static constexpr auto groups = split_v<S, Removal>;
  static constexpr auto value = [] {
    return []<std::size_t... Is>(std::index_sequence<Is...>) {
      return meta_string{std::span<const char, groups[Is].size()>{
          groups[Is].data(), groups[Is].size()}...};
    }
    (std::make_index_sequence<groups.size()>{});
  }();
};

template <meta_string S, meta_string Removal>
inline constexpr auto&& remove_v = remove<S, Removal>::value;
#endif
}  // namespace refvalue
