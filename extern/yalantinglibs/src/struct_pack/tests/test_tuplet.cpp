#if __cplusplus >= 202002L
#include <cassert>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <ylt/struct_pack.hpp>
#include <ylt/struct_pack/tuple.hpp>

#include "doctest.h"

using namespace std::string_view_literals;

template <typename Tuple, size_t... Indexes, typename Fn>
constexpr decltype(auto) tuple_for_each_impl(
    Tuple&& tuple, std::integer_sequence<size_t, Indexes...>, Fn&& fn) {
  (fn(tuplet::get<Indexes>(std::forward<Tuple>(tuple))), ...);

  return std::forward<Fn>(fn);
}
template <typename Tuple, typename Fn>
constexpr decltype(auto) tuple_for_each(Tuple&& tuple, Fn&& fn) {
  using indexes =
      std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>::value>;

  return tuple_for_each_impl(std::forward<Tuple>(tuple), indexes{},
                             std::forward<Fn>(fn));
}

template <size_t Alignment, typename T>
constexpr T align_value(T value) {
  constexpr auto alignment{static_cast<T>(Alignment)};

  value += alignment - 1;
  value &= ~(alignment - 1);

  return value;
}

#if (defined _MSC_VER)
constexpr bool is_cl_or_clang_cl = true;
#else
constexpr bool is_cl_or_clang_cl = false;
#endif

#define DO_STRINGIZE(a) #a
#define STRINGIZE(a) DO_STRINGIZE(a)

constexpr bool has_no_unique_address =
    sizeof(STRINGIZE(TUPLET_NO_UNIQUE_ADDRESS)) > 1;

struct empty {};

struct struct_with_empty {
  alignas(sizeof(int) * 2) int a;
  TUPLET_NO_UNIQUE_ADDRESS empty b;
  int c;
};

#if TUPLET_HAS_NO_UNIQUE_ADDRESS && !(defined _MSC_VER)
static_assert(offsetof(struct_with_empty, b) == (has_no_unique_address)
                  ? 0
                  : sizeof(int));
static_assert(sizeof(struct_with_empty) ==
              (has_no_unique_address ? (sizeof(int) * 2) : (sizeof(int) * 4)));

// c's offset is different with cl + [[msvc::no_unique_address]] than gcc/clang
// with [[no_unique_address]].
static_assert(offsetof(struct_with_empty, c) ==
                      (has_no_unique_address && !is_cl_or_clang_cl)
                  ? sizeof(int)
                  : 2 * sizeof(int));
#endif

template <typename Tuple>
void test_tuple_alignment() {
  Tuple t;
  const auto base_addr{reinterpret_cast<uintptr_t>(&t)};
  size_t offset{0};

  tuple_for_each(t, [&](auto& element) {
    using element_type = std::decay_t<decltype(element)>;

    const auto addr{reinterpret_cast<uintptr_t>(&element)};
    const auto element_offset{addr - base_addr};
    constexpr auto element_size{sizeof(element_type)};

    offset = align_value<alignof(element_type)>(offset);

    if (has_no_unique_address) {
      // cl with [[msvc::no_unique_address]] will not optimize out empty
      // tuple elements.
      if (is_cl_or_clang_cl || (element_type::s_size != 0)) {
        CHECK(element_offset == offset);

        if (!is_cl_or_clang_cl && !std::is_aggregate_v<element_type>) {
          // Only non-aggregates allow their padding to be used for
          // the next element.
          // cl doesn't do this either.
          offset += element_type::s_size;
        }
        else {
          offset += element_size;
        }
      }
      else {
        // gcc and clang will optimize away and the offset is 0, not the
        // offset of the previous element.
        CHECK(element_offset == 0);
      }
    }
    else {
      // Simple case; no [[no_unique_address]] affecting things.
      CHECK(element_offset == offset);
      offset += element_size;
    }
  });

  offset = align_value<alignof(Tuple)>(offset);

  CHECK(offset == sizeof(Tuple));
}

template <size_t Size, size_t Alignment>
struct alignas(Alignment) aligned_buffer_aggregate {
  static constexpr size_t s_size{Size};

  uint8_t m_buff[s_size];
};

template <size_t Alignment>
struct alignas(Alignment) aligned_buffer_aggregate<0, Alignment> {
  static constexpr size_t s_size{0};
};

template <size_t Size, size_t Alignment>
struct alignas(Alignment) aligned_buffer {
  static constexpr size_t s_size{Size};

  uint8_t m_buff[s_size];

  aligned_buffer() {}
};

template <size_t Alignment>
struct alignas(Alignment) aligned_buffer<0, Alignment> {
  static constexpr size_t s_size{0};

  aligned_buffer() {}
};

TEST_CASE("Check alignment") {
  using buff40_64_a = aligned_buffer_aggregate<40, 64>;
  using buff10_16_a = aligned_buffer_aggregate<10, 16>;
  using buff15_32_a = aligned_buffer_aggregate<15, 32>;
  using buff0_16_a = aligned_buffer_aggregate<0, 16>;
  using buff13_8_a = aligned_buffer_aggregate<13, 8>;

  using buff40_64 = aligned_buffer<40, 64>;
  using buff10_16 = aligned_buffer<10, 16>;
  using buff15_32 = aligned_buffer<15, 32>;
  using buff0_16 = aligned_buffer<0, 16>;
  using buff13_8 = aligned_buffer<13, 8>;

  test_tuple_alignment<tuplet::tuple<buff40_64_a, buff10_16_a, buff15_32_a,
                                     buff0_16_a, buff13_8_a>>();

  test_tuple_alignment<
      tuplet::tuple<buff40_64, buff10_16, buff15_32, buff0_16, buff13_8>>();

  test_tuple_alignment<tuplet::tuple<buff40_64_a, buff10_16, buff15_32_a,
                                     buff0_16, buff13_8_a>>();

  test_tuple_alignment<
      tuplet::tuple<buff40_64, buff10_16_a, buff15_32, buff0_16_a, buff13_8>>();
}

TEST_CASE("Check tuplet::apply with tuplet::tie") {
  int a = 0;
  int b = 0;
  std::string c;
  REQUIRE((a == 0 && b == 0 && c == ""));

  auto func = [](auto& x, auto& y, auto& z) {
    x = 1;
    y = 2;
    z = "Hello, world!";
  };

  apply(func, tuplet::tie(a, b, c));

  REQUIRE(a == 1);
  REQUIRE(b == 2);
  REQUIRE(c == "Hello, world!");
}
TEST_CASE("Test that for_each works") {
  std::string str;

  tuplet::tuple tup{"Hello world!", std::string("This is a test..."), 10, 20,
                    3.141592};
  static_assert(struct_pack::detail::is_trivial_tuple<decltype(tup)>);
  static_assert(std::is_aggregate_v<decltype(tup)>);
  static_assert(std::is_trivially_copyable_v<tuplet::tuple<int, double>>);
  static_assert(!std::is_trivially_copyable_v<decltype(tup)>);
  static_assert(!std::is_trivially_copyable_v<std::tuple<int, double>>);
  static_assert(!struct_pack::detail::tuple<decltype(tup)>);
  static_assert(tuplet::tuple_size_v<decltype(tup)>);
  static_assert(
      std::is_same_v<tuplet::tuple_element_t<1, decltype(tup)>, std::string>);

  tup.for_each([&]<typename T>(T& value) {
    if constexpr (std::is_fundamental_v<T>) {
      str.append(std::to_string(value)).append("\n");
    }
    else {
      str.append(value).append("\n");
    }
  });

  REQUIRE(str ==
          "Hello world!\n"       //
          "This is a test...\n"  //
          "10\n"                 //
          "20\n"                 //
          "3.141592\n");
}

TEST_CASE("Test that for_each moves values when given moved-from tuple") {
  using tuplet::tuple;

  auto tup = tuple{std::make_unique<int>(10), std::make_unique<int>(20),
                   std::make_unique<int>(30)};

  SUBCASE("Check that the tuple is valid") {
    REQUIRE(tup.all([](auto& val) {
      return bool(val);
    }));
  }
  int sum = 0;
  std::move(tup).for_each([&](auto value) {
    sum += *value;
  });

  SUBCASE("check that the tuple was moved from and the sum is 60") {
    REQUIRE(tup.all([](auto& val) {
      return bool(val) == false;
    }));
    REQUIRE(sum == 60);
  }
}

TEST_CASE("Move elements with tuple.map") {
  using tuplet::tuple;
  using std::string_view_literals::operator""sv;

  auto tup = tuple{10, 20.4, "Hello, world"sv, std::make_unique<int>(10)};

  auto expected = tuple{10, 20.4, "Hello, world"sv, std::make_unique<int>(10)};

  auto fucking_yeet = [](auto val) {
    return val;
  };

  auto result = std::move(tup).map(fucking_yeet);

  INFO("Check that the tuple was moved-from");
  REQUIRE(bool(get<3>(tup)) == false);
  REQUIRE(*get<3>(result) == *get<3>(expected));
}

TEST_CASE("Check tuple decomposition") {
  auto tup = tuplet::tuple{1, 2, std::string("Hello, world!")};

  auto [a, b, c] = tup;

  assert(a == 1);
  assert(b == 2);
  assert(c == "Hello, world!");
}

TEST_CASE("Check tuple decomposition by reference") {
  auto tup = tuplet::tuple{0, 0, std::string()};

  auto& [a, b, c] = tup;

  a = 1;
  b = 2;
  c = "Hello, world!";

  get<0>(tup);

  assert(get<0>(tup) == 1);
  assert(get<1>(tup) == 2);
  assert(get<2>(tup) == "Hello, world!");
}

TEST_CASE("Check tuple decomposition by move") {
  auto tup =
      tuplet::tuple{1, std::make_unique<int>(2), std::string("Hello, world!")};

  // Check that moving a tuple moves the elements when
  // doing a structured bind
  auto [a, b, c] = std::move(tup);

  assert(a == 1);
  assert(*b == 2);
  assert(c == "Hello, world!");
}
#endif