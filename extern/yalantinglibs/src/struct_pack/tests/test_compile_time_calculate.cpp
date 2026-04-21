#include <iostream>
#include <memory>
#include <type_traits>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
#include "ylt/struct_pack/reflection.hpp"
#include "ylt/struct_pack/type_calculate.hpp"
#if __cplusplus >= 202002L
#include <ylt/struct_pack/tuple.hpp>
#endif

using namespace struct_pack;

struct bug_member_count_struct1 {
  int i;
  struct hello {
    std::optional<int> j;
  } hi;
  double k = 3;
};

struct bug_member_count_struct2 : bug_member_count_struct1 {};

template <>
constexpr std::size_t struct_pack::members_count<bug_member_count_struct2> = 3;

TEST_CASE("test members_count") {
  {
    using t = bug_member_count_struct1;
    t b{};
    auto res = struct_pack::serialize(b);
    auto ret = struct_pack::deserialize<t>(res.data(), res.size());
    CHECK(ret);
  }
  {
    using t = bug_member_count_struct2;
    t b{};
    auto res = struct_pack::serialize(b);
    auto ret = struct_pack::deserialize<t>(res.data(), res.size());
    CHECK(ret);
  }
}
#if __cplusplus >= 202002L
struct type_calculate_test_1 {
  tuplet::tuple<
      std::pair<std::map<int, std::string>, std::vector<std::list<int>>>,
      std::set<int>, std::array<int64_t, 64>, std::optional<int>>
      a;
};
struct type_calculate_test_2 {
  struct {
    tuplet::tuple<std::unordered_map<int, std::string>,
                  std::deque<std::vector<int>>>
        a;
    std::multiset<int> b;
    int64_t c[64];
    std::optional<int> d;
  } e;
};
#endif
#ifndef NDEBUG
TEST_CASE("test hash conflict detected") {
  int32_t value = 42;
  auto ret = serialize(value);
  auto fake_hash = struct_pack::get_type_code<float>() | 0b1;
  if constexpr (!detail::is_system_little_endian) {
    fake_hash = detail::bswap32(fake_hash);
  }
  memcpy(ret.data(), &fake_hash, sizeof(fake_hash));
  auto res = deserialize<float>(ret);
  CHECK(!res);
  CHECK(res.error() == struct_pack::errc::hash_conflict);
}
#endif

struct type_calculate_test_3 {
  struct {
    std::pair<std::unordered_map<int, std::string>,
              std::deque<std::vector<double>>>
        a;
    std::multiset<int> b;
    int64_t c[64];
    std::optional<int> d;
  } e;
};

TEST_CASE("type calculate") {
  static_assert(std::is_trivially_copyable<struct_pack::compatible<int>>::value,
                "must be true");

  {
    static_assert(get_type_code<std::vector<int>>() !=
                      get_type_code<std::vector<float>>(),
                  "vector<T> with different T should get different MD5");
    CHECK(!deserialize<std::vector<int>>(serialize(std::vector<float>{})));
  }
  {
    static_assert(
        get_type_code<std::deque<int>>() != get_type_code<std::deque<float>>(),
        "deque<T> with different T should get different MD5");
    CHECK(!deserialize<std::deque<int>>(serialize(std::deque<float>{})));
  }
  {
    static_assert(
        get_type_code<std::list<int>>() != get_type_code<std::list<float>>(),
        "list<T> with different T should get different MD5");
    CHECK(!deserialize<std::list<int>>(serialize(std::list<float>{})));
  }
  {
    static_assert(
        get_type_code<std::set<int>>() != get_type_code<std::set<float>>(),
        "set<T> with different T should get different MD5");
    CHECK(!deserialize<std::set<int>>(serialize(std::set<float>{})));
  }
  {
    static_assert(get_type_code<std::map<int, std::string>>() !=
                      get_type_code<std::map<float, std::string>>(),
                  "map<T1,T2> with different T1 should get different MD5");
    CHECK(!deserialize<std::map<int, std::string>>(
        serialize(std::map<float, std::string>{})));
  }

  {
    static_assert(get_type_code<std::map<std::string, int>>() !=
                      get_type_code<std::map<std::string, float>>(),
                  "map<T1,T2> with different T2 should get different MD5");
    CHECK(!deserialize<std::map<std::string, int>>(
        serialize(std::map<std::string, float>{})));
  }
  {
    static_assert(
        get_type_code<std::unordered_map<int, std::string>>() !=
            get_type_code<std::unordered_map<float, std::string>>(),
        "unordered_map<T1,T2> with different T1 should get different MD5");
    CHECK(!deserialize<std::unordered_map<int, std::string>>(
        serialize(std::unordered_map<float, std::string>{})));
  }
  {
    static_assert(
        get_type_code<std::unordered_map<std::string, int>>() !=
            get_type_code<std::unordered_map<std::string, float>>(),
        "unordered_map<T1,T2> with different T2 should get different MD5");
    CHECK(!deserialize<std::unordered_map<std::string, int>>(
        serialize(std::unordered_map<std::string, float>{})));
  }
  {
    static_assert(get_type_code<std::unordered_set<int>>() !=
                      get_type_code<std::unordered_set<float>>(),
                  "unordered_set<T> with different T should get different MD5");
    CHECK(!deserialize<std::unordered_set<int>>(
        serialize(std::unordered_set<float>{})));
  }
  {
    static_assert(get_type_code<std::array<int, 5>>() !=
                      get_type_code<std::array<float, 5>>(),
                  "array<T,sz> with different T should get different MD5");
    CHECK(!deserialize<std::array<int, 5>>(serialize(std::array<float, 5>{})));
  }
  {
    static_assert(get_type_code<std::array<int, 5>>() !=
                      get_type_code<std::array<int, 6>>(),
                  "array<T,sz> with different sz should get different MD5");
    CHECK(!deserialize<std::array<int, 5>>(serialize(std::array<int, 6>{})));
  }
  {
    static_assert(get_type_code<int[5]>() != get_type_code<float[5]>(),
                  "T[sz] with different T should get different MD5");
    int ar[5] = {};
    float ar2[5] = {};
    CHECK(deserialize_to(ar2, serialize(ar)));
  }
  {
    static_assert(get_type_code<int[5]>() != get_type_code<int[6]>(),
                  "T[sz] with different sz should get different MD5");
    int ar[5] = {};
    int ar2[6] = {};
    CHECK(deserialize_to(ar2, serialize(ar)));
  }
  {
    static_assert(get_type_code<std::optional<int>>() !=
                      get_type_code<std::optional<float>>(),
                  "optional<T> with different T should get different MD5");
    CHECK(!deserialize<std::array<int, 5>>(serialize(std::array<int, 6>{})));
  }
  {
    static_assert(
        get_type_code<int, float, int>() != get_type_code<int, int, int>(),
        "T... with different T... should get different MD5");
    CHECK(!deserialize<int, int>(serialize(1, 2, 3)));
  }
  {
    static_assert(get_type_code<std::tuple<int, float, int>>() !=
                      get_type_code<std::tuple<int, int, int>>(),
                  "tuple<T...> with different T... should get different MD5");
    CHECK(!deserialize<int, int>(serialize(1, 2, 3)));
  }
  {
    static_assert(get_type_code<std::tuple<int, float, int>>() ==
                      get_type_code<int, float, int>(),
                  "tuple<T...> and T... should get same MD5");
    CHECK(deserialize<std::tuple<int, float, int>>(serialize(1, 2.0f, 3)));
  }
  {
    static_assert(get_type_code<std::pair<int, int>>() !=
                      get_type_code<std::pair<float, int>>(),
                  "pair<T1,T2> with different T1 should get different MD5");
    CHECK(!deserialize<std::pair<int, int>>(serialize(std::pair{1.3f, 1})));
  }
  {
    static_assert(get_type_code<std::pair<int, float>>() !=
                      get_type_code<std::pair<int, int>>(),
                  "pair<T1,T2> with different T2 should get different MD5");
    CHECK(!deserialize<std::pair<int, float>>(serialize(std::pair{1, 1})));
  }
  {
    static_assert(
        get_type_code<complicated_object>() != get_type_code<person>(),
        "class{T...} with different T... should get different MD5");
    CHECK(!deserialize<complicated_object>(serialize(person{})));
  }
  {
    static_assert(
        get_type_code<compatible<int>>() == get_type_code<compatible<float>>(),
        "compatible<T> with different T should get same MD5");
    CHECK(deserialize<compatible<int>>(serialize(compatible<float>{1})));
  }
  {
    static_assert(
        get_type_code<std::list<int>>() == get_type_code<std::vector<int>>(),
        "different class accord with container concept should get same MD5");
    CHECK(deserialize<std::list<int>>(serialize(std::vector<int>{})));
  }
  {
    static_assert(
        get_type_code<std::deque<int>>() == get_type_code<std::vector<int>>(),
        "different class accord with container concept should get same MD5");
    CHECK(deserialize<std::deque<int>>(serialize(std::vector<int>{})));
  }
  {
    static_assert(
        get_type_code<std::deque<int>>() == get_type_code<std::list<int>>(),
        "different class accord with container concept should get same MD5");
    CHECK(deserialize<std::deque<int>>(serialize(std::list<int>{})));
  }
  {
    static_assert(
        get_type_code<std::array<int, 5>>() == get_type_code<int[5]>(),
        "different class accord with array concept should get same MD5");
    int ar[5] = {};
    CHECK(deserialize<std::array<int, 5>>(serialize(ar)));
  }
  {
    static_assert(
        get_type_code<std::map<int, int>>() ==
            get_type_code<std::unordered_map<int, int>>(),
        "different class accord with map concept should get same MD5");
    CHECK(deserialize<std::map<int, int>>(
        serialize(std::unordered_map<int, int>{})));
  }
  {
    static_assert(
        get_type_code<std::set<int>>() ==
            get_type_code<std::unordered_set<int>>(),
        "different class accord with set concept should get same MD5");
    CHECK(deserialize<std::set<int>>(serialize(std::unordered_set<int>{})));
  }
  {
    static_assert(
        get_type_code<std::pair<int, std::string>>() == get_type_code<person>(),
        "different class accord with trival_class concept should "
        "get same MD5");
    CHECK(deserialize<std::pair<int, std::string>>(serialize(person{})));
  }
#if __cplusplus >= 202002L
  {
    static_assert(get_type_code<tuplet::tuple<int, std::string>>() ==
                      get_type_code<person>(),
                  "different class accord with trival_class concept should "
                  "get same MD5");
    CHECK(deserialize<tuplet::tuple<int, std::string>>(serialize(person{})));
  }
  {
    static_assert(get_type_code<std::pair<int, std::string>>() ==
                      get_type_code<tuplet::tuple<int, std::string>>(),
                  "different class accord with trival_class concept should "
                  "get same MD5");
    CHECK(deserialize<std::pair<int, std::string>>(
        serialize(tuplet::tuple<int, std::string>{})));
  }
  {
    static_assert(get_type_code<std::tuple<int, float>>() !=
                      get_type_code<std::pair<int, float>>(),
                  "tuple is not trival copyable, we got different layout");
    CHECK(!deserialize<std::pair<int, float>>(
        serialize(std::tuple<int, float>{})));
  }
  {
    static_assert(get_type_code<std::tuple<int, std::string>>() ==
                      get_type_code<person>(),
                  "same type should "
                  "get same MD5");
    CHECK(deserialize<std::tuple<int, std::string>>(serialize(person{})));
  }
  {
    static_assert(get_type_code<std::pair<int, std::string>>() ==
                      get_type_code<std::tuple<int, std::string>>(),
                  "same type should "
                  "get same MD5");
    CHECK(deserialize<std::pair<int, std::string>>(
        serialize(std::tuple<int, std::string>{})));
  }
  {
    static_assert(get_type_code<type_calculate_test_1>() ==
                      get_type_code<type_calculate_test_2>(),
                  "struct type_calculate_test_1 && type_calculate_test_2 "
                  "should get the "
                  "same MD5");
    CHECK(
        deserialize<type_calculate_test_1>(serialize(type_calculate_test_2{})));
  }

  {
    static_assert(get_type_code<type_calculate_test_1>() !=
                      get_type_code<type_calculate_test_3>(),
                  "struct type_calculate_test_1 && type_calculate_test_3 "
                  "should get the "
                  "different MD5");
    CHECK(!deserialize<type_calculate_test_1>(
        serialize(type_calculate_test_3{})));
  }

  {
    static_assert(get_type_code<int>() != get_type_code<std::tuple<int>>(),
                  "T & tuple<T> should get different MD5");
    CHECK(!deserialize<int>(serialize(std::tuple<int>{})));
  }

  {
    static_assert(get_type_code<int>() != get_type_code<tuplet::tuple<int>>(),
                  "T & tuple_::tuple<T> should get different MD5");
    CHECK(!deserialize<int>(serialize(tuplet::tuple<int>{})));
  }

#endif
  {
    static_assert(get_type_code<std::tuple<int>>() !=
                      get_type_code<std::tuple<std::tuple<int>>>(),
                  "tuple<T> & tuple<tuple<T>> should get different MD5");
    CHECK(!deserialize<std::tuple<int>>(
        serialize(std::tuple<std::tuple<int>>{})));
  }

  {
    static_assert(
        get_type_code<std::tuple<int>, int>() !=
            get_type_code<std::tuple<int, int>>(),
        "tuple<tuple<T1>,T2> & tuple<tuple<T1,T2>> should get different MD5");
    CHECK(!deserialize<std::tuple<std::tuple<int>, int>>(
        serialize(std::tuple<std::tuple<int, int>>{})));
  }
}

struct foo_trivial {
  int a;
  double b;
};

struct foo_trivial_with_ID {
  int a;
  double b;
  constexpr static std::size_t struct_pack_id = 1;
};
struct foo_trivial_with_ID2 {
  int a;
  double b;
  constexpr static int struct_pack_id = 2;
};
struct bar_trivial_with_ID2 {
  uint32_t a;
  double b;
  constexpr static int struct_pack_id = 2;
};

struct foo {
  std::vector<int> a;
  std::list<foo> b;
};
struct bar;
struct bar {
  std::vector<int> a;
  std::vector<bar> b;
};

struct foo_with_ID {
  std::vector<int> a;
  std::list<foo_with_ID> b;
  constexpr static std::size_t struct_pack_id = 0;
};

struct bar_with_ID {
  std::vector<int> a;
  std::map<int, bar_with_ID> b;
  constexpr static std::size_t struct_pack_id = 0;
};

struct foo_with_ID1 {
  std::vector<int> a;
  std::list<foo_with_ID1> b;
  constexpr static std::size_t struct_pack_id = 1;
};

struct bar_with_ID1 {
  std::vector<int> a;
  std::vector<bar_with_ID1> b;
  constexpr static std::size_t struct_pack_id = 1;
};

struct bar_with_ID2 {
  std::vector<int> a;
  std::vector<bar_with_ID2> b;
};
constexpr int struct_pack_id(bar_with_ID2*) { return 11; }

TEST_CASE("test user defined ID") {
  static_assert(struct_pack::detail::has_user_defined_id_ADL<bar_with_ID2>);
  {
    static_assert(
        struct_pack::detail::has_user_defined_id<foo_trivial_with_ID>);
    static_assert(struct_pack::get_type_literal<foo_trivial>() !=
                  struct_pack::get_type_literal<foo_trivial_with_ID>());
    static_assert(struct_pack::get_type_literal<foo_trivial_with_ID2>() !=
                  struct_pack::get_type_literal<foo_trivial_with_ID>());
    static_assert(struct_pack::get_type_literal<foo_trivial_with_ID2>() !=
                  struct_pack::get_type_literal<bar_trivial_with_ID2>());
  }
  {
    static_assert(struct_pack::detail::has_user_defined_id<foo_with_ID>);
    static_assert(struct_pack::get_type_literal<foo>() !=
                  struct_pack::get_type_literal<foo_with_ID>());
    static_assert(struct_pack::get_type_literal<foo_with_ID>() !=
                  struct_pack::get_type_literal<foo_with_ID1>());
    auto a = struct_pack::get_type_literal<foo_with_ID1>();
    auto b = struct_pack::get_type_literal<bar_with_ID1>();
    static_assert(struct_pack::get_type_literal<foo_with_ID1>() ==
                  struct_pack::get_type_literal<bar_with_ID1>());
    static_assert(struct_pack::get_type_literal<foo_with_ID>() !=
                  struct_pack::get_type_literal<bar_with_ID>());
    static_assert(struct_pack::get_type_literal<bar_with_ID2>() !=
                  struct_pack::get_type_literal<bar_with_ID1>());
  }
}
template <typename T>
struct test_has_container0 {
  T hi;
};

template <typename T>
struct test_has_container {
  int a;
  double b;
  std::array<std::tuple<std::pair<int, test_has_container0<T>>, int, int>, 10>
      c;
};

TEST_CASE("test has container") {
  static_assert(
      !struct_pack::detail::check_if_has_container<test_has_container<int>>());
  static_assert(struct_pack::detail::check_if_has_container<
                test_has_container<std::vector<int>>>());
  static_assert(struct_pack::detail::check_if_has_container<
                test_has_container<std::string>>());
  static_assert(struct_pack::detail::check_if_has_container<
                test_has_container<std::map<int, int>>>());
  static_assert(struct_pack::detail::check_if_has_container<
                test_has_container<std::set<int>>>());
}

struct fast_varint_example_ct1 {
  var_int32_t a;
  var_int64_t b;
  var_uint32_t c;
  var_uint64_t d;
  static constexpr struct_pack::sp_config struct_pack_config = USE_FAST_VARINT;
};

struct fast_varint_example_ct2 {
  var_int32_t a;
  var_int64_t b;
  var_uint32_t c;
  var_uint64_t d;
};
constexpr auto set_sp_config(fast_varint_example_ct2*) {
  return struct_pack::sp_config::USE_FAST_VARINT;
}
struct fast_varint_example_ct3 {
  int32_t a;
  int64_t b;
  uint32_t c;
  uint64_t d;
  static constexpr auto struct_pack_config =
      USE_FAST_VARINT | ENCODING_WITH_VARINT;
};

struct fast_varint_example_ct4 {
  int32_t a;
  int64_t b;
  uint32_t c;
  uint64_t d;
  static constexpr auto struct_pack_config = ENCODING_WITH_VARINT;
};

TEST_CASE("test fast varint tag") {
  using namespace struct_pack::detail;
  static_assert(user_defined_config<fast_varint_example_ct1>);
  static_assert(user_defined_config_by_ADL<fast_varint_example_ct2>);
  static_assert(user_defined_config<fast_varint_example_ct3>);
  constexpr auto type_info1 =
      struct_pack::get_type_literal<fast_varint_example_ct1>();
  constexpr auto type_info2 =
      struct_pack::get_type_literal<fast_varint_example_ct2>();
  constexpr auto type_info3 =
      struct_pack::get_type_literal<fast_varint_example_ct3>();
  constexpr auto type_info4 =
      struct_pack::get_type_literal<fast_varint_example_ct4>();
  constexpr auto fast_varint_info = struct_pack::string_literal<char, 6>{
      {(char)type_id::struct_t, (char)type_id::fast_vint32_t,
       (char)type_id::fast_vint64_t, (char)type_id::fast_vuint32_t,
       (char)type_id::fast_vuint64_t, (char)type_id::type_end_flag}};
  constexpr auto varint_info = struct_pack::string_literal<char, 6>{
      {(char)type_id::struct_t, (char)type_id::vint32_t,
       (char)type_id::vint64_t, (char)type_id::vuint32_t,
       (char)type_id::vuint64_t, (char)type_id::type_end_flag}};
  static_assert(type_info1 == type_info2);
  static_assert(type_info1 == type_info3);
  static_assert(type_info3 != type_info4);
  static_assert(type_info1 == fast_varint_info);
}

struct int_key {
  int kKey;
  YLT_REFL(int_key, kKey);
};

struct int_key2 {
  int kKey;
};

TEST_CASE("test const in trivial serializable") {
  static_assert(!struct_pack::detail::is_trivial_serializable<
                std::pair<int_key const, int>>::value);
  static_assert(!struct_pack::detail::is_trivial_serializable<
                std::pair<int_key, int>>::value);
  static_assert(struct_pack::detail::is_trivial_serializable<
                std::pair<int_key2 const, int>>::value);
  static_assert(struct_pack::detail::is_trivial_serializable<
                std::pair<int_key2, int>>::value);
}