#include <climits>
#include <cstdint>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
using namespace struct_pack;

TEST_CASE("test monostate") {
  {
    std::monostate var, var2;
#ifdef NDEBUG
    static_assert(struct_pack::get_needed_size(std::monostate{}) == 4);
#else
    static_assert(struct_pack::get_needed_size(std::monostate{}) == 7);
#endif
    auto ret = struct_pack::serialize(var);
    auto ec = struct_pack::deserialize_to(var2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(var2 == var);
  }
}

TEST_CASE("test expected") {
  {
    tl::expected<int, struct_pack::errc> exp{42}, exp2;
    auto ret = serialize(exp);
    auto res = deserialize_to(exp2, ret.data(), ret.size());
    CHECK(!res);
    CHECK(exp2 == exp);
  }
  {
    tl::expected<std::vector<int>, struct_pack::errc> exp{
        std::vector{41, 42, 43}},
        exp2;
    auto ret = serialize(exp);
    auto res = deserialize_to(exp2, ret.data(), ret.size());
    CHECK(!res);
    CHECK(exp2 == exp);
  }
  {
    tl::expected<std::vector<int>, std::errc> exp{
        tl::unexpected{std::errc::address_in_use}},
        exp2;

    auto ret = serialize(exp);
    auto res = deserialize_to(exp2, ret.data(), ret.size());
    CHECK(!res);
    CHECK(exp2 == exp);
  }
}

TEST_CASE("testing object with containers, enum, tuple array, and pair") {
  complicated_object v = create_complicated_object();
  auto ret = serialize(v);

  complicated_object v1{};
  auto ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v.a == v1.a);

  CHECK(v == v1);

  SUBCASE("test nested object") {
    nested_object nested{2, "tom", {20, "tom"}, v};
    ret = serialize(nested);

    nested_object nested1{};
    auto ec = deserialize_to(nested1, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(nested == nested1);
  }

  SUBCASE("test get_field") {
    auto ret = serialize(v);
    complicated_object v1{};
    auto ec = deserialize_to(v1, ret.data(), ret.size());
    auto pair = get_field<complicated_object, 2>(ret.data(), ret.size());
    CHECK(!ec);
    CHECK(pair);
    CHECK(pair.value() == "hello");
    pair = get_field<complicated_object, 2>(ret);
    CHECK(pair);
    CHECK(pair.value() == "hello");
    auto pair1 = get_field<complicated_object, 14>(ret.data(), ret.size());
    CHECK(pair1);
    CHECK(pair1.value() == v.o);
    pair1 = get_field<complicated_object, 14>(ret);
    CHECK(pair1);
    CHECK(pair1.value() == v.o);

    auto res = get_field<complicated_object, 14>(ret.data(), 24);
    CHECK(!res);
    if (!res) {
      CHECK(res.error() == struct_pack::errc::no_buffer_space);
    }
  }
  SUBCASE("test get_field_to") {
    std::string res1;
    auto ec = get_field_to<complicated_object, 2>(res1, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(res1 == "hello");
    ec = get_field_to<complicated_object, 2>(res1, ret);
    CHECK(!ec);
    CHECK(res1 == "hello");
    std::pair<std::string, person> res2;
    ec = get_field_to<complicated_object, 14>(res2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(res2 == v.o);
    ec = get_field_to<complicated_object, 14>(res2, ret);
    CHECK(!ec);
    CHECK(res2 == v.o);

    auto res = get_field_to<complicated_object, 14>(res2, ret.data(), 24);

    CHECK(res == struct_pack::errc::no_buffer_space);
  }
}

TEST_CASE("testing string_view deserialize") {
  using namespace std;
  {
    auto ret = serialize("hello"sv);
    auto res = deserialize<string_view>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == "hello"sv);
  }
#ifdef TEST_IN_LITTLE_ENDIAN
  {
    auto sv = std::wstring_view(L"你好, struct pack");
    auto ret = serialize(sv);
    std::wstring str;
    auto ec = deserialize_to(str, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(str == sv);
  }
  {
    std::u32string_view sv = U"你好";
    auto ret = serialize(sv);
    auto res = deserialize<std::u32string_view>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == sv);
  }
  {
    std::u16string_view sv = u"你好";
    auto ret = serialize(sv);
    auto res = deserialize<std::u16string_view>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == sv);
  }
#endif
#if __cpp_char8_t >= 201811L
  {
    std::u8string_view sv = u8"你好";
    auto ret = serialize(sv);
    auto res = deserialize<std::u8string_view>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == sv);
  }
#endif
}

TEST_CASE("test wide string") {
  using namespace std;
  {
    auto sv = std::wstring(L"你好, struct pack");
    auto ret = serialize(sv);
    std::wstring str;
    auto ec = deserialize_to(str, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(str == sv);
  }
#if __cpp_char8_t >= 201811L
  {
    auto sv = std::u8string(u8"你好, struct pack");
    auto ret = serialize(sv);
    std::u8string str;
    auto ec = deserialize_to(str, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(str == sv);
  }
#endif
  {
    auto sv = std::u16string(u"你好, struct pack");
    auto ret = serialize(sv);
    std::u16string str;
    auto ec = deserialize_to(str, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(str == sv);
  }
  {
    auto sv = std::u32string(U"你好, struct pack");
    auto ret = serialize(sv);
    std::u32string str;
    auto ec = deserialize_to(str, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(str == sv);
  }
}

TEST_CASE("char test") {
  {
    char ch = '1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
  {
    signed char ch = '1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
  {
    unsigned char ch = '1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
  {
    wchar_t ch = L'1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
#ifdef __cpp_lib_char8_t
  {
    char8_t ch = u8'1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
#endif
  {
    char16_t ch = u'1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
  {
    char32_t ch = U'1', ch2;
    auto ret = serialize(ch);
    auto ec = deserialize_to(ch2, ret.data(), ret.size());
    REQUIRE(!ec);
    CHECK(ch == ch2);
  }
}

TEST_CASE("test deque") {
  auto raw = std::deque(1e4, 1);
  auto ret = struct_pack::serialize(raw);
  std::deque<int> res;
  auto ec = struct_pack::deserialize_to(res, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(raw == res);
}

TEST_CASE("testing long long") {
  if constexpr (sizeof(long long) == 8) {
    auto ret = serialize(-1ll);
    auto res = deserialize<long long>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == -1ll);
  }
  if constexpr (sizeof(unsigned long long) == 8) {
    auto ret = serialize(1ull);
    auto res = deserialize<unsigned long long>(ret.data(), ret.size());
    CHECK(res);
    CHECK(res.value() == 1ull);
  }
}

struct Node {
  int Type;
  bool IsArray;
  std::list<uint32_t> Dimensions;
  bool IsStructure;
  std::string TypeName;
  uint16_t Index;
  bool BoolValue;
  std::string StringValue;
  float FloatValue;
  double DoubleValue;
  int16_t Int16Value;
  int32_t Int32Value;
  int64_t Int64Value;
  uint16_t UInt16Value;
  uint32_t UInt32Value;
  uint64_t UInt64Value;
  std::list<Node> Values;  // Recursive member
  friend bool operator==(const Node& a, const Node& b) {
    return a.Type == b.Type && a.IsArray == b.IsArray &&
           a.Dimensions == b.Dimensions && a.IsStructure == b.IsStructure &&
           a.TypeName == b.TypeName && a.Index == b.Index &&
           a.BoolValue == b.BoolValue && a.StringValue == b.StringValue &&
           a.FloatValue == b.FloatValue && a.DoubleValue == b.DoubleValue &&
           a.Int16Value == b.Int16Value && a.Int32Value == b.Int32Value &&
           a.Int64Value == b.Int64Value && a.UInt16Value == b.UInt16Value &&
           a.UInt32Value == b.UInt32Value && a.UInt64Value == b.UInt64Value &&
           a.Values == b.Values;
  }
};

TEST_CASE("testing recursive type") {
  Node node = {1,
               false,
               {1, 2, 3},
               true,
               "Hello",
               1141,
               true,
               "1",
               1.2,
               1.23,
               1,
               2,
               3,
               4,
               5,
               6,
               {
                   Node{1,
                        false,
                        {1, 2, 3},
                        true,
                        "Hello",
                        1141,
                        true,
                        "1",
                        1.2,
                        1.23,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        {}},
                   Node{1,
                        false,
                        {1, 2, 3},
                        true,
                        "Hello",
                        1141,
                        true,
                        "1",
                        1.2,
                        1.23,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        {}},
                   Node{1,
                        false,
                        {1, 2, 3},
                        true,
                        "Hello",
                        1141,
                        true,
                        "1",
                        1.2,
                        1.23,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        {}},
                   Node{1,
                        false,
                        {1, 2, 3},
                        true,
                        "Hello",
                        1141,
                        true,
                        "1",
                        1.2,
                        1.23,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        {Node{1,
                              false,
                              {1, 2, 3},
                              true,
                              "Hello",
                              1141,
                              true,
                              "1",
                              1.2,
                              1.23,
                              1,
                              2,
                              3,
                              4,
                              5,
                              6,
                              {Node{}}}}},
               }};
  auto buffer = struct_pack::serialize(node);
  auto node2 = struct_pack::deserialize<Node>(buffer);
  CHECK(node2.value() == node);
}
namespace trival_test {
struct B {
  double a;
  struct C {
    int a;
    struct D {
      short a;
      struct E {
        char a;
        int b;
        char c;
        int d;
      } b;
      double d;
      char c;
    } b;
    char d;
    char c;
  } b;
  std::array<int64_t, 10000> d;
  char c;
} b;
struct A_v1 {
  char a;
  B b;
  char c;
};

struct A_v2 {
  char a;
  struct B {
    double a;
    struct C {
      int a;
      struct D {
        short a;
        struct E {
          char a;
          int b;
          char c;
          struct_pack::trivial_view<int> d;
        } b;
        struct_pack::trivial_view<double> d;
        char c;
      } b;
      struct_pack::trivial_view<char> d;
      char c;
    } b;
    struct_pack::trivial_view<std::array<int64_t, 10000>> d;
    char c;
  } b;
  char c;
};

struct A_v3 {
  char a;
  struct_pack::trivial_view<B> b;
  char c;
};
}  // namespace trival_test

bool test_equal(const trival_test::A_v1& v1, const trival_test::A_v1& v2) {
  return v1.a == v2.a && v1.c == v2.c &&
         (v1.b.a == v2.b.a && v1.b.c == v2.b.c && v1.b.d == v2.b.d &&
          (v1.b.b.a == v2.b.b.a && v1.b.b.c == v2.b.b.c &&
           v1.b.b.d == v2.b.b.d &&
           (v1.b.b.b.a == v2.b.b.b.a && v1.b.b.b.c == v2.b.b.b.c &&
            v1.b.b.b.d == v2.b.b.b.d &&
            (v1.b.b.b.b.a == v2.b.b.b.b.a && v1.b.b.b.b.c == v2.b.b.b.b.c &&
             v1.b.b.b.b.d == v2.b.b.b.b.d))));
}

bool test_equal(const trival_test::A_v2& v1, const trival_test::A_v2& v2) {
  return v1.a == v2.a && v1.c == v2.c &&
         (v1.b.a == v2.b.a && v1.b.c == v2.b.c &&
          v1.b.d.get() == v2.b.d.get() &&
          (v1.b.b.a == v2.b.b.a && v1.b.b.c == v2.b.b.c &&
           v1.b.b.d.get() == v2.b.b.d.get() &&
           (v1.b.b.b.a == v2.b.b.b.a && v1.b.b.b.c == v2.b.b.b.c &&
            v1.b.b.b.d.get() == v2.b.b.b.d.get() &&
            (v1.b.b.b.b.a == v2.b.b.b.b.a && v1.b.b.b.b.c == v2.b.b.b.b.c &&
             v1.b.b.b.b.d.get() == v2.b.b.b.b.d.get()))));
}

bool test_equal(const trival_test::A_v3& v1, const trival_test::A_v3& v2) {
  return v1.a == v2.a && v1.c == v2.c &&
         (v1.b->a == v2.b->a && v1.b->c == v2.b->c && v1.b->d == v2.b->d &&
          (v1.b->b.a == v2.b->b.a && v1.b->b.c == v2.b->b.c &&
           v1.b->b.d == v2.b->b.d &&
           (v1.b->b.b.a == v2.b->b.b.a && v1.b->b.b.c == v2.b->b.b.c &&
            v1.b->b.b.d == v2.b->b.b.d &&
            (v1.b->b.b.b.a == v2.b->b.b.b.a && v1.b->b.b.b.c == v2.b->b.b.b.c &&
             v1.b->b.b.b.d == v2.b->b.b.b.d))));
}
#ifdef TEST_IN_LITTLE_ENDIAN
TEST_CASE("testing trivial_view type") {
  trival_test::A_v1 a_v1 = {
      'A',
      {123.12,
       {1232132,
        {12331,
         {'G', 114515, 'A', 104},

         104.1423,
         'B'},
        'A',
        'C'},
       {123, 3214, 2134, 3214, 1324, 3214890, 184320, 832140, 321984},
       'D'},
      'E'};
  trival_test::A_v2 a_v2 = {
      'A',
      {123.12,
       {1232132,
        {12331, {'G', 114515, 'A', a_v1.b.b.b.b.d}, a_v1.b.b.b.d, 'B'},
        a_v1.b.b.d,
        'C'},
       a_v1.b.d,
       'D'},
      'E'};
  trival_test::A_v3 a_v3 = {'A', a_v1.b, 'E'};
  {
    std::vector<char> buffer = struct_pack::serialize(a_v1);
    auto result = struct_pack::deserialize<trival_test::A_v1>(buffer);
    CHECK(test_equal(result.value(), a_v1));
  }
  {
    auto buffer = struct_pack::serialize(a_v2);
    auto result = struct_pack::deserialize<trival_test::A_v1>(buffer);
    CHECK(test_equal(result.value(), a_v1));
  }
  {
    auto buffer = struct_pack::serialize(a_v3);
    auto result = struct_pack::deserialize<trival_test::A_v1>(buffer);
    CHECK(test_equal(result.value(), a_v1));
  }
  {
    std::vector<char> buffer = struct_pack::serialize(a_v1);
    auto result = struct_pack::deserialize<trival_test::A_v2>(buffer);
    CHECK(test_equal(result.value(), a_v2));
  }
  {
    auto buffer = struct_pack::serialize(a_v2);
    auto result = struct_pack::deserialize<trival_test::A_v2>(buffer);
    CHECK(test_equal(result.value(), a_v2));
  }
  {
    auto buffer = struct_pack::serialize(a_v3);
    auto result = struct_pack::deserialize<trival_test::A_v2>(buffer);
    CHECK(test_equal(result.value(), a_v2));
  }
  {
    std::vector<char> buffer = struct_pack::serialize(a_v1);
    auto result = struct_pack::deserialize<trival_test::A_v3>(buffer);
    CHECK(test_equal(result.value(), a_v3));
  }
  {
    auto buffer = struct_pack::serialize(a_v2);
    auto result = struct_pack::deserialize<trival_test::A_v3>(buffer);
    CHECK(test_equal(result.value(), a_v3));
  }
  {
    auto buffer = struct_pack::serialize(a_v3);
    auto result = struct_pack::deserialize<trival_test::A_v3>(buffer);
    CHECK(test_equal(result.value(), a_v3));
  }
};
#endif

struct long_test {
  long hi = -1;
  unsigned long hi2 = ULONG_MAX;
};

struct long_test2 {
  int64_t hi = -1;
  uint64_t hi2 = UINT64_MAX;
};

struct long_test3 {
  int32_t hi = -1;
  uint32_t hi2 = UINT32_MAX;
};

TEST_CASE("testing long type") {
  if constexpr (sizeof(long) == 8) {
    auto buffer = struct_pack::serialize(long_test{});
    auto result = struct_pack::deserialize<long_test2>(buffer);
    CHECK(result.has_value());
    CHECK(result->hi == -1);
    CHECK(result->hi2 == ULONG_MAX);
  }
  else if constexpr (sizeof(long) == 4) {
    auto buffer = struct_pack::serialize(long_test{});
    auto result = struct_pack::deserialize<long_test3>(buffer);
    CHECK(result.has_value());
    CHECK(result->hi == -1);
    CHECK(result->hi2 == ULONG_MAX);
  }
}