#include <cstdint>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
#include "ylt/struct_pack/reflection.hpp"
#include "ylt/struct_pack/varint.hpp"

using namespace struct_pack;

struct fast_varint_example_1 {
  var_int32_t a;
  var_int32_t b;
  var_int32_t c;
  var_int32_t d;
  bool operator==(const fast_varint_example_1& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varint_example_2 {
  var_int32_t a;
  var_int64_t b;
  var_int32_t c;
  var_int64_t d;
  bool operator==(const fast_varint_example_2& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varint_example_3 {
  var_int64_t a;
  var_int64_t b;
  var_int64_t c;
  var_int64_t d;
  bool operator==(const fast_varint_example_3& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varint_example_4 {
  var_int64_t a;
  var_int64_t b;
  var_int64_t c;
  var_int64_t d;
  var_int64_t e;
  var_int64_t f;
  var_int64_t g;
  bool operator==(const fast_varint_example_4& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e &&
           f == o.f && g == o.g;
  }
};

struct fast_varint_example_5 {
  var_int64_t a;
  var_int64_t b;
  var_int64_t c;
  var_int64_t d;
  var_int64_t e;
  var_int64_t f;
  bool operator==(const fast_varint_example_5& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e && f == o.f;
  }
};

struct fast_varuint_example_1 {
  var_uint64_t a;
  var_uint64_t b;
  var_uint64_t c;
  var_uint64_t d;
  bool operator==(const fast_varuint_example_1& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varuint_example_2 {
  var_uint32_t a;
  var_uint32_t b;
  var_uint32_t c;
  var_uint32_t d;
  bool operator==(const fast_varuint_example_2& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varuint_example_3 {
  var_uint32_t a;
  var_uint64_t b;
  var_uint32_t c;
  var_uint64_t d;
  bool operator==(const fast_varuint_example_3& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
};

struct fast_varmixedint_example_1 {
  var_int32_t a;
  var_uint64_t b;
  var_uint32_t c;
  var_int64_t d;
  bool operator==(const fast_varmixedint_example_1& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT;
};

constexpr sp_config set_sp_config(fast_varint_example_1*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varint_example_2*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varint_example_3*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varint_example_4*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varint_example_5*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varuint_example_1*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varuint_example_2*) {
  return USE_FAST_VARINT;
}

constexpr sp_config set_sp_config(fast_varuint_example_3*) {
  return USE_FAST_VARINT;
}

TEST_CASE("fast varint test 0") {
  fast_varint_example_1 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varint test 1") {
  fast_varint_example_1 o{1, -1, 127, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 2") {
  fast_varint_example_1 o{1, -127, 127, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 3") {
  fast_varint_example_1 o{1, -127, 0, 127};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 4") {
  fast_varint_example_1 o{1, 0, -127, 127};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 5") {
  fast_varint_example_1 o{0, 1, -127, 127};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 6") {
  fast_varint_example_1 o{1, -1, 128, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 7);
  return;
}

TEST_CASE("fast varint test 7") {
  fast_varint_example_1 o{1, -128, 1, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint test 8") {
  fast_varint_example_1 o{32767, -32767, 1123, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 7);
  return;
}

TEST_CASE("fast varint test 8") {
  fast_varint_example_1 o{0, 0, 1123, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varint test 9") {
  fast_varint_example_1 o{32768, 1, 0, 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result->a == o.a);
  CHECK(result->b == o.b);
  CHECK(result->c == o.c);
  CHECK(result->d == o.d);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint test 10") {
  fast_varint_example_1 o{-32768, 1, 0, 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 7);
  return;
}

TEST_CASE("fast varint test 12") {
  fast_varint_example_1 o{-83219132, 114514, 0, 2123321213};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint test 12") {
  fast_varint_example_1 o{INT_MIN, INT_MAX, 0, INT_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint2 test 1") {
  fast_varint_example_2 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varint2 test 2") {
  fast_varint_example_2 o{0, 1, -1, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varint2 test 2") {
  fast_varint_example_2 o{0, 127, -128, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varint2 test 3") {
  fast_varint_example_2 o{0, 128, -127, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varint2 test 4") {
  fast_varint_example_2 o{0, INT16_MAX, INT16_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varint2 test 5") {
  fast_varint_example_2 o{0, INT16_MAX + 1, INT16_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varint2 test 6") {
  fast_varint_example_2 o{0, INT16_MAX, INT16_MIN - 1, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varint2 test 7") {
  fast_varint_example_2 o{0, INT32_MAX, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varint2 test 8") {
  fast_varint_example_2 o{0, INT32_MAX, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varint2 test 9") {
  fast_varint_example_2 o{0, INT32_MAX + 1ll, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint2 test 10") {
  fast_varint_example_2 o{0, INT32_MIN - 1ll, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint2 test 11") {
  fast_varint_example_2 o{0, INT64_MIN, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint2 test 12") {
  fast_varint_example_2 o{0, INT64_MAX, INT32_MIN, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varint3 test 1") {
  fast_varint_example_3 o{0, INT64_MAX, 0, INT64_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_3>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varint3 test 2") {
  fast_varint_example_3 o{0, INT32_MAX, 0, INT32_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_3>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varint4 test 1") {
  fast_varint_example_4 o{0, 0, 0, 0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_4>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 2);
  return;
}

TEST_CASE("fast varint4 test 2") {
  fast_varint_example_4 o{0, 0, 0, 0, 0, 0, 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_4>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varint4 test 3") {
  fast_varint_example_4 o{0, 0, 0, 0, 0, 0, INT16_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_4>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varint4 test 4") {
  fast_varint_example_4 o{0, 0, 0, 0, 0, 0, INT32_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_4>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 6);
  return;
}

TEST_CASE("fast varint4 test 5") {
  fast_varint_example_4 o{0, 0, 0, 0, 0, 0, INT64_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_4>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 10);
  return;
}

TEST_CASE("fast varint5 test 1") {
  fast_varint_example_5 o{0, 0, 0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_5>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varint5 test 2") {
  fast_varint_example_5 o{0, 0, 0, 0, 0, INT8_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_5>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 2);
  return;
}

TEST_CASE("fast varint5 test 3") {
  fast_varint_example_5 o{0, 0, 0, 0, 0, INT16_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_5>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varint5 test 4") {
  fast_varint_example_5 o{0, 0, 0, 0, 0, INT32_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_5>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varint5 test 5") {
  fast_varint_example_5 o{0, 0, 0, 0, 0, INT64_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varint_example_5>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varuint1 test 1") {
  fast_varuint_example_1 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varuint1 test 2") {
  fast_varuint_example_1 o{UINT8_MAX, UINT8_MAX, 0, UINT8_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 4);
  return;
}

TEST_CASE("fast varuint1 test 3") {
  fast_varuint_example_1 o{UINT8_MAX, UINT8_MAX + 1, 0, UINT8_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 7);
  return;
}

TEST_CASE("fast varuint1 test 4") {
  fast_varuint_example_1 o{UINT16_MAX, UINT16_MAX, 0, UINT16_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 7);
  return;
}

TEST_CASE("fast varuint1 test 5") {
  fast_varuint_example_1 o{UINT16_MAX, UINT16_MAX, 0, UINT16_MAX + 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varuint1 test 6") {
  fast_varuint_example_1 o{UINT32_MAX, UINT32_MAX, 0, UINT32_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varuint1 test 7") {
  fast_varuint_example_1 o{UINT32_MAX + 1ull, UINT32_MAX, 0, UINT32_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}

TEST_CASE("fast varuint1 test 8") {
  fast_varuint_example_1 o{UINT64_MAX, UINT64_MAX, 0, UINT64_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}

TEST_CASE("fast varuint2 test 1") {
  fast_varuint_example_2 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varuint2 test 2") {
  fast_varuint_example_2 o{0, 0, 0, UINT8_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 2);
  return;
}

TEST_CASE("fast varuint2 test 3") {
  fast_varuint_example_2 o{0, 0, 0, UINT8_MAX + 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varuint2 test 4") {
  fast_varuint_example_2 o{0, 0, 0, UINT16_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 3);
  return;
}

TEST_CASE("fast varuint2 test 5") {
  fast_varuint_example_2 o{0, 0, 0, UINT16_MAX + 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}
TEST_CASE("fast varuint2 test 6") {
  fast_varuint_example_2 o{0, 0, 0, UINT32_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varuint3 test 1") {
  fast_varuint_example_3 o{0, 0, UINT32_MAX, UINT32_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_3>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varuint3 test 2") {
  fast_varuint_example_3 o{0, 0, UINT32_MAX, UINT32_MAX + 1ull};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_3>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varuint3 test 3") {
  fast_varuint_example_3 o{0, 0, UINT32_MAX, UINT64_MAX};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varuint_example_3>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 13);
  return;
}

TEST_CASE("fast varmixedint1 test 1") {
  fast_varmixedint_example_1 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varmixedint1 test 2") {
  fast_varmixedint_example_1 o{INT8_MIN, UINT8_MAX, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varmixedint1 test 3") {
  fast_varmixedint_example_1 o{INT8_MIN - 1, UINT8_MAX, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varmixedint1 test 5") {
  fast_varmixedint_example_1 o{INT8_MIN, UINT8_MAX + 1, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint1 test 6") {
  fast_varmixedint_example_1 o{INT8_MIN, UINT8_MAX, UINT8_MAX + 1, INT8_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint1 test 7") {
  fast_varmixedint_example_1 o{INT8_MIN, UINT8_MAX, UINT8_MAX, INT8_MIN - 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint1 test 8") {
  fast_varmixedint_example_1 o{INT16_MIN, UINT16_MAX, UINT16_MAX, INT16_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint1 test 9") {
  fast_varmixedint_example_1 o{INT16_MIN - 1, UINT16_MAX, UINT16_MAX,
                               INT16_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}
TEST_CASE("fast varmixedint1 test 10") {
  fast_varmixedint_example_1 o{INT16_MIN, UINT16_MAX + 1, UINT16_MAX,
                               INT16_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}
TEST_CASE("fast varmixedint1 test 11") {
  fast_varmixedint_example_1 o{INT16_MIN, UINT16_MAX, UINT16_MAX + 1,
                               INT16_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint1 test 12") {
  fast_varmixedint_example_1 o{INT16_MIN, UINT16_MAX, UINT16_MAX,
                               INT16_MIN - 1};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint1 test 13") {
  fast_varmixedint_example_1 o{INT32_MIN, UINT32_MAX, UINT32_MAX, INT32_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint1 test 14") {
  fast_varmixedint_example_1 o{INT32_MIN, UINT32_MAX + 1ull, UINT32_MAX,
                               INT32_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}
TEST_CASE("fast varmixedint1 test 15") {
  fast_varmixedint_example_1 o{INT32_MIN, UINT32_MAX, UINT32_MAX,
                               INT32_MIN - 1ll};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}
TEST_CASE("fast varmixedint1 test 16") {
  fast_varmixedint_example_1 o{INT32_MIN, UINT64_MAX, UINT32_MAX, INT64_MIN};
  auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(o);
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         fast_varmixedint_example_1>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}

struct varmixedint2 {
  int32_t a;
  uint64_t b;
  uint32_t c;
  int64_t d;
  bool operator==(const varmixedint2& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
  static constexpr auto struct_pack_config = struct_pack::ENCODING_WITH_VARINT |
                                             struct_pack::USE_FAST_VARINT |
                                             struct_pack::DISABLE_ALL_META_INFO;
};

TEST_CASE("fast varmixedint2 test 1") {
  varmixedint2 o{0, 0, 0, 0};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 1);
  return;
}

TEST_CASE("fast varmixedint2 test 2") {
  varmixedint2 o{INT8_MIN, UINT8_MAX, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 5);
  return;
}

TEST_CASE("fast varmixedint2 test 3") {
  varmixedint2 o{INT8_MIN - 1, UINT8_MAX, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}

TEST_CASE("fast varmixedint2 test 5") {
  varmixedint2 o{INT8_MIN, UINT8_MAX + 1, UINT8_MAX, INT8_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint2 test 6") {
  varmixedint2 o{INT8_MIN, UINT8_MAX, UINT8_MAX + 1, INT8_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint2 test 7") {
  varmixedint2 o{INT8_MIN, UINT8_MAX, UINT8_MAX, INT8_MIN - 1};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint2 test 8") {
  varmixedint2 o{INT16_MIN, UINT16_MAX, UINT16_MAX, INT16_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 9);
  return;
}
TEST_CASE("fast varmixedint2 test 9") {
  varmixedint2 o{INT16_MIN - 1, UINT16_MAX, UINT16_MAX, INT16_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}
TEST_CASE("fast varmixedint2 test 10") {
  varmixedint2 o{INT16_MIN, UINT16_MAX + 1, UINT16_MAX, INT16_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}
TEST_CASE("fast varmixedint2 test 11") {
  varmixedint2 o{INT16_MIN, UINT16_MAX, UINT16_MAX + 1, INT16_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint2 test 12") {
  varmixedint2 o{INT16_MIN, UINT16_MAX, UINT16_MAX, INT16_MIN - 1};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint2 test 13") {
  varmixedint2 o{INT32_MIN, UINT32_MAX, UINT32_MAX, INT32_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 17);
  return;
}

TEST_CASE("fast varmixedint2 test 14") {
  varmixedint2 o{INT32_MIN, UINT32_MAX + 1ull, UINT32_MAX, INT32_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}
TEST_CASE("fast varmixedint2 test 15") {
  varmixedint2 o{INT32_MIN, UINT32_MAX, UINT32_MAX, INT32_MIN - 1ll};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}
TEST_CASE("fast varmixedint2 test 16") {
  varmixedint2 o{INT32_MIN, UINT64_MAX, UINT32_MAX, INT64_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<varmixedint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 25);
  return;
}

struct six_int {
  int a, b, c, d, e, f;
  bool operator==(const six_int& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e && f == o.f;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT |
      struct_pack::sp_config::ENCODING_WITH_VARINT;
};

TEST_CASE("six int test") {
  six_int o{INT32_MIN, 2435, INT32_MAX, 0, 0, INT32_MIN};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<six_int>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  return;
}

struct seven_int {
  int a, b, c, d, e, f, g;
  bool operator==(const seven_int& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e &&
           f == o.f && g == o.g;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT |
      struct_pack::sp_config::ENCODING_WITH_VARINT;
};

TEST_CASE("seven int test") {
  seven_int o{INT32_MIN, 21314, INT32_MAX, 0, 0, INT32_MIN, 0};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<seven_int>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  return;
}

struct eight_int {
  int a, b, c, d, e, f, g, h;
  bool operator==(const eight_int& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e &&
           f == o.f && g == o.g && h == o.h;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT |
      struct_pack::sp_config::ENCODING_WITH_VARINT;
};

TEST_CASE("eight int test") {
  eight_int o{INT32_MIN, 521, INT32_MAX, 0, 0, INT32_MIN, 0, 2123};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<eight_int>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  return;
}

TEST_CASE("vector<eight int> test") {
  eight_int o{INT32_MIN, 521, INT32_MAX, 0, 0, INT32_MIN, 0, 2123};
  std::vector<eight_int> vec;
  vec.resize(100, o);
  auto buffer = struct_pack::serialize(vec);
  auto result = struct_pack::deserialize<std::vector<eight_int>>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == vec);
  return;
}

struct mixed_fast_varint {
  int a, b, c;
  std::string d;
  int e, f, g, h;
  bool operator==(const mixed_fast_varint& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e &&
           f == o.f && g == o.g && h == o.h;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT |
      struct_pack::sp_config::ENCODING_WITH_VARINT;
};

TEST_CASE("test mixed 1") {
  mixed_fast_varint o{INT32_MIN, 521, INT32_MAX, "Hello", 0, 0, INT32_MIN, 0};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<mixed_fast_varint>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  return;
}

struct mixed_fast_varint2 {
  struct_pack::var_int64_t a, b, c;
  std::string d;
  int e, f, g, h;
  bool operator==(const mixed_fast_varint2& o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d && e == o.e &&
           f == o.f && g == o.g && h == o.h;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::USE_FAST_VARINT |
      struct_pack::sp_config::DISABLE_ALL_META_INFO;
};

TEST_CASE("test mixed 2") {
  mixed_fast_varint2 o{0, 0, 0, "Hello", 0, 0, 0, 0};
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<mixed_fast_varint2>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == o);
  CHECK(buffer.size() == 24);
  return;
}
