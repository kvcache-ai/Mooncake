#include <cstdint>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
#include "ylt/struct_pack/reflection.hpp"

using namespace struct_pack;
TEST_CASE("test uint32") {
  {
    var_uint32_t v = 0;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint32_t v = UINT32_MAX;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
}

TEST_CASE("test int32") {
  {
    var_int32_t v = 0;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 * 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = 128 * 128 * 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = (128 * 128 * 128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = INT32_MAX;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int32_t v = INT32_MIN;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int32_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
}

TEST_CASE("test uint64") {
  {
    var_uint64_t v = 0;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v =
        1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = 1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_uint64_t v = UINT64_MAX;
    auto buf = serialize(v);
    auto v2 = deserialize<var_uint64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
}

TEST_CASE("test int64") {
  {
    var_int64_t v = 0;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 1);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 2);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 3);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 * 128 * 128 / 2 - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 4);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = 128 * 128 * 128 * 128 / 2;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (128 * 128 * 128 * 128 / 2 + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 / 2) - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 5);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 / 2);
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = ((1ll * 128 * 128 * 128 * 128 * 128 / 2) + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 / 2) - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 6);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 / 2);
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = ((1ll * 128 * 128 * 128 * 128 * 128 * 128 / 2) + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 7);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2);
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        ((1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 8);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = (1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2);
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        ((1ll * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) + 1) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        (1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) - 1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        (1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) * -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 9);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        (1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2);
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v =
        ((1ull * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128 / 2) + 1) *
        -1;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = INT64_MAX;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
  {
    var_int64_t v = INT64_MIN;
    auto buf = serialize(v);
    auto v2 = deserialize<var_int64_t>(buf);
    CHECK(detail::calculate_payload_size(v).total == 10);
    CHECK(v2.has_value());
    CHECK(v == v2.value());
  }
}

template <typename T>
struct VarRect {
  T x0, y0, x1, y1;
  bool operator==(const VarRect<T> &o) const {
    return x0 == o.x0 && y0 == o.y0 && x1 == o.x1 && y1 == o.y1;
  }
};
template <typename T>
struct nested_object2 {
  int id;
  std::string name;
  person p;
  complicated_object o;
  VarRect<T> x;
  bool operator==(const nested_object2<T> &O) const {
    return id == O.id && name == O.name && p == O.p && o == O.o && x == O.x;
  }
};

TEST_CASE("test nested") {
  {
    VarRect<var_int32_t> rect{INT32_MAX, INT32_MIN, 123, 0};
    auto buf = serialize(rect);
    auto rect2 = deserialize<VarRect<var_int32_t>>(buf);
    CHECK(rect2.has_value());
    CHECK(rect == rect2);
    nested_object2<var_int32_t> obj{123, "jfslkf", person{24, "Hello"},
                                    complicated_object{}, rect};
    auto buf2 = serialize(obj);
    auto obj2 = deserialize<nested_object2<var_int32_t>>(buf2);
    CHECK(obj2.has_value());
    CHECK(obj2 == obj);
  }
  {
    VarRect<var_int64_t> rect{INT64_MAX, INT64_MIN, 123, 0};
    auto buf = serialize(rect);
    auto rect2 = deserialize<VarRect<var_int64_t>>(buf);
    CHECK(rect2.has_value());
    CHECK(rect == rect2);
    nested_object2<var_int64_t> obj{123, "jfslkf", person{24, "Hello"},
                                    complicated_object{}, rect};
    auto buf2 = serialize(obj);
    auto obj2 = deserialize<nested_object2<var_int64_t>>(buf2);
    CHECK(obj2.has_value());
    CHECK(obj2 == obj);
  }
  {
    VarRect<var_uint32_t> rect{UINT32_MAX, 21321343, 123, 0};
    auto buf = serialize(rect);
    auto rect2 = deserialize<VarRect<var_uint32_t>>(buf);
    CHECK(rect2.has_value());
    CHECK(rect == rect2);
    nested_object2<var_uint32_t> obj{123, "jfslkf", person{24, "Hello"},
                                     complicated_object{}, rect};
    auto buf2 = serialize(obj);
    auto obj2 = deserialize<nested_object2<var_uint32_t>>(buf2);
    CHECK(obj2.has_value());
    CHECK(obj2 == obj);
  }
  {
    VarRect<var_uint64_t> rect{UINT64_MAX, 1233143, 123, 0};
    auto buf = serialize(rect);
    auto rect2 = deserialize<VarRect<var_uint64_t>>(buf);
    CHECK(rect2.has_value());
    CHECK(rect == rect2);
    nested_object2<var_uint64_t> obj{123, "jfslkf", person{24, "Hello"},
                                     complicated_object{}, rect};
    auto buf2 = serialize(obj);
    auto obj2 = deserialize<nested_object2<var_uint64_t>>(buf2);
    CHECK(obj2.has_value());
    CHECK(obj2 == obj);
  }
}

TEST_CASE("test varint zip size") {
  {
    std::vector<var_int32_t> vec;
    for (int i = 0; i < 100; ++i) vec.push_back(rand() % 128 - 64);
    auto buf =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_TYPE_INFO>(vec);
    CHECK(buf.size() == 4 + 1 + 1 * 100);
    CHECK(detail::calculate_payload_size(vec).total == 100);
    CHECK(detail::calculate_payload_size(vec).size_cnt == 1);
  }
  {
    std::vector<var_int64_t> vec;
    for (int i = 0; i < 100; ++i) vec.push_back(rand() % 128 - 64);
    auto buf =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_TYPE_INFO>(vec);
    CHECK(buf.size() == 4 + 1 + 1 * 100);
    CHECK(detail::calculate_payload_size(vec).total == 100);
    CHECK(detail::calculate_payload_size(vec).size_cnt == 1);
  }
  {
    std::vector<var_uint32_t> vec;
    for (int i = 0; i < 100; ++i) vec.push_back(rand() % 128);
    auto buf =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_TYPE_INFO>(vec);
    CHECK(buf.size() == 4 + 1 + 1 * 100);
    CHECK(detail::calculate_payload_size(vec).total == 100);
    CHECK(detail::calculate_payload_size(vec).size_cnt == 1);
  }
  {
    std::vector<var_uint64_t> vec;
    for (int i = 0; i < 100; ++i) vec.push_back(rand() % 128);
    auto buf =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_TYPE_INFO>(vec);
    CHECK(buf.size() == 4 + 1 + 1 * 100);
    CHECK(detail::calculate_payload_size(vec).total == 100);
    CHECK(detail::calculate_payload_size(vec).size_cnt == 1);
  }
}

struct var_int_struct {
  int32_t a;
  uint32_t b;
  int64_t c;
  uint64_t d;
  bool operator==(const var_int_struct &o) const {
    return a == o.a && b == o.b && c == o.c && d == o.d;
  }
  static constexpr auto struct_pack_config =
      struct_pack::sp_config::ENCODING_WITH_VARINT;
};

TEST_CASE("test varint by normal int") {
  var_int_struct v{0, 1, 2, 3};
  auto buffer = struct_pack::serialize<sp_config::DISABLE_ALL_META_INFO>(v);
  auto result = struct_pack::deserialize<sp_config::DISABLE_ALL_META_INFO,
                                         var_int_struct>(buffer);
  REQUIRE(result.has_value());
  CHECK(result == v);
  CHECK(buffer.size() == 4);
}