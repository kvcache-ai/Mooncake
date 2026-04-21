#include <cstdint>

#include "doctest.h"
#include "test_struct.hpp"
#include "ylt/struct_pack.hpp"
#include "ylt/struct_pack/error_code.hpp"
#include "ylt/struct_pack/reflection.hpp"

TEST_CASE("test serialize/deserialize rect") {
  {
    rect r{1, 11, 333, 44444}, r2{333, 222, 444, 11};
    auto buffer =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
            r);
    CHECK(buffer.size() == sizeof(rect));
    auto ec = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(r2, buffer);
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    rect r{1, 11, 333, 44444};
    std::string buffer;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        buffer, r);
    CHECK(buffer.size() == sizeof(rect));
    auto result =
        struct_pack::deserialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                                 rect>(buffer);
    CHECK(result.has_value());
    CHECK(r == *result);
  }
  {
    rect r{1, 11, 333, 44444}, r2{333, 222, 444, 11};
    auto buffer =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
            r);
    CHECK(buffer.size() == sizeof(rect));
    auto ec = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(r2, buffer.data(),
                                                       buffer.size());
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    rect r{1, 11, 333, 44444};
    std::string buffer;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        buffer, r);
    CHECK(buffer.size() == sizeof(rect));
    auto result =
        struct_pack::deserialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                                 rect>(buffer.data(), buffer.size());
    CHECK(result.has_value());
    CHECK(r == *result);
  }
}

inline constexpr struct_pack::sp_config set_sp_config(rect*) {
  return struct_pack::DISABLE_ALL_META_INFO;
}

TEST_CASE("test serialize/deserialize rect by ADL") {
  {
    rect r{1, 11, 333, 44444}, r2{333, 222, 444, 11};
    auto buffer = struct_pack::serialize(r);
    CHECK(buffer.size() == sizeof(rect));
    auto ec = struct_pack::deserialize_to(r2, buffer);
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    rect r{1, 11, 333, 44444};
    std::string buffer;
    struct_pack::serialize_to(buffer, r);
    CHECK(buffer.size() == sizeof(rect));
    auto result = struct_pack::deserialize<rect>(buffer);
    CHECK(result.has_value());
    CHECK(r == *result);
  }
  {
    rect r{1, 11, 333, 44444}, r2{333, 222, 444, 11};
    auto buffer = struct_pack::serialize(r);
    CHECK(buffer.size() == sizeof(rect));
    auto ec = struct_pack::deserialize_to(r2, buffer.data(), buffer.size());
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    rect r{1, 11, 333, 44444};
    std::string buffer;
    struct_pack::serialize_to(buffer, r);
    CHECK(buffer.size() == sizeof(rect));
    auto result = struct_pack::deserialize<rect>(buffer.data(), buffer.size());
    CHECK(result.has_value());
    CHECK(r == *result);
  }
}

TEST_CASE("test serialize/deserialize person") {
  {
    person r{25, "Betty"}, r2{0, "none"};
    auto buffer =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                               std::string>(r);
    CHECK(buffer.size() == 11);
    auto ec = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(r2, buffer);
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    person r{25, "Betty"};
    std::string buffer;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        buffer, r);
    CHECK(buffer.size() == 11);
    auto result =
        struct_pack::deserialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                                 person>(buffer);
    CHECK(result.has_value());
    CHECK(r == *result);
  }
  {
    person r{25, "Betty"}, r2{0, "none"};
    auto buffer =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
            r);
    CHECK(buffer.size() == 11);
    auto ec = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(r2, buffer.data(),
                                                       buffer.size());
    CHECK(!ec);
    CHECK(r == r2);
  }
  {
    person r{25, "Betty"};
    std::string buffer;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        buffer, r);
    CHECK(buffer.size() == 11);
    auto result =
        struct_pack::deserialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                                 person>(buffer.data(), buffer.size());
    CHECK(result.has_value());
    CHECK(r == *result);
  }
}