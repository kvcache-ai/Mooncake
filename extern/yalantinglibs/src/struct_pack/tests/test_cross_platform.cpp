#include <cstdint>
#include <fstream>
#include <iostream>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"
#include "ylt/struct_pack/reflection.hpp"
using namespace struct_pack;
using namespace doctest;

void data_gen() {
  {
    std::ofstream ifs("binary_data/test_cross_platform.dat");
    auto object = create_complicated_object();
    serialize_to<struct_pack::sp_config::ENABLE_TYPE_INFO>(ifs, object);
  }
  {
    std::ofstream ifs("binary_data/test_cross_platform_without_debug_info.dat");
    auto object = create_complicated_object();
    serialize_to<struct_pack::sp_config::DISABLE_TYPE_INFO>(ifs, object);
  }
}

TEST_CASE("testing deserialize other platform data") {
  std::cout << "Now endian:"
            << (struct_pack::detail::is_system_little_endian ? "little" : "big")
            << std::endl;
  std::ifstream ifs("binary_data/test_cross_platform.dat");
  if (!ifs.is_open()) {
    ifs.open("src/struct_pack/tests/binary_data/test_cross_platform.dat");
  }
  REQUIRE(ifs.is_open());
  auto result = struct_pack::deserialize<complicated_object>(ifs);
  REQUIRE(result.has_value());
  auto object = create_complicated_object();
  CHECK(result.value() == object);
}

TEST_CASE("testing deserialize other platform data without debug info") {
  std::ifstream ifs("binary_data/test_cross_platform_without_debug_info.dat");
  if (!ifs.is_open()) {
    ifs.open(
        "src/struct_pack/tests/binary_data/"
        "test_cross_platform_without_debug_info.dat");
  }
  REQUIRE(ifs.is_open());
  auto result = struct_pack::deserialize<complicated_object>(ifs);
  REQUIRE(result.has_value());
  auto object = create_complicated_object();
  CHECK(result.value() == object);
}