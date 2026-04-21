#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"

TEST_CASE("test serialize_to/deserialize file") {
  person p1{24, "Betty"}, p2{45, "Tom"};
  complicated_object v{
      Color::red,
      42,
      "hello",
      {{20, "tom"}, {22, "jerry"}},
      {"hello", "world"},
      {1, 2},
      {{1, {20, "tom"}}},
      {{1, {20, "tom"}}, {1, {22, "jerry"}}},
      {"aa", "bb"},
      {1, 2},
      {{1, {20, "tom"}}, {1, {22, "jerry"}}},
      {{1, 2}, {1, 3}},
      {person{20, "tom"}, {22, "jerry"}},
      {person{20, "tom"}, {22, "jerry"}},
      std::make_pair("aa", person{20, "tom"}),
  };
  nested_object nested{2, "tom", {20, "tom"}, v};
  SUBCASE("serialize_to empty file") {
    // serialize to file
    std::ofstream ofs("1.save", std::ofstream::binary | std::ofstream::out);
    for (int i = 0; i < 100; ++i) {
      struct_pack::serialize_to(ofs, p1);
      struct_pack::serialize_to(ofs, nested);
    }
    auto total_size = (struct_pack::get_needed_size(p1) +
                       struct_pack::get_needed_size(nested)) *
                      100;
    CHECK(ofs.tellp() == total_size);
  }
  SUBCASE("serialize_to non-empty file") {
    // serialize to file
    std::ofstream ofs("1.save", std::ofstream::binary | std::ofstream::out |
                                    std::ostream::app);
    for (int i = 0; i < 100; ++i) {
      struct_pack::serialize_to(ofs, p2);
      struct_pack::serialize_to(ofs, nested);
    }
  }
  SUBCASE("test deserialize") {
    std::ifstream ifs("1.save", std::ofstream::binary | std::ofstream::in);
    for (int i = 0; i < 100; ++i) {
      auto person1 = struct_pack::deserialize<person>(ifs);
      CHECK(person1 == p1);
      auto nested1 = struct_pack::deserialize<nested_object>(ifs);
      CHECK(nested1 == nested);
    }
    for (int i = 0; i < 100; ++i) {
      auto person2 = struct_pack::deserialize<person>(ifs);
      CHECK(person2 == p2);
      auto nested1 = struct_pack::deserialize<nested_object>(ifs);
      CHECK(nested1 == nested);
    }
  }
  SUBCASE("test deserialize_to") {
    std::ifstream ifs("1.save", std::ofstream::binary | std::ofstream::in);
    for (int i = 0; i < 100; ++i) {
      person person1;
      auto ec = struct_pack::deserialize_to(person1, ifs);
      CHECK(!ec);
      CHECK(person1 == p1);
      nested_object nested1;
      ec = struct_pack::deserialize_to(nested1, ifs);
      CHECK(!ec);
      CHECK(nested1 == nested);
    }
    for (int i = 0; i < 100; ++i) {
      person person2;
      auto ec = struct_pack::deserialize_to(person2, ifs);
      CHECK(!ec);
      CHECK(person2 == p2);
      nested_object nested1;
      ec = struct_pack::deserialize_to(nested1, ifs);
      CHECK(!ec);
      CHECK(nested1 == nested);
    }
  }
}

TEST_CASE("test get_field file") {
  std::array<person, 2> p{person{24, "Betty"}, person{45, "Tom"}};
  std::ofstream ofs("2.save", std::ofstream::binary | std::ofstream::out);
  for (int i = 0; i < 100; ++i) {
    for (auto &p1 : p) {
      auto pos = ofs.tellp();
      CHECK(ofs.seekp(4, std::ios_base::cur));
      struct_pack::serialize_to(ofs, p1);
      auto npos = ofs.tellp();
      CHECK(ofs.seekp(pos));
      uint32_t sz = npos - pos;
      CHECK(ofs.write((char *)&sz, 4));
      CHECK(ofs.seekp(npos));
    }
  }
  auto total_size = (struct_pack::get_needed_size(p[0]) +
                     struct_pack::get_needed_size(p[1]) + 8) *
                    100;
  CHECK(ofs.tellp() == total_size);

  SUBCASE("test get_field") {
    std::ifstream ifs("2.save", std::ofstream::binary | std::ofstream::in);
    for (int i = 0; i < 100; ++i) {
      for (auto &p1 : p) {
        std::size_t pos = ifs.tellg();
        std::uint32_t sz;
        CHECK(ifs.read((char *)&sz, 4));
        auto name = struct_pack::get_field<person, 1>(ifs);
        CHECK(name == p1.name);
        CHECK(ifs.seekg(pos + sz));
      }
    }
  }
  SUBCASE("test get_field") {
    std::ifstream ifs("2.save", std::ofstream::binary | std::ofstream::in);
    for (int i = 0; i < 100; ++i) {
      for (auto &p1 : p) {
        std::size_t pos = ifs.tellg();
        std::uint32_t sz;
        CHECK(ifs.read((char *)&sz, 4));
        std::string name;
        auto ec = struct_pack::get_field_to<person, 1>(name, ifs);
        CHECK(!ec);
        CHECK(name == p1.name);
        CHECK(ifs.seekg(pos + sz));
      }
    }
  }
}

TEST_CASE("test compatible obj") {
  person p{24, "Betty"};
  person1 p1{24, "Betty"};
  person1 p2{24, "Betty", 114514};
  person1 p3{24, "Betty", 114514, true};
  SUBCASE("forward") {
    {
      std::ofstream ofs("3.save", std::ofstream::binary | std::ofstream::out);
      for (int i = 0; i < 100; ++i) {
        struct_pack::serialize_to(ofs, p1);
        struct_pack::serialize_to(ofs, p2);
        struct_pack::serialize_to(ofs, p3);
      }
      auto total_size =
          (struct_pack::get_needed_size(p1) + struct_pack::get_needed_size(p2) +
           struct_pack::get_needed_size(p3)) *
          100;
      CHECK(ofs.tellp() == total_size);
    }
    {
      std::ifstream ifs("3.save", std::ofstream::binary | std::ofstream::in);
      for (int i = 0; i < 100; ++i) {
        auto person1 = struct_pack::deserialize<person>(ifs);
        CHECK(person1 == p);
        auto person2 = struct_pack::deserialize<person>(ifs);
        CHECK(person2 == p);
        auto person3 = struct_pack::deserialize<person>(ifs);
        CHECK(person3 == p);
      }
    }
  }
  SUBCASE("backward") {
    {
      std::ofstream ofs("4.save", std::ofstream::binary | std::ofstream::out);
      for (int i = 0; i < 100; ++i) {
        struct_pack::serialize_to(ofs, p);
        struct_pack::serialize_to(ofs, p1);
        struct_pack::serialize_to(ofs, p2);
      }
      auto total_size =
          (struct_pack::get_needed_size(p) + struct_pack::get_needed_size(p1) +
           struct_pack::get_needed_size(p2)) *
          100;
      CHECK(ofs.tellp() == total_size);
    }
    {
      std::ifstream ifs("4.save", std::ofstream::binary | std::ofstream::in);
      for (int i = 0; i < 100; ++i) {
        auto res1 = struct_pack::deserialize<person1>(ifs);
        CHECK(res1 == p1);
        auto res2 = struct_pack::deserialize<person1>(ifs);
        CHECK(res2 == p1);
        auto res3 = struct_pack::deserialize<person1>(ifs);
        CHECK(res3 == p2);
      }
    }
    {
      std::ifstream ifs("4.save", std::ofstream::binary | std::ofstream::in);
      auto res = struct_pack::get_field<person1, 2>(ifs);
      CHECK(res.has_value() == true);
      CHECK(*res == std::nullopt);
    }
  }
}

TEST_CASE("testing file size no enough") {
  std::vector<std::string> data = {"Hello", "Hi", "Hey", "Hoo"};
  {
    std::ofstream of("tmp.data", std::ofstream::binary | std::ofstream::out);
    auto buffer = struct_pack::serialize(data);
    for (int i = 0; i < 5; ++i) buffer.pop_back();
    of.write(buffer.data(), buffer.size());
  }
  {
    std::ifstream ifi("tmp.data", std::ios::in | std::ios::binary);
    std::vector<std::string> data2;
    std::vector<std::string> data3 = {"Hello", "Hi", ""};
    auto ec = struct_pack::deserialize_to(data2, ifi);
    CHECK(ec == struct_pack::errc::no_buffer_space);
    CHECK(data3 == data2);
    CHECK(data2.capacity() == 3);
  }
  std::filesystem::remove("tmp.data");
}

TEST_CASE("testing broken container size") {
  SUBCASE("hacker 1") {
    std::string data(2 * 1024 * 1024, 'A');
    {
      std::ofstream of("tmp.data", std::ofstream::binary | std::ofstream::out);
      auto buffer = struct_pack::serialize<std::string>(data);
      buffer = buffer.substr(0, 16);
      of.write(buffer.data(), buffer.size());
    }
    {
      std::ifstream ifi("tmp.data", std::ios::in | std::ios::binary);
      std::string data2;
      auto ec = struct_pack::deserialize_to(data2, ifi);
      CHECK(ec == struct_pack::errc::no_buffer_space);
      CHECK(data2.size() == 0);
      CHECK(data2.capacity() < 100);  // SSO
    }
  }
  SUBCASE("hacker 2") {
    std::string data(2 * 1024 * 1024, 'A');
    {
      std::ofstream of("tmp.data", std::ofstream::binary | std::ofstream::out);
      auto buffer = struct_pack::serialize<std::string>(data);
      buffer = buffer.substr(0, 2 * 1024 * 1024 - 10000);
      of.write(buffer.data(), buffer.size());
    }
    {
      std::ifstream ifi("tmp.data", std::ios::in | std::ios::binary);
      std::string data2;
      auto ec = struct_pack::deserialize_to(data2, ifi);
      CHECK(ec == struct_pack::errc::no_buffer_space);
      CHECK(data2.size() == 1 * 1024 * 1024);
      CHECK(data2.capacity() < 2 * 1024 * 1024);
    }
  }
  std::filesystem::remove("tmp.data");
}