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

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>

// It's easy to use struct_pack, just include it!
#include <ylt/struct_pack.hpp>

struct fwrite_stream {
  FILE* file;
  bool write(const char* data, std::size_t sz) {
    return fwrite(data, sz, 1, file) == 1;
  }
  fwrite_stream(const char* file_name) : file(fopen(file_name, "wb")) {}
  ~fwrite_stream() { fclose(file); }
};

struct fread_stream {
  FILE* file;
  bool read(char* data, std::size_t sz) {
    return fread(data, sz, 1, file) == 1;
  }
  bool ignore(std::size_t sz) { return fseek(file, sz, SEEK_CUR) == 0; }
  std::size_t tellg() {
    // if you worry about ftell performance, just use an variable to record it.
    return ftell(file);
  }
  fread_stream(const char* file_name) : file(fopen(file_name, "rb")) {}
  ~fread_stream() { fclose(file); }
};

struct person {
  int age;
  std::string name;

  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
};

//clang-format off
void basic_usage() {
  person p{20, "tom"};

  person p2{/*.age =*/21, /*.name =*/"Betty"};

  // serialize api
  // api 1. serialize with default container
  { auto buffer = struct_pack::serialize(p); }
  // api 2. use specifier container
  { auto buffer = struct_pack::serialize<std::string>(p); }
  // api 3. serialize to container's back
  {
    std::string buffer = "The next line is struct_pack data.\n";
    struct_pack::serialize_to(buffer, p);
  }
  // api 4. serialize to continuous buffer
  {
    auto size = struct_pack::get_needed_size(p);
    auto array = std::make_unique<char[]>(size);
    struct_pack::serialize_to(array.get(), size, p);
  }
  // api 5. serialize with offset
  {
    auto buffer = struct_pack::serialize_with_offset(/* offset = */ 2, p);
    auto buffer2 = struct_pack::serialize(p);
    [[maybe_unused]] bool result =
        std::string_view{buffer.data() + 2, buffer.size() - 2} ==
        std::string_view{buffer2.data(), buffer2.size()};
    assert(result);
  }
  // api 6. serialize varadic param
  {
    person p2{/*.age =*/21, /*.name =*/"Betty"};
    auto buffer = struct_pack::serialize(p.age, p2.name);
  }
  // api 7. serialize to stream
  {
    // std::ofstream writer("struct_pack_demo.data",
    //                      std::ofstream::out | std::ofstream::binary);
    fwrite_stream writer("struct_pack_demo.data");
    struct_pack::serialize_to(writer, p);
  }

  // deserialize api
  auto buffer = struct_pack::serialize(p);
  // api 1. deserialize object to return value
  {
    auto p2 = struct_pack::deserialize<person>(buffer);
    assert(p2);  // no error
    assert(p2.value() == p2);
  }
  // api 2. deserialize object to reference
  {
    person p2;
    [[maybe_unused]] auto ec = struct_pack::deserialize_to(p2, buffer);
    assert(!ec);
    assert(p == p2);
  }
  // api 3. partial deserialize
  {
    auto name = struct_pack::get_field<person, 1>(buffer);
    assert(name);  // no error
    assert(p.name == name.value());
  }
  // api 4. varadic deserialize
  {
    auto buffer = struct_pack::serialize(p.age, p2.name);
    auto result = struct_pack::deserialize<int, std::string>(buffer);
    assert(result);  // no error
    [[maybe_unused]] auto&& [age, name] = result.value();
    assert(age == p.age);
    assert(name == p2.name);
  }
  // api 4. varadic deserialize_to
  {
    person p3;
    auto buffer = struct_pack::serialize(p.age, p2.name);
    [[maybe_unused]] auto result =
        struct_pack::deserialize_to(p3.age, buffer, p3.name);
    assert(!result);
    assert(p3.age == p.age);
    assert(p3.name == p2.name);
  }
  // api 5. deserialize from stream
  {
    // std::ifstream ifs("struct_pack_demo.data",
    //                   std::ofstream::in | std::ofstream::binary);
    fread_stream ifs("struct_pack_demo.data");
    auto p4 = struct_pack::deserialize<person>(ifs);
    assert(p4 == p);
  }
}