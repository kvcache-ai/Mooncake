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

#include <cstdlib>

#include "ylt/struct_pack.hpp"
namespace my_name_space {

struct array2D {
  unsigned int x;
  unsigned int y;
  float* p;
  array2D(unsigned int x, unsigned int y) : x(x), y(y) {
    p = (float*)calloc(1ull * x * y, sizeof(float));
  }
  array2D(const array2D&) = delete;
  array2D(array2D&& o) : x(o.x), y(o.y), p(o.p) { o.p = nullptr; };
  array2D& operator=(const array2D&) = delete;
  array2D& operator=(array2D&& o) {
    x = o.x;
    y = o.y;
    p = o.p;
    o.p = nullptr;
    return *this;
  }
  float& operator()(std::size_t i, std::size_t j) { return p[i * y + j]; }
  bool operator==(const array2D& o) const {
    return x == o.x && y == o.y &&
           memcmp(p, o.p, 1ull * x * y * sizeof(float)) == 0;
  }
  array2D() : x(0), y(0), p(nullptr) {}
  ~array2D() { free(p); }
};

// If you want to add a new type for struct_pack which is not in the struct_pack
// type system. You need add those functions:

// 1. sp_get_needed_size: calculate length of serialization
std::size_t sp_get_needed_size(const array2D& ar) {
  return 2 * struct_pack::get_write_size(ar.x) +
         struct_pack::get_write_size(ar.p, 1ull * ar.x * ar.y);
}
// 2. sp_serialize_to: serilize object to writer
template </*struct_pack::writer_t*/ typename Writer>
void sp_serialize_to(Writer& writer, const array2D& ar) {
  struct_pack::write(writer, ar.x);
  struct_pack::write(writer, ar.y);
  struct_pack::write(writer, ar.p, 1ull * ar.x * ar.y);
}
// 3. sp_deserialize_to: deserilize object from reader
template </*struct_pack::reader_t*/ typename Reader>
struct_pack::err_code sp_deserialize_to(Reader& reader, array2D& ar) {
  if (auto ec = struct_pack::read(reader, ar.x); ec) {
    return ec;
  }
  if (auto ec = struct_pack::read(reader, ar.y); ec) {
    return ec;
  }
  auto length = 1ull * ar.x * ar.y * sizeof(float);
  if constexpr (struct_pack::checkable_reader_t<Reader>) {
    if (!reader.check(length)) {  // some input(such as memory) allow us check
                                  // length before read data.
      return struct_pack::errc::no_buffer_space;
    }
  }
  ar.p = (float*)malloc(length);
  auto ec = struct_pack::read(reader, ar.p, 1ull * ar.x * ar.y);
  if (ec) {
    free(ar.p);
  }
  return ec;
}

// 4. The default name for type checking is it's literal type name. You can also
// config it by:

// constexpr std::string_view sp_set_type_name(test*) { return "myarray2D"; }

// 5. If you want to use struct_pack::get_field/struct_pack::get_field_to, you
// need also define this function to skip deserialization of your type:

// template <typename Reader>
// struct_pack::errc sp_deserialize_to_with_skip(Reader& reader, array2D& ar);
}  // namespace my_name_space

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

void user_defined_serialization() {
  std::vector<my_name_space::array2D> ar;
  ar.emplace_back(11, 22);
  ar.emplace_back(114, 514);
  ar[0](1, 6) = 3.14;
  ar[1](87, 111) = 2.71;
  {
    auto buffer = struct_pack::serialize(ar);
    auto result = struct_pack::deserialize<decltype(ar)>(buffer);
    assert(result.has_value());
    assert(ar == result);
  }
  {
    {
      fwrite_stream out("1.tmp");
      struct_pack::serialize_to(out, ar);
    }
    fread_stream in("1.tmp");
    auto result = struct_pack::deserialize<decltype(ar)>(in);
    assert(result.has_value());
    assert(ar == result);
  }
}