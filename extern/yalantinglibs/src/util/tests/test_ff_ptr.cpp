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
#include <cstdint>
#include <memory>

#include "doctest.h"
#include "ylt/util/ff_ptr.hpp"
using namespace doctest;
static std::string err_string;
int initer = (ylt::util::regist_ff_ptr_error_handler(
                  [](ylt::util::ff_ptr_error_type error, void* raw_pointer) {
                    auto str = ylt::util::b_stacktrace_get_string();
                    err_string = to_string(error) + '\n' + str;
                    free(str);
                  }),
              1);
volatile char hi;
template <typename T>
void read(ylt::util::ff_ptr<T> ptr) {
  auto tmp = *ptr;
  hi = *(char*)&tmp;
}
TEST_CASE("testing shared_ptr heap-use-after-free") {
  err_string = "";
  ylt::util::ff_ptr<int64_t> raw_ptr;
  {
    auto ptr = ylt::util::ff_make_shared<int64_t>(1);
    raw_ptr = ptr.get();
    read<int64_t>(raw_ptr);
    CHECK(err_string.empty());
  }
  read<int64_t>(raw_ptr);
  std::cout << err_string << std::endl;
  CHECK(err_string.find("heap use after free") != std::string::npos);
}
TEST_CASE("testing shared_ptr double-free") {
  err_string = "";
  int64_t hi;
  {
    auto ptr = ylt::util::ff_shared_ptr<int64_t>(&hi, [](int64_t*) {
    });
    ylt::util::ff_shared_ptr<int64_t> ptr2(ptr.get(), [](int64_t*) {
    });
  }
  std::cout << err_string << std::endl;
  CHECK(err_string.find("double free") != std::string::npos);
}
TEST_CASE("testing unique_ptr heap-use-after free") {
  err_string = "";
  ylt::util::ff_ptr<int64_t> raw_ptr;
  int64_t hi;
  {
    auto ptr = ylt::util::ff_unique_ptr<int64_t, decltype([](int64_t*) {
                                        })>(&hi);
    raw_ptr = ptr.get();
    read<int64_t>(raw_ptr);
    CHECK(err_string.empty());
  }
  read<int64_t>(raw_ptr);
  std::cout << err_string << std::endl;
  CHECK(err_string.find("heap use after free") != std::string::npos);
}
TEST_CASE("testing unique_ptr heap-use-after free2") {
  err_string = "";
  ylt::util::ff_ptr<std::pair<int64_t, int64_t>> raw_ptr;
  std::pair<int64_t, int64_t> hi;
  {
    auto ptr =
        ylt::util::ff_unique_ptr<std::pair<int64_t, int64_t>,
                                 decltype([](std::pair<int64_t, int64_t>*) {
                                 })>(&hi);
    raw_ptr = ptr.get();
    read<std::pair<int64_t, int64_t>>(raw_ptr);
    CHECK(err_string.empty());
  }
  read<std::pair<int64_t, int64_t>>(raw_ptr);
  std::cout << err_string << std::endl;
  CHECK(err_string.find("heap use after free") != std::string::npos);
}
TEST_CASE("testing unique_ptr double-free") {
  err_string = "";

  std::shared_ptr<int64_t> hi;
  hi = std::make_shared<int64_t>(0);
  auto deleter = [hi](int64_t*) {
    std::cout << "hi" << std::endl;
  };
  {
    [[maybe_unused]] auto ptr =
        ylt::util::ff_unique_ptr<int64_t, decltype(deleter)>(hi.get(), deleter);
    [[maybe_unused]] ylt::util::ff_unique_ptr<int64_t, decltype(deleter)> ptr2(
        hi.get(), deleter);
  }
  std::cout << err_string << std::endl;
  CHECK(err_string.find("double free") != std::string::npos);
}

TEST_CASE("testing invaild ptr compare") {
  err_string = "";
  ff_ptr<int64_t> v1, v2;
  int64_t* w1;
  bool val = false;
  CHECK(v1 == v2);
  CHECK(v1 == nullptr);
  CHECK(nullptr == v2);
  {
    auto hi = ylt::util::ff_make_shared<int64_t>(0),
         hi2 = ylt::util::ff_make_shared<int64_t>(0);
    v1 = hi.get();
    v2 = hi2.get();
    w1 = v1.get();
    val = v1 < v2;
  }
  CHECK(w1 == v1);
  CHECK(w1 <= v1);
  CHECK(w1 >= v1);
  CHECK(!(w1 != v1));
  CHECK(!(w1 > v1));
  CHECK(!(w1 < v1));
  CHECK(v1 == v1);
  CHECK(v1 <= v1);
  CHECK(v1 >= v1);
  CHECK(!(v1 != v1));
  CHECK(!(v1 > v1));
  CHECK(!(v1 < v1));
  CHECK(v1 == w1);
  CHECK(v1 <= w1);
  CHECK(v1 >= w1);
  CHECK(!(v1 != w1));
  CHECK(!(v1 > w1));
  CHECK(!(v1 < w1));
  CHECK(val == (v1 < v2));
  CHECK(v1 != v2);
  CHECK(v1);
  std::cout << err_string << std::endl;
  CHECK(err_string.empty());
}
TEST_CASE("testing invaild ptr add") {
  err_string = "";
  ff_ptr<int64_t> v1;
  {
    auto hi = ylt::util::ff_make_shared<int64_t>(42);
    v1 = hi.get();
  }
  v1++;
  CHECK(v1);
  ++v1;
  CHECK(v1);
  v1 += 1;
  CHECK(v1);
  v1 -= 1;
  CHECK(v1);
  --v1;
  CHECK(v1);
  v1--;
  CHECK(v1);
  std::cout << err_string << std::endl;
  CHECK(err_string.empty());
}