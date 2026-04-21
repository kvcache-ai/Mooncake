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
#ifndef _WIN32
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "doctest.h"
#include "rpc_api.hpp"
using namespace doctest;
using namespace coro_rpc;
// #ifndef _MSC_VER
// TEST_CASE("register duplication") {
//   register_handler<hi>();
//   HelloService s;
//   register_handler<&HelloService::hello>(&s);
//   SUBCASE("free function duplication") {
//     auto pid = fork();
//     if (pid == 0) {
//       register_handler<hi>();
//       REQUIRE_MESSAGE(false, "cannot reach");
//     }
//     else {
//       int status = 10;
//       wait(&status);
//       CHECK(WEXITSTATUS(status) == 1);
//     }
//   }
//   SUBCASE("member function duplication") {
//     auto pid = fork();
//     if (pid == 0) {
//       register_handler<&HelloService::hello>(&s);
//       REQUIRE_MESSAGE(false, "cannot reach");
//     }
//     else {
//       int status = 10;
//       wait(&status);
//       CHECK(WEXITSTATUS(status) == 1);
//     }
//   }
// }

// TEST_CASE("register with nullptr") {
//   auto pid = fork();
//   if (pid == 0) {
//     register_handler<&HelloService::hello>(nullptr);
//     REQUIRE_MESSAGE(false, "cannot reach");
//   }
//   else {
//     int status = 10;
//     wait(&status);
//     CHECK(WEXITSTATUS(status) == 1);
//   }
// }
// #endif