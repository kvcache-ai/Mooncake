/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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
#include "ylt/coro_io/ibverbs/ib_device.hpp"
#define DOCTEST_CONFIG_IMPLEMENT

#include "doctest.h"
#include "ylt/coro_io/ibverbs/ib_buffer.hpp"
#include "ylt/easylog.hpp"
// doctest comments
// 'function' : must be 'attribute' - see issue #182
DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char** argv) {
  // easylog::logger<>::instance().init(easylog::Severity::TRACE, false, true,
  //                                  "1.log", 1000000000, 1000, true);
  coro_io::get_global_ib_device(
      {.buffer_pool_config = {.buffer_size = 8 * 1024}});
  return doctest::Context(argc, argv).run();
}
DOCTEST_MSVC_SUPPRESS_WARNING_POP