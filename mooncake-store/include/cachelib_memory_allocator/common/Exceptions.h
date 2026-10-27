/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#pragma once

#include <stdexcept>
#include <string>

#ifndef CACHELIB_CHECK_THROW
#define CACHELIB_CHECK_THROW(cond, msg)          \
  do {                                           \
    if (UNLIKELY(!(cond))) {                     \
      XLOG(ERR, "CHECK FAILED: " #cond "," msg); \
      throw std::invalid_argument(msg);          \
    }                                            \
  } while (0)
#endif

#ifndef CACHELIB_CHECK_THROWF
#define CACHELIB_CHECK_THROWF(cond, fmt, arg1, ...)                          \
  do {                                                                       \
    if (UNLIKELY(!(cond))) {                                                 \
      XLOGF(ERR, "CHECK FAILED: " #cond "," fmt, arg1, ##__VA_ARGS__);       \
      throw std::invalid_argument(fmt::format(fmt, arg1, ##__VA_ARGS__)); \
    }                                                                        \
  } while (0)
#endif

namespace facebook {
namespace cachelib {
namespace exception {
class OutOfMemory : public std::bad_alloc {
 public:
  OutOfMemory(std::string what) : what_{std::move(what)} {}

  const char* what() const noexcept override { return what_.c_str(); }

 private:
  const std::string what_;
};

class RefcountOverflow : public std::overflow_error {
 public:
  using std::overflow_error::overflow_error;
};

class RefcountUnderflow : public std::underflow_error {
 public:
  using std::underflow_error::underflow_error;
};

class SlabReleaseAborted : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class ChainedItemInvalid : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// An allocation error. This could be a genuine std::bad_alloc from
// the global allocator, or it can be an internal allocation error
// from the backing cachelib item.
class ObjectCacheAllocationError : public OutOfMemory {
 public:
  using OutOfMemory::OutOfMemory;
};

// Bad arguments were fed into deallocate(). This indicates the alloc
// argument was invalid, or the size was different from the originally
// requested size.
class ObjectCacheDeallocationBadArgs : public std::invalid_argument {
  using std::invalid_argument::invalid_argument;
};
} // namespace exception
} // namespace cachelib
} // namespace facebook
