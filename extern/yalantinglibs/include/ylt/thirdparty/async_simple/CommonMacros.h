/*
 * Copyright (c) 2024, Alibaba Group Holding Limited;
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

#ifndef ASYNC_SIMPLE_COMMON_MACROS_H
#define ASYNC_SIMPLE_COMMON_MACROS_H

#if __has_cpp_attribute(likely) && __has_cpp_attribute(unlikely)
#define AS_LIKELY [[likely]]
#define AS_UNLIKELY [[unlikely]]
#else
#define AS_LIKELY
#define AS_UNLIKELY
#endif

#ifdef _WIN32
#define AS_INLINE
#else
#define AS_INLINE __attribute__((__always_inline__)) inline
#endif

#ifdef __clang__
#if __has_feature(address_sanitizer)
#define AS_INTERNAL_USE_ASAN 1
#endif  // __has_feature(address_sanitizer)
#endif  // __clang__

#ifdef __GNUC__
#ifdef __SANITIZE_ADDRESS__  // GCC
#define AS_INTERNAL_USE_ASAN 1
#endif  // __SANITIZE_ADDRESS__
#endif  // __GNUC__

#if __has_cpp_attribute(clang::coro_only_destroy_when_complete)
#define CORO_ONLY_DESTROY_WHEN_DONE [[clang::coro_only_destroy_when_complete]]
#else
#if defined(__alibaba_clang__) && \
    __has_cpp_attribute(ACC::coro_only_destroy_when_complete)
#define CORO_ONLY_DESTROY_WHEN_DONE [[ACC::coro_only_destroy_when_complete]]
#else
#define CORO_ONLY_DESTROY_WHEN_DONE
#endif
#endif

#if __has_cpp_attribute(clang::coro_await_elidable)
#define ELIDEABLE_AFTER_AWAIT [[clang::coro_await_elidable]]
#else
#if defined(__alibaba_clang__) && \
    __has_cpp_attribute(ACC::elideable_after_await)
#define ELIDEABLE_AFTER_AWAIT [[ACC::elideable_after_await]]
#else
#define ELIDEABLE_AFTER_AWAIT
#endif
#endif

#endif
