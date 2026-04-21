/*
 * Copyright (c) 2022, Alibaba Group Holding Limited;
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
#ifndef ASYNC_SIMPLE_UNIT_H
#define ASYNC_SIMPLE_UNIT_H

namespace async_simple {

// Unit plays the role of a simplest type in case we couldn't
// use void directly.
//
// User shouldn't use this directly.
struct Unit {
    constexpr bool operator==(const Unit&) const { return true; }
    constexpr bool operator!=(const Unit&) const { return false; }
};

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UNIT_H
