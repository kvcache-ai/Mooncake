/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef ASYNC_SIMPLE_UTHREAD_INTERNAL_THREAD_H
#define ASYNC_SIMPLE_UTHREAD_INTERNAL_THREAD_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <memory>

#include "async_simple/Future.h"
#include "async_simple/Promise.h"
#include "async_simple/uthread/internal/thread_impl.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {
namespace internal {

inline constexpr size_t default_base_stack_size = 512 * 1024;
size_t get_base_stack_size();

class thread_context {
    struct stack_deleter {
        void operator()(char* ptr) const noexcept;
    };
    using stack_holder = std::unique_ptr<char[], stack_deleter>;

    const size_t stack_size_;
    stack_holder stack_{make_stack()};
    std::function<void()> func_;
    jmp_buf_link context_;

public:
    bool joined_ = false;
    Promise<bool> done_;

private:
    static void s_main(transfer_t t);
    void setup();
    void main();
    stack_holder make_stack();

public:
    explicit thread_context(std::function<void()> func, size_t stack_size = 0);
    ~thread_context();
    void switch_in();
    void switch_out();
    friend void thread_impl::switch_in(thread_context*);
    friend void thread_impl::switch_out(thread_context*);
};

}  // namespace internal
}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_INTERNAL_THREAD_H