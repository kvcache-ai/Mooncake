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
 * Copyright (C) 2016 ScyllaDB.
 */

#ifndef ASYNC_SIMPLE_UTHREAD_INTERNAL_UTHREAD_IMPL_H
#define ASYNC_SIMPLE_UTHREAD_INTERNAL_UTHREAD_IMPL_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Common.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {
namespace internal {

typedef void* fcontext_t;
struct transfer_t {
    fcontext_t fctx;
    void* data;
};
extern "C" __attribute__((__visibility__("default"))) transfer_t
_fl_jump_fcontext(fcontext_t const to, void* vp);
extern "C" __attribute__((__visibility__("default"))) fcontext_t
_fl_make_fcontext(void* sp, std::size_t size, void (*fn)(transfer_t));

class thread_context;

struct jmp_buf_link {
    fcontext_t fcontext;
    jmp_buf_link* link = nullptr;
    thread_context* thread = nullptr;

#ifdef AS_INTERNAL_USE_ASAN
    const void* asan_stack_bottom = nullptr;
    std::size_t asan_stack_size = 0;
#endif

public:
    void switch_in();
    void switch_out();
    void initial_switch_in_completed();
    void final_switch_out();
};

extern thread_local jmp_buf_link* g_current_context;

namespace thread_impl {

inline thread_context* get() { return g_current_context->thread; }

void switch_in(thread_context* to);
void switch_out(thread_context* from);
bool can_switch_out();

}  // namespace thread_impl

}  // namespace internal
}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_INTERNAL_UTHREAD_IMPL_H