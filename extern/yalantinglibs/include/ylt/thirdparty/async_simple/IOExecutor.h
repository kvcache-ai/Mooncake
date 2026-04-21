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
#ifndef ASYNC_SIMPLE_IO_EXECUTOR_H
#define ASYNC_SIMPLE_IO_EXECUTOR_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <cstdint>
#include <functional>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

// IOExecutor accepts and performs io requests, callers will be notified by
// callback. IO type and arguments are similar to Linux AIO.
enum iocb_cmd {
    IOCB_CMD_PREAD = 0,
    IOCB_CMD_PWRITE = 1,
    IOCB_CMD_FSYNC = 2,
    IOCB_CMD_FDSYNC = 3,
    /* These two are experimental.
     * IOCB_CMD_PREADX = 4,
     * IOCB_CMD_POLL = 5,
     */
    IOCB_CMD_NOOP = 6,
    IOCB_CMD_PREADV = 7,
    IOCB_CMD_PWRITEV = 8,
};

struct io_event_t {
    void* data;
    void* obj;
    uint64_t res;
    uint64_t res2;
};

struct iovec_t {
    void* iov_base;
    size_t iov_len;
};

using AIOCallback = std::function<void(io_event_t&)>;

// The IOExecutor would accept IO read/write requests.
// After the user implements an IOExecutor, he should associate
// the IOExecutor with the corresponding Executor implementation.
class IOExecutor {
public:
    using Func = std::function<void()>;

    IOExecutor() {}
    virtual ~IOExecutor() {}

    IOExecutor(const IOExecutor&) = delete;
    IOExecutor& operator=(const IOExecutor&) = delete;

public:
    virtual void submitIO(int fd, iocb_cmd cmd, void* buffer, size_t length,
                          off_t offset, AIOCallback cbfn) = 0;
    virtual void submitIOV(int fd, iocb_cmd cmd, const iovec_t* iov,
                           size_t count, off_t offset, AIOCallback cbfn) = 0;
};

}  // namespace async_simple

#endif
