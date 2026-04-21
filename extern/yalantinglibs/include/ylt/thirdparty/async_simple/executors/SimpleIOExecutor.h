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
#ifndef FUTURE_SIMPLE_IO_EXECUTOR_H
#define FUTURE_SIMPLE_IO_EXECUTOR_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/IOExecutor.h"

#if !__has_include(<libaio.h>) && !defined(ASYNC_SIMPLE_HAS_NOT_AIO)
#define ASYNC_SIMPLE_HAS_NOT_AIO
#endif

#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
#include <libaio.h>
#endif
#include <thread>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

namespace executors {

// This is a demo IOExecutor.
//  submitIO and submitIOV should be implemented.
class SimpleIOExecutor : public IOExecutor {
public:
    static constexpr int kMaxAio = 8;

public:
    SimpleIOExecutor() {}
    virtual ~SimpleIOExecutor() {}

    SimpleIOExecutor(const IOExecutor&) = delete;
    SimpleIOExecutor& operator=(const IOExecutor&) = delete;

public:
    class Task {
    public:
        Task(AIOCallback& func) : _func(func) {}
        ~Task() {}

    public:
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        void process(io_event_t& event) { _func(event); }
#endif

    private:
        AIOCallback _func;
    };

public:
    bool init() {
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        auto r = io_setup(kMaxAio, &_ioContext);
        if (r < 0) {
            return false;
        }
        _loopThread = std::thread([this]() mutable { this->loop(); });
        return true;
#else
        return false;
#endif
    }

    void destroy() {
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        _shutdown = true;
        if (_loopThread.joinable()) {
            _loopThread.join();
        }
        io_destroy(_ioContext);
#endif
    }

    void loop() {
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        while (!_shutdown) {
            io_event events[kMaxAio];
            struct timespec timeout = {0, 1000 * 300};
            auto n = io_getevents(_ioContext, 1, kMaxAio, events, &timeout);
            if (n < 0) {
                continue;
            }
            for (auto i = 0; i < n; ++i) {
                auto task = reinterpret_cast<Task*>(events[i].data);
                io_event_t evt{events[i].data, events[i].obj, events[i].res,
                               events[i].res2};
                task->process(evt);
                delete task;
            }
        }
#endif
    }

public:
    void submitIO([[maybe_unused]] int fd, [[maybe_unused]] iocb_cmd cmd,
                  [[maybe_unused]] void* buffer, [[maybe_unused]] size_t length,
                  [[maybe_unused]] off_t offset,
                  [[maybe_unused]] AIOCallback cbfn) override {
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        iocb io;
        memset(&io, 0, sizeof(iocb));
        io.aio_fildes = fd;
        io.aio_lio_opcode = cmd;
        io.u.c.buf = buffer;
        io.u.c.offset = offset;
        io.u.c.nbytes = length;
        io.data = new Task(cbfn);
        struct iocb* iocbs[] = {&io};
        auto r = io_submit(_ioContext, 1, iocbs);
        if (r < 0) {
            auto task = reinterpret_cast<Task*>(iocbs[0]->data);
            io_event_t event;
            event.res = r;
            task->process(event);
            delete task;
            return;
        }
#endif
    }
    void submitIOV([[maybe_unused]] int fd, [[maybe_unused]] iocb_cmd cmd,
                   [[maybe_unused]] const iovec_t* iov,
                   [[maybe_unused]] size_t count, [[maybe_unused]] off_t offset,
                   [[maybe_unused]] AIOCallback cbfn) override {
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
        iocb io;
        memset(&io, 0, sizeof(iocb));
        io.aio_fildes = fd;
        io.aio_lio_opcode = cmd;
        io.u.c.buf = (void*)iov;
        io.u.c.offset = offset;
        io.u.c.nbytes = count;
        io.data = new Task(cbfn);
        struct iocb* iocbs[] = {&io};
        auto r = io_submit(_ioContext, 1, iocbs);
        if (r < 0) {
            auto task = reinterpret_cast<Task*>(iocbs[0]->data);
            io_event_t event;
            event.res = r;
            task->process(event);
            delete task;
            return;
        }
#endif
    }

private:
#ifndef ASYNC_SIMPLE_HAS_NOT_AIO
    volatile bool _shutdown = false;
    io_context_t _ioContext = 0;
#endif
    std::thread _loopThread;
};

}  // namespace executors

}  // namespace async_simple

#endif  // FUTURE_SIMPLE_IO_EXECUTOR_H
