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
#ifndef ASYNC_SIMPLE_EXECUTOR_H
#define ASYNC_SIMPLE_EXECUTOR_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <ratio>
#include <string>
#include <thread>
#include "async_simple/MoveWrapper.h"
#include "async_simple/Signal.h"
#include "async_simple/experimental/coroutine.h"
#include "async_simple/util/move_only_function.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
// Stat information for an executor.
// It contains the number of pending task
// for the executor now.
struct ExecutorStat {
    size_t pendingTaskCount = 0;
    ExecutorStat() = default;
};
// Options for a schedule.
// The option contains:
// - bool prompt. Whether or not this schedule
//   should be prompted.
struct ScheduleOptions {
    bool prompt = true;
    ScheduleOptions() = default;
};

// Awaitable to get the current executor.
// For example:
// ```
//  auto current_executor =
//      co_await CurrentExecutor{};
// ```
struct CurrentExecutor {};

// Executor is a scheduler for functions.
//
// Executor is a key component for scheduling coroutines.
// Considering that there should be already an executor
// in most production-level programs, Executor is designed
// to be able to fit the scheduling strategy in existing programs.
//
// User should derive from Executor and implement their scheduling
// strategy.

class IOExecutor;

class Executor {
public:
    // Context is an identification for the context where an executor
    // should run. See checkin/checkout for details.
    using Context = void *;
    static constexpr Context NULLCTX = nullptr;

    // A time duration in microseconds.
    using Duration = std::chrono::duration<int64_t, std::micro>;

    // The schedulable function. Func should accept no argument and
    // return void.
    using Func = std::function<void()>;
    class TimeAwaitable;
    class TimeAwaiter;

    Executor(std::string name = "default") : _name(std::move(name)) {}
    virtual ~Executor() {}

    Executor(const Executor &) = delete;
    Executor &operator=(const Executor &) = delete;

    // Schedule a function.
    // `schedule` would return false if schedule failed, which means function
    // func will not be executed. In case schedule return true, the executor
    // should guarantee that the func would be executed.
    virtual bool schedule(Func func) = 0;

    // 4-bits priority, less level is more important. Default
    // value of async-simple schedule is DEFAULT. For scheduling level >=
    // YIELD, if executor always execute the work immediately if other
    // works, it may cause dead lock. are waiting.
    enum class Priority {
        HIGHEST = 0x0,
        DEFAULT = 0x7,
        YIELD = 0x8,
        LOWEST = 0xF
    };

    // Low 16-bit of schedule_info is reserved for async-simple, and the lowest
    // 4-bit is stand for priority level. The implementation of scheduling logic
    // isn't necessary, which is determined by implementation. However, to avoid
    // spinlock/yield deadlock, when priority level >= YIELD, scheduler
    // can't always execute the work immediately when other works are
    // waiting.
    virtual bool schedule(Func func, uint64_t schedule_info) {
        return schedule(std::move(func));
    }

    // Schedule a move only functor
    bool schedule_move_only(util::move_only_function<void()> func) {
        MoveWrapper<decltype(func)> tmp(std::move(func));
        return schedule([func = tmp]() { func.get()(); });
    }

    bool schedule_move_only(util::move_only_function<void()> func,
                            uint64_t schedule_info) {
        MoveWrapper<decltype(func)> tmp(std::move(func));
        return schedule([func = tmp]() { func.get()(); }, schedule_info);
    }

    // Return true if caller runs in the executor.
    virtual bool currentThreadInExecutor() const {
        throw std::logic_error("Not implemented");
    }
    virtual ExecutorStat stat() const {
        throw std::logic_error("Not implemented");
    }

    // checkout() return current "Context", which defined by executor
    // implementation, then checkin(func, "Context") should schedule func to the
    // same "Context" as before.
    virtual size_t currentContextId() const { return 0; };
    virtual Context checkout() { return NULLCTX; }
    virtual bool checkin(Func func, [[maybe_unused]] Context ctx,
                         [[maybe_unused]] ScheduleOptions opts) {
        return schedule(std::move(func));
    }
    virtual bool checkin(Func func, Context ctx) {
        static ScheduleOptions opts;
        return checkin(std::move(func), ctx, opts);
    }

    const std::string &name() const { return _name; }

    // Use co_await executor.after(sometime)
    // to schedule current execution after some time.
    TimeAwaitable after(Duration dur);

    // Use co_await executor.after(sometime)
    // to schedule current execution after some time.
    TimeAwaitable after(Duration dur, uint64_t schedule_info,
                        Slot *slot = nullptr);

    // IOExecutor accepts IO read/write requests.
    // Return nullptr if the executor doesn't offer an IOExecutor.
    virtual IOExecutor *getIOExecutor() {
        throw std::logic_error("Not implemented");
    }

protected:
    virtual void schedule(Func func, Duration dur) {
        return schedule(func, dur,
                        static_cast<uint64_t>(Executor::Priority::DEFAULT),
                        nullptr);
    }
    virtual void schedule(Func func, Duration dur, uint64_t schedule_info,
                          Slot *slot = nullptr) {
        std::thread([this, func = std::move(func), dur, slot]() {
            auto promise = std::make_unique<std::promise<void>>();
            auto future = promise->get_future();
            bool hasnt_canceled = signalHelper{Terminate}.tryEmplace(
                slot, [p = std::move(promise)](SignalType, Signal *) {
                    p->set_value();
                });
            if (hasnt_canceled)
                future.wait_for(dur);
            schedule(std::move(func));
        }).detach();
    }

private:
    std::string _name;
};

// Awaiter to implement Executor::after.
class Executor::TimeAwaiter {
public:
    TimeAwaiter(Executor *ex, Executor::Duration dur, uint64_t schedule_info,
                Slot *slot)
        : _ex(ex), _dur(dur), _schedule_info(schedule_info), _slot(slot) {}

public:
    bool await_ready() const noexcept {
        return signalHelper{Terminate}.hasCanceled(_slot);
    }

    template <typename PromiseType>
    void await_suspend(coro::CoroHandle<PromiseType> continuation) {
        _ex->schedule(std::move(continuation), _dur, _schedule_info, _slot);
    }
    void await_resume() {
        signalHelper{Terminate}.checkHasCanceled(
            _slot, "async_simple's timer is canceled!");
    }

private:
    Executor *_ex;
    Executor::Duration _dur;
    uint64_t _schedule_info;
    Slot *_slot;
};

// Awaitable to implement Executor::after.
class Executor::TimeAwaitable {
public:
    TimeAwaitable(Executor *ex, Executor::Duration dur, uint64_t schedule_info,
                  Slot *slot)
        : _ex(ex), _dur(dur), _schedule_info(schedule_info), _slot(slot) {}

    auto coAwait(Executor *) {
        return Executor::TimeAwaiter(_ex, _dur, _schedule_info, _slot);
    }

private:
    Executor *_ex;
    Executor::Duration _dur;
    uint64_t _schedule_info;
    Slot *_slot;
};

Executor::TimeAwaitable inline Executor::after(Executor::Duration dur) {
    return Executor::TimeAwaitable(
        this, dur, static_cast<uint64_t>(Executor::Priority::DEFAULT), nullptr);
}

Executor::TimeAwaitable inline Executor::after(Executor::Duration dur,
                                               uint64_t schedule_info,
                                               Slot *slot) {
    return Executor::TimeAwaitable(this, dur, schedule_info, slot);
}

}  // namespace async_simple

#endif
