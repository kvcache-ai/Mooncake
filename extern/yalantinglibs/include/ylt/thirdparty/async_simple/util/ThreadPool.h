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
/* The file implements a simple thread pool. The scheduling strategy is a simple
 * random strategy. Simply, ThreadPool would create n threads. And for a task
 * which is waiting to be scheduled. The ThreadPool would choose a thread for
 * this task randomly.
 *
 * The purpose of ThreadPool is for testing. People who want to use async_simple
 * in actual development should implement/use more complex executor.
 */
#ifndef FUTURE_THREAD_POOL_H
#define FUTURE_THREAD_POOL_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>

#include "async_simple/util/Queue.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple::util {
class ThreadPool {
public:
    struct WorkItem {
        // Whether or not do work steal for fn.
        // If the user don't assign a thread to fn,
        // thread pool will apply random policy to fn.
        // If enable canSteal,
        // thread pool will apply work steal policy firstly , if failed, will
        // apply random policy to fn.
        bool canSteal = false;
        std::function<void()> fn = nullptr;
    };

    enum ERROR_TYPE {
        ERROR_NONE = 0,
        ERROR_POOL_HAS_STOP,
        ERROR_POOL_ITEM_IS_NULL,
    };

    explicit ThreadPool(size_t threadNum = std::thread::hardware_concurrency(),
                        bool enableWorkSteal = false,
                        bool enableCoreBindings = false);
    ~ThreadPool();

    ThreadPool::ERROR_TYPE scheduleById(std::function<void()> fn,
                                        int32_t id = -1);
    int32_t getCurrentId() const;
    size_t getItemCount() const;
    int32_t getThreadNum() const { return _threadNum; }

private:
    std::pair<size_t, ThreadPool *> *getCurrent() const;
    int32_t _threadNum;

    std::vector<Queue<WorkItem>> _queues;
    std::vector<std::thread> _threads;

    std::atomic<bool> _stop;
    bool _enableWorkSteal;
    bool _enableCoreBindings;
};

#ifdef __linux__
inline void getCurrentCpus(std::vector<uint32_t> &ids) {
    cpu_set_t set;
    ids.clear();
    if (sched_getaffinity(0, sizeof(set), &set) == 0)
        for (uint32_t i = 0; i < CPU_SETSIZE; i++)
            if (CPU_ISSET(i, &set))
                ids.emplace_back(i);
}
#endif

inline ThreadPool::ThreadPool(size_t threadNum, bool enableWorkSteal,
                              bool enableCoreBindings)
    : _threadNum(threadNum ? threadNum : std::thread::hardware_concurrency()),
      _queues(_threadNum),
      _stop(false),
      _enableWorkSteal(enableWorkSteal),
      _enableCoreBindings(enableCoreBindings) {
    auto worker = [this](size_t id) {
        auto current = getCurrent();
        current->first = id;
        current->second = this;
        while (true) {
            WorkItem workerItem = {};
            if (_enableWorkSteal) {
                // Try to do work steal firstly.
                for (auto n = 0; n < _threadNum * 2; ++n) {
                    if (_queues[(id + n) % _threadNum].try_pop_if(
                            workerItem,
                            [](auto &item) { return item.canSteal; }))
                        break;
                }
            }

            if (!workerItem.fn && !_queues[id].pop(workerItem)) {
                // If thread is going to stop, don't wait for any new task any
                // more. Otherwise wait for a pop task if _enableWorkSteal false
                // or work steal failed,
                if (_stop)
                    break;
                else
                    continue;
            }

            if (workerItem.fn)
                workerItem.fn();
        }
    };

    _threads.reserve(_threadNum);

#ifdef __linux__
    // Since the CPU IDs might not start at 0 and might not be continuous
    // in the containers,
    // we need to get the available cpus at first.
    std::vector<uint32_t> cpu_ids;
    if (_enableCoreBindings)
        getCurrentCpus(cpu_ids);
#else
    // Avoid unused member warning.
    // [[maybe_unused]] in non-static data members is ignored in GCC.
    (void)_enableCoreBindings;
#endif

    for (auto i = 0; i < _threadNum; ++i) {
        _threads.emplace_back(worker, i);

#ifdef __linux__
        if (!_enableCoreBindings)
            continue;

        // Run threads per core.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_ids[i % cpu_ids.size()], &cpuset);
        int rc = sched_setaffinity(_threads[i].native_handle(),
                                   sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
            std::cerr << "Error calling sched_setaffinity: " << rc << "\n";
#endif
    }
}

inline ThreadPool::~ThreadPool() {
    _stop = true;
    for (auto &queue : _queues)
        queue.stop();
    for (auto &thread : _threads)
        thread.join();
}

inline ThreadPool::ERROR_TYPE ThreadPool::scheduleById(std::function<void()> fn,
                                                       int32_t id) {
    if (nullptr == fn) {
        return ERROR_POOL_ITEM_IS_NULL;
    }

    if (_stop) {
        return ERROR_POOL_HAS_STOP;
    }

    if (id == -1) {
        if (_enableWorkSteal) {
            // Try to push to a non-block queue firstly.
            WorkItem workerItem{/*canSteal = */ true, fn};
            for (auto n = 0; n < _threadNum * 2; ++n) {
                if (_queues.at(n % _threadNum).try_push(workerItem))
                    return ERROR_NONE;
            }
        }

        id = std::rand() % _threadNum;
        _queues[id].push(
            WorkItem{/*canSteal = */ _enableWorkSteal, std::move(fn)});
    } else {
        assert(id < _threadNum);
        _queues[id].push(WorkItem{/*canSteal = */ false, std::move(fn)});
    }

    return ERROR_NONE;
}

inline std::pair<size_t, ThreadPool *> *ThreadPool::getCurrent() const {
    static thread_local std::pair<size_t, ThreadPool *> current(-1, nullptr);
    return &current;
}

inline int32_t ThreadPool::getCurrentId() const {
    auto current = getCurrent();
    if (this == current->second) {
        return current->first;
    }
    return -1;
}

inline size_t ThreadPool::getItemCount() const {
    size_t ret = 0;
    for (auto i = 0; i < _threadNum; ++i) {
        ret += _queues[i].size();
    }
    return ret;
}
}  // namespace async_simple::util

#endif  // FUTURE_THREAD_POOL_H
