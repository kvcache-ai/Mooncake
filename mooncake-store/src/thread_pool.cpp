// ThreadPool.cpp
#include "thread_pool.h"
#include <stdexcept>
namespace mooncake {
ThreadPool::ThreadPool(size_t num_threads) : stop_flag(false) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    this->condition.wait(lock, [this] {
                        return this->stop_flag || !this->tasks.empty();
                    });

                    if (this->stop_flag && this->tasks.empty()) return;

                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                task();
            }
        });
    }
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop_flag = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers) {
        if (worker.joinable()) worker.join();
    }
}

ThreadPool::~ThreadPool() { stop(); }
}  // namespace mooncake