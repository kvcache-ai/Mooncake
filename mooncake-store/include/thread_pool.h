#pragma once
#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

namespace mooncake {


class ThreadPool {
   public:
    ThreadPool(size_t);
    template <class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();
    void exit();
    void clear();

    private:
    // Need to keep track of threads so we can join them
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    // Synchronization
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool stop_;
};

// The constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads) : stop_(false) {
    assert(threads >= 1);
    for (size_t i = 0; i < threads; ++i)
    workers_.emplace_back([this] {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(this->queue_mutex_);
                this->condition_.wait(
                    lock, [this] {
                        return this->stop_ || !this->tasks_.empty(); });
                if (this->stop_ && this->tasks_.empty()) return;
                task = std::move(this->tasks_.front());
                this->tasks_.pop();
            }

            try {
                task();
            } catch (const std::exception& e) {
                // Handle or log the exception
            } catch (...) {
                // Handle or log unknown exceptions
            }
        }
    });
}

// Add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()> >(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        if (stop_) throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks_.emplace([task]() { (*task)(); });
    }
    condition_.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() { exit(); }

inline void ThreadPool::clear() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (!tasks_.empty()) {
        tasks_.pop();
    }
}

inline void ThreadPool::exit() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    condition_.notify_all();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

}  // namespace mooncake
