// ThreadPool.h
#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
namespace mooncake {
class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    template<class F, class... Args>
    void enqueue(F&& f, Args&&... args);

    void stop();

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop_flag;
};

template<class F, class... Args>
void ThreadPool::enqueue(F&& f, Args&&... args) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if(stop_flag) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }
        tasks.emplace([=] { 
            std::invoke(f, args...); 
        });
    }
    condition.notify_one();
}
}

#endif // THREAD_POOL_H