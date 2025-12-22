#pragma once
#ifndef MOONCAKE_THREADSAFEQUEUE_H
#define MOONCAKE_THREADSAFEQUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <atomic>
#include <vector>
#include <type_traits>


namespace mooncake {
/**
 * @brief Thread-safe queue implementation with bounded capacity and shutdown mechanism
 *
 * This template class provides a producer-consumer queue that can be safely accessed
 * from multiple threads. It supports both blocking and timeout-based operations,
 * and includes a shutdown mechanism to gracefully terminate waiting threads.
 *
 * Optimized for MPSC (Multiple Producers, Single Consumer) scenarios with
 * atomic size tracking to reduce lock contention.
 *
 * @note Type T must be nothrow move constructible or copy constructible
 *       for exception safety in batch operations.
 *
 * @tparam T Type of elements stored in the queue
 */
template<typename T>
class ThreadSafeQueue {
    // Static assertion for exception safety requirements
    static_assert(std::is_nothrow_move_constructible_v<T> || 
                  std::is_copy_constructible_v<T>,
                  "Type T must be nothrow move constructible or copy constructible "
                  "for exception safety in batch operations");

public:
    /**
     * @brief Construct a new ThreadSafeQueue with specified maximum capacity
     * @param max_size Maximum number of elements the queue can hold
     */
    explicit ThreadSafeQueue(size_t max_size = 100000) 
        : max_size_(max_size) {}

    /**
     * @brief Push an item into the queue (blocking)
     * @param item Item to be pushed
     * @return true if item was successfully pushed, false if queue is shutdown
     */
    bool push(T item);

    /**
     * @brief Push an item into the queue with timeout
     * @param item Item to be pushed
     * @param timeout Maximum time to wait for push operation
     * @return true if item was successfully pushed, false on timeout or shutdown
     */
    bool push(T item, std::chrono::milliseconds timeout);

    /**
     * @brief Attempt to push an item into the queue without blocking
     * 
     * This method attempts to push an item immediately without waiting.
     * It returns immediately with the result of the attempt.
     * 
     * @param item Item to be pushed
     * @return true if item was successfully pushed, false if queue is full or shutdown
     */
    bool try_push(T item);

    /**
     * @brief Pop an item from the queue (blocking)
     * @return std::optional containing the popped item, or std::nullopt if queue is shutdown
     */
    std::optional<T> pop();

    /**
     * @brief Pop an item from the queue with timeout
     * @param timeout Maximum time to wait for pop operation
     * @return std::optional containing the popped item, or std::nullopt on timeout or shutdown
     */
    std::optional<T> pop(std::chrono::milliseconds timeout);

    /**
     * @brief Batch pop items from the queue
     * @param max_batch_size Maximum number of items to pop
     * @param timeout Maximum time to wait for at least one item
     * @return Vector of popped items, empty if timeout or shutdown
     */
    std::optional<std::vector<T>> pop_batch(size_t max_batch_size, std::chrono::milliseconds timeout);
    
    /**
     * @brief Non-blocking batch pop
     * @param max_batch_size Maximum number of items to pop
     * @return Vector of popped items, empty if queue is empty
     */
    std::optional<std::vector<T>> try_pop_batch(size_t max_batch_size);

    /**
     * @brief Check if the queue is empty
     * @return true if queue contains no elements, false otherwise
     */
    bool empty() const;

    /**
     * @brief Get approximate queue size without locking
     * 
     * This method provides fast size approximation using atomic operations.
     * In MPSC scenarios, this provides good accuracy with minimal contention.
     * 
     * @return Approximate number of elements in the queue
     */
    size_t size_approx() const { 
        // Use relaxed memory order as this is an approximate value
        return approximate_size_.load(std::memory_order_relaxed); 
    }

    /**
    * @brief Get the exact number of elements in the queue
    * 
    * This method acquires a lock to get the exact size of the queue.
    * It is safe to call this method after shutdown.
    * 
    * @return Exact number of elements in the queue
    */
    size_t size() const;

    /**
    * @brief Get the maximum capacity of the queue
    * 
    * This method returns the maximum number of elements the queue can hold.
    * The value is set during construction and remains constant throughout
    * the lifetime of the queue.
    * 
    * @return Maximum capacity of the queue
    */
    size_t capacity() const { 
        return max_size_; 
    }

    /**
     * @brief Shutdown the queue and wake up all waiting threads
     *
     * After shutdown, all push operations will fail immediately, while pop operations
     * will continue to return remaining elements until the queue is empty.
     */
    void shutdown();

    /**
     * @brief Check if the queue has been shutdown
     * @return true if queue is shutdown, false otherwise
     */
    bool is_shutdown() const;

    /**
     * @brief Check if the queue is full
     * @return true if queue is at maximum capacity, false otherwise
     */
    bool is_full() const;

private:
    // Helper methods for batch operations
    template<bool UseCopy>
    std::vector<T> batch_pop_impl(size_t max_batch_size, size_t original_size, bool& was_full);
    
    // Helper to check if we should proceed with push operation
    bool should_proceed_with_push() const {
        // For push operations, we should not proceed if shutdown
        return !shutdown_.load(std::memory_order_acquire);
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::queue<T> queue_;
    size_t max_size_;
    std::atomic<bool> shutdown_{false};
    std::atomic<size_t> approximate_size_{0};
};

// Implementation
template<typename T>
bool ThreadSafeQueue<T>::push(T item)
{
    if (!should_proceed_with_push()) {
        return false;
    }
    
    std::unique_lock lock(mutex_);
    not_full_.wait(lock, [this] {
        return queue_.size() < max_size_ || shutdown_.load(std::memory_order_relaxed);
    });

    if (shutdown_.load(std::memory_order_relaxed)) {
        return false;
    }

    bool was_empty = queue_.empty();
    queue_.push(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();

    if (was_empty) {
        not_empty_.notify_one();
    }
    return true;
}

template<typename T>
bool ThreadSafeQueue<T>::push(T item, std::chrono::milliseconds timeout)
{
    if (!should_proceed_with_push()) {
        return false;
    }
    
    std::unique_lock lock(mutex_);
    if (!not_full_.wait_for(lock, timeout, [this] {
        return queue_.size() < max_size_ || shutdown_.load(std::memory_order_relaxed);
    })) {
        return false;
    }

    if (shutdown_.load(std::memory_order_relaxed)) {
        return false;
    }

    bool was_empty = queue_.empty();
    queue_.push(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();

    if (was_empty) {
        not_empty_.notify_one();
    }
    return true;
}

template<typename T>
bool ThreadSafeQueue<T>::try_push(T item)
{
    if (!should_proceed_with_push()) {
        return false;
    }
    
    if (approximate_size_.load(std::memory_order_relaxed) >= max_size_) {
        return false;
    }

    std::unique_lock lock(mutex_, std::try_to_lock);
    
    if (!lock.owns_lock() || shutdown_.load(std::memory_order_relaxed)) {
        return false;
    }
    
    if (queue_.size() >= max_size_) {
        return false;
    }
    
    bool was_empty = queue_.empty();
    queue_.push(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();
    
    if (was_empty) {
        not_empty_.notify_one();
    }
    
    return true;
}

template<typename T>
std::optional<T> ThreadSafeQueue<T>::pop() {
    std::unique_lock lock(mutex_);
    not_empty_.wait(lock, [this] {
        return !queue_.empty() || shutdown_.load(std::memory_order_relaxed);
    });

    if (queue_.empty()) {
        return std::nullopt;
    }

    bool was_full = (queue_.size() == max_size_);
    T item = std::move(queue_.front());
    queue_.pop();
    approximate_size_.fetch_sub(1, std::memory_order_release);
    lock.unlock();

    if (was_full) {
        not_full_.notify_one();
    }
    return item;
}

template<typename T>
std::optional<T> ThreadSafeQueue<T>::pop(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout, [this] {
        return !queue_.empty() || shutdown_.load(std::memory_order_relaxed);
    })) {
        return std::nullopt;
    }

    if (queue_.empty()) {
        return std::nullopt;
    }

    bool was_full = (queue_.size() == max_size_);
    T item = std::move(queue_.front());
    queue_.pop();
    approximate_size_.fetch_sub(1, std::memory_order_release);
    lock.unlock();

    if (was_full) {
        not_full_.notify_one();
    }
    return item;
}

template<typename T>
template<bool UseCopy>
std::vector<T> ThreadSafeQueue<T>::batch_pop_impl(size_t max_batch_size, 
                                                 size_t original_size, 
                                                 bool& was_full) {
    std::vector<T> batch;
    batch.reserve(std::min(max_batch_size, original_size));
    was_full = (original_size == max_size_);
    
    size_t popped_count = 0;
    
    while (!queue_.empty() && batch.size() < max_batch_size) {
        if constexpr (UseCopy) {
            batch.emplace_back(queue_.front());
        } else {
            batch.emplace_back(std::move(queue_.front()));
        }
        queue_.pop();
        ++popped_count;
    }
    
    if (popped_count > 0) {
        approximate_size_.fetch_sub(popped_count, std::memory_order_release);
    }
    
    return batch;
}

template<typename T>
std::optional<std::vector<T>> ThreadSafeQueue<T>::pop_batch(
    size_t max_batch_size, std::chrono::milliseconds timeout) {
    
    if (max_batch_size == 0) {
        return std::nullopt;
    }

    std::unique_lock lock(mutex_);
    
    if (!not_empty_.wait_for(lock, timeout, [this] {
        return !queue_.empty() || shutdown_.load(std::memory_order_relaxed);
    })) {
        return std::nullopt;
    }
    
    if (queue_.empty()) {
        return std::nullopt;
    }
    
    const size_t original_size = queue_.size();
    bool was_full = false;
    
    std::vector<T> batch;
    if constexpr (std::is_nothrow_move_constructible_v<T>) {
        batch = batch_pop_impl<false>(max_batch_size, original_size, was_full);
    } else {
        batch = batch_pop_impl<true>(max_batch_size, original_size, was_full);
    }
    
    lock.unlock();
    
    if (was_full) {
        not_full_.notify_all();
    }
    
    if (!batch.empty()) {
        return batch;
    }
    
    return std::nullopt;
}

template<typename T>
std::optional<std::vector<T>> ThreadSafeQueue<T>::try_pop_batch(size_t max_batch_size) {
    
    if (max_batch_size == 0) {
        return std::nullopt;
    }

    std::unique_lock lock(mutex_);
    
    if (queue_.empty()) {
        return std::nullopt;
    }
    
    const size_t original_size = queue_.size();
    bool was_full = false;
    
    std::vector<T> batch;
    if constexpr (std::is_nothrow_move_constructible_v<T>) {
        batch = batch_pop_impl<false>(max_batch_size, original_size, was_full);
    } else {
        batch = batch_pop_impl<true>(max_batch_size, original_size, was_full);
    }
    
    lock.unlock();
    
    if (was_full) {
        not_full_.notify_all();
    }
    
    if (!batch.empty()) {
        return batch;
    }
    
    return std::nullopt;
}

template<typename T>
size_t ThreadSafeQueue<T>::size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

template<typename T>
bool ThreadSafeQueue<T>::empty() const {
    if (approximate_size_.load(std::memory_order_acquire) > 0) {
        return false;
    }

    std::lock_guard lock(mutex_);
    return queue_.empty();
}

template<typename T>
void ThreadSafeQueue<T>::shutdown() {
    shutdown_.store(true, std::memory_order_release);
    
    {
        std::lock_guard lock(mutex_);
        not_empty_.notify_all();
        not_full_.notify_all();
    }
}

template<typename T>
bool ThreadSafeQueue<T>::is_shutdown() const {
    return shutdown_.load(std::memory_order_acquire);
}

template<typename T>
bool ThreadSafeQueue<T>::is_full() const {
    std::lock_guard lock(mutex_);
    return queue_.size() >= max_size_;
}

}  // namespace mooncake

#endif // MOONCAKE_THREADSAFEQUEUE_H