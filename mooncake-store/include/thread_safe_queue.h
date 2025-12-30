#pragma once

#ifndef MOONCAKE_THREAD_SAFE_QUEUE_H
#define MOONCAKE_THREAD_SAFE_QUEUE_H

#include <deque>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <atomic>
#include <vector>
#include <type_traits>

namespace mooncake {
/**
 * @brief Thread-safe queue implementation with bounded capacity and shutdown
 * mechanism
 *
 * This template class provides a producer-consumer queue that can be safely
 * accessed from multiple threads. It supports both blocking and timeout-based
 * operations, and includes a shutdown mechanism to gracefully terminate waiting
 * threads.
 *
 * Optimized for MPSC (Multiple Producers, Single Consumer) scenarios with
 * atomic size tracking to reduce lock contention.
 *
 * @note Type T must be nothrow move constructible or copy constructible
 *       for exception safety in batch operations. Uses std::deque as the
 *       underlying container to support efficient random access for
 *       peek operations.
 *
 * @tparam T Type of elements stored in the queue
 */
template <typename T>
class ThreadSafeQueue {
    // Static assertion for exception safety requirements
    static_assert(
        std::is_nothrow_move_constructible_v<T> ||
            std::is_copy_constructible_v<T>,
        "Type T must be nothrow move constructible or copy constructible "
        "for exception safety in batch operations");

   public:
    /**
     * @brief Construct a new ThreadSafeQueue with specified maximum capacity
     * @param max_size Maximum number of elements the queue can hold
     */
    explicit ThreadSafeQueue(size_t max_size = 100000) : max_size_(max_size) {}

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
     * @return true if item was successfully pushed, false on timeout or
     * shutdown
     */
    bool push(T item, std::chrono::milliseconds timeout);

    /**
     * @brief Attempt to push an item into the queue without blocking
     *
     * This method attempts to push an item immediately without waiting.
     * It returns immediately with the result of the attempt.
     *
     * @param item Item to be pushed
     * @return true if item was successfully pushed, false if queue is full or
     * shutdown
     */
    bool try_push(T item);

    /**
     * @brief Pop an item from the queue (blocking)
     * @return std::optional containing the popped item, or std::nullopt if
     * queue is shutdown
     */
    std::optional<T> pop();

    /**
     * @brief Pop an item from the queue with timeout
     * @param timeout Maximum time to wait for pop operation
     * @return std::optional containing the popped item, or std::nullopt on
     * timeout or shutdown
     */
    std::optional<T> pop(std::chrono::milliseconds timeout);

    /**
     * @brief Batch pop items from the queue
     * @param max_batch_size Maximum number of items to pop
     * @param timeout Maximum time to wait for at least one item
     * @return Vector of popped items, std::nullopt if timeout or shutdown
     */
    std::optional<std::vector<T>> pop_batch(size_t max_batch_size,
                                            std::chrono::milliseconds timeout);

    /**
     * @brief Non-blocking batch pop
     * @param max_batch_size Maximum number of items to pop
     * @return Vector of popped items, empty if queue is empty
     */
    std::optional<std::vector<T>> try_pop_batch(size_t max_batch_size);

    /**
     * @brief Get the first max_batch_size elements from the queue without
     * removing them
     *
     * This method returns a copy of the first max_batch_size elements in the
     * queue without popping. The operation is thread-safe and efficient due to
     * the use of std::deque as the underlying container, which supports random
     * access.
     *
     * In MPSC scenarios, producers are only blocked for the minimal time
     * needed to create a copy of the required elements.
     *
     * @param max_batch_size Number of elements to retrieve. If
     * max_batch_size=0, returns std::nullopt.
     * @return std::optional<std::vector<T>> containing the first max_batch_size
     * elements, or std::nullopt if the queue is empty
     */
    std::optional<std::vector<T>> peek_batch(size_t max_batch_size) const;

    /**
     * @brief Get elements at specific positions in the queue without removing
     * them
     *
     * This method returns copies of elements at the specified positions
     * without removing them. Positions are 0-based from the front of the queue.
     * The operation is thread-safe and efficient with std::deque.
     *
     * @param positions Vector of 0-based positions to peek
     * @return std::vector<std::optional<T>> containing the elements at the
     *         requested positions, with nullopt for invalid positions
     */
    std::vector<std::optional<T>> peek_at(
        const std::vector<size_t>& positions) const;

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
        return approximate_size_.load(std::memory_order_acquire);
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
    size_t capacity() const { return max_size_; }

    /**
     * @brief Shutdown the queue and wake up all waiting threads
     *
     * After shutdown, all push operations will fail immediately, while pop
     * operations will continue to return remaining elements until the queue is
     * empty.
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
    template <bool UseCopy>
    std::vector<T> batch_pop_impl(size_t max_batch_size, size_t original_size,
                                  bool& was_full);

    // Helper to check if we should proceed with push operation
    bool should_proceed_with_push() const {
        // For push operations, we should not proceed if shutdown
        return !shutdown_.load(std::memory_order_acquire);
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> deque_;
    size_t max_size_;
    std::atomic<bool> shutdown_{false};
    std::atomic<size_t> approximate_size_{0};
};

// Implementation
template <typename T>
bool ThreadSafeQueue<T>::push(T item) {
    if (!should_proceed_with_push()) {
        return false;
    }

    std::unique_lock lock(mutex_);
    not_full_.wait(lock, [this] {
        return deque_.size() < max_size_ ||
               shutdown_.load(std::memory_order_acquire);
    });

    if (shutdown_.load(std::memory_order_acquire)) {
        return false;
    }

    bool was_empty = deque_.empty();
    deque_.push_back(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();

    if (was_empty) {
        not_empty_.notify_one();
    }
    return true;
}

template <typename T>
bool ThreadSafeQueue<T>::push(T item, std::chrono::milliseconds timeout) {
    if (!should_proceed_with_push()) {
        return false;
    }

    std::unique_lock lock(mutex_);
    if (!not_full_.wait_for(lock, timeout, [this] {
            return deque_.size() < max_size_ ||
                   shutdown_.load(std::memory_order_acquire);
        })) {
        return false;
    }

    if (shutdown_.load(std::memory_order_acquire)) {
        return false;
    }

    bool was_empty = deque_.empty();
    deque_.push_back(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();

    if (was_empty) {
        not_empty_.notify_one();
    }
    return true;
}

template <typename T>
bool ThreadSafeQueue<T>::try_push(T item) {
    if (!should_proceed_with_push()) {
        return false;
    }

    if (approximate_size_.load(std::memory_order_acquire) >= max_size_) {
        return false;
    }

    std::unique_lock lock(mutex_, std::try_to_lock);

    if (!lock.owns_lock() || shutdown_.load(std::memory_order_acquire)) {
        return false;
    }

    if (deque_.size() >= max_size_) {
        return false;
    }

    bool was_empty = deque_.empty();
    deque_.push_back(std::move(item));
    approximate_size_.fetch_add(1, std::memory_order_release);
    lock.unlock();

    if (was_empty) {
        not_empty_.notify_one();
    }

    return true;
}

template <typename T>
std::optional<T> ThreadSafeQueue<T>::pop() {
    std::unique_lock lock(mutex_);
    not_empty_.wait(lock, [this] {
        return !deque_.empty() || shutdown_.load(std::memory_order_acquire);
    });

    if (deque_.empty()) {
        return std::nullopt;
    }

    bool was_full = (deque_.size() == max_size_);
    T item = std::move(deque_.front());
    deque_.pop_front();
    approximate_size_.fetch_sub(1, std::memory_order_release);
    lock.unlock();

    if (was_full) {
        not_full_.notify_one();
    }
    return item;
}

template <typename T>
std::optional<T> ThreadSafeQueue<T>::pop(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout, [this] {
            return !deque_.empty() || shutdown_.load(std::memory_order_acquire);
        })) {
        return std::nullopt;
    }

    if (deque_.empty()) {
        return std::nullopt;
    }

    bool was_full = (deque_.size() == max_size_);
    T item = std::move(deque_.front());
    deque_.pop_front();
    approximate_size_.fetch_sub(1, std::memory_order_release);
    lock.unlock();

    if (was_full) {
        not_full_.notify_one();
    }
    return item;
}

template <typename T>
template <bool UseCopy>
std::vector<T> ThreadSafeQueue<T>::batch_pop_impl(size_t max_batch_size,
                                                  size_t original_size,
                                                  bool& was_full) {
    const size_t count = std::min(max_batch_size, original_size);
    was_full = (original_size == max_size_);

    if (count == 0) {
        return {};
    }

    std::vector<T> batch;
    batch.reserve(count);

    auto begin = deque_.begin();
    auto end = begin;
    std::advance(end, count);

    if constexpr (UseCopy) {
        batch.insert(batch.end(), begin, end);
    } else {
        batch.insert(batch.end(), std::make_move_iterator(begin),
                     std::make_move_iterator(end));
    }

    approximate_size_.fetch_sub(count, std::memory_order_release);

    deque_.erase(begin, end);
    return batch;
}

template <typename T>
std::optional<std::vector<T>> ThreadSafeQueue<T>::pop_batch(
    size_t max_batch_size, std::chrono::milliseconds timeout) {
    if (max_batch_size == 0) {
        return std::nullopt;
    }

    std::unique_lock lock(mutex_);

    if (!not_empty_.wait_for(lock, timeout, [this] {
            return !deque_.empty() || shutdown_.load(std::memory_order_acquire);
        })) {
        return std::nullopt;
    }

    if (deque_.empty()) {
        return std::nullopt;
    }

    const size_t original_size = deque_.size();
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

template <typename T>
std::optional<std::vector<T>> ThreadSafeQueue<T>::try_pop_batch(
    size_t max_batch_size) {
    if (max_batch_size == 0) {
        return std::nullopt;
    }

    std::unique_lock lock(mutex_);

    if (deque_.empty()) {
        return std::nullopt;
    }

    const size_t original_size = deque_.size();
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

template <typename T>
std::optional<std::vector<T>> ThreadSafeQueue<T>::peek_batch(
    size_t max_batch_size) const {
    if (max_batch_size == 0) {
        return std::nullopt;
    }

    if (approximate_size_.load(std::memory_order_acquire) == 0) {
        return std::nullopt;
    }

    std::unique_lock lock(mutex_);

    if (deque_.empty()) {
        return std::nullopt;
    }

    const size_t count = std::min(max_batch_size, deque_.size());

    return std::make_optional<std::vector<T>>(deque_.begin(),
                                              deque_.begin() + count);
}

template <typename T>
std::vector<std::optional<T>> ThreadSafeQueue<T>::peek_at(
    const std::vector<size_t>& positions) const {
    std::vector<std::optional<T>> result;

    std::unique_lock lock(mutex_);

    if (deque_.empty()) {
        result.resize(positions.size());
        return result;
    }

    const size_t deque_size = deque_.size();

    result.reserve(positions.size());

    for (size_t pos : positions) {
        if (pos < deque_size) {
            result.emplace_back(deque_[pos]);
        } else {
            result.emplace_back(std::nullopt);
        }
    }

    return result;
}

template <typename T>
size_t ThreadSafeQueue<T>::size() const {
    std::lock_guard lock(mutex_);
    return deque_.size();
}

template <typename T>
bool ThreadSafeQueue<T>::empty() const {
    if (approximate_size_.load(std::memory_order_acquire) > 0) {
        return false;
    }

    std::lock_guard lock(mutex_);
    return deque_.empty();
}

template <typename T>
void ThreadSafeQueue<T>::shutdown() {
    shutdown_.store(true, std::memory_order_release);

    {
        std::lock_guard lock(mutex_);
        not_empty_.notify_all();
        not_full_.notify_all();
    }
}

template <typename T>
bool ThreadSafeQueue<T>::is_shutdown() const {
    return shutdown_.load(std::memory_order_acquire);
}

template <typename T>
bool ThreadSafeQueue<T>::is_full() const {
    std::lock_guard lock(mutex_);
    return deque_.size() >= max_size_;
}

}  // namespace mooncake

#endif  // MOONCAKE_THREAD_SAFE_QUEUE_H