#include "data_manager_submitter.h"
#include <glog/logging.h>
#include <algorithm>

namespace mooncake {

DataManagerSubmitter::DataManagerSubmitter(DataManager& data_manager,
                                             size_t thread_pool_size)
    : data_manager_(data_manager),
      thread_pool_size_(thread_pool_size),
      shutdown_(false) {
    // Start worker threads
    worker_threads_.reserve(thread_pool_size_);
    for (size_t i = 0; i < thread_pool_size_; ++i) {
        worker_threads_.emplace_back(&DataManagerSubmitter::WorkerThreadMain,
                                     this);
    }
    LOG(INFO) << "DataManagerSubmitter initialized with " << thread_pool_size_
              << " worker threads";
}

DataManagerSubmitter::~DataManagerSubmitter() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_ = true;
    }
    queue_cv_.notify_all();

    // Wait for all worker threads
    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    LOG(INFO) << "DataManagerSubmitter destroyed";
}

std::future<tl::expected<void, ErrorCode>> DataManagerSubmitter::SubmitPut(
    const std::string& key, std::unique_ptr<char[]> data, size_t size,
    std::optional<UUID> tier_id) {
    auto task = std::make_unique<Task>();
    task->type = TaskType::PUT;
    task->key = key;
    task->put_data = std::move(data);
    task->put_size = size;
    task->tier_id = tier_id;

    auto future = task->void_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();

    return future;
}

std::future<tl::expected<AllocationHandle, ErrorCode>>
DataManagerSubmitter::SubmitGet(const std::string& key,
                                 std::optional<UUID> tier_id) {
    auto task = std::make_unique<Task>();
    task->type = TaskType::GET;
    task->key = key;
    task->tier_id = tier_id;

    auto future = task->handle_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();

    return future;
}

tl::expected<void, ErrorCode> DataManagerSubmitter::SubmitDelete(
    const std::string& key, std::optional<UUID> tier_id) {
    // Directly call DataManager::Delete, not through thread pool
    return data_manager_.Delete(key, tier_id);
}

std::future<tl::expected<void, ErrorCode>>
DataManagerSubmitter::SubmitReadRemoteData(
    const std::string& key,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    auto task = std::make_unique<Task>();
    task->type = TaskType::READ_REMOTE;
    task->key = key;
    task->remote_buffers = dest_buffers;

    auto future = task->void_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();

    return future;
}

std::future<tl::expected<void, ErrorCode>>
DataManagerSubmitter::SubmitWriteRemoteData(
    const std::string& key,
    const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    auto task = std::make_unique<Task>();
    task->type = TaskType::WRITE_REMOTE;
    task->key = key;
    task->remote_buffers = src_buffers;
    task->tier_id = tier_id;

    auto future = task->void_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();

    return future;
}

void DataManagerSubmitter::WorkerThreadMain() {
    while (true) {
        std::unique_ptr<Task> task;

        // Wait for task or shutdown
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !task_queue_.empty() || shutdown_; });

            if (shutdown_ && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        if (task) {
            ProcessTask(std::move(task));
        }
    }
}

void DataManagerSubmitter::ProcessTask(std::unique_ptr<Task> task) {
    // Process task based on type
    try {
        switch (task->type) {
            case TaskType::PUT: {
                auto result = data_manager_.Put(task->key, std::move(task->put_data),
                                                task->put_size, task->tier_id);
                task->void_promise.set_value(result);
                break;
            }
            case TaskType::GET: {
                auto result = data_manager_.Get(task->key, task->tier_id);
                task->handle_promise.set_value(result);
                break;
            }
            case TaskType::READ_REMOTE: {
                auto result = data_manager_.ReadRemoteData(task->key,
                                                           task->remote_buffers);
                task->void_promise.set_value(result);
                break;
            }
            case TaskType::WRITE_REMOTE: {
                auto result = data_manager_.WriteRemoteData(
                    task->key, task->remote_buffers, task->tier_id);
                task->void_promise.set_value(result);
                break;
            }
        }
    } catch (...) {
        // Handle exceptions
        if (task->type == TaskType::GET) {
            task->handle_promise.set_value(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        } else {
            task->void_promise.set_value(
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        }
    }
}


}  // namespace mooncake
