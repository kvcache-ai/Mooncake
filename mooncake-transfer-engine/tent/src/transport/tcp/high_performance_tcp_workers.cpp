// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transport/tcp/high_performance_tcp_workers.h"

#include <exception>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers()
    : HighPerformanceTcpWorkers(Config{}) {}

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers(Config config)
    : config_(config) {}

HighPerformanceTcpWorkers::~HighPerformanceTcpWorkers() { stop(); }

Status HighPerformanceTcpWorkers::start() {
    std::lock_guard<std::mutex> lifecycle_guard(lifecycle_mutex_);
    if (running_.load(std::memory_order_acquire)) return Status::OK();
    if (started_) {
        return Status::InvalidArgument(
            "High-performance TCP workers cannot be restarted" LOC_MARK);
    }
    if (config_.worker_count == 0 || config_.queue_capacity == 0) {
        return Status::InvalidArgument(
            "High-performance TCP worker_count and queue_capacity must be "
            "positive" LOC_MARK);
    }

    workers_.reserve(config_.worker_count);
    for (size_t worker_id = 0; worker_id < config_.worker_count; ++worker_id) {
        workers_.push_back(std::make_unique<WorkerContext>());
    }

    running_.store(true, std::memory_order_release);
    started_ = true;
    for (size_t worker_id = 0; worker_id < config_.worker_count; ++worker_id) {
        workers_[worker_id]->thread =
            std::thread([this, worker_id] { workerLoop(worker_id); });
    }
    return Status::OK();
}

Status HighPerformanceTcpWorkers::stop() {
    {
        std::lock_guard<std::mutex> lifecycle_guard(lifecycle_mutex_);
        if (!running_.exchange(false, std::memory_order_acq_rel)) {
            return Status::OK();
        }
        for (const auto& worker : workers_) {
            {
                std::lock_guard<std::mutex> worker_guard(worker->mutex);
                worker->stopping = true;
            }
            worker->cv.notify_one();
        }
    }

    // Workers drain already-admitted tasks before exit. That is required for
    // the future transport: a batch may be freed only after its terminal task
    // notifications are produced.
    for (const auto& worker : workers_) {
        if (worker->thread.joinable()) worker->thread.join();
    }
    return Status::OK();
}

Status HighPerformanceTcpWorkers::submit(Task task) {
    if (!task) {
        return Status::InvalidArgument(
            "Cannot submit an empty high-performance TCP task" LOC_MARK);
    }
    if (!running_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "High-performance TCP workers are not running" LOC_MARK);
    }

    const size_t first = next_worker_.fetch_add(1, std::memory_order_relaxed) %
                         config_.worker_count;
    for (size_t offset = 0; offset < config_.worker_count; ++offset) {
        const size_t worker_id = (first + offset) % config_.worker_count;
        auto status = enqueue(worker_id, task);
        if (status.ok() || !status.IsTooManyRequests()) return status;
    }
    return Status::TooManyRequests(
        "All high-performance TCP worker queues are full" LOC_MARK);
}

Status HighPerformanceTcpWorkers::submitToWorker(size_t worker_id, Task task) {
    if (!task) {
        return Status::InvalidArgument(
            "Cannot submit an empty high-performance TCP task" LOC_MARK);
    }
    if (!running_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "High-performance TCP workers are not running" LOC_MARK);
    }
    if (worker_id >= workers_.size()) {
        return Status::InvalidArgument(
            "High-performance TCP worker id is out of range" LOC_MARK);
    }
    return enqueue(worker_id, std::move(task));
}

Status HighPerformanceTcpWorkers::enqueue(size_t worker_id, Task task) {
    auto& worker = *workers_[worker_id];
    {
        std::lock_guard<std::mutex> worker_guard(worker.mutex);
        if (worker.stopping) {
            return Status::InternalError(
                "High-performance TCP worker is stopping" LOC_MARK);
        }
        if (worker.queue.size() >= config_.queue_capacity) {
            return Status::TooManyRequests(
                "High-performance TCP worker queue is full" LOC_MARK);
        }
        worker.queue.push(std::move(task));
    }
    worker.cv.notify_one();
    return Status::OK();
}

void HighPerformanceTcpWorkers::workerLoop(size_t worker_id) {
    auto& worker = *workers_[worker_id];
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> worker_guard(worker.mutex);
            worker.cv.wait(worker_guard, [&] {
                return worker.stopping || !worker.queue.empty();
            });
            if (worker.queue.empty()) {
                if (worker.stopping) return;
                continue;
            }
            task = std::move(worker.queue.front());
            worker.queue.pop();
        }

        try {
            task(worker_id);
        } catch (const std::exception& error) {
            LOG(ERROR) << "High-performance TCP worker " << worker_id
                       << " task failed: " << error.what();
        } catch (...) {
            LOG(ERROR) << "High-performance TCP worker " << worker_id
                       << " task failed with a non-standard exception";
        }
    }
}

}  // namespace tent
}  // namespace mooncake
