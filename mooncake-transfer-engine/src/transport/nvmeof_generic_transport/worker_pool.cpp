// Copyright 2025 Alibaba Cloud and its affiliates
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

#include "transport/nvmeof_generic_transport/worker_pool.h"

#include <latch>

#include <glog/logging.h>

#define WORKER_QUEUE_DEPTH 256
#define WORKER_MAX_NUM_TASKS 4096

namespace mooncake {
NVMeoFWorker::NVMeoFWorker(size_t id)
    : id(id), stopping(false), clock(0), tasks(nullptr), curr_task(nullptr) {
    this->tasks = new NVMeoFWorkerTask[WORKER_MAX_NUM_TASKS];
    for (size_t i = 0; i < WORKER_MAX_NUM_TASKS; i++) {
        this->free_tasks.push(&this->tasks[i]);
    }

    this->thread = std::thread(std::bind(&NVMeoFWorker::poll, this));
}

NVMeoFWorker::~NVMeoFWorker() {
    this->stopping = true;
    if (this->thread.joinable()) {
        this->thread.join();
    }

    delete[] this->tasks;
}

void NVMeoFWorker::addController(std::shared_ptr<NVMeoFController> ctrlr) {
    auto it = this->queues.find(ctrlr.get());
    if (it != this->queues.end()) {
        LOG(WARNING) << "Controller exists: " << ctrlr.get();
        return;
    }

    auto queue = ctrlr->createQueue(WORKER_QUEUE_DEPTH);
    if (queue == nullptr) {
        LOG(ERROR) << "Failed to create nvmeof queue";
        return;
    }

    this->queues[ctrlr.get()] = std::move(queue);
}

void NVMeoFWorker::removeController(NVMeoFController *ctrlr) {
    auto it = this->queues.find(ctrlr);
    if (it == this->queues.end()) {
        return;
    }

    this->queues.erase(it);
}

void NVMeoFWorker::sendMsg(const std::function<void(NVMeoFWorker *)> &func) {
    this->msg_queue.push(&func);
}

int NVMeoFWorker::submitTask(NVMeoFController *ctrlr, Slice *slice) {
    NVMeoFWorkerTask *task;

    if (!this->free_tasks.pop(task)) {
        return -ENOMEM;
    }

    task->ctrlr = ctrlr;
    task->slice = slice;
    task->timestamp = this->clock.load();
    this->task_queue.push(task);

    return 0;
}

void NVMeoFWorker::dispatchTasks() {
    uint64_t prev = this->clock.fetch_add(1);

    if (this->curr_task == nullptr && !this->task_queue.pop(this->curr_task)) {
        return;
    }

    do {
        auto task = std::exchange(this->curr_task, nullptr);

        auto queue = this->queues.find(task->ctrlr);
        if (queue == this->queues.end()) {
            task->slice->markFailed();
            this->free_tasks.push(task);
        } else {
            int rc = queue->second->submitRequest(task->slice);
            if (rc == 0) {
                this->free_tasks.push(task);
            } else if (rc == -EAGAIN || rc == -EWOULDBLOCK) {
                task->timestamp = this->clock.load();
                this->task_queue.push(task);
            } else {
                LOG(ERROR) << "Failed to submit request, rc = " << rc;
                task->slice->markFailed();
                this->free_tasks.push(task);
            }
        }
    } while (this->task_queue.pop(this->curr_task) &&
             this->curr_task->timestamp == prev);
}

void NVMeoFWorker::poll() {
    // Allow thread to be scheduled to different CPU cores.
    cpu_set_t cpuset;
    memset(&cpuset, -1, sizeof(cpuset));
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);

    std::function<void(NVMeoFWorker *)> *func;
    while (!this->stopping) {
        while (this->msg_queue.pop(func)) {
            (*func)(this);
        }

        this->dispatchTasks();

        // Take a break before reaping completions.
        std::this_thread::yield();

        for (auto &it : this->queues) {
            it.second->reapCompletions();
        }
    }

    while (this->msg_queue.pop(func)) {
        (*func)(this);
    }

    if (this->curr_task != nullptr) {
        this->curr_task->slice->markFailed();
        this->free_tasks.push(this->curr_task);
        this->curr_task = nullptr;
    }

    while (this->task_queue.pop(this->curr_task)) {
        this->curr_task->slice->markFailed();
        this->free_tasks.push(this->curr_task);
        this->curr_task = nullptr;
    }
}

NVMeoFWorkerPool::NVMeoFWorkerPool(size_t num_workers)
    : num_workers(num_workers), next_worker(0) {
    for (size_t i = 0; i < num_workers; i++) {
        auto worker = std::make_unique<NVMeoFWorker>(i);
        this->workers.push_back(std::move(worker));
    }
}

NVMeoFWorkerPool::~NVMeoFWorkerPool() { this->workers.clear(); }

int NVMeoFWorkerPool::addController(std::shared_ptr<NVMeoFController> ctrlr) {
    std::latch latch(this->num_workers);
    auto msg_fn = [&ctrlr, &latch](NVMeoFWorker *worker) {
        worker->addController(ctrlr);
        latch.count_down();
    };

    for (size_t i = 0; i < this->num_workers; i++) {
        auto worker = this->workers[i].get();
        worker->sendMsg(msg_fn);
    }

    latch.wait();
    return 0;
}

int NVMeoFWorkerPool::removeController(NVMeoFController *ctrlr) {
    std::latch latch(this->num_workers);
    auto msg_fn = [&ctrlr, &latch](NVMeoFWorker *worker) {
        worker->removeController(ctrlr);
        latch.count_down();
    };

    for (size_t i = 0; i < this->num_workers; i++) {
        auto worker = this->workers[i].get();
        worker->sendMsg(msg_fn);
    }

    latch.wait();
    return 0;
}

int NVMeoFWorkerPool::submitTask(NVMeoFController *ctrlr, Slice *slice) {
    uint32_t worker_idx;
    int rc;

    /// Randomly pick a worker.
    worker_idx = std::rand() % this->num_workers;
    rc = this->workers[worker_idx]->submitTask(ctrlr, slice);
    if (rc == 0) {
        return 0;
    }

    /// Try all workers.
    uint32_t failed_worker_idx = worker_idx;
    worker_idx = (worker_idx + 1) % this->num_workers;
    while (rc != 0 && worker_idx != failed_worker_idx) {
        rc = this->workers[worker_idx]->submitTask(ctrlr, slice);
        worker_idx = (worker_idx + 1) % this->num_workers;
    }

    if (rc != 0) {
        LOG(ERROR) << "Failed to submit transfer task";
    }

    return rc;
}

}  // namespace mooncake
