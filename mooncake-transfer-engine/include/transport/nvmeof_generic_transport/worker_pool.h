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

#ifndef NVMEOF_GENERIC_WORKER_POOL_H_
#define NVMEOF_GENERIC_WORKER_POOL_H_

#include <libaio.h>
#include <functional>
#include <thread>
#include <unordered_map>
#include <atomic>

#include <boost/lockfree/queue.hpp>

#include "nvmeof_initiator.h"

namespace mooncake {
struct NVMeoFWorkerTask {
    NVMeoFController *ctrlr;
    Slice *slice;
    uint64_t timestamp;
};

class NVMeoFWorker {
    friend class NVMeoFWorkerPool;

   public:
    NVMeoFWorker(size_t id);
    ~NVMeoFWorker();

    void addController(std::shared_ptr<NVMeoFController> ctrlr);
    void removeController(NVMeoFController *ctrlr);

   private:
    void sendMsg(const std::function<void(NVMeoFWorker *)> &func);
    int submitTask(NVMeoFController *ctrlr, Slice *slice);
    void dispatchTasks();
    void poll();

    const size_t id;
    std::thread thread;
    bool stopping;
    std::atomic<uint64_t> clock;

    boost::lockfree::queue<const std::function<void(NVMeoFWorker *)> *>
        msg_queue;
    std::unordered_map<NVMeoFController *, std::unique_ptr<NVMeoFQueue>> queues;

    NVMeoFWorkerTask *tasks;
    NVMeoFWorkerTask *curr_task;
    boost::lockfree::queue<NVMeoFWorkerTask *> free_tasks;
    boost::lockfree::queue<NVMeoFWorkerTask *> task_queue;
};

class NVMeoFWorkerPool {
   public:
    NVMeoFWorkerPool(size_t num_workers);
    ~NVMeoFWorkerPool();

    int addController(std::shared_ptr<NVMeoFController> ctrlr);
    int removeController(NVMeoFController *ctrlr);

    int submitTask(NVMeoFController *ctrlr, Slice *slice);

   private:
    const size_t num_workers;
    std::atomic<uint64_t> next_worker;
    std::vector<std::unique_ptr<NVMeoFWorker>> workers;
};
}  // namespace mooncake

#endif