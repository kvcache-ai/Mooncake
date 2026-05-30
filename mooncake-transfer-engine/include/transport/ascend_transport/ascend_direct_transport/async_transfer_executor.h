// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
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

#ifndef ASYNC_TRANSFER_EXECUTOR_H
#define ASYNC_TRANSFER_EXECUTOR_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "transfer_executor_base.h"
#include "transport/transport.h"

namespace mooncake {

/**
 * AsyncTransferExecutor: Async-only ADXL transfer. Extends Base with
 * TransferAsync and query thread for status polling.
 */
class AsyncTransferExecutor : public TransferExecutorBase {
   public:
    explicit AsyncTransferExecutor(const InitParams& params);
    ~AsyncTransferExecutor() override;

    int initialize() override;
    void finalize() override;
    ExecuteResult execute(
        size_t local_engine_idx, const std::string& target_adxl_engine_name,
        adxl::TransferOp operation,
        const std::vector<Transport::Slice*>& slice_list) override;

   private:
    void queryThreadLoop();
    void fetchPendingBatches(
        std::vector<std::vector<Transport::Slice*>>& out_batches);
    bool processOneBatch(std::vector<Transport::Slice*>& slice_list);
    void handleTaskFinished(adxl::TransferReq handle);

    std::atomic<bool> running_{false};
    std::thread query_thread_;
    std::queue<std::vector<Transport::Slice*>> query_slice_queue_;
    std::mutex query_mutex_;
    std::condition_variable query_cv_;

    int64_t transfer_timeout_in_nano_;
    size_t active_async_tasks_{0};
    std::mutex async_task_mutex_;
    std::condition_variable async_task_cv_;
    std::mutex async_handle_map_mutex_;
    std::unordered_map<uintptr_t, size_t> async_handle_to_engine_idx_;
    bool finalized_{false};
};

}  // namespace mooncake

#endif  // ASYNC_TRANSFER_EXECUTOR_H
