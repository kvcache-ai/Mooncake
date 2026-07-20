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
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

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
    struct QueryBatch {
        std::vector<Transport::Slice*> slices;
        size_t engine_idx;
        std::string target_adxl_engine_name;
    };

    struct BatchPollResult {
        bool done = false;
        // When true, disconnect once and fail every batch already in
        // pending_batches for this route. Batches enqueued after disconnect
        // are not affected.
        bool fail_entire_route = false;
        std::string fail_reason;
    };

    void queryThreadLoop();
    void fetchPendingBatches(std::vector<QueryBatch>& out_batches);
    void processOneBatch(QueryBatch& batch, BatchPollResult& result);
    void failAllPendingOnRoute(size_t engine_idx, const std::string& target,
                               std::vector<QueryBatch>& pending,
                               const std::string& reason);
    void markBatchFailed(QueryBatch& batch, const std::string& reason,
                         bool log_error = true);
    void handleTaskFinished();

    std::atomic<bool> running_{false};
    std::thread query_thread_;
    std::queue<QueryBatch> query_slice_queue_;
    std::mutex query_mutex_;
    std::condition_variable query_cv_;

    int64_t transfer_timeout_in_nano_;
    size_t active_async_tasks_{0};
    std::mutex async_task_mutex_;
    std::condition_variable async_task_cv_;
    bool finalized_{false};

    size_t last_context_engine_idx_{SIZE_MAX};
};

}  // namespace mooncake

#endif  // ASYNC_TRANSFER_EXECUTOR_H
