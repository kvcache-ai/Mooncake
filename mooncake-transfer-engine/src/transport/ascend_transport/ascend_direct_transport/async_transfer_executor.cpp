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

#include "transport/ascend_transport/ascend_direct_transport/async_transfer_executor.h"

#include <glog/logging.h>

#include <chrono>
#include <utility>

#include "common.h"

namespace mooncake {
namespace {
constexpr int64_t kMillisToNano = 1000000;
constexpr size_t kAsyncTaskLimit = 100;
constexpr int32_t kDefaultDisconnectTime = 1000;
constexpr int64_t kQueryPollIntervalMicros = 1;
}  // namespace

AsyncTransferExecutor::AsyncTransferExecutor(const InitParams& params)
    : TransferExecutorBase(params),
      transfer_timeout_in_nano_(params.transfer_timeout * kMillisToNano) {}

AsyncTransferExecutor::~AsyncTransferExecutor() {
    AsyncTransferExecutor::finalize();
}

int AsyncTransferExecutor::initialize() {
    int ret = initEngines();
    if (ret != 0) {
        return ret;
    }
    running_ = true;
    query_thread_ = std::thread(&AsyncTransferExecutor::queryThreadLoop, this);
    LOG(INFO) << "AsyncTransferExecutor query thread started";
    return 0;
}

void AsyncTransferExecutor::finalize() {
    if (finalized_) {
        return;
    }
    running_ = false;
    query_cv_.notify_all();
    async_task_cv_.notify_all();
    if (query_thread_.joinable()) {
        query_thread_.join();
    }
    LOG(INFO) << "AsyncTransferExecutor query thread stopped";
    cleanupConnections();
    finalizeEngines();
    finalized_ = true;
}

TransferExecutorBase::ExecuteResult AsyncTransferExecutor::execute(
    size_t local_engine_idx, const std::string& target_adxl_engine_name,
    adxl::TransferOp operation,
    const std::vector<Transport::Slice*>& slice_list) {
    if (local_engine_idx >= adxl_engines_.size()) {
        LOG(ERROR) << "Invalid local_engine_idx: " << local_engine_idx;
        return {.ret = -1, .status = adxl::FAILED, .retryable = false};
    }

    if (!params_.auto_connect) {
        int ret = checkAndConnect(local_engine_idx, target_adxl_engine_name);
        if (ret != 0) {
            return {.ret = -1, .status = adxl::FAILED, .retryable = true};
        }
    }

    auto start_time = getCurrentTimeInNano();
    for (auto* slice : slice_list) {
        slice->ascend_direct.start_time = start_time;
    }

    {
        std::unique_lock<std::mutex> lock(async_task_mutex_);
        async_task_cv_.wait(lock, [this] {
            return !running_ || active_async_tasks_ < kAsyncTaskLimit;
        });
        if (!running_) {
            return {.ret = -1, .status = adxl::FAILED, .retryable = false};
        }
        active_async_tasks_++;
    }

    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(slice_list.size());
    for (const auto* slice : slice_list) {
        adxl::TransferOpDesc op_desc{};
        op_desc.local_addr = reinterpret_cast<uintptr_t>(slice->source_addr);
        op_desc.remote_addr =
            reinterpret_cast<uintptr_t>(slice->ascend_direct.dest_addr);
        op_desc.len = slice->length;
        op_descs.emplace_back(op_desc);
    }

    adxl::TransferReq req_handle;
    auto status = adxl_engines_[local_engine_idx]->TransferAsync(
        target_adxl_engine_name.c_str(), operation, op_descs,
        adxl::TransferArgs(), req_handle);

    if (status == adxl::SUCCESS) {
        recordConnectedSegment(local_engine_idx, target_adxl_engine_name);
        QueryBatch batch;
        batch.slices = slice_list;
        batch.engine_idx = local_engine_idx;
        batch.target_adxl_engine_name = target_adxl_engine_name;

        for (auto* slice : slice_list) {
            slice->ascend_direct.handle = req_handle;
        }

        {
            std::unique_lock<std::mutex> lock(query_mutex_);
            query_slice_queue_.push(std::move(batch));
        }
        query_cv_.notify_one();
        return {.ret = 0, .status = status, .retryable = false};
    }

    {
        std::lock_guard<std::mutex> lock(async_task_mutex_);
        if (active_async_tasks_ > 0) {
            active_async_tasks_--;
        }
        async_task_cv_.notify_one();
    }
    disconnect(local_engine_idx, target_adxl_engine_name,
               kDefaultDisconnectTime);
    return {.ret = -1, .status = status, .retryable = true};
}

void AsyncTransferExecutor::queryThreadLoop() {
    std::vector<QueryBatch> pending_batches;
    pending_batches.reserve(64);

    while (running_) {
        fetchPendingBatches(pending_batches);
        if (pending_batches.empty()) {
            continue;
        }

        size_t i = 0;
        while (i < pending_batches.size()) {
            auto& batch = pending_batches[i];
            BatchPollResult poll_result;
            processOneBatch(batch, poll_result);
            if (!poll_result.done) {
                ++i;
                continue;
            }

            if (poll_result.fail_entire_route) {
                const std::string& reason =
                    poll_result.fail_reason.empty()
                        ? "Route failed for target: " +
                              batch.target_adxl_engine_name
                        : poll_result.fail_reason;
                failAllPendingOnRoute(batch.engine_idx,
                                      batch.target_adxl_engine_name,
                                      pending_batches, reason);
                // failAllPendingOnRoute swap-pops may move unvisited batches to
                // indices < i; restart the scan so none are skipped this cycle.
                i = 0;
                continue;
            }

            std::swap(pending_batches[i], pending_batches.back());
            pending_batches.pop_back();
        }

        if (!pending_batches.empty()) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(kQueryPollIntervalMicros));
        }
    }
}

void AsyncTransferExecutor::fetchPendingBatches(
    std::vector<QueryBatch>& out_batches) {
    std::unique_lock<std::mutex> lock(query_mutex_);
    if (out_batches.empty()) {
        query_cv_.wait(
            lock, [this] { return !running_ || !query_slice_queue_.empty(); });
    }
    if (!running_) {
        return;
    }
    while (!query_slice_queue_.empty()) {
        out_batches.emplace_back(std::move(query_slice_queue_.front()));
        query_slice_queue_.pop();
    }
}

void AsyncTransferExecutor::markBatchFailed(QueryBatch& batch,
                                            const std::string& reason,
                                            bool log_error) {
    if (log_error) {
        LOG(ERROR) << reason;
    }
    for (auto* slice : batch.slices) {
        slice->markFailed();
    }
    handleTaskFinished();
}

void AsyncTransferExecutor::failAllPendingOnRoute(
    size_t engine_idx, const std::string& target,
    std::vector<QueryBatch>& pending, const std::string& reason) {
    LOG(ERROR) << reason;
    bool disconnected = false;
    for (size_t j = 0; j < pending.size();) {
        auto& batch = pending[j];
        if (batch.engine_idx != engine_idx ||
            batch.target_adxl_engine_name != target) {
            ++j;
            continue;
        }

        markBatchFailed(batch, reason, false);
        if (!disconnected && engine_idx < adxl_engines_.size() &&
            !target.empty()) {
            disconnect(engine_idx, target, params_.connect_timeout);
            disconnected = true;
        }

        std::swap(pending[j], pending.back());
        pending.pop_back();
    }
}

void AsyncTransferExecutor::processOneBatch(QueryBatch& batch,
                                            BatchPollResult& result) {
    result = {};

    if (batch.slices.empty()) {
        result.done = true;
        return;
    }

    if (batch.engine_idx >= adxl_engines_.size() ||
        batch.engine_idx >= local_engine_contexts_.size()) {
        markBatchFailed(
            batch, "Invalid engine_idx: " + std::to_string(batch.engine_idx));
        result.done = true;
        return;
    }

    if (last_context_engine_idx_ != batch.engine_idx) {
        auto context_ret =
            aclrtSetCurrentContext(local_engine_contexts_[batch.engine_idx]);
        if (context_ret != ACL_ERROR_NONE) {
            markBatchFailed(
                batch, "aclrtSetCurrentContext failed, ret: " +
                           std::to_string(context_ret) +
                           ", engine_idx: " + std::to_string(batch.engine_idx));
            result.done = true;
            return;
        }
        last_context_engine_idx_ = batch.engine_idx;
    }

    auto handle =
        static_cast<adxl::TransferReq>(batch.slices[0]->ascend_direct.handle);

    adxl::TransferStatus task_status;
    auto ret =
        adxl_engines_[batch.engine_idx]->GetTransferStatus(handle, task_status);

    if (ret != adxl::SUCCESS || task_status == adxl::TransferStatus::FAILED) {
        result.done = true;
        result.fail_entire_route = true;
        result.fail_reason = "Get transfer status failed, ret: " +
                             std::to_string(static_cast<int>(ret)) +
                             ", errmsg: " + aclGetRecentErrMsg();
        return;
    }

    if (task_status == adxl::TransferStatus::COMPLETED) {
        for (auto* slice : batch.slices) {
            slice->markSuccess();
        }
        handleTaskFinished();
        if (params_.use_short_connection) {
            disconnect(batch.engine_idx, batch.target_adxl_engine_name,
                       params_.connect_timeout);
        }
        result.done = true;
        return;
    }

    auto now = getCurrentTimeInNano();
    if (now - batch.slices[0]->ascend_direct.start_time >
        transfer_timeout_in_nano_) {
        result.done = true;
        result.fail_entire_route = true;
        result.fail_reason =
            "Transfer timeout to: " + batch.target_adxl_engine_name;
        return;
    }
}

void AsyncTransferExecutor::handleTaskFinished() {
    std::lock_guard<std::mutex> lock(async_task_mutex_);
    if (active_async_tasks_ > 0) {
        active_async_tasks_--;
    }
    async_task_cv_.notify_one();
}

}  // namespace mooncake
