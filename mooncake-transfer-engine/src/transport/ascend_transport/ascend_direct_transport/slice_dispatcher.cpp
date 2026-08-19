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

#include "transport/ascend_transport/ascend_direct_transport/slice_dispatcher.h"
#include "transport/ascend_transport/ascend_direct_transport/transfer_executor_base.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

#include "common.h"

#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <map>
#include <utility>

#include <acl/acl.h>

namespace mooncake {
namespace {
constexpr size_t kDefaultThreadPoolSize = 8U;
constexpr size_t kBufferModeThreadPoolSize = 1U;
constexpr size_t kMaxThreadPoolSize = 16U;
}  // namespace

size_t DefaultSliceDispatcher::resolveThreadPoolSize() const {
    if (transfer_executor_->getUseBufferPool()) {
        return kBufferModeThreadPoolSize;
    }
    char *ascend_thread_pool_size_str = std::getenv("ASCEND_THREAD_POOL_SIZE");
    if (ascend_thread_pool_size_str) {
        auto opt = parseFromString<int32_t>(ascend_thread_pool_size_str);
        if (opt.has_value()) {
            auto size = static_cast<size_t>(opt.value());
            if (size <= kMaxThreadPoolSize) {
                LOG(INFO) << "Set thread pool size to:" << size;
                return size;
            }
            LOG(WARNING) << "Invalid thread pool size:" << size;
        }
    }
    return kDefaultThreadPoolSize;
}

DefaultSliceDispatcher::DefaultSliceDispatcher(
    TransferExecutorBase *transfer_executor,
    const std::vector<aclrtContext> &local_engine_contexts)
    : transfer_executor_(transfer_executor),
      thread_pool_(std::make_unique<AscendThreadPool>(resolveThreadPoolSize())),
      local_engine_contexts_(local_engine_contexts) {}

void DefaultSliceDispatcher::enqueue(
    std::vector<Transport::Slice *> slice_list) {
    std::unordered_map<Transport::SegmentID, std::vector<Transport::Slice *>>
        seg_to_slices;
    for (auto slice : slice_list) {
        seg_to_slices[slice->target_id].push_back(slice);
    }
    auto *executor = transfer_executor_;
    auto contexts = local_engine_contexts_;
    auto *pool = thread_pool_.get();
    for (auto &[seg_id, slices] : seg_to_slices) {
        pool->enqueue([executor, contexts, moved_slices = std::move(slices)] {
            size_t engine_idx = 0;
            if (!moved_slices.empty() &&
                moved_slices[0]->ascend_direct.engine_id >= 0) {
                engine_idx = static_cast<size_t>(
                    moved_slices[0]->ascend_direct.engine_id);
            }
            if (engine_idx < contexts.size()) {
                auto ret = aclrtSetCurrentContext(contexts[engine_idx]);
                if (ret != ACL_ERROR_NONE) {
                    LOG(ERROR) << "DefaultSliceDispatcher: "
                                  "aclrtSetCurrentContext failed, "
                               << "engine_idx: " << engine_idx
                               << ", errmsg: " << aclGetRecentErrMsg();
                    for (auto *s : moved_slices) {
                        s->markFailed();
                    }
                    return;
                }
            }
            executor->processSliceList(moved_slices);
        });
    }
}

void DefaultSliceDispatcher::stop() {
    if (thread_pool_) {
        thread_pool_->stop();
    }
}

RoceDummyRealSliceDispatcher::RoceDummyRealSliceDispatcher(
    TransferExecutorBase *transfer_executor,
    const std::vector<aclrtContext> &local_engine_contexts)
    : num_engines_(transfer_executor->getNumEngines()),
      local_engine_contexts_(local_engine_contexts),
      transfer_executor_(transfer_executor),
      engine_queues_(num_engines_),
      queue_mutexes_(num_engines_),
      queue_cvs_(num_engines_) {
    engine_threads_.reserve(num_engines_);
    for (size_t i = 0; i < num_engines_; ++i) {
        engine_threads_.emplace_back(
            &RoceDummyRealSliceDispatcher::engineWorker, this, i);
    }
    LOG(INFO) << "RoceDummyRealSliceDispatcher: started " << num_engines_
              << " engine threads";
}

RoceDummyRealSliceDispatcher::~RoceDummyRealSliceDispatcher() {
    RoceDummyRealSliceDispatcher::stop();
}

void RoceDummyRealSliceDispatcher::enqueue(
    std::vector<Transport::Slice *> slice_list) {
    std::map<std::pair<size_t, Transport::SegmentID>,
             std::vector<Transport::Slice *>>
        grouped;
    for (auto slice : slice_list) {
        int32_t dev_id = slice->ascend_direct.engine_id;
        if (dev_id < 0) {
            LOG(ERROR) << "Invalid device_id: " << dev_id;
            slice->markFailed();
            continue;
        }
        size_t engine_idx = static_cast<size_t>(dev_id);
        if (engine_idx >= num_engines_) {
            LOG(ERROR) << "Invalid engine_idx: " << engine_idx
                       << " >= num_engines: " << num_engines_;
            slice->markFailed();
            continue;
        }
        grouped[{engine_idx, slice->target_id}].push_back(slice);
    }
    for (auto &[key, slices] : grouped) {
        size_t engine_idx = key.first;
        SliceTask task{std::move(slices)};
        {
            std::lock_guard<std::mutex> lock(queue_mutexes_[engine_idx]);
            engine_queues_[engine_idx].push(std::move(task));
        }
        queue_cvs_[engine_idx].notify_one();
    }
}

void RoceDummyRealSliceDispatcher::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;
    }
    for (size_t i = 0; i < num_engines_; ++i) {
        queue_cvs_[i].notify_all();
    }
    for (auto &t : engine_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    LOG(INFO) << "RoceDummyRealSliceDispatcher: stopped";
}

void RoceDummyRealSliceDispatcher::engineWorker(size_t engine_idx) {
    if (engine_idx >= local_engine_contexts_.size()) {
        LOG(ERROR) << "engineWorker: invalid engine_idx " << engine_idx;
        return;
    }
    auto ret = aclrtSetCurrentContext(local_engine_contexts_[engine_idx]);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "engineWorker: aclrtSetCurrentContext failed for engine "
                   << engine_idx << ", errmsg: " << aclGetRecentErrMsg();
        return;
    }
    while (running_) {
        SliceTask task;
        {
            std::unique_lock<std::mutex> lock(queue_mutexes_[engine_idx]);
            queue_cvs_[engine_idx].wait(lock, [this, engine_idx] {
                return !running_ || !engine_queues_[engine_idx].empty();
            });
            if (!running_) {
                break;
            }
            if (engine_queues_[engine_idx].empty()) {
                continue;
            }
            task = std::move(engine_queues_[engine_idx].front());
            engine_queues_[engine_idx].pop();
        }
        if (!task.slices.empty() && transfer_executor_) {
            transfer_executor_->processSliceList(task.slices);
        }
    }
}

}  // namespace mooncake
