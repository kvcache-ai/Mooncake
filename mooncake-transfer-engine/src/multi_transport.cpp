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

#include "multi_transport.h"
#include <algorithm>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>

#include "config.h"
#include "transport/rdma_transport/rdma_transport.h"
#ifdef USE_BAREX
#include "transport/barex_transport/barex_transport.h"
#endif
#ifdef USE_TCP
#include "transport/tcp_transport/tcp_transport.h"
#endif
#include "transport/transport.h"
#ifdef USE_NVMEOF
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif
#ifdef USE_ASCEND_DIRECT
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#endif
#if defined(USE_ASCEND) && !defined(USE_ASCEND_DIRECT)
#include "transport/ascend_transport/hccl_transport/hccl_transport.h"
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
#include "transport/ascend_transport/heterogeneous_rdma_transport.h"
#endif
#ifdef USE_INTRA_NVLINK
#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"
#endif
#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#endif
#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
#ifdef USE_CXL
#include "transport/cxl_transport/cxl_transport.h"
#endif
#ifdef USE_UBSHMEM
#include "transport/ascend_transport/ubshmem_transport/ubshmem_transport.h"
#endif
#ifdef USE_EFA
#include "transport/efa_transport/efa_transport.h"
#endif
#ifdef USE_UB
#include "transport/kunpeng_transport/ub_transport.h"
#endif

#include <cassert>

namespace mooncake {

namespace {

class BatchLifecycle {
   public:
    struct SubmitTasks {
        using Map = std::unordered_map<Transport*,
                                       std::vector<Transport::TransferTask*>>;
        using Iterator = Map::iterator;
    };

    explicit BatchLifecycle(Transport::BatchDesc& batch_desc)
        : batch_desc_(batch_desc) {}

    Status sealEmptyBatch() {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        if (batch_desc_.sealed.load(std::memory_order_acquire)) {
            if (batch_desc_.batch_size == 0) {
                return Status::OK();
            }
            return Status::InvalidArgument(
                "Cannot append transfers to a sealed batch");
        }
        if (batch_desc_.batch_size != 0) {
            return Status::InvalidArgument(
                "Empty submit is only valid for zero-capacity batches");
        }
        batch_desc_.sealed.store(true, std::memory_order_release);
        publishBatchCompletionIfReadyLocked();
        return Status::OK();
    }

    Status validateFreeable() {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        if (batch_desc_.active_submit_count.load(std::memory_order_acquire) >
            0) {
            return Status::BatchBusy(
                "BatchID cannot be freed while transfers are being submitted");
        }
        const size_t submitted_task_count = static_cast<size_t>(
            batch_desc_.submitted_task_count.load(std::memory_order_acquire));
        const size_t finished_task_count = static_cast<size_t>(
            batch_desc_.finished_task_count.load(std::memory_order_acquire));
        if (finished_task_count < submitted_task_count) {
            return Status::BatchBusy(
                "BatchID cannot be freed until all submitted tasks are done");
        }
        return Status::OK();
    }

    bool publishTaskCompletionLocked(Transport::TransferTask& task) {
        return task.publish_completion_locked();
    }

    void markTransportSubmitted(
        const std::vector<Transport::TransferTask*>& task_list) {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        for (auto* task : task_list) {
            task->is_submitted = true;
        }
    }

    void completeZeroLengthTasksWithoutSlices(
        const std::vector<Transport::TransferTask*>& task_list) {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        bool should_notify = false;
        for (auto* task : task_list) {
            if (hasOnlyZeroLengthRequests(*task)) {
                should_notify |= completeTaskWithoutSlicesLocked(task);
            }
        }
        notifyCompletionIfNeeded(should_notify);
    }

    void failUnsubmittedTasks(SubmitTasks::Iterator begin,
                              SubmitTasks::Iterator end) {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        bool should_notify = false;
        for (auto it = begin; it != end; ++it) {
            for (auto* task : it->second) {
                task->is_submitted = true;
                should_notify |= failUnsubmittedTaskLocked(task);
            }
        }
        notifyCompletionIfNeeded(should_notify);
    }

    void markBatchFailedIfReady() {
        std::lock_guard<std::mutex> lock(batch_desc_.lifecycle_mutex);
        batch_desc_.has_failure.store(true, std::memory_order_release);
        publishBatchCompletionIfReadyLocked();
    }

    void publishBatchCompletionIfReadyLocked() {
        batch_desc_.publish_completion_if_ready_locked();
#ifdef USE_EVENT_DRIVEN_COMPLETION
        batch_desc_.completion_cv.notify_all();
#endif
    }

    template <typename SubmitTasksIt>
    Status submitTransportTaskGroups(SubmitTasksIt begin, SubmitTasksIt end) {
        Status overall_status = Status::OK();
        for (auto it = begin; it != end; ++it) {
            auto status = it->first->submitTransferTask(it->second);
            if (!status.ok()) {
                overall_status = status;
                markTransportSubmitted(it->second);
                failUnsubmittedTasks(it, end);
                break;
            }
            markTransportSubmitted(it->second);
            completeZeroLengthTasksWithoutSlices(it->second);
        }
        if (!overall_status.ok()) {
            markBatchFailedIfReady();
        }
        return overall_status;
    }

    class SubmitGuard {
       public:
        explicit SubmitGuard(Transport::BatchDesc& batch_desc)
            : batch_desc_(batch_desc) {
            batch_desc_.active_submit_count.fetch_add(
                1, std::memory_order_acq_rel);
        }

        ~SubmitGuard() {
            batch_desc_.active_submit_count.fetch_sub(
                1, std::memory_order_acq_rel);
        }

        SubmitGuard(const SubmitGuard&) = delete;
        SubmitGuard& operator=(const SubmitGuard&) = delete;

       private:
        Transport::BatchDesc& batch_desc_;
    };

    void notifyCompletionIfNeeded(bool should_notify) {
#ifdef USE_EVENT_DRIVEN_COMPLETION
        if (should_notify) {
            batch_desc_.completion_cv.notify_all();
        }
#else
        (void)should_notify;
#endif
    }

   private:
    bool failUnsubmittedTaskLocked(Transport::TransferTask* task) {
        if (__atomic_load_n(&task->slice_count, __ATOMIC_ACQUIRE) != 0) {
            return false;
        }
        __atomic_store_n(&task->failed_slice_count, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&task->slice_count, 1, __ATOMIC_RELEASE);
        return publishTaskCompletionLocked(*task);
    }

    static bool hasOnlyZeroLengthRequests(const Transport::TransferTask& task) {
        if (!task.request_group.empty()) {
            return std::all_of(
                task.request_group.begin(), task.request_group.end(),
                [](const auto& request) { return request.length == 0; });
        }
        return task.request && task.request->length == 0;
    }

    bool completeTaskWithoutSlicesLocked(Transport::TransferTask* task) {
        if (__atomic_load_n(&task->slice_count, __ATOMIC_ACQUIRE) != 0) {
            return false;
        }
        __atomic_store_n(&task->success_slice_count, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&task->slice_count, 1, __ATOMIC_RELEASE);
        return publishTaskCompletionLocked(*task);
    }

    Transport::BatchDesc& batch_desc_;
};

Status getTransferStatusLocked(Transport::BatchDesc& batch_desc, size_t task_id,
                               Transport::TransferStatus& status) {
    BatchLifecycle lifecycle(batch_desc);
    std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
    const size_t task_count = static_cast<size_t>(
        batch_desc.submitted_task_count.load(std::memory_order_acquire));
    if (task_id >= task_count) {
        return Status::InvalidArgument("Task ID out of range");
    }
    auto& task = batch_desc.task_list[task_id];
    if (!task.is_submitted) {
        status.s = Transport::TransferStatusEnum::WAITING;
        status.transferred_bytes = 0;
        return Status::OK();
    }
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_ACQUIRE);
    const uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    const uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    const uint64_t slice_count =
        __atomic_load_n(&task.slice_count, __ATOMIC_ACQUIRE);
    if (slice_count == 0) {
        status.s = Transport::TransferStatusEnum::WAITING;
    } else if (success_slice_count + failed_slice_count == slice_count) {
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
        const bool should_notify = lifecycle.publishTaskCompletionLocked(task);
        lifecycle.notifyCompletionIfNeeded(should_notify);
    } else {
        if (globalConfig().slice_timeout > 0) {
            auto current_ts = getCurrentTimeInNano();
            const int64_t kPacketDeliveryTimeout =
                globalConfig().slice_timeout * 1000000000;
            for (auto& slice : task.slice_list) {
                auto ts = slice->ts;
                if (ts > 0 && current_ts > ts &&
                    current_ts - ts > kPacketDeliveryTimeout) {
                    LOG(INFO) << "Slice timeout detected";
                    status.s = Transport::TransferStatusEnum::TIMEOUT;
                    return Status::OK();
                }
            }
        }
        status.s = Transport::TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

}  // namespace

MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string& local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {}

MultiTransport::~MultiTransport() {}

MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
    auto batch_desc = std::make_shared<BatchDesc>();
    batch_desc->id = BatchID(batch_desc.get());
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
    if (batch_size == 0) {
        batch_desc->sealed.store(true, std::memory_order_release);
        batch_desc->is_finished.store(true, std::memory_order_release);
    }
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_[batch_desc->id] = batch_desc;
    return batch_desc->id;
}

Status MultiTransport::freeBatchID(BatchID batch_id) {
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    auto it = batch_desc_set_.find(batch_id);
    if (it == batch_desc_set_.end()) {
        return Status::InvalidArgument("Invalid BatchID");
    }
    auto& batch_desc = *it->second;
    auto status = BatchLifecycle(batch_desc).validateFreeable();
    if (!status.ok()) {
        return status;
    }
    batch_desc_set_.erase(it);
    return Status::OK();
}

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    if (entries.empty()) {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        return BatchLifecycle(*it->second).sealEmptyBatch();
    }

    std::shared_ptr<BatchDesc> batch_desc_holder;
    std::optional<BatchLifecycle::SubmitGuard> submit_guard;
    {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        batch_desc_holder = it->second;
        submit_guard.emplace(*batch_desc_holder);
    }
    auto& batch_desc = *batch_desc_holder;
    BatchLifecycle lifecycle(batch_desc);

    struct ResolvedEntry {
        size_t request_idx;
        Transport* transport;
    };
    std::vector<ResolvedEntry> resolved;
    resolved.reserve(entries.size());

    bool all_standalone = true;
    for (size_t i = 0; i < entries.size(); ++i) {
        Transport* transport = nullptr;
        auto status = selectTransport(entries[i], transport);
        if (!status.ok()) return status;
        assert(transport);
        resolved.push_back({i, transport});
        all_standalone &=
            entries[i].task_group_id == TransferRequest::kNoTaskGroup;
    }

    if (all_standalone) {
        const size_t num_logical = entries.size();
        std::unordered_map<Transport*, std::vector<Transport::TransferTask*>>
            submit_tasks;
        submit_tasks.reserve(resolved.size());
        {
            std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
            if (batch_desc.sealed.load(std::memory_order_acquire)) {
                return Status::InvalidArgument(
                    "Cannot append transfers to a sealed batch");
            }
            const size_t submitted_task_count =
                static_cast<size_t>(batch_desc.submitted_task_count.load(
                    std::memory_order_acquire));
            if (submitted_task_count + num_logical > batch_desc.batch_size) {
                return Status::TooManyRequests(
                    "Exceed the limitation of batch capacity");
            }

            size_t task_id = submitted_task_count;
            batch_desc.task_list.resize(task_id + num_logical);
            for (const auto& entry : resolved) {
                auto& task = batch_desc.task_list[task_id++];
                task.batch_id = batch_id;
                task.is_submitted = false;
                task.request_group.clear();
                task.owned_request = entries[entry.request_idx];
                task.request = &task.owned_request;
                submit_tasks[entry.transport].push_back(&task);
            }

            const size_t new_submitted_task_count = task_id;
            if (new_submitted_task_count == batch_desc.batch_size) {
                batch_desc.sealed.store(true, std::memory_order_release);
            }
            batch_desc.is_finished.store(false, std::memory_order_release);
            batch_desc.submitted_task_count.store(new_submitted_task_count,
                                                  std::memory_order_release);
        }

        return lifecycle.submitTransportTaskGroups(submit_tasks.begin(),
                                                   submit_tasks.end());
    }

    // Phase 2: Compute logical tasks by coalescing adjacent requests that
    // share the same non-default task_group_id and the same transport.
    // Each run of grouped requests becomes one logical task; ungrouped
    // requests are each their own logical task.
    struct LogicalTask {
        size_t start;  // first index in resolved[]
        size_t count;  // number of entries in this logical task
        Transport* transport;
    };
    std::vector<LogicalTask> logical_tasks;
    logical_tasks.reserve(entries.size());
    {
        size_t i = 0;
        while (i < resolved.size()) {
            const auto& req = entries[resolved[i].request_idx];
            Transport* tp = resolved[i].transport;
            if (req.task_group_id != TransferRequest::kNoTaskGroup &&
                tp->supportsGroupedScatter()) {
                size_t group_start = i;
                uint64_t gid = req.task_group_id;
                while (i < resolved.size() &&
                       entries[resolved[i].request_idx].task_group_id == gid &&
                       resolved[i].transport == tp) {
                    ++i;
                }
                logical_tasks.push_back({group_start, i - group_start, tp});
            } else {
                logical_tasks.push_back({i, 1, tp});
                ++i;
            }
        }
    }

    size_t num_logical = logical_tasks.size();
    std::unordered_map<Transport*, std::vector<Transport::TransferTask*>>
        submit_tasks;
    {
        std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
        if (batch_desc.sealed.load(std::memory_order_acquire)) {
            return Status::InvalidArgument(
                "Cannot append transfers to a sealed batch");
        }
        const size_t submitted_task_count = static_cast<size_t>(
            batch_desc.submitted_task_count.load(std::memory_order_acquire));
        if (submitted_task_count + num_logical > batch_desc.batch_size) {
            return Status::TooManyRequests(
                "Exceed the limitation of batch capacity");
        }

        size_t task_id = submitted_task_count;
        batch_desc.task_list.resize(task_id + num_logical);
        for (size_t lt = 0; lt < num_logical; ++lt) {
            auto& ltask = logical_tasks[lt];
            auto& task = batch_desc.task_list[task_id];
            task.batch_id = batch_id;
            task.is_submitted = false;
            task.request_group.clear();
            task.request_group.reserve(ltask.count);
            for (size_t offset = 0; offset < ltask.count; ++offset) {
                task.request_group.push_back(
                    entries[resolved[ltask.start + offset].request_idx]);
            }
            task.request = &task.request_group[0];
            ++task_id;
            submit_tasks[ltask.transport].push_back(&task);
        }

        const size_t new_submitted_task_count = task_id;
        if (new_submitted_task_count == batch_desc.batch_size) {
            batch_desc.sealed.store(true, std::memory_order_release);
        }
        batch_desc.is_finished.store(false, std::memory_order_release);
        batch_desc.submitted_task_count.store(new_submitted_task_count,
                                              std::memory_order_release);
    }

    return lifecycle.submitTransportTaskGroups(submit_tasks.begin(),
                                               submit_tasks.end());
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::string& proto) {
    if (entries.empty()) {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        return BatchLifecycle(*it->second).sealEmptyBatch();
    }

    std::shared_ptr<BatchDesc> batch_desc_holder;
    std::optional<BatchLifecycle::SubmitGuard> submit_guard;
    {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        batch_desc_holder = it->second;
        submit_guard.emplace(*batch_desc_holder);
    }
    auto& batch_desc = *batch_desc_holder;
    BatchLifecycle lifecycle(batch_desc);

    struct ResolvedEntry {
        size_t request_idx;
        Transport* transport;
    };
    std::vector<ResolvedEntry> resolved;
    resolved.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        Transport* transport = nullptr;
        auto status = mp_selectTransport(entries[i], transport, proto);
        if (!status.ok()) return status;
        assert(transport);
        resolved.push_back({i, transport});
    }

    std::unordered_map<Transport*, std::vector<Transport::TransferTask*>>
        submit_tasks;
    {
        std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
        if (batch_desc.sealed.load(std::memory_order_acquire)) {
            return Status::InvalidArgument(
                "Cannot append transfers to a sealed batch");
        }
        const size_t submitted_task_count = static_cast<size_t>(
            batch_desc.submitted_task_count.load(std::memory_order_acquire));
        if (submitted_task_count + resolved.size() > batch_desc.batch_size) {
            return Status::TooManyRequests(
                "Exceed the limitation of batch capacity");
        }

        size_t task_id = submitted_task_count;
        batch_desc.task_list.resize(task_id + resolved.size());
        for (const auto& entry : resolved) {
            auto& task = batch_desc.task_list[task_id];
            task.batch_id = batch_id;
            task.is_submitted = false;
            task.request_group.clear();
            task.request_group.push_back(entries[entry.request_idx]);
            task.request = &task.request_group[0];
            ++task_id;
            submit_tasks[entry.transport].push_back(&task);
        }

        const size_t new_submitted_task_count = task_id;
        if (new_submitted_task_count == batch_desc.batch_size) {
            batch_desc.sealed.store(true, std::memory_order_release);
        }
        batch_desc.is_finished.store(false, std::memory_order_release);
        batch_desc.submitted_task_count.store(new_submitted_task_count,
                                              std::memory_order_release);
    }
    return lifecycle.submitTransportTaskGroups(submit_tasks.begin(),
                                               submit_tasks.end());
}
#endif

Status MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    std::shared_ptr<BatchDesc> batch_desc_holder;
    {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        batch_desc_holder = it->second;
    }
    return getTransferStatusLocked(*batch_desc_holder, task_id, status);
}

Status MultiTransport::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    std::shared_ptr<BatchDesc> batch_desc_holder;
    {
        RWSpinlock::ReadGuard batch_guard(batch_desc_lock_);
        auto it = batch_desc_set_.find(batch_id);
        if (it == batch_desc_set_.end()) {
            return Status::InvalidArgument("Invalid BatchID");
        }
        batch_desc_holder = it->second;
    }
    auto& batch_desc = *batch_desc_holder;
    BatchLifecycle lifecycle(batch_desc);
    size_t task_count = 0;
    status.transferred_bytes = 0;

    {
        std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
        task_count = static_cast<size_t>(
            batch_desc.submitted_task_count.load(std::memory_order_acquire));
        if (batch_desc.is_finished.load(std::memory_order_acquire)) {
            status.s = batch_desc.has_failure.load(std::memory_order_acquire)
                           ? Transport::TransferStatusEnum::FAILED
                           : Transport::TransferStatusEnum::COMPLETED;
            status.transferred_bytes = batch_desc.finished_transfer_bytes.load(
                std::memory_order_acquire);
            return Status::OK();
        }
    }

    size_t success_count = 0;
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        TransferStatus task_status;
        auto ret = getTransferStatusLocked(batch_desc, task_id, task_status);

        if (!ret.ok()) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }

        if (task_status.s == Transport::TransferStatusEnum::COMPLETED) {
            status.transferred_bytes += task_status.transferred_bytes;
            success_count++;
        } else if (task_status.s == Transport::TransferStatusEnum::FAILED ||
                   task_status.s == Transport::TransferStatusEnum::CANCELED ||
                   task_status.s == Transport::TransferStatusEnum::INVALID ||
                   task_status.s == Transport::TransferStatusEnum::TIMEOUT) {
            status.s = Transport::TransferStatusEnum::FAILED;
            lifecycle.markBatchFailedIfReady();
            return Status::OK();
        }
    }

    status.s = (success_count == task_count)
                   ? Transport::TransferStatusEnum::COMPLETED
                   : Transport::TransferStatusEnum::WAITING;
    if (status.s == Transport::TransferStatusEnum::COMPLETED) {
        std::lock_guard<std::mutex> lock(batch_desc.lifecycle_mutex);
        const size_t current_task_count = static_cast<size_t>(
            batch_desc.submitted_task_count.load(std::memory_order_acquire));
        if (current_task_count != task_count || task_count == 0) {
            status.s = Transport::TransferStatusEnum::WAITING;
            status.transferred_bytes = 0;
            return Status::OK();
        }
        lifecycle.publishBatchCompletionIfReadyLocked();
    }
    return Status::OK();
}

Transport* MultiTransport::installTransport(const std::string& proto,
                                            std::shared_ptr<Topology> topo) {
    Transport* transport = nullptr;
    if (std::string(proto) == "rdma") {
        transport = new RdmaTransport();
    }
#ifdef USE_UB
    else if (std::string(proto) == "ub") {
        transport = new UbTransport();
    }
#endif
#ifdef USE_BAREX
    else if (std::string(proto) == "barex") {
        transport = new BarexTransport();
    }
#endif
#ifdef USE_TCP
    else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#endif
#ifdef USE_NVMEOF
    else if (std::string(proto) == "nvmeof") {
        transport = new NVMeoFTransport();
    }
#endif
#ifdef USE_ASCEND_DIRECT
    else if (std::string(proto) == "ascend") {
        transport = new AscendDirectTransport();
    }
#endif
#ifdef USE_ASCEND
    else if (std::string(proto) == "ascend") {
        transport = new HcclTransport();
    }
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
    else if (std::string(proto) == "ascend") {
        transport = new HeterogeneousRdmaTransport();
    }
#endif

#ifdef USE_INTRA_NVLINK
    else if (std::string(proto) == "nvlink_intra") {
        transport = new IntraNodeNvlinkTransport();
    }
#endif

#ifdef USE_HIP
    else if (std::string(proto) == "hip") {
        transport = new HipTransport();
    }
#endif
#ifdef USE_MNNVL
    else if (std::string(proto) == "nvlink") {
        transport = new NvlinkTransport();
    }
#endif  // USE_MNNVL
#ifdef USE_CXL
    else if (std::string(proto) == "cxl") {
        transport = new CxlTransport();
    }
#endif
#ifdef USE_UBSHMEM
    else if (std::string(proto) == "ubshmem") {
        transport = new UBShmemTransport();
    }
#endif
#ifdef USE_EFA
    else if (std::string(proto) == "efa") {
        transport = new EfaTransport();
    }
#endif

    if (!transport) {
        LOG(ERROR) << "Unsupported transport " << proto
                   << ", please rebuild Mooncake";
        return nullptr;
    }

#ifdef USE_BAREX
    bool use_eic = false;
    for (auto& dev : topo->getHcaList()) {
        if (dev.find("soe") != std::string::npos ||
            dev.find("solar") != std::string::npos) {
            use_eic = true;
        }
    }

    if (std::string(proto) == "barex") {
        std::string nics;
        for (auto& dev : topo->getHcaList()) {
            if (use_eic) {
                if (dev.find("soe") == std::string::npos &&
                    dev.find("solar") == std::string::npos) {
                    // ignore no eic nics
                    continue;
                }
            }
            nics += dev;
            nics += ",";
        }

        // Remove the last extra comma
        if (!nics.empty()) {
            nics.pop_back();
        }

        if (!nics.empty()) {
            LOG(INFO) << "ACCL_USE_NICS is set to " << nics;
            setenv("ACCL_USE_NICS", nics.c_str(), 1);
        }
    }
#endif
    if (transport->install(local_server_name_, metadata_, topo)) {
        return nullptr;
    }

    transport_map_[proto] = std::shared_ptr<Transport>(transport);
    return transport;
}

Status MultiTransport::selectTransport(const TransferRequest& entry,
                                       Transport*& transport) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    auto proto = target_segment_desc->protocol;
#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (target_segment_desc->protocol == "rdma") {
        proto = "ascend";
    }
#endif
    if (!transport_map_.count(proto)) {
        return Status::NotSupportedTransport("Transport " + proto +
                                             " not installed");
    }
    transport = transport_map_[proto].get();
    return Status::OK();
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_selectTransport(const TransferRequest& entry,
                                          Transport*& transport,
                                          std::string& preferred_proto) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    // Parse comma-separated protocols
    std::vector<std::string> protos;
    std::stringstream ss(target_segment_desc->protocol);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) protos.push_back(item);
    }

#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (preferred_proto == "rdma" &&
        std::find(protos.begin(), protos.end(), "rdma") != protos.end()) {
        preferred_proto = "ascend";
    }
#endif
    if (!transport_map_.count(preferred_proto)) {
        return Status::NotSupportedTransport("Transport " + preferred_proto +
                                             " not installed");
    }
    if (std::find(protos.begin(), protos.end(), preferred_proto) ==
        protos.end()) {
        return Status::NotSupportedTransport(
            "Transport " + preferred_proto +
            " not supported by target segment");
    }
    transport = transport_map_[preferred_proto].get();
    return Status::OK();
}
#endif

Transport* MultiTransport::getTransport(const std::string& proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

bool MultiTransport::isTcpOnly() const {
    return transport_map_.size() == 1 && transport_map_.count("tcp") == 1;
}

std::vector<Transport*> MultiTransport::listTransports() {
    std::vector<Transport*> transport_list;
    for (auto& entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

void* MultiTransport::getBaseAddr() {
#ifdef USE_CXL
    Transport* transport = getTransport("cxl");
    if (transport) {
        auto* cxl_transport = dynamic_cast<CxlTransport*>(transport);
        return cxl_transport ? cxl_transport->getCxlBaseAddr() : 0;
    }
#endif
    return 0;
}

}  // namespace mooncake
