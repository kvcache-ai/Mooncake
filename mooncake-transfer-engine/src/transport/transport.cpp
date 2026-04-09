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

#include "transport/transport.h"

#include "error.h"
#include "transfer_engine.h"

namespace mooncake {
namespace {
const char* ToSliceStatusName(Transport::Slice::SliceStatus status) {
    switch (status) {
        case Transport::Slice::PENDING:
            return "PENDING";
        case Transport::Slice::POSTED:
            return "POSTED";
        case Transport::Slice::SUCCESS:
            return "SUCCESS";
        case Transport::Slice::TIMEOUT:
            return "TIMEOUT";
        case Transport::Slice::FAILED:
            return "FAILED";
    }
    return "UNKNOWN";
}
}  // namespace

thread_local static Transport::ThreadLocalSliceCache tl_slice_cache;

void Transport::Slice::ResetTraceState() {
    trace_batch_id_ = 0;
    trace_task_id_ = 0;
    trace_slice_id_ = 0;
    trace_transport_name_.clear();
    trace_span_ = tracing::Span();
    trace_started_ = false;
    trace_terminal_recorded_ = false;
}

void Transport::Slice::StartTrace(const tracing::TraceContext& parent_context,
                                  BatchID batch_id, size_t task_id,
                                  size_t slice_id,
                                  const std::string& transport_name) {
    if (trace_started_) {
        return;
    }

    trace_batch_id_ = batch_id;
    trace_task_id_ = task_id;
    trace_slice_id_ = slice_id;
    trace_transport_name_ = transport_name;

    auto& tracing = tracing::TracingFacade::Instance("mooncake-transfer-engine",
                                                     "multi-transport");
    trace_span_ =
        tracing.StartSpan("te.slice", &parent_context,
                          {{"te.batch_id", std::to_string(batch_id)},
                           {"task.id", std::to_string(task_id)},
                           {"slice.id", std::to_string(slice_id)},
                           {"transport.name", transport_name},
                           {"slice.length", std::to_string(length)},
                           {"slice.status", ToSliceStatusName(status)}});
    if (!trace_span_.valid()) {
        return;
    }

    trace_started_ = true;
    trace_span_.AddEvent("slice execution started",
                         {{"slice.status", ToSliceStatusName(status)}});

    if (status == Slice::SUCCESS) {
        FinishTrace("SUCCESS", false);
    } else if (status == Slice::FAILED) {
        FinishTrace("FAILED", true);
    } else if (status == Slice::TIMEOUT) {
        FinishTrace("TIMEOUT", true);
    }
}

void Transport::Slice::markSuccess() {
    status = Slice::SUCCESS;
    __atomic_fetch_add(&task->transferred_bytes, length, __ATOMIC_RELAXED);
    __atomic_fetch_add(&task->success_slice_count, 1, __ATOMIC_RELAXED);

    FinishTrace("SUCCESS", false);
    check_batch_completion(false);
}

void Transport::Slice::markFailed() {
    status = Slice::FAILED;
    __atomic_fetch_add(&task->failed_slice_count, 1, __ATOMIC_RELAXED);

    FinishTrace("FAILED", true);
    check_batch_completion(true);
}

void Transport::Slice::markTimeoutForTrace() { FinishTrace("TIMEOUT", true); }

void Transport::Slice::FinishTrace(const char* status_name, bool error) {
    if (!trace_started_ || trace_terminal_recorded_) {
        return;
    }

    trace_span_.SetAttribute("status", status_name);
    trace_span_.AddEvent("slice terminal status",
                         {{"te.batch_id", std::to_string(trace_batch_id_)},
                          {"task.id", std::to_string(trace_task_id_)},
                          {"slice.id", std::to_string(trace_slice_id_)},
                          {"transport.name", trace_transport_name_},
                          {"slice.length", std::to_string(length)},
                          {"status", status_name}});
    if (error) {
        trace_span_.SetStatus("ERROR");
    }
    trace_span_.End();
    trace_terminal_recorded_ = true;
}

Transport::ThreadLocalSliceCache& Transport::getSliceCache() {
    return tl_slice_cache;
}

Transport::BatchID Transport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

Status Transport::freeBatchID(BatchID batch_id) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            LOG(ERROR) << "BatchID cannot be freed until all tasks are done";
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return Status::OK();
}

int Transport::install(std::string& local_server_name,
                       std::shared_ptr<TransferMetadata> meta,
                       std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    metadata_ = meta;
    return 0;
}
}  // namespace mooncake
