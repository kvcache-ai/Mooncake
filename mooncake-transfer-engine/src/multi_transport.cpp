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
#include <sstream>
#include <string>

#include "config.h"
#include "tracing_facade.h"
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
#ifdef USE_MNNVL
#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#else
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
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
const char* ToTransferStatusName(Transport::TransferStatusEnum status) {
    switch (status) {
        case Transport::TransferStatusEnum::WAITING:
            return "WAITING";
        case Transport::TransferStatusEnum::PENDING:
            return "PENDING";
        case Transport::TransferStatusEnum::INVALID:
            return "INVALID";
        case Transport::TransferStatusEnum::CANCELED:
            return "CANCELED";
        case Transport::TransferStatusEnum::COMPLETED:
            return "COMPLETED";
        case Transport::TransferStatusEnum::TIMEOUT:
            return "TIMEOUT";
        case Transport::TransferStatusEnum::FAILED:
            return "FAILED";
    }
    return "UNKNOWN";
}

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

const char* ToOpcodeName(Transport::TransferRequest::OpCode opcode) {
    return opcode == Transport::TransferRequest::READ ? "READ" : "WRITE";
}

struct ObservedTaskState {
    uint64_t success_slice_count{0};
    uint64_t failed_slice_count{0};
    uint64_t successful_bytes{0};
    bool timed_out{false};
};

ObservedTaskState ObserveTaskState(
    const std::vector<Transport::Slice*>& slice_list) {
    ObservedTaskState observed;
    for (const auto* slice : slice_list) {
        switch (slice->status) {
            case Transport::Slice::SUCCESS:
                ++observed.success_slice_count;
                observed.successful_bytes += slice->length;
                break;
            case Transport::Slice::FAILED:
                ++observed.failed_slice_count;
                break;
            case Transport::Slice::TIMEOUT:
                observed.timed_out = true;
                break;
            case Transport::Slice::PENDING:
            case Transport::Slice::POSTED:
                break;
        }
    }
    return observed;
}
}  // namespace

MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string &local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {}

MultiTransport::~MultiTransport() {}

MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
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

Status MultiTransport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    trace_registry_.ClearBatch(batch_id);
    return Status::OK();
}

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_begin = batch_desc.task_list.size();
    if (task_begin + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    size_t task_id = task_begin;
    batch_desc.task_list.resize(task_id + entries.size());

    std::unordered_map<Transport *, std::vector<Transport::TransferTask *> >
        submit_tasks;
    std::vector<std::string> task_transport_names(batch_desc.task_list.size());
    for (auto &request : entries) {
        Transport *transport = nullptr;
        auto status = selectTransport(request, transport);
        if (!status.ok()) return status;
        assert(transport);
        auto &task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
        task.total_bytes = request.length;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest *>(&request);
#else
        task.request = &request;
#endif
        task_transport_names[task_id] = transport->getName();
        ++task_id;
        submit_tasks[transport].push_back(&task);
    }

    auto batch_context = trace_registry_.LookupBatchContext(batch_id);
    auto& tracing = tracing::TracingFacade::Instance("mooncake-transfer-engine",
                                                     "multi-transport");
    auto batch_submit_span = batch_context.has_value()
                                 ? tracing.StartSpan(
                                       "te.batch.submit", &batch_context.value(),
                                       {{"te.batch_id", std::to_string(batch_id)},
                                        {"sampling.priority", "structural"},
                                        {"task.count",
                                         std::to_string(entries.size())}})
                                 : tracing.StartSpan(
                                       "te.batch.submit", nullptr,
                                       {{"te.batch_id", std::to_string(batch_id)},
                                        {"sampling.priority", "structural"},
                                        {"task.count",
                                         std::to_string(entries.size())}});
    auto batch_submit_context = batch_submit_span.context();
    Status overall_status = Status::OK();
    for (auto &entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second);
        if (!status.ok()) {
            // LOG(ERROR) << "Failed to submit transfer task to "
            //            << entry.first->getName();
            overall_status = status;
        }
    }

    for (size_t idx = task_begin; idx < batch_desc.task_list.size(); ++idx) {
        auto& task = batch_desc.task_list[idx];
        mooncake::tracing::TraceContext task_context;
        auto task_span = tracing.StartSpan(
            "te.task.submit", &batch_submit_context,
            {{"te.batch_id", std::to_string(batch_id)},
             {"sampling.priority", "structural"},
             {"task.id", std::to_string(idx)},
             {"transport.name", task_transport_names[idx]},
             {"transfer.op", ToOpcodeName(task.request->opcode)},
             {"bytes.total", std::to_string(task.total_bytes)}});
        task_context = task_span.context();
        trace_registry_.RegisterTask(batch_id, idx, task_context,
                                     task_transport_names[idx],
                                     task.slice_list.size(), task.total_bytes);
        task_span.AddEvent("transport selected",
                           {{"transport.name", task_transport_names[idx]}});
        for (size_t slice_idx = 0; slice_idx < task.slice_list.size();
             ++slice_idx) {
            auto* slice = task.slice_list[slice_idx];
            slice->StartTrace(task_context, batch_id, idx, slice_idx,
                              task_transport_names[idx]);
            if (!trace_registry_.MarkSliceQueued(batch_id, idx, slice_idx)) {
                continue;
            }
            task_span.AddEvent(
                "slice queued",
                {{"slice.id", std::to_string(slice_idx)},
                 {"slice.length", std::to_string(slice->length)},
                 {"slice.status", ToSliceStatusName(slice->status)}});
        }
    }

    if (!overall_status.ok()) {
        batch_submit_span.SetStatus("ERROR");
    }
    return overall_status;
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries,
    std::string &proto) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::unordered_map<Transport *, std::vector<Transport::TransferTask *> >
        submit_tasks;
    for (auto &request : entries) {
        Transport *transport = nullptr;
        auto status = mp_selectTransport(request, transport, proto);
        if (!status.ok()) return status;
        assert(transport);
        auto &task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest *>(&request);
#else
        task.request = &request;
#endif
        ++task_id;
        submit_tasks[transport].push_back(&task);
    }
    Status overall_status = Status::OK();
    for (auto &entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second);
        if (!status.ok()) {
            // LOG(ERROR) << "Failed to submit transfer task to "
            //            << entry.first->getName();
            overall_status = status;
        }
    }
    return overall_status;
}
#endif

Status MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument("Task ID out of range");
    }
    auto &task = batch_desc.task_list[task_id];
    status.s = Transport::TransferStatusEnum::WAITING;
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    assert(task.slice_count);
    const auto observed = ObserveTaskState(task.slice_list);
    success_slice_count =
        std::max<uint64_t>(success_slice_count, observed.success_slice_count);
    failed_slice_count =
        std::max<uint64_t>(failed_slice_count, observed.failed_slice_count);
    status.transferred_bytes =
        std::max<uint64_t>(status.transferred_bytes, observed.successful_bytes);

    if (observed.timed_out) {
        status.s = Transport::TransferStatusEnum::TIMEOUT;
        task.is_finished = true;
    } else if (success_slice_count + failed_slice_count == task.slice_count) {
        status.s = failed_slice_count ? Transport::TransferStatusEnum::FAILED
                                      : Transport::TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        if (globalConfig().slice_timeout > 0) {
            auto current_ts = getCurrentTimeInNano();
            const int64_t kPacketDeliveryTimeout =
                globalConfig().slice_timeout * 1000000000;
            for (auto &slice : task.slice_list) {
                auto ts = slice->ts;
                if (ts > 0 && current_ts > ts &&
                    current_ts - ts > kPacketDeliveryTimeout) {
                    LOG(INFO) << "Slice timeout detected";
                    slice->status = Transport::Slice::TIMEOUT;
                    status.s = Transport::TransferStatusEnum::TIMEOUT;
                    task.is_finished = true;
                    break;
                }
            }
        }
        if (status.s != Transport::TransferStatusEnum::TIMEOUT) {
            status.s = Transport::TransferStatusEnum::WAITING;
        }
    }

    auto task_trace = trace_registry_.LookupTask(batch_id, task_id);
    if (task_trace.has_value()) {
        auto& tracing_facade = tracing::TracingFacade::Instance(
            "mooncake-transfer-engine", "multi-transport");
        std::vector<std::pair<std::string, tracing::TraceAttrs>> pending_events;
        pending_events.reserve(task.slice_list.size() + 1);

        for (size_t slice_idx = 0; slice_idx < task.slice_list.size();
             ++slice_idx) {
            auto* slice = task.slice_list[slice_idx];
            if (slice->status != Transport::Slice::SUCCESS &&
                slice->status != Transport::Slice::FAILED &&
                slice->status != Transport::Slice::TIMEOUT) {
                continue;
            }
            if (!trace_registry_.MarkSliceTerminal(batch_id, task_id, slice_idx,
                                                   slice->status)) {
                continue;
            }
            if (slice->status == Transport::Slice::TIMEOUT) {
                slice->markTimeoutForTrace();
            }
            pending_events.emplace_back(
                slice->status == Transport::Slice::SUCCESS
                    ? "slice completed"
                    : (slice->status == Transport::Slice::FAILED
                           ? "slice failed"
                           : "slice timeout detected"),
                tracing::TraceAttrs{{"slice.id", std::to_string(slice_idx)},
                                    {"slice.length", std::to_string(slice->length)},
                                    {"slice.status",
                                     ToSliceStatusName(slice->status)}});
        }

        if ((status.s == Transport::TransferStatusEnum::COMPLETED ||
             status.s == Transport::TransferStatusEnum::FAILED ||
             status.s == Transport::TransferStatusEnum::TIMEOUT) &&
            trace_registry_.MarkTaskTerminal(batch_id, task_id)) {
            pending_events.emplace_back(
                "task terminal status",
                tracing::TraceAttrs{{"task.id", std::to_string(task_id)},
                                    {"status", ToTransferStatusName(status.s)},
                                    {"transferred.bytes",
                                     std::to_string(status.transferred_bytes)}});
        }

        if (!pending_events.empty()) {
            auto span = tracing_facade.StartSpan(
                "te.task.status", &task_trace->context,
                {{"te.batch_id", std::to_string(batch_id)},
                 {"task.id", std::to_string(task_id)},
                 {"transport.name", task_trace->transport_name},
                 {"status", ToTransferStatusName(status.s)}});
            for (const auto& event : pending_events) {
                span.AddEvent(event.first, event.second);
            }
            if (status.s == Transport::TransferStatusEnum::FAILED ||
                status.s == Transport::TransferStatusEnum::TIMEOUT ||
                status.s == Transport::TransferStatusEnum::CANCELED) {
                span.SetStatus("ERROR");
            }
        }
    }
    return Status::OK();
}

Status MultiTransport::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.transferred_bytes = 0;

    if (batch_desc.is_finished.load(std::memory_order_acquire) ||
        task_count == 0) {
        status.s = Transport::TransferStatusEnum::COMPLETED;
        status.transferred_bytes =
            batch_desc.finished_transfer_bytes.load(std::memory_order_relaxed);
        return Status::OK();
    }

    size_t success_count = 0;
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        TransferStatus task_status;
        auto ret = getTransferStatus(batch_id, task_id, task_status);

        if (!ret.ok()) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }

        if (task_status.s == Transport::TransferStatusEnum::COMPLETED) {
            status.transferred_bytes += task_status.transferred_bytes;
            success_count++;
        } else if (task_status.s == Transport::TransferStatusEnum::TIMEOUT) {
            status.s = Transport::TransferStatusEnum::TIMEOUT;
            return Status::OK();
        } else if (task_status.s == Transport::TransferStatusEnum::FAILED) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }
    }

    status.s = (success_count == task_count)
                   ? Transport::TransferStatusEnum::COMPLETED
                   : Transport::TransferStatusEnum::WAITING;
    if (status.s == Transport::TransferStatusEnum::COMPLETED) {
        batch_desc.is_finished.store(true, std::memory_order_release);
        batch_desc.finished_transfer_bytes.store(status.transferred_bytes,
                                                 std::memory_order_release);
    } else if (status.s == Transport::TransferStatusEnum::FAILED) {
        batch_desc.has_failure.store(true, std::memory_order_release);
    }

    if ((status.s == Transport::TransferStatusEnum::COMPLETED ||
         status.s == Transport::TransferStatusEnum::FAILED ||
         status.s == Transport::TransferStatusEnum::TIMEOUT) &&
        trace_registry_.MarkBatchTerminal(batch_id)) {
        auto batch_context = trace_registry_.LookupBatchContext(batch_id);
        auto& tracing = tracing::TracingFacade::Instance(
            "mooncake-transfer-engine", "multi-transport");
        auto span = batch_context.has_value()
                        ? tracing.StartSpan(
                              "te.batch.status", &batch_context.value(),
                              {{"te.batch_id", std::to_string(batch_id)},
                               {"status", ToTransferStatusName(status.s)},
                               {"transferred.bytes",
                                std::to_string(status.transferred_bytes)}})
                        : tracing.StartSpan(
                              "te.batch.status", nullptr,
                              {{"te.batch_id", std::to_string(batch_id)},
                               {"status", ToTransferStatusName(status.s)},
                               {"transferred.bytes",
                                std::to_string(status.transferred_bytes)}});
        span.AddEvent("batch terminal status",
                      {{"status", ToTransferStatusName(status.s)}});
        if (status.s != Transport::TransferStatusEnum::COMPLETED) {
            span.SetStatus("ERROR");
        }
    }
    return Status::OK();
}

Transport *MultiTransport::installTransport(const std::string &proto,
                                            std::shared_ptr<Topology> topo) {
    Transport *transport = nullptr;
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

#ifdef USE_MNNVL
#ifdef USE_HIP
    else if (std::string(proto) == "hip") {
        transport = new HipTransport();
    }
#else
    else if (std::string(proto) == "nvlink") {
        transport = new NvlinkTransport();
    }
#endif  // USE_HIP
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
    for (auto &dev : topo->getHcaList()) {
        if (dev.find("soe") != std::string::npos ||
            dev.find("solar") != std::string::npos) {
            use_eic = true;
        }
    }

    if (std::string(proto) == "barex") {
        std::string nics;
        for (auto &dev : topo->getHcaList()) {
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

Status MultiTransport::selectTransport(const TransferRequest &entry,
                                       Transport *&transport) {
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
Status MultiTransport::mp_selectTransport(const TransferRequest &entry,
                                          Transport *&transport,
                                          std::string &preferred_proto) {
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

Transport *MultiTransport::getTransport(const std::string &proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

std::vector<Transport *> MultiTransport::listTransports() {
    std::vector<Transport *> transport_list;
    for (auto &entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

void *MultiTransport::getBaseAddr() {
#ifdef USE_CXL
    Transport *transport = getTransport("cxl");
    if (transport) {
        auto *cxl_transport = dynamic_cast<CxlTransport *>(transport);
        return cxl_transport ? cxl_transport->getCxlBaseAddr() : 0;
    }
#endif
    return 0;
}

}  // namespace mooncake
