// Copyright 2025 KVCache.AI
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

#include "tent/transport/ub/ub_tent_transport.h"

#include <glog/logging.h>

#include "tent/runtime/segment.h"
#include "tent/runtime/segment_manager.h"

namespace mooncake {
namespace tent {

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

UbTentTransport::~UbTentTransport() { uninstall(); }

Status UbTentTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> /*local_topology*/,
                                std::shared_ptr<Config> conf) {
    local_segment_name_ = local_segment_name;
    control_service_ = metadata;

    // --- Build old-TE Topology -------------------------------------------
    // Discover UB HCAs on this host.  Falls back to mock_urma_device when no
    // real HCAs are present (handled inside
    // UbTransport::initializeUbResources).
    te_topology_ = std::make_shared<mooncake::Topology>();
    te_topology_->discover();  // non-fatal: empty list → mock device

    // --- Build old-TE TransferMetadata ------------------------------------
    // Derive the connection string from TENT config so that UbTransport can
    // publish its UB-specific segment info (tseg handles, device EIDs) to the
    // same metadata store that TENT uses.
    std::string metadata_type = "p2p";
    std::string metadata_servers = "";
    if (conf) {
        metadata_type = conf->get("metadata_type", "p2p");
        metadata_servers = conf->get("metadata_servers", "");
    }
    std::string conn_string = metadata_servers.empty()
                                  ? metadata_type
                                  : (metadata_type + "://" + metadata_servers);

    te_metadata_ = std::make_shared<mooncake::TransferMetadata>(conn_string);

    // --- Instantiate and install UbTransport ------------------------------
    ub_transport_ = std::make_unique<mooncake::UbTransport>(URMA_ENDPOINT);
    int rc =
        ub_transport_->install(local_segment_name_, te_metadata_, te_topology_);
    if (rc != 0) {
        LOG(ERROR) << "UbTentTransport: UbTransport::install() failed, rc="
                   << rc;
        ub_transport_.reset();
        te_metadata_.reset();
        te_topology_.reset();
        return Status::Internal(
            "UbTentTransport: UbTransport install failed, rc=" +
            std::to_string(rc));
    }

    // Only claim DRAM↔DRAM capability in Phase 1.  GPU/NPU paths are enabled
    // later once confirmed safe via real Kunpeng validation.
    caps.dram_to_dram = true;

    LOG(INFO) << "UbTentTransport: installed on segment '"
              << local_segment_name_ << "'";
    return Status::OK();
}

Status UbTentTransport::uninstall() {
    ub_transport_.reset();
    te_metadata_.reset();
    te_topology_.reset();
    control_service_.reset();
    {
        std::lock_guard<std::mutex> lock(seg_id_cache_mutex_);
        tent_to_te_seg_id_.clear();
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Memory registration
// ---------------------------------------------------------------------------

Status UbTentTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    if (!ub_transport_) {
        return Status::Internal("UbTentTransport: not installed" LOC_MARK);
    }

    void* addr = reinterpret_cast<void*>(desc.addr);
    size_t length = static_cast<size_t>(desc.length);
    const std::string& location =
        options.location.empty() ? kWildcardLocation : options.location;
    bool remote_accessible = (options.perm != kLocalReadWrite);

    int rc = ub_transport_->registerLocalMemory(addr, length, location,
                                                remote_accessible);
    if (rc != 0) {
        LOG(ERROR) << "UbTentTransport: registerLocalMemory failed, rc=" << rc
                   << " addr=" << addr << " length=" << length;
        return Status::Internal(
            "UbTentTransport: registerLocalMemory failed, rc=" +
            std::to_string(rc));
    }

    desc.transports.push_back(TransportType::UB);
    return Status::OK();
}

Status UbTentTransport::removeMemoryBuffer(BufferDesc& desc) {
    if (!ub_transport_) {
        return Status::Internal("UbTentTransport: not installed" LOC_MARK);
    }

    void* addr = reinterpret_cast<void*>(desc.addr);
    int rc = ub_transport_->unregisterLocalMemory(addr);
    if (rc != 0) {
        LOG(WARNING) << "UbTentTransport: unregisterLocalMemory failed, rc="
                     << rc << " addr=" << addr;
    }

    // Remove UB from the transports list in the descriptor.
    auto& ts = desc.transports;
    ts.erase(std::remove(ts.begin(), ts.end(), TransportType::UB), ts.end());

    return Status::OK();
}

// ---------------------------------------------------------------------------
// SubBatch management
// ---------------------------------------------------------------------------

Status UbTentTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    if (!ub_transport_) {
        return Status::Internal("UbTentTransport: not installed" LOC_MARK);
    }

    auto* ub_batch = new UbSubBatch();
    ub_batch->ub_batch_id = ub_transport_->allocateBatchID(max_size);
    if (ub_batch->ub_batch_id == 0) {
        delete ub_batch;
        return Status::Internal(
            "UbTentTransport: allocateBatchID failed" LOC_MARK);
    }
    // Pre-reserve request storage to avoid reallocation after submit.
    ub_batch->te_requests.reserve(max_size);

    batch = ub_batch;
    return Status::OK();
}

Status UbTentTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }

    if (ub_transport_ && ub_batch->ub_batch_id != 0) {
        auto s = ub_transport_->freeBatchID(ub_batch->ub_batch_id);
        if (!s.ok()) {
            LOG(WARNING) << "UbTentTransport: freeBatchID failed: "
                         << s.message();
        }
    }

    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Transfer submission and status
// ---------------------------------------------------------------------------

Status UbTentTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    if (!ub_transport_) {
        return Status::Internal("UbTentTransport: not installed" LOC_MARK);
    }

    auto& batch_desc = mooncake::Transport::toBatchDesc(ub_batch->ub_batch_id);
    if (batch_desc.task_list.size() + request_list.size() >
        batch_desc.batch_size) {
        return Status::TooManyRequests(
            "UbTentTransport: exceed batch capacity" LOC_MARK);
    }

    // Convert TENT Requests → old-TE TransferRequests.
    // The converted requests are stored inside ub_batch->te_requests so that
    // the raw pointers assigned to TransferTask::request remain valid until
    // freeSubBatch() is called.
    //
    // IMPORTANT: reserve() was called in allocateSubBatch(); never call
    // push_back() again after pointers are handed to submitTransferTask().
    size_t first_new = ub_batch->te_requests.size();
    for (const auto& req : request_list) {
        mooncake::Transport::TransferRequest te_req{};
        te_req.opcode = (req.opcode == Request::READ)
                            ? mooncake::Transport::TransferRequest::READ
                            : mooncake::Transport::TransferRequest::WRITE;
        te_req.source = req.source;
        te_req.target_offset = req.target_offset;
        te_req.length = req.length;
        te_req.target_id =
            (req.target_id == LOCAL_SEGMENT_ID)
                ? static_cast<mooncake::Transport::SegmentID>(LOCAL_SEGMENT_ID)
                : getTESegmentID(req.target_id);
        ub_batch->te_requests.push_back(te_req);
    }

    // Set up old-TE task list inside the existing BatchDesc.
    size_t first_task = batch_desc.task_list.size();
    batch_desc.task_list.resize(first_task + request_list.size());

    std::vector<mooncake::Transport::TransferTask*> task_ptrs;
    task_ptrs.reserve(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        auto& task = batch_desc.task_list[first_task + i];
        task.batch_id = ub_batch->ub_batch_id;
        task.request = &ub_batch->te_requests[first_new + i];
        task_ptrs.push_back(&task);
    }

    ub_batch->task_count_ += request_list.size();

    return ub_transport_->submitTransferTask(task_ptrs);
}

Status UbTentTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    if (!ub_transport_) {
        return Status::Internal("UbTentTransport: not installed" LOC_MARK);
    }

    mooncake::Transport::TransferStatus te_status{};
    auto s = ub_transport_->getTransferStatus(
        ub_batch->ub_batch_id, static_cast<size_t>(task_id), te_status);
    if (!s.ok()) return s;

    status.transferred_bytes = te_status.transferred_bytes;

    // Map old-TE status enum → TENT status enum.
    switch (te_status.s) {
        case mooncake::Transport::WAITING:
        case mooncake::Transport::PENDING:
            status.s = TransferStatusEnum::PENDING;
            break;
        case mooncake::Transport::COMPLETED:
            status.s = TransferStatusEnum::COMPLETED;
            break;
        case mooncake::Transport::FAILED:
            status.s = TransferStatusEnum::FAILED;
            break;
        case mooncake::Transport::TIMEOUT:
            status.s = TransferStatusEnum::TIMEOUT;
            break;
        case mooncake::Transport::CANCELED:
            status.s = TransferStatusEnum::CANCELED;
            break;
        case mooncake::Transport::INVALID:
            status.s = TransferStatusEnum::INVALID;
            break;
        default:
            status.s = TransferStatusEnum::INVALID;
            break;
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Segment ID translation
// ---------------------------------------------------------------------------

mooncake::Transport::SegmentID UbTentTransport::getTESegmentID(
    SegmentID tent_id) {
    // Check the per-transport cache first.
    {
        std::lock_guard<std::mutex> lock(seg_id_cache_mutex_);
        auto it = tent_to_te_seg_id_.find(tent_id);
        if (it != tent_to_te_seg_id_.end()) return it->second;
    }

    // Look up the TENT segment name from the segment manager.
    if (control_service_) {
        tent::SegmentDesc* desc = nullptr;
        auto s =
            control_service_->segmentManager().getRemoteCached(desc, tent_id);
        if (s.ok() && desc) {
            // The segment name is the same string both TENT and old-TE use as
            // the key in the metadata store.
            auto old_te_id = ub_transport_->getSegmentID(desc->name);
            std::lock_guard<std::mutex> lock(seg_id_cache_mutex_);
            tent_to_te_seg_id_[tent_id] = old_te_id;
            return old_te_id;
        }
        LOG(WARNING) << "UbTentTransport: cannot resolve TENT segment ID "
                     << tent_id << " (status: " << s.message() << ")";
    }

    // Fallback: pass through unchanged.  This works when both sides use the
    // same integer ID space (e.g. mock / single-process tests).
    LOG(WARNING) << "UbTentTransport: falling back to raw TENT segment ID "
                 << tent_id << " as old-TE segment ID";
    return static_cast<mooncake::Transport::SegmentID>(tent_id);
}

}  // namespace tent
}  // namespace mooncake
