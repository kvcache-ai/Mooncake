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
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

using json = nlohmann::json;

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

    // Discover UB HCAs on this host.  Falls back to mock_urma_device when no
    // real HCAs are present (handled inside
    // UbTransport::initializeUbResources).
    te_topology_ = std::make_shared<mooncake::Topology>();
    te_topology_->discover();

    // Build the bridge that replaces the standalone te_metadata_.
    // The bridge uses P2P mode for local-segment operations (no external store)
    // and delegates remote lookups to the TENT SegmentManager.
    te_metadata_bridge_ =
        std::make_shared<UbTentMetadataBridge>(control_service_, "p2p");

    // Instantiate and install UbTransport, using the bridge as metadata.
    ub_transport_ = std::make_unique<mooncake::UbTransport>(URMA_ENDPOINT);
    int rc = ub_transport_->install(local_segment_name_, te_metadata_bridge_,
                                    te_topology_);
    if (rc != 0) {
        LOG(ERROR) << "UbTentTransport: UbTransport::install() failed, rc="
                   << rc;
        ub_transport_.reset();
        te_metadata_bridge_.reset();
        te_topology_.reset();
        return Status::InternalError(
            "UbTentTransport: UbTransport install failed, rc=" +
            std::to_string(rc) + LOC_MARK);
    }

    // Register the UB bootstrap callback so that incoming BootstrapUb RPCs
    // (from remote nodes initiating URMA connections) are dispatched to
    // UbTransport::onSetupConnections.
    if (control_service_) {
        auto cb = te_metadata_bridge_->handshakeCallback();
        if (cb) {
            control_service_->setBootstrapUbCallback(
                [cb](const UbBootstrapDesc& peer,
                     UbBootstrapDesc& local) -> int {
                    mooncake::TransferMetadata::HandShakeDesc peer_hs, local_hs;
                    peer_hs.local_nic_path = peer.local_nic_path;
                    peer_hs.peer_nic_path = peer.peer_nic_path;
#ifdef USE_UB
                    peer_hs.jetty_num = peer.jetty_num;
#endif
                    int ret = cb(peer_hs, local_hs);
                    local.local_nic_path = local_hs.local_nic_path;
                    local.peer_nic_path = local_hs.peer_nic_path;
                    local.reply_msg = local_hs.reply_msg;
#ifdef USE_UB
                    local.jetty_num = local_hs.jetty_num;
#endif
                    return ret;
                });
        }
    }

    caps.dram_to_dram = true;

    // Publish UB device EIDs to the TENT local segment.
    Status s = setupUbLocalSegment();
    if (!s.ok()) {
        LOG(WARNING) << "UbTentTransport: setupUbLocalSegment() failed: "
                     << s.message()
                     << " (non-fatal; remote nodes may lack UB device info)";
    }

    LOG(INFO) << "UbTentTransport: installed on segment '"
              << local_segment_name_ << "'";
    return Status::OK();
}

// ---------------------------------------------------------------------------
// setupUbLocalSegment
//
// Writes UB device EIDs into the TENT local MemorySegmentDesc so that remote
// nodes can extract them via the bridge's convertFromTent().
// ---------------------------------------------------------------------------

Status UbTentTransport::setupUbLocalSegment() {
    if (!control_service_ || !ub_transport_) {
        return Status::InternalError(
            "UbTentTransport: setupUbLocalSegment: not ready" LOC_MARK);
    }

    auto& manager = control_service_->segmentManager();
    auto segment = manager.getLocal();
    if (!segment) {
        return Status::InternalError(
            "UbTentTransport: setupUbLocalSegment: local segment not "
            "found" LOC_MARK);
    }

    if (segment->type != tent::SegmentType::Memory) {
        return Status::InvalidArgument(
            "UbTentTransport: local segment is not a memory segment" LOC_MARK);
    }

    auto& detail = std::get<tent::MemorySegmentDesc>(segment->detail);

    // The old-TE local SegmentDesc (built by allocateLocalSegmentID) already
    // has devices with EIDs.  Mirror them into the TENT MemorySegmentDesc.
    auto local_te_desc =
        te_metadata_bridge_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (local_te_desc) {
        for (const auto& te_dev : local_te_desc->devices) {
            // Check if this device is already present in the TENT descriptor.
            bool found = false;
            for (auto& tent_dev : detail.devices) {
                if (tent_dev.name == te_dev.name) {
                    tent_dev.transport_attrs[TransportType::UB] = te_dev.eid;
                    found = true;
                    break;
                }
            }
            if (!found) {
                tent::DeviceDesc d;
                d.name = te_dev.name;
                d.transport_attrs[TransportType::UB] = te_dev.eid;
                detail.devices.push_back(std::move(d));
            }
        }
    }

    // Tag the segment so remote nodes know UB is available.
    detail.transport_attrs[static_cast<int>(TransportType::UB)] = "ub";

    return manager.synchronizeLocal();
}

Status UbTentTransport::uninstall() {
    ub_transport_.reset();
    te_metadata_bridge_.reset();
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
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
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
        return Status::InternalError(
            "UbTentTransport: registerLocalMemory failed, rc=" +
            std::to_string(rc) + LOC_MARK);
    }

    // After registration, the old-TE local SegmentDesc (accessible via bridge)
    // contains the tseg handles for this buffer.  Serialize them into the TENT
    // BufferDesc so that remote nodes can extract them via convertFromTent().
    auto local_te_desc =
        te_metadata_bridge_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (local_te_desc) {
        for (const auto& te_buf : local_te_desc->buffers) {
            if (te_buf.addr == desc.addr && !te_buf.tseg.empty()) {
                try {
                    json j(te_buf.tseg);
                    desc.transport_attrs[TransportType::UB] = j.dump();
                } catch (const std::exception& e) {
                    LOG(WARNING)
                        << "UbTentTransport: failed to serialize tseg: "
                        << e.what();
                }
                break;
            }
        }
    }

    desc.transports.push_back(TransportType::UB);
    return Status::OK();
}

Status UbTentTransport::removeMemoryBuffer(BufferDesc& desc) {
    if (!ub_transport_) {
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
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
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
    }

    auto* ub_batch = new UbSubBatch();
    ub_batch->ub_batch_id = ub_transport_->allocateBatchID(max_size);
    if (ub_batch->ub_batch_id == 0) {
        delete ub_batch;
        return Status::InternalError(
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
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
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
    size_t first_new = ub_batch->te_requests.size();
    for (const auto& req : request_list) {
        mooncake::Transport::TransferRequest te_req{};
        te_req.opcode = (req.opcode == Request::READ)
                            ? mooncake::Transport::TransferRequest::READ
                            : mooncake::Transport::TransferRequest::WRITE;
        te_req.source = req.source;
        te_req.target_offset = req.target_offset;
        te_req.length = req.length;

        if (req.target_id == LOCAL_SEGMENT_ID) {
            te_req.target_id =
                static_cast<mooncake::Transport::SegmentID>(LOCAL_SEGMENT_ID);
        } else {
            auto te_id = getTESegmentID(req.target_id);
            if (te_id == static_cast<mooncake::Transport::SegmentID>(-1)) {
                return Status::InvalidArgument(
                    "UbTentTransport: cannot resolve TENT segment ID " +
                    std::to_string(req.target_id) + LOC_MARK);
            }
            te_req.target_id = te_id;
        }
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

    auto old_s = ub_transport_->submitTransferTask(task_ptrs);
    if (!old_s.ok()) {
        return Status::InternalError(
            "UbTentTransport: submitTransferTask failed: " +
            std::string(old_s.message()) + LOC_MARK);
    }
    return Status::OK();
}

Status UbTentTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument(
            "UbTentTransport: invalid sub-batch" LOC_MARK);
    }
    if (!ub_transport_) {
        return Status::InternalError("UbTentTransport: not installed" LOC_MARK);
    }

    mooncake::Transport::TransferStatus te_status{};
    auto old_s = ub_transport_->getTransferStatus(
        ub_batch->ub_batch_id, static_cast<size_t>(task_id), te_status);
    if (!old_s.ok()) {
        return Status::InternalError(
            "UbTentTransport: getTransferStatus failed: " +
            std::string(old_s.message()) + LOC_MARK);
    }

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

    if (!control_service_ || !te_metadata_bridge_) {
        LOG(ERROR) << "UbTentTransport: cannot resolve TENT segment ID "
                   << tent_id << " (not installed)";
        return static_cast<mooncake::Transport::SegmentID>(-1);
    }

    // Look up the TENT segment name from the segment manager.
    tent::SegmentDesc* tent_desc = nullptr;
    auto s =
        control_service_->segmentManager().getRemoteCached(tent_desc, tent_id);
    if (!s.ok() || !tent_desc) {
        LOG(ERROR) << "UbTentTransport: cannot resolve TENT segment ID "
                   << tent_id << ": " << s.message();
        return static_cast<mooncake::Transport::SegmentID>(-1);
    }

    // Obtain the old-TE ID via the bridge (which maps name → TENT SegmentID).
    auto old_te_id = te_metadata_bridge_->getSegmentID(tent_desc->name);
    if (old_te_id == static_cast<mooncake::Transport::SegmentID>(-1)) {
        LOG(ERROR) << "UbTentTransport: bridge could not resolve segment '"
                   << tent_desc->name << "'";
        return static_cast<mooncake::Transport::SegmentID>(-1);
    }

    std::lock_guard<std::mutex> lock(seg_id_cache_mutex_);
    tent_to_te_seg_id_[tent_id] = old_te_id;
    return old_te_id;
}

}  // namespace tent
}  // namespace mooncake
