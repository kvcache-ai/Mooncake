// Copyright 2026 KVCache.AI
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

#include "tent/transport/mpcomm/mpcomm_transport.h"

#include <glog/logging.h>

#include <cstdlib>
#include <fstream>
#include <utility>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

// MPComm registers device memory through the standard ibv_reg_mr path and
// relies on the nvidia-peermem kernel module to pin GPU pages; it has no
// dma-buf fallback. That is the same dependency RdmaTransport gates its GPU
// capabilities on, so the same probe is applied here.
static bool isGpuDirectRdmaSupported(const std::shared_ptr<Config> &conf) {
    if (conf && conf->get("transports/mpcomm/disable_gpu_direct_rdma", false)) {
        return false;
    }
    std::ifstream modules("/proc/modules");
    std::string line;
    while (std::getline(modules, line)) {
        if (line.find("nvidia_peermem") != std::string::npos) {
            return true;
        }
    }
    return false;
}

MpcommTransport::MpcommTransport()
    : MpcommTransport(createDefaultMpcommAdapter()) {}

MpcommTransport::MpcommTransport(std::shared_ptr<MpcommAdapter> adapter)
    : adapter_(std::move(adapter)),
      peers_(std::make_unique<MpcommPeerRegistry>(adapter_)) {}

MpcommTransport::~MpcommTransport() { uninstall(); }

Status MpcommTransport::install(std::string &local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "MpcommTransport has already been installed" LOC_MARK);
    }
    if (!adapter_ || !adapter_->available()) {
        return Status::InternalError(
            "MpcommTransport: MPComm provider is not available" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;

    // A previous uninstall() may have left entries behind if a handshake was
    // still running; they refer to connections the provider has torn down.
    peers_->clear();

    // Resolve the listener port before initialising the provider, so that a
    // malformed value fails with nothing to unwind. MPComm needs a non-zero
    // port and does not fall back to an ephemeral one, so a value that atoi()
    // would quietly turn into 0 has to be rejected rather than used.
    tcp_port_ = 13579;
    if (const char *env_port = std::getenv("MPCOMM_TCP_PORT")) {
        auto status = parseMpcommTcpPort(env_port, tcp_port_);
        if (!status.ok()) {
            LOG(ERROR) << "MpcommTransport: invalid MPCOMM_TCP_PORT: "
                       << status.ToString();
            return status;
        }
    }

    // Extract device names from TENT Topology
    std::string device_names;
    if (local_topology_) {
        bool first = true;
        for (size_t i = 0; i < local_topology_->nic_list_.size(); ++i) {
            auto &nic = local_topology_->nic_list_[i];
            if (nic.type == Topology::NIC_RDMA) {
                if (!first) device_names += ",";
                device_names += nic.name;
                first = false;
            }
        }
    }

    // Initialize mpcomm with local_segment_name as host_id
    // mpcomm manages its own topology discovery internally
    auto status = adapter_->init(local_segment_name_, device_names, tcp_port_);
    if (!status.ok()) {
        LOG(ERROR) << "MpcommTransport: Failed to initialize mpcomm: "
                   << status.ToString();
        uninstall();
        return status;
    }

    // Update tcp_port_ with the actual assigned port
    tcp_port_ = adapter_->tcpPort();

    // Start accept thread for incoming connections
    status = adapter_->startAcceptThread();
    if (!status.ok()) {
        LOG(ERROR) << "MpcommTransport: Failed to start accept thread: "
                   << status.ToString();
        uninstall();
        return status;
    }

    // MPComm always handles host DRAM. Device memory additionally requires the
    // nvidia-peermem module (see isGpuDirectRdmaSupported above). Capabilities
    // feed transport selection, so advertising them unconditionally would let
    // routing pick MPComm on a host without the module for device memory
    // published by a peer that has it, where local registration never
    // succeeded.
    caps.dram_to_dram = true;
    if (isGpuDirectRdmaSupported(conf)) {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    } else if (conf &&
               conf->get("transports/mpcomm/disable_gpu_direct_rdma", false)) {
        LOG(INFO) << "MpcommTransport: GPU memory support disabled by "
                     "transports/mpcomm/disable_gpu_direct_rdma";
    } else {
        LOG(INFO) << "MpcommTransport: nvidia_peermem not detected, GPU memory "
                     "support is disabled";
    }

    // Publish mpcomm connection info to segment transport_attrs so remote
    // peers know how to connect via mpcomm. All mutations of the local
    // SegmentDesc must go through updateLocal(): snapshots returned by
    // getLocal() are copy-on-write and must never be written through.
    {
        std::string mpcomm_addr =
            buildMpcommEndpointAttr(local_segment_name_, tcp_port_);

        auto &manager = metadata_->segmentManager();
        auto publish = manager.updateLocal([&](SegmentDesc &segment) -> Status {
            auto &detail = std::get<MemorySegmentDesc>(segment.detail);
            detail.transport_attrs[static_cast<int>(TransportType::MPCOMM)] =
                mpcomm_addr;
            return Status::OK();
        });
        if (publish.ok()) publish = manager.synchronizeLocal();
        if (!publish.ok()) {
            LOG(ERROR) << "MpcommTransport: Failed to publish transport attrs: "
                       << publish.ToString();
            uninstall();
            return publish;
        }
    }

    installed_ = true;

    LOG(INFO) << "MpcommTransport: Installed successfully, host_id="
              << local_segment_name_ << ", tcp_port=" << tcp_port_
              << ", devices=" << device_names;

    return Status::OK();
}

Status MpcommTransport::uninstall() {
    // Tear down unconditionally rather than gating on installed_: install()
    // may fail after the provider was initialised and its accept thread
    // started, leaving installed_ false while resources still need releasing.
    if (adapter_) {
        adapter_->stopAcceptThread();
        adapter_->shutdown();
    }

    // Entries describe connections the provider has just released. Clearing
    // also wakes anyone waiting on a handshake that will never finish; such a
    // caller then fails through the adapter, which now reports that the
    // provider is no longer initialised.
    if (peers_) peers_->clear();

    installed_ = false;
    return Status::OK();
}

Status MpcommTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mpcomm_batch = Slab<MpcommSubBatch>::Get().allocate();
    if (!mpcomm_batch)
        return Status::InternalError(
            "Unable to allocate mpcomm sub-batch" LOC_MARK);
    batch = mpcomm_batch;
    mpcomm_batch->task_list.reserve(max_size);
    mpcomm_batch->max_size = max_size;
    return Status::OK();
}

Status MpcommTransport::freeSubBatch(SubBatchRef &batch) {
    auto mpcomm_batch = dynamic_cast<MpcommSubBatch *>(batch);
    if (!mpcomm_batch)
        return Status::InvalidArgument("Invalid mpcomm sub-batch" LOC_MARK);

    // Handles are released in getTransferStatus() once a transfer reaches a
    // terminal state. Do not call into MPComm from here: the engine documents
    // that freeSubBatch() must not touch transport-internal state, and that
    // callers ensure no transfer is in flight. A non-zero handle at this point
    // means the transfer was not drained; the handle is reclaimed when MPComm
    // shuts down. Logged as a warning rather than an error because the engine
    // frees active batches during teardown without draining them first.
    for (auto &task : mpcomm_batch->task_list) {
        if (task.mpcomm_handle != kInvalidMpcommTransferHandle) {
            LOG(WARNING) << "MpcommTransport: sub-batch freed while a transfer "
                            "is still in flight (handle="
                         << task.mpcomm_handle << ")";
        }
    }

    Slab<MpcommSubBatch>::Get().deallocate(mpcomm_batch);
    batch = nullptr;
    return Status::OK();
}

Status MpcommTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    if (!installed_ || !adapter_) {
        return Status::InternalError(
            "MpcommTransport: mpcomm not initialized" LOC_MARK);
    }

    auto mpcomm_batch = dynamic_cast<MpcommSubBatch *>(batch);
    if (!mpcomm_batch)
        return Status::InvalidArgument("Invalid mpcomm sub-batch" LOC_MARK);
    if (request_list.size() + mpcomm_batch->task_list.size() >
        mpcomm_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Pre-allocate task slots to avoid vector reallocation during the loop,
    // which would invalidate pointers/references to earlier tasks.
    size_t base_index = mpcomm_batch->task_list.size();
    mpcomm_batch->task_list.resize(base_index + request_list.size());

    for (size_t i = 0; i < request_list.size(); ++i) {
        auto &request = request_list[i];
        size_t task_index = base_index + i;
        auto &task = mpcomm_batch->task_list[task_index];
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        task.mpcomm_handle = kInvalidMpcommTransferHandle;

        // Resolve the remote host_id from SegmentID
        std::string remote_host_id;
        auto status = ensurePeerConnected(request.target_id, remote_host_id);
        if (!status.ok()) {
            LOG(ERROR) << "MpcommTransport: Failed to connect to target "
                       << request.target_id << ": " << status.ToString();
            task.status_word = TransferStatusEnum::FAILED;
            continue;
        }

        task.mpcomm_handle =
            issueMpcommTransfer(*adapter_, request, remote_host_id);
        if (task.mpcomm_handle == kInvalidMpcommTransferHandle) {
            LOG(ERROR) << "MpcommTransport: Async transfer failed for "
                       << (request.opcode == Request::WRITE ? "WRITE" : "READ")
                       << " to " << remote_host_id;
            task.status_word = TransferStatusEnum::FAILED;
        }
    }

    return Status::OK();
}

Status MpcommTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus &status) {
    auto mpcomm_batch = dynamic_cast<MpcommSubBatch *>(batch);
    if (!mpcomm_batch)
        return Status::InvalidArgument("Invalid mpcomm sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)mpcomm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mpcomm_batch->task_list[task_id];

    if (task.status_word == TransferStatusEnum::PENDING &&
        task.mpcomm_handle != kInvalidMpcommTransferHandle) {
        if (!installed_ || !adapter_) {
            // uninstall() ran with this transfer still outstanding, so its
            // outcome can no longer be learned. Fail it rather than leave the
            // caller polling a task that can never reach a terminal state.
            LOG(WARNING) << "MpcommTransport: transfer abandoned, transport "
                            "was uninstalled";
            task.status_word = TransferStatusEnum::FAILED;
            task.mpcomm_handle = kInvalidMpcommTransferHandle;
        } else {
            // Lazy poll: check mpcomm completion status on demand.
            pollMpcommTask(*adapter_, task);
        }
    }

    status.s = task.status_word;
    status.transferred_bytes = task.transferred_bytes;
    return Status::OK();
}

Status MpcommTransport::addMemoryBuffer(BufferDesc &desc,
                                        const MemoryOptions &options) {
    if (!installed_ || !adapter_) {
        return Status::InternalError(
            "MpcommTransport: mpcomm not initialized" LOC_MARK);
    }

    void *addr = reinterpret_cast<void *>(desc.addr);
    size_t length = desc.length;

    auto status = adapter_->registerMemory(addr, length);
    if (!status.ok()) {
        LOG(ERROR) << "MpcommTransport: Failed to register memory at " << addr
                   << " length=" << length << ": " << status.ToString();
        return status;
    }

    // Publish buffer so remote peers can discover it
    // mpcomm handles NUMA awareness internally
    int numa_node = -1;  // Auto-detect
    status = adapter_->publishBuffer(addr, length, numa_node);
    if (!status.ok()) {
        LOG(ERROR) << "MpcommTransport: Failed to publish buffer: "
                   << status.ToString();
        adapter_->unregisterMemory(addr);
        return status;
    }

    // Mark this buffer as using MPCOMM transport
    desc.transports.push_back(TransportType::MPCOMM);

    return Status::OK();
}

Status MpcommTransport::removeMemoryBuffer(BufferDesc &desc) {
    if (!installed_ || !adapter_) return Status::OK();

    void *addr = reinterpret_cast<void *>(desc.addr);

    // Unpublish and unregister from mpcomm
    adapter_->unpublishBuffer(addr);
    adapter_->unregisterMemory(addr);

    return Status::OK();
}

Status MpcommTransport::ensurePeerConnected(SegmentID target_id,
                                            std::string &host_id) {
    // Pin the snapshot. getLocal() returns the owning reference by value, and
    // the raw-pointer form of getRemoteCached() hands out a pointer into a
    // per-thread snapshot cache; either pointer outlives its guarantee once the
    // reference is dropped or the cache is refreshed. SegmentManager offers
    // owning variants for exactly this, and the descriptor is read throughout
    // this function, so hold the reference rather than a bare pointer.
    SegmentDescRef desc_ref;
    if (target_id == LOCAL_SEGMENT_ID) {
        desc_ref = metadata_->segmentManager().getLocal();
    } else {
        auto status =
            metadata_->segmentManager().getRemoteCached(desc_ref, target_id);
        if (!status.ok()) {
            return Status::InvalidArgument(
                "MpcommTransport: Cannot find segment descriptor for sid=" +
                std::to_string(target_id));
        }
    }
    SegmentDesc *desc = desc_ref.get();
    if (!desc) {
        return Status::InvalidArgument(
            "MpcommTransport: Null segment descriptor for sid=" +
            std::to_string(target_id));
    }
    if (desc->type != SegmentType::Memory) {
        // getMemory() would throw on a file segment. MPComm advertises no file
        // capability, so reaching here means a selection bug upstream.
        return Status::InvalidArgument(
            "MpcommTransport: Not a memory segment, sid=" +
            std::to_string(target_id));
    }

    // The segment name serves as the remote host_id
    const std::string remote_host_id = desc->name;
    const auto &mem_desc = desc->getMemory();

    std::string advertised_addr;
    int advertised_port = 0;
    const auto *attr =
        mem_desc.getTransportAttrs(static_cast<int>(TransportType::MPCOMM));
    if (attr && !attr->empty()) {
        auto status =
            parseMpcommEndpointAttr(*attr, advertised_addr, advertised_port);
        if (!status.ok()) return status;
    } else {
        // install() publishes this attribute, so its absence means the peer
        // runs no MPComm transport. Deriving a port from our own would hand
        // connect() an endpoint owned by an unrelated process, and MPComm's
        // handshake has no timeout, so a wrong guess can block the submitting
        // thread indefinitely. Require an explicit override instead.
        const char *env_port = std::getenv("MPCOMM_REMOTE_TCP_PORT");
        if (!env_port) {
            return Status::InvalidArgument("MpcommTransport: segment " +
                                           remote_host_id +
                                           " publishes no mpcomm endpoint");
        }
        auto status = parseMpcommTcpPort(env_port, advertised_port);
        if (!status.ok()) return status;
        advertised_addr = remote_host_id;
        auto sep = advertised_addr.rfind(':');
        if (sep != std::string::npos) advertised_addr.erase(sep);
        LOG(WARNING) << "MpcommTransport: segment " << remote_host_id
                     << " publishes no mpcomm endpoint, using "
                     << advertised_addr << ":" << advertised_port
                     << " from MPCOMM_REMOTE_TCP_PORT";
    }

    // Reused per thread: this runs once per submitted request, so a fresh
    // vector here would allocate on every transfer.
    thread_local std::vector<MpcommBufferRange> buffers;
    buffers.clear();
    buffers.reserve(mem_desc.buffers.size());
    for (const auto &buffer : mem_desc.buffers) {
        buffers.push_back({buffer.addr, buffer.length});
    }

    auto status = peers_->ensure(remote_host_id, advertised_addr,
                                 advertised_port, buffers);
    if (!status.ok()) return status;

    host_id = remote_host_id;
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
