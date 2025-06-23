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

#include "v1/transfer_engine.h"

#include <fstream>
#include <random>

#include "v1/transport/rdma/rdma_transport.h"
#include "v1/transport/shm/shm_transport.h"
#include "v1/utility/ip.h"

#define CHECK_STATUS(cmd)                \
    do {                                 \
        Status status = cmd;             \
        if (!status.ok()) return status; \
    } while (0)

namespace mooncake {
namespace v1 {

struct Batch {
    Batch() {
        sub_batch.resize(kSupportedTransportTypes, nullptr);
        next_sub_task_id.resize(kSupportedTransportTypes, 0);
    }

    ~Batch() {
        for (auto &entry : sub_batch) delete entry;
    }

    std::vector<Transport::SubBatchRef> sub_batch;
    std::vector<int> next_sub_task_id;
    std::vector<std::pair<TransportType, size_t>> task_id_lookup;
    size_t max_size;
};

TransferEngine::TransferEngine()
    : conf_(std::make_shared<ConfigManager>()), available_(false) {
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngine::TransferEngine(std::shared_ptr<ConfigManager> conf)
    : conf_(conf), available_(false) {
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngine::~TransferEngine() { deconstruct(); }

std::string randomSegmentName() {
    std::string name = "segment_noname_";
    for (int i = 0; i < 8; ++i) name += 'a' + SimpleRandom::Get().next(26);
    return name;
}

void setLogLevel(const std::string level) {
    if (level == "info")
        FLAGS_minloglevel = google::INFO;
    else if (level == "warning")
        FLAGS_minloglevel = google::WARNING;
    else if (level == "error")
        FLAGS_minloglevel = google::ERROR;
}

Status TransferEngine::setupLocalSegment() {
    auto &manager = metadata_->segmentManager();
    auto segment = std::make_shared<SegmentDesc>();
    segment->name = local_segment_name_;
    segment->type = SegmentType::Memory;
    auto &detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.topology = *(topology_.get());
    detail.rpc_server_addr = buildIpAddrWithPort(hostname_, port_, ipv6_);
    manager.setLocal(segment);
    return manager.applyLocal();
}

Status TransferEngine::construct() {
    auto metadata_type = conf_->get("metadata_type", "p2p");
    auto metadata_servers = conf_->get("metadata_servers", "");
    setLogLevel(conf_->get("log_level", "info"));
    hostname_ = conf_->get("rpc_server_hostname", "");
    local_segment_name_ = conf_->get("local_segment_name", "");
    if (!hostname_.empty())
        CHECK_STATUS(checkLocalIpAddress(hostname_, ipv6_));
    else
        CHECK_STATUS(discoverLocalIpAddress(hostname_, ipv6_));

    port_ = conf_->get("rpc_server_port", 0);
    metadata_ =
        std::make_shared<MetadataService>(metadata_type, metadata_servers);

    CHECK_STATUS(metadata_->start(port_));

    if (metadata_type == "p2p")
        local_segment_name_ = buildIpAddrWithPort(hostname_, port_, ipv6_);
    else if (local_segment_name_.empty())
        local_segment_name_ = randomSegmentName();

    topology_ = std::make_shared<Topology>();
    CHECK_STATUS(topology_->discover(conf_));
    CHECK_STATUS(setupLocalSegment());

    LOG(INFO) << "========== Transfer Engine Parameters ==========";
    LOG(INFO) << " - Segment Name:       " << local_segment_name_;
    LOG(INFO) << " - RPC Server Address: "
              << buildIpAddrWithPort(hostname_, port_, ipv6_);
    LOG(INFO) << " - Metadata Type:      " << metadata_type;
    LOG(INFO) << " - Metadata Servers:   " << metadata_servers;
    LOG(INFO) << "================================================";

    transport_list_.resize(kSupportedTransportTypes, nullptr);
    if (!topology_->getHcaList().empty()) {
        auto transport = std::make_shared<RdmaTransport>();
        CHECK_STATUS(transport->install(local_segment_name_, metadata_,
                                        topology_, conf_));
        transport_list_[RDMA] = transport;
    }

    return Status::OK();
}

Status TransferEngine::deconstruct() {
    metadata_.reset();
    transport_list_.clear();
    for (auto &batch : batch_set_) delete batch;
    batch_set_.clear();
    deferred_free_batch_set_.clear();
    return Status::OK();
}

const std::string TransferEngine::getSegmentName() const {
    return local_segment_name_;
}

const std::string TransferEngine::getRpcServerAddress() const {
    return hostname_;
}

uint16_t TransferEngine::getRpcServerPort() const { return port_; }

Status TransferEngine::exportLocalSegment(std::string &shared_handle) {
    return Status::NotImplemented(
        "exportLocalSegment not implemented" LOC_MARK);
}

Status TransferEngine::importRemoteSegment(SegmentID &handle,
                                           const std::string &shared_handle) {
    return Status::NotImplemented(
        "importRemoteSegment not implemented" LOC_MARK);
}

Status TransferEngine::openRemoteSegment(SegmentID &handle,
                                         const std::string &segment_name) {
    if (segment_name.empty())
        return Status::InvalidArgument("Invalid segment name" LOC_MARK);
    return metadata_->segmentManager().openRemote(handle, segment_name);
}

Status TransferEngine::closeRemoteSegment(SegmentID handle) {
    return Status::OK();
}

Status TransferEngine::registerLocalMemory(BufferEntry &buffer) {
    std::vector<BufferEntry> buffer_list;
    buffer_list.push_back(buffer);
    return registerLocalMemoryBatch(buffer_list);
}

Status TransferEngine::unregisterLocalMemory(BufferEntry &buffer) {
    std::vector<BufferEntry> buffer_list;
    buffer_list.push_back(buffer);
    return unregisterLocalMemoryBatch(buffer_list);
}

Status TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (!transport_list_[type]) continue;
        CHECK_STATUS(transport_list_[type]->registerLocalMemory(buffer_list));
    }
    return Status::OK();
}

Status TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (!transport_list_[type]) continue;
        CHECK_STATUS(transport_list_[type]->unregisterLocalMemory(buffer_list));
    }
    return Status::OK();
}

BatchID TransferEngine::allocateBatch(size_t batch_size) {
    Batch *batch = new Batch();
    batch->max_size = batch_size;
    std::lock_guard<std::mutex> lock(mutex_);
    batch_set_.insert(batch);
    return (BatchID)batch;
}

Status TransferEngine::freeBatch(BatchID batch_id) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    std::lock_guard<std::mutex> lock(mutex_);
    deferred_free_batch_set_.push_back(batch);
    lazyFreeBatch();
    return Status::OK();
}

void TransferEngine::lazyFreeBatch() {
    for (auto it = deferred_free_batch_set_.begin();
         it != deferred_free_batch_set_.end();) {
        auto &batch = *it;
        bool has_task = false;
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            auto &transport = transport_list_[type];
            auto &sub_batch = batch->sub_batch[type];
            if (!transport || !sub_batch) continue;
            std::vector<int> task_id_list;
            transport->queryOutstandingTasks(sub_batch, task_id_list);
            if (task_id_list.empty()) {
                transport->freeSubBatch(sub_batch);
            } else {
                has_task = true;
            }
        }
        if (!has_task) {
            batch_set_.erase(batch);
            delete batch;
            it = deferred_free_batch_set_.erase(it);
        } else {
            ++it;
        }
    }
}

TransportType TransferEngine::getTransportType(const Request &request) {
    return RDMA;
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<Request> &request_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);

    std::vector<Request> classified_request_list[kSupportedTransportTypes];
    for (auto &request : request_list) {
        auto transport_type = getTransportType(request);
        auto &sub_task_id = batch->next_sub_task_id[transport_type];
        classified_request_list[transport_type].push_back(request);
        batch->task_id_lookup.push_back(
            std::make_pair(transport_type, sub_task_id));
        sub_task_id++;
    }

    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (classified_request_list[type].empty()) continue;
        auto &transport = transport_list_[type];
        auto &sub_batch = batch->sub_batch[type];
        if (!sub_batch) {
            CHECK_STATUS(
                transport->allocateSubBatch(sub_batch, batch->max_size));
        }
        CHECK_STATUS(transport->submitTransferTasks(
            sub_batch, classified_request_list[type]));
    }

    return Status::OK();
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    if (task_id >= batch->task_id_lookup.size())
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    auto [type, sub_task_id] = batch->task_id_lookup[task_id];
    auto transport = transport_list_[type];
    auto sub_batch = batch->sub_batch[type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("Transport not available" LOC_MARK);
    }
    return transport->getTransferStatus(sub_batch, sub_task_id, status);
}

Status TransferEngine::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    for (size_t task_id = 0; task_id < batch->task_id_lookup.size(); ++task_id) {
        auto [type, sub_task_id] = batch->task_id_lookup[task_id];
        auto transport = transport_list_[type];
        auto sub_batch = batch->sub_batch[type];
        if (!transport || !sub_batch) {
            return Status::InvalidArgument("Transport not available" LOC_MARK);
        }
        TransferStatus xfer_status;
        CHECK_STATUS(transport->getTransferStatus(sub_batch, sub_task_id, xfer_status));
        status_list.push_back(xfer_status);
    }
    return Status::OK();
}

std::shared_ptr<SegmentDesc> TransferEngine::getSegmentDesc(SegmentID handle) {
    auto &manager = metadata_->segmentManager();
    std::shared_ptr<SegmentDesc> desc;
    if (handle == 0) return manager.getLocal();
    auto status = manager.getRemote(desc, handle);
    if (!status.ok()) {
        LOG(ERROR) << status.ToString();
        return nullptr;
    }
    return desc;
}

Status TransferEngine::allocateLocalMemory(void **pptr, TransportType type,
                                           size_t size, size_t align,
                                           const Location &location) {
    if (!pptr) return Status::InvalidArgument("pptr is nullptr");
    auto transport = transport_list_[type];
    return transport->allocateLocalMemory(pptr, size, align, location);
}

Status TransferEngine::freeLocalMemory(void *ptr, TransportType type,
                                       size_t size) {
    auto transport = transport_list_[type];
    return transport->freeLocalMemory(ptr, size);
}

}  // namespace v1
}  // namespace mooncake
