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

#include "transfer_engine_v1.h"

#include <fstream>

#include "metadata/handshake.h"
#include "transport_v1/rdma/rdma_transport.h"
#include "transport_v1/shm/shm_transport.h"

namespace mooncake {
namespace v1 {

struct Batch {
    Batch() { sub_batch.resize(kSupportedTransportTypes, nullptr); }
    ~Batch() {
        for (auto &entry : sub_batch) delete entry;
    }

    std::vector<Transport::SubBatchRef> sub_batch;
    std::vector<std::pair<TransportType, size_t>> task_id_lookup;
    size_t max_size;
};

static std::string loadTopologyJsonFile(const std::string &path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    file.close();
    return content;
}

int TransferEngine::registerRdmaTransport() {
    auto transport = std::make_shared<RdmaTransport>();
    auto status =
        transport->install(local_server_name_, metadata_, local_topology_);
    if (!status.ok()) {
        return (int)status.code();
    }
    transport_list_[RDMA] = transport;
    return 0;
}

int TransferEngine::registerShmTransport() {
    auto transport = std::make_shared<ShmTransport>();
    auto status =
        transport->install(local_server_name_, metadata_, local_topology_);
    if (!status.ok()) {
        return (int)status.code();
    }
    transport_list_[SHM] = transport;
    return 0;
}

int TransferEngine::registerLocalSegment() {
    RpcMetaDesc desc;
    auto *ip_address = getenv("MC_TCP_BIND_ADDRESS");
    if (ip_address)
        desc.ip_or_host_name = ip_address;
    else {
        auto ip_list = findLocalIpAddresses();
        if (ip_list.empty()) {
            LOG(ERROR) << "not valid LAN address found";
            return -1;
        } else {
            desc.ip_or_host_name = ip_list[0];
        }
    }

    desc.rpc_port = findAvailableTcpPort(desc.sockfd);
    if (desc.rpc_port == 0) {
        LOG(ERROR) << "not valid port for serving local TCP service";
        return -1;
    }

    LOG(INFO) << "Transfer Engine uses address " << desc.ip_or_host_name
              << " and port " << desc.rpc_port
              << " for serving local TCP service";

    return metadata_->addRpcMetaEntry(local_server_name_, desc);
}

int TransferEngine::buildTopology() {
    if (getenv("MC_CUSTOM_TOPO_JSON")) {
        auto path = getenv("MC_CUSTOM_TOPO_JSON");
        auto topo_json = loadTopologyJsonFile(path);
        if (!topo_json.empty())
            local_topology_->parse(topo_json);
        else {
            LOG(WARNING) << "Unable to read custom topology file from " << path
                         << ", fall back to auto-detect";
            local_topology_->discover(filter_);
        }
    } else {
        local_topology_->discover(filter_);
    }
    return 0;
}

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name) {
    transport_list_.resize(kSupportedTransportTypes, nullptr);
    local_server_name_ = local_server_name;
    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
    int rc = registerLocalSegment();
    if (rc) return rc;

    rc = buildTopology();
    if (rc) return rc;

    if (local_topology_->getHcaList().size() > 0) {
        rc = registerRdmaTransport();
        if (rc) return rc;
    }

    // rc = registerShmTransport();
    // if (rc) return rc;

    return 0;
}

int TransferEngine::freeEngine() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
    transport_list_.clear();
    return 0;
}

int TransferEngine::getRpcPort() { return metadata_->localRpcMeta().rpc_port; }

std::string TransferEngine::getLocalIpAndPort() {
    return metadata_->localRpcMeta().ip_or_host_name + ":" +
           std::to_string(metadata_->localRpcMeta().rpc_port);
}

SegmentHandle TransferEngine::openSegment(const std::string &segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->getSegmentID(trimmed_segment_name);
}

int TransferEngine::closeSegment(SegmentHandle handle) { return 0; }

int TransferEngine::registerLocalMemory(BufferEntry &buffer) {
    std::vector<BufferEntry> buffer_list;
    buffer_list.push_back(buffer);
    return registerLocalMemoryBatch(buffer_list);
}

int TransferEngine::unregisterLocalMemory(BufferEntry &buffer) {
    std::vector<BufferEntry> buffer_list;
    buffer_list.push_back(buffer);
    return unregisterLocalMemoryBatch(buffer_list);
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    Batch *batch = new Batch();
    batch->max_size = batch_size;
    mutex_.lock();
    batch_set_.insert(batch);
    mutex_.unlock();
    return (BatchID)batch;
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
    Batch *batch = (Batch *)(batch_id);
    mutex_.lock();
    deferred_free_batch_set_.push_back(batch);
    mutex_.unlock();
    lazyFreeBatch();
    return Status::OK();
}

void TransferEngine::lazyFreeBatch() {
    std::lock_guard<std::mutex> lock(mutex_);
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

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &request_list) {
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
    Batch *batch = (Batch *)(batch_id);

    std::vector<TransferRequest>
        classified_request_list[kSupportedTransportTypes];

    for (auto &request : request_list) {
        classified_request_list[RDMA].push_back(request);
        batch->task_id_lookup.push_back(
            std::make_pair(RDMA, batch->task_id_lookup.size()));
    }

    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (classified_request_list[type].empty()) continue;
        auto &transport = transport_list_[type];
        auto &sub_batch = batch->sub_batch[type];
        if (!sub_batch) {
            auto ret = transport->allocateSubBatch(sub_batch, batch->max_size);
            if (!ret.ok()) return ret;
        }
        auto ret = transport->submitTransferTasks(
            sub_batch, classified_request_list[type]);
        if (!ret.ok()) return ret;
    }
    return Status::OK();
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
    Batch *batch = (Batch *)(batch_id);
    if (task_id >= batch->task_id_lookup.size())
        return Status::InvalidArgument("invalid task id");
    auto [type, sub_task_id] = batch->task_id_lookup[task_id];
    auto transport = transport_list_[type];
    auto sub_batch = batch->sub_batch[type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("transport not available");
    }
    status = transport->getTransferStatus(sub_batch, sub_task_id);
    return Status::OK();
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (!transport_list_[type]) continue;
        auto status = transport_list_[type]->registerLocalMemory(buffer_list);
        if (!status.ok()) return (int)status.code();
    }
    return 0;
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (!transport_list_[type]) continue;
        auto status = transport_list_[type]->unregisterLocalMemory(buffer_list);
        if (!status.ok()) return (int)status.code();
    }
    return 0;
}
}  // namespace v1
}  // namespace mooncake
