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

namespace mooncake {
namespace v1 {

struct Batch {
    RdmaSubBatch rdma;
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

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name) {
    local_server_name_ = local_server_name;
    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
    transport_ = std::make_shared<RdmaTransport>();

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

    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

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

    if (local_topology_->getHcaList().size() > 0) {
        auto status =
            transport_->install(local_server_name_, metadata_, local_topology_);
        if (!status.ok()) {
            return (int)status.code();
        }
    }

    return 0;
}

int TransferEngine::freeEngine() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
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
    auto ret = transport_->allocateSubBatch(&batch->rdma, batch_size);
    if (!ret.ok()) {
        delete batch;
        return (BatchID)(0);
    }
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
        std::vector<int> task_id_list;
        transport_->queryOutstandingTasks(&batch->rdma, task_id_list);
        if (task_id_list.empty()) {
            batch_set_.erase(batch);
            auto ret = transport_->freeSubBatch(&batch->rdma);
            delete batch;
            it = deferred_free_batch_set_.erase(it);
        } else {
            ++it;
        }
    }
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
    Batch *batch = (Batch *)(batch_id);
    return transport_->submitTransferTasks(&batch->rdma, entries);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
    Batch *batch = (Batch *)(batch_id);
    status = transport_->getTransferStatus(&batch->rdma, task_id);
    return Status::OK();
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    auto status = transport_->registerLocalMemory(buffer_list);
    return (int)status.code();
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list) {
    auto status = transport_->unregisterLocalMemory(buffer_list);
    return (int)status.code();
}
}  // namespace v1
}  // namespace mooncake
