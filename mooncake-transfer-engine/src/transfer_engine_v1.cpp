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
#include <random>

#include "metadata/handshake.h"
#include "transport_v1/rdma/rdma_transport.h"
#include "transport_v1/shm/shm_transport.h"

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

TransferEngine::TransferEngine() : available_(false) {
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngine::TransferEngine(TEConfig &conf)
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

static std::string generateRandomHexString(size_t nbytes) {
    const std::string kHexCharSet = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::string result;
    result.reserve(nbytes * 2);
    for (size_t i = 0; i < nbytes; ++i) {
        unsigned char byte = gen() % 256;
        result.push_back(kHexCharSet[(byte >> 4) & 0x0f]);
        result.push_back(kHexCharSet[byte & 0x0f]);
    }
    return result;
}

static std::string generateSegmentName(const TEConfig &conf) {
    if (conf.count(TEConfigKeyLocalSegmentName)) {
        return conf.at(TEConfigKeyLocalSegmentName);
    }
    return std::string("segment-") + generateRandomHexString(4);
}

static std::string getMetadataConnString(const TEConfig &conf) {
    if (conf.count(TEConfigKeyMetadataConnString)) {
        return conf.at(TEConfigKeyMetadataConnString);
    }
    return "P2PHANDSHAKE";
}

static Status getEthIpPort(RpcMetaDesc &desc, const TEConfig &conf) {
    if (conf.count(TEConfigKeyBindEthIP)) {
        desc.ip_or_host_name = conf.at(TEConfigKeyBindEthIP);
    }
    if (desc.ip_or_host_name.empty()) {
        auto ip_list = findLocalIpAddresses();
        desc.ip_or_host_name = ip_list.empty() ? "127.0.0.1" : ip_list[0];
    }

    desc.rpc_port = 0;
    if (conf.count(TEConfigKeyBindEthPort)) {
        desc.rpc_port = std::atoi(conf.at(TEConfigKeyBindEthPort).c_str());
    }
    if (desc.rpc_port == 0) {
        desc.rpc_port = findAvailableTcpPort(desc.sockfd);
    }

    return desc.rpc_port == 0
               ? Status::Socket("not ethernet port found for out-of-band comm")
               : Status::OK();
}

std::vector<std::string> splitString(const std::string &str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter)) {
        std::string cleanedToken;
        for (char c : token) {
            if (!std::isspace(static_cast<unsigned char>(c))) cleanedToken += c;
        }
        tokens.push_back(cleanedToken);
    }
    return tokens;
}

std::vector<std::string> getRdmaDeviceList(const TEConfig &conf) {
    if (!conf.count(TEConfigKeyRdmaDeviceList)) {
        return {};
    }
    return splitString(conf.at(TEConfigKeyRdmaDeviceList), ',');
}

Status TransferEngine::construct() {
    transport_list_.resize(kSupportedTransportTypes, nullptr);
    segment_name_ = generateSegmentName(conf_);
    metadata_ =
        std::make_shared<TransferMetadata>(getMetadataConnString(conf_));

    RpcMetaDesc rpc_desc;
    CHECK_STATUS(getEthIpPort(rpc_desc, conf_));
    metadata_->addRpcMetaEntry(segment_name_, rpc_desc);

    LOG(INFO) << "Transfer Engine uses address " << rpc_desc.ip_or_host_name
              << " and port " << rpc_desc.rpc_port
              << " for serving local TCP service";

    topology_ = std::make_shared<Topology>();
    if (conf_.count(TEConfigKeyTopology)) {
        auto topology_data = conf_.at(TEConfigKeyTopology);
        if (!topology_data.empty()) topology_->parse(topology_data);
    }

    auto rdma_device_list = getRdmaDeviceList(conf_);
    topology_->discover(rdma_device_list);
    if (topology_->getHcaList().size() > 0) {
        CHECK_STATUS(registerRdmaTransport());
    }
    // TODO other protocols
    return Status::OK();
}

Status TransferEngine::registerRdmaTransport() {
    auto transport = std::make_shared<RdmaTransport>();
    CHECK_STATUS(transport->install(segment_name_, metadata_, topology_));
    transport_list_[RDMA] = transport;
    return Status::OK();
}

Status TransferEngine::deconstruct() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(segment_name_);
        metadata_.reset();
    }
    transport_list_.clear();
    for (auto &batch : batch_set_) delete batch;
    batch_set_.clear();
    deferred_free_batch_set_.clear();
    return Status::OK();
}

const std::string TransferEngine::getEthIP() const {
    return metadata_->localRpcMeta().ip_or_host_name;
}

uint16_t TransferEngine::getEthPort() const {
    return metadata_->localRpcMeta().rpc_port;
}

Status TransferEngine::exportLocalSegment(std::string &shared_handle) {
    return Status::NotImplemented("not implement");
}

Status TransferEngine::importRemoteSegment(SegmentID &handle,
                                           const std::string &shared_handle) {
    return Status::NotImplemented("not implement");
}

Status TransferEngine::openRemoteSegment(SegmentID &handle,
                                         const std::string &segment_name) {
    if (segment_name.empty())
        return Status::InvalidArgument("invalid segment name");
    handle = metadata_->getSegmentID(segment_name);
    return Status::OK();
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
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
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
    if (!batch_id) return Status::InvalidArgument("invalid batch id");
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

}  // namespace v1
}  // namespace mooncake
