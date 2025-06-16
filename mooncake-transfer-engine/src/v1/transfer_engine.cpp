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

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <ifaddrs.h>
#include <jsoncpp/json/value.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/socket.h>

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

static std::string getMetadataConnString(const TEConfig &conf) {
    if (conf.count(TEConfigKeyMetadataConnString)) {
        return conf.at(TEConfigKeyMetadataConnString);
    }
    return "P2PHANDSHAKE";
}

static std::vector<std::string> findLocalIpAddresses() {
    std::vector<std::string> ips;
    struct ifaddrs *ifaddr, *ifa;

    if (getifaddrs(&ifaddr) == -1) {
        PLOG(ERROR) << "getifaddrs failed";
        return ips;
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == AF_INET) {
            if (strcmp(ifa->ifa_name, "lo") == 0) {
                continue;
            }

            char host[NI_MAXHOST];
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host,
                            NI_MAXHOST, nullptr, 0, NI_NUMERICHOST) == 0) {
                ips.push_back(host);
            }
        }
    }

    freeifaddrs(ifaddr);
    return ips;
}

static void getEthIpPort(std::string &hostname, uint16_t &port,
                         const TEConfig &conf) {
    if (conf.count(TEConfigKeyBindEthIP))
        hostname = conf.at(TEConfigKeyBindEthIP);
    if (hostname.empty()) {
        auto ip_list = findLocalIpAddresses();
        hostname = ip_list.empty() ? "127.0.0.1" : ip_list[0];
    }
    port = 0;
    if (conf.count(TEConfigKeyBindEthPort))
        port = std::atoi(conf.at(TEConfigKeyBindEthPort).c_str());
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
    getEthIpPort(hostname_, port_, conf_);

    metadata_ =
        std::make_shared<TransferMetadata>(getMetadataConnString(conf_));
    metadata_->start(port_);

    LOG(INFO) << "Segment name of this instance: " << hostname_ << ":" << port_;

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

    return Status::OK();
}

Status TransferEngine::registerRdmaTransport() {
    auto transport = std::make_shared<RdmaTransport>();
    auto segment_name = hostname_ + ":" + std::to_string(port_);
    CHECK_STATUS(transport->install(segment_name, metadata_, topology_));
    transport_list_[RDMA] = transport;
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

const std::string TransferEngine::getEthIP() const { return hostname_; }

uint16_t TransferEngine::getEthPort() const { return port_; }

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
