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
#include "v1/transport/tcp/tcp_transport.h"
#ifdef USE_CUDA
#include "v1/transport/mnnvl/mnnvl_transport.h"
#endif
#ifdef USE_GDS
#include "v1/transport/gds/gds_transport.h"
#endif
#include "v1/utility/ip.h"

namespace mooncake {
namespace v1 {

struct Batch {
    Batch() : max_size(0) {
        sub_batch.fill(nullptr);
        next_sub_task_id.fill(0);
    }

    ~Batch() {}

    std::array<Transport::SubBatchRef, kSupportedTransportTypes> sub_batch;
    std::array<int, kSupportedTransportTypes> next_sub_task_id;

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

std::string getMachineID() {
    std::ifstream file("/etc/machine-id");
    if (file) {
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        if (!content.empty() && content.back() == '\n') content.pop_back();
        return content;
    } else {
        std::string content = "undefined_machine_";
        for (int i = 0; i < 16; ++i)
            content += 'a' + SimpleRandom::Get().next(26);
        return content;
    }
}

Status TransferEngine::setupLocalSegment() {
    auto &manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    segment->name = local_segment_name_;
    segment->type = SegmentType::Memory;
    segment->machine_id = getMachineID();
    auto &detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.topology = *(topology_.get());
    detail.rpc_server_addr = buildIpAddrWithPort(hostname_, port_, ipv6_);
    local_segment_tracker_ = std::make_unique<LocalSegmentTracker>(segment);
    return manager.synchronizeLocal();
}

Status TransferEngine::construct() {
    auto metadata_type = conf_->get("metadata_type", "p2p");
    auto metadata_servers = conf_->get("metadata_servers", "");
    setLogLevel(conf_->get("log_level", "info"));
    hostname_ = conf_->get("rpc_server_hostname", "");
    local_segment_name_ = conf_->get("local_segment_name", "");
    port_ = conf_->get("rpc_server_port", 0);
    if (!hostname_.empty())
        CHECK_STATUS(checkLocalIpAddress(hostname_, ipv6_));
    else
        CHECK_STATUS(discoverLocalIpAddress(hostname_, ipv6_));

    topology_ = std::make_shared<Topology>();
    CHECK_STATUS(topology_->discover(conf_));

    metadata_ =
        std::make_shared<MetadataService>(metadata_type, metadata_servers);

    CHECK_STATUS(metadata_->start(port_, ipv6_));

    if (metadata_type == "p2p")
        local_segment_name_ = buildIpAddrWithPort(hostname_, port_, ipv6_);
    else if (local_segment_name_.empty())
        local_segment_name_ = randomSegmentName();

    CHECK_STATUS(setupLocalSegment());

    if (conf_->get("transports/rdma/enable", true) &&
        !topology_->getHcaList().empty()) {
        transport_list_[RDMA] = std::make_unique<RdmaTransport>();
    }

    if (conf_->get("transports/tcp/enable", true))
        transport_list_[TCP] = std::make_unique<TcpTransport>();

    if (conf_->get("transports/shm/enable", true))
        transport_list_[SHM] = std::make_unique<ShmTransport>();

#ifdef USE_CUDA
    if (conf_->get("transports/mnnvl/enable", false))
        transport_list_[MNNVL] = std::make_unique<MnnvlTransport>();
#endif

#ifdef USE_GDS
    if (conf_->get("transports/gds/enable", false))
        transport_list_[GDS] = std::make_unique<GdsTransport>();
#endif

    std::string transport_string;
    for (auto &transport : transport_list_) {
        if (transport) {
            CHECK_STATUS(transport->install(local_segment_name_, metadata_,
                                            topology_, conf_));
            transport_string += transport->getName();
            transport_string += " ";
        }
    }

    LOG(INFO) << "========== Transfer Engine Parameters ==========";
    LOG(INFO) << " - Segment Name:       " << local_segment_name_;
    LOG(INFO) << " - RPC Server Address: "
              << buildIpAddrWithPort(hostname_, port_, ipv6_);
    LOG(INFO) << " - Metadata Type:      " << metadata_type;
    LOG(INFO) << " - Metadata Servers:   " << metadata_servers;
    LOG(INFO) << " - Loaded Transports:  " << transport_string;
    LOG(INFO) << "================================================";

    return Status::OK();
}

Status TransferEngine::deconstruct() {
    for (auto &transport : transport_list_) transport.reset();
    local_segment_tracker_.reset();
    metadata_->segmentManager().deleteLocal();
    metadata_.reset();
    batch_set_.forEach([&](BatchSet &entry) {
        for (auto &batch : entry.active) {
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                auto &transport = transport_list_[type];
                auto &sub_batch = batch->sub_batch[type];
                if (!transport || !sub_batch) continue;
                transport->freeSubBatch(sub_batch);
            }
            Slab<Batch>::Get().deallocate(batch);
        }
        entry.active.clear();
        entry.freelist.clear();
    });
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
    return metadata_->segmentManager().closeRemote(handle);
}

Status TransferEngine::allocateLocalMemory(void **addr, size_t size,
                                           MemoryOptions &options) {
    auto &transport = transport_list_[options.type];
    if (!transport)
        return Status::InvalidArgument(
            "Not supported type in memory options" LOC_MARK);
    CHECK_STATUS(transport->allocateLocalMemory(addr, size, options));
    std::lock_guard<std::mutex> lock(mutex_);
    AllocatedMemory entry{
        .addr = *addr, .size = size, .transport = transport.get()};
    allocated_memory_.push_back(entry);
    return Status::OK();
}

Status TransferEngine::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = allocated_memory_.begin(); it != allocated_memory_.end();
         ++it) {
        if (it->addr == addr && it->size == size) {
            auto status = it->transport->freeLocalMemory(addr, size);
            allocated_memory_.erase(it);
            return status;
        }
    }
    return Status::InvalidArgument("Address region not registered" LOC_MARK);
}

Status TransferEngine::registerLocalMemory(void *addr, size_t size,
                                           MemoryOptions &options) {
    return local_segment_tracker_->add(
        (uint64_t)addr, size, [&](BufferDesc &desc) -> Status {
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                if (!transport_list_[type]) continue;
                CHECK_STATUS(
                    transport_list_[type]->addMemoryBuffer(desc, options));
            }
            return Status::OK();
        });
}

// WARNING: before exiting TE, make sure that all local memory are
// unregistered, otherwise the CUDA may halt!
Status TransferEngine::unregisterLocalMemory(void *addr, size_t size) {
    return local_segment_tracker_->remove(
        (uint64_t)addr, size, [&](BufferDesc &desc) -> Status {
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                if (!transport_list_[type]) continue;
                CHECK_STATUS(transport_list_[type]->removeMemoryBuffer(desc));
            }
            return Status::OK();
        });
}

BatchID TransferEngine::allocateBatch(size_t batch_size) {
    Batch *batch = Slab<Batch>::Get().allocate();
    if (!batch) return (BatchID)0;
    batch->max_size = batch_size;
    batch_set_.get().active.insert(batch);
    return (BatchID)batch;
}

Status TransferEngine::freeBatch(BatchID batch_id) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    batch_set_.get().freelist.push_back(batch);
    lazyFreeBatch();
    return Status::OK();
}

void TransferEngine::lazyFreeBatch() {
    auto &batch_set = batch_set_.get();
    for (auto it = batch_set.freelist.begin();
         it != batch_set.freelist.end();) {
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
            batch_set.active.erase(batch);
            Slab<Batch>::Get().deallocate(batch);
            it = batch_set.freelist.erase(it);
        } else {
            ++it;
        }
    }
}

TransportType TransferEngine::getTransportType(const Request &request) {
    if (transport_list_[SHM] && transport_list_[SHM]->taskSupported(request))
        return SHM;
    if (transport_list_[RDMA])
        return RDMA;
    else
        return TCP;
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

Status TransferEngine::sendNotify(SegmentID target_id,
                                  const NotifyMessage &notify) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto &transport = transport_list_[type];
        if (!transport || !transport->hasNotifyFeature()) continue;
        return transport->sendNotify(target_id, notify);
    }
    return Status::InvalidArgument("Notify feature not supported" LOC_MARK);
}

Status TransferEngine::getNotifyList(std::vector<NotifyMessage> &notify_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto &transport = transport_list_[type];
        if (!transport || !transport->hasNotifyFeature()) continue;
        return transport->getNotifyList(notify_list);
    }
    return Status::InvalidArgument("Notify feature not supported" LOC_MARK);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    if (task_id >= batch->task_id_lookup.size())
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    auto [type, sub_task_id] = batch->task_id_lookup[task_id];
    auto &transport = transport_list_[type];
    auto &sub_batch = batch->sub_batch[type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("Transport not available" LOC_MARK);
    }
    return transport->getTransferStatus(sub_batch, sub_task_id, status);
}

Status TransferEngine::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    status_list.clear();
    for (size_t task_id = 0; task_id < batch->task_id_lookup.size();
         ++task_id) {
        auto [type, sub_task_id] = batch->task_id_lookup[task_id];
        auto &transport = transport_list_[type];
        auto &sub_batch = batch->sub_batch[type];
        if (!transport || !sub_batch) {
            return Status::InvalidArgument("Transport not available" LOC_MARK);
        }
        TransferStatus xfer_status;
        CHECK_STATUS(
            transport->getTransferStatus(sub_batch, sub_task_id, xfer_status));
        status_list.push_back(xfer_status);
    }
    return Status::OK();
}

std::shared_ptr<SegmentDesc> TransferEngine::getSegmentDesc(SegmentID handle) {
    auto &manager = metadata_->segmentManager();
    std::shared_ptr<SegmentDesc> desc;
    if (handle == LOCAL_SEGMENT_ID) return manager.getLocal();
    auto status = manager.getRemote(desc, handle);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to retrieve segment: " << status.ToString();
        return nullptr;
    }
    return desc;
}

}  // namespace v1
}  // namespace mooncake
