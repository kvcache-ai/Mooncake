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

#include "v1/common/status.h"
#include "v1/memory/location.h"
#include "v1/metadata/metadata.h"
#include "v1/metadata/segment.h"
#include "v1/transport/rdma/rdma_transport.h"
#include "v1/transport/shm/shm_transport.h"
#include "v1/transport/tcp/tcp_transport.h"
#include "v1/transport/transport.h"
#ifdef USE_CUDA
#include "v1/transport/mnnvl/mnnvl_transport.h"
#endif
#ifdef USE_GDS
#include "v1/transport/gds/gds_transport.h"
#endif
#include "v1/transport/io_uring/io_uring_transport.h"
#include "v1/utility/ip.h"
#include "v1/utility/random.h"

namespace mooncake {
namespace v1 {

struct TaskInfo {
    TransportType type;
    int sub_task_id;
    bool derived;  // merged by other tasks
};

struct Batch {
    Batch() : max_size(0) { sub_batch.fill(nullptr); }

    ~Batch() {}

    std::array<Transport::SubBatchRef, kSupportedTransportTypes> sub_batch;
    std::vector<TaskInfo> task_list;
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
        !topology_->getDeviceList().empty()) {
        transport_list_[RDMA] = std::make_unique<RdmaTransport>();
    }

    if (conf_->get("transports/tcp/enable", true))
        transport_list_[TCP] = std::make_unique<TcpTransport>();

    if (conf_->get("transports/shm/enable", true))
        transport_list_[SHM] = std::make_unique<ShmTransport>();

    if (conf_->get("transports/io_uring/enable", true))
        transport_list_[IOURING] = std::make_unique<IOUringTransport>();

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
    local_segment_tracker_->forEach([&](BufferDesc &desc) -> Status {
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            if (transport_list_[type])
                transport_list_[type]->removeMemoryBuffer(desc);
        }
        return Status::OK();
    });
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

Status TransferEngine::openSegment(SegmentID &handle,
                                   const std::string &segment_name) {
    if (segment_name.empty())
        return Status::InvalidArgument("Invalid segment name" LOC_MARK);
    if (segment_name == local_segment_name_) {
        handle = LOCAL_SEGMENT_ID;
        return Status::OK();
    }
    return metadata_->segmentManager().openRemote(handle, segment_name);
}

Status TransferEngine::closeSegment(SegmentID handle) {
    if (handle == LOCAL_SEGMENT_ID) return Status::OK();
    return metadata_->segmentManager().closeRemote(handle);
}

Status TransferEngine::getSegmentInfo(SegmentID handle, SegmentInfo &info) {
    SegmentDesc *desc = nullptr;
    if (handle == LOCAL_SEGMENT_ID) {
        desc = metadata_->segmentManager().getLocal().get();
    } else {
        CHECK_STATUS(metadata_->segmentManager().getRemoteCached(desc, handle));
    }
    if (desc->type == SegmentType::File) {
        info.type = SegmentInfo::File;
        auto &detail = std::get<FileSegmentDesc>(desc->detail);
        for (auto &entry : detail.buffers) {
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = entry.offset,
                                    .length = entry.length,
                                    .location = kWildcardLocation});
        }
    } else {
        info.type = SegmentInfo::Memory;
        auto &detail = std::get<MemorySegmentDesc>(desc->detail);
        for (auto &entry : detail.buffers) {
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = (uint64_t)entry.addr,
                                    .length = entry.length,
                                    .location = entry.location});
        }
    }
    return Status::OK();
}

Status TransferEngine::allocateLocalMemory(void **addr, size_t size,
                                           Location location) {
    MemoryOptions options;
    options.location = location;
    if (location == kWildcardLocation || location.starts_with("cpu")) {
        if (transport_list_[RDMA])
            options.type = RDMA;
        else if (transport_list_[SHM])
            options.type = SHM;
        else
            options.type = TCP;
    } else {
        if (transport_list_[MNNVL])
            options.type = MNNVL;
        else if (transport_list_[RDMA])
            options.type = RDMA;
        else
            options.type = TCP;
    }
    return allocateLocalMemory(addr, size, options);
}

Status TransferEngine::allocateLocalMemory(void **addr, size_t size,
                                           MemoryOptions &options) {
    if (!transport_list_[options.type]) options.type = TCP;
    auto &transport = transport_list_[options.type];
    if (!transport)
        return Status::InvalidArgument(
            "Not supported type in memory options" LOC_MARK);
    CHECK_STATUS(transport->allocateLocalMemory(addr, size, options));
    std::lock_guard<std::mutex> lock(mutex_);
    AllocatedMemory entry{.addr = *addr,
                          .size = size,
                          .transport = transport.get(),
                          .options = options};
    allocated_memory_.push_back(entry);
    return Status::OK();
}

Status TransferEngine::freeLocalMemory(void *addr) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = allocated_memory_.begin(); it != allocated_memory_.end();
         ++it) {
        if (it->addr == addr) {
            auto status = it->transport->freeLocalMemory(addr, it->size);
            allocated_memory_.erase(it);
            return status;
        }
    }
    return Status::InvalidArgument("Address region not registered" LOC_MARK);
}

Status TransferEngine::registerLocalMemory(void *addr, size_t size,
                                           Permission permission) {
    MemoryOptions options;
    {
        // If the buffer is allocated by allocateLocalMemory, reuse the
        // memory option with permission override (if needed)
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = allocated_memory_.begin(); it != allocated_memory_.end();
             ++it) {
            if (it->addr == addr) {
                options = it->options;
                break;
            }
        }
    }
    options.perm = permission;
    return registerLocalMemory(addr, size, options);
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

Status TransferEngine::lazyFreeBatch() {
    auto &batch_set = batch_set_.get();
    for (auto it = batch_set.freelist.begin();
         it != batch_set.freelist.end();) {
        auto &batch = *it;
        TransferStatus overall_status;
        CHECK_STATUS(getTransferStatus((BatchID)batch, overall_status));
        if (overall_status.s == WAITING) {
            it++;
            continue;
        }
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            auto &transport = transport_list_[type];
            auto &sub_batch = batch->sub_batch[type];
            if (transport && sub_batch) transport->freeSubBatch(sub_batch);
        }
        batch_set.active.erase(batch);
        Slab<Batch>::Get().deallocate(batch);
        it = batch_set.freelist.erase(it);
    }
    return Status::OK();
}

TransportType TransferEngine::getTransportType(const Request &request) {
    SegmentDesc *remote_desc;
    auto status = metadata_->segmentManager().getRemoteCached(
        remote_desc, request.target_id);
    if (!status.ok()) return UNSPEC;
    if (remote_desc->type == SegmentType::File) {
        if (isCudaMemory(request.source) && transport_list_[GDS]) return GDS;
        if (transport_list_[IOURING]) return IOURING;
        return UNSPEC;
    } else {
        auto entry =
            getBufferDesc(remote_desc, request.target_offset, request.length);
        if (!entry) return UNSPEC;
        if (!entry->mnnvl_handle.empty() && transport_list_[MNNVL])
            return MNNVL;
        if (remote_desc->machine_id ==
            metadata_->segmentManager().getLocal()->machine_id) {
            if (entry->location.starts_with("cuda")) {
                if (entry->shm_path.empty() && transport_list_[SHM]) return SHM;
                if (transport_list_[RDMA]) return RDMA;
            } else {
                if (transport_list_[RDMA]) return RDMA;
                if (transport_list_[SHM]) return SHM;
            }
            return TCP;
        }
        if (transport_list_[RDMA]) return RDMA;
        return TCP;
    }
    return UNSPEC;
}

std::string printRequest(const Request &request) {
    std::stringstream ss;
    ss << "opcode " << request.opcode << " source " << request.source
       << " target_id " << request.target_id << " target_offset "
       << request.target_offset << " length " << request.length;
    return ss.str();
}

struct MergeResult {
    std::vector<Request> request_list;
    std::map<size_t, size_t> task_lookup;
};

MergeResult mergeRequests(const std::vector<Request> &requests) {
    MergeResult result;
    if (requests.empty()) return result;

    struct Item {
        Request req;
        size_t orig_idx;
    };

    std::vector<Item> items;
    items.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); i++)
        items.push_back({requests[i], i});

    std::sort(items.begin(), items.end(), [](const Item &a, const Item &b) {
        if (a.req.opcode != b.req.opcode) return a.req.opcode < b.req.opcode;
        if (a.req.target_id != b.req.target_id)
            return a.req.target_id < b.req.target_id;
        if (a.req.target_offset != b.req.target_offset)
            return a.req.target_offset < b.req.target_offset;
        return a.req.source < b.req.source;
    });

    for (const auto &item : items) {
        if (result.request_list.empty()) {
            result.request_list.push_back(item.req);
            result.task_lookup[item.orig_idx] = result.request_list.size() - 1;
        } else {
            Request &last = result.request_list.back();
            char *last_src_end = static_cast<char *>(last.source) + last.length;
            char *curr_src = static_cast<char *>(item.req.source);
            uint64_t last_tgt_end = last.target_offset + last.length;
            if (last.opcode == item.req.opcode &&
                last.target_id == item.req.target_id &&
                last_src_end == curr_src &&
                last_tgt_end == item.req.target_offset) {
                last.length += item.req.length;
                result.task_lookup[item.orig_idx] =
                    result.request_list.size() - 1;
            } else {
                result.request_list.push_back(item.req);
                result.task_lookup[item.orig_idx] =
                    result.request_list.size() - 1;
            }
        }
    }

    return result;
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<Request> &request_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);

    std::vector<Request> classified_request_list[kSupportedTransportTypes];
    std::vector<size_t> task_id_list[kSupportedTransportTypes];
    std::unordered_map<size_t, TaskInfo> merged_task_id_map;

    size_t start_task_id = batch->task_list.size();
    batch->task_list.insert(batch->task_list.end(), request_list.size(),
                            {UNSPEC, -1, false});

    auto merged = mergeRequests(request_list);
    for (auto &kv : merged.task_lookup) {
        size_t task_id = start_task_id + kv.first;
        size_t merged_task_id = kv.second;
        auto &task = batch->task_list[task_id];
        auto &merged_request = merged.request_list[merged_task_id];
        if (merged_task_id_map.count(merged_task_id)) {
            task = merged_task_id_map[merged_task_id];
            task.derived = true;
            if (task.type != UNSPEC) task_id_list[task.type].push_back(task_id);
            continue;
        }

        auto type = getTransportType(merged_request);
        if (type == UNSPEC) {
            metadata_->segmentManager().invalidateRemote(
                merged_request.target_id);
            type = getTransportType(merged_request);
        }

        if (type == UNSPEC) {
            LOG(WARNING) << "Unable to find registered buffer for request: "
                         << printRequest(merged_request);
            merged_task_id_map[merged_task_id] = task;
            continue;
        }

        if (!batch->sub_batch[type]) {
            auto &transport = transport_list_[type];
            auto status = transport->allocateSubBatch(batch->sub_batch[type],
                                                      batch->max_size);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to allocate SubBatch " << type << ":"
                             << status.ToString();
                merged_task_id_map[merged_task_id] = task;
                continue;
            }
        }

        auto sub_task_id = batch->sub_batch[type]->size();
        classified_request_list[type].push_back(merged_request);
        task.type = type;
        task.sub_task_id = sub_task_id;
        task.derived = false;
        task_id_list[type].push_back(task_id);
        merged_task_id_map[merged_task_id] = task;
    }

    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (classified_request_list[type].empty()) continue;
        auto &transport = transport_list_[type];
        auto &sub_batch = batch->sub_batch[type];
        auto status = transport->submitTransferTasks(
            sub_batch, classified_request_list[type]);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to submit SubBatch " << type << ":"
                         << status.ToString();
            for (auto &task_id : task_id_list[type])
                batch->task_list[task_id].type = UNSPEC;
        }
    }

    return Status::OK();
}

Status TransferEngine::sendNotification(SegmentID target_id,
                                        const Notification &notifi) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto &transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->sendNotification(target_id, notifi);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngine::receiveNotification(
    std::vector<Notification> &notifi_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto &transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->receiveNotification(notifi_list);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    if (task_id >= batch->task_list.size())
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    auto &task = batch->task_list[task_id];
    if (task.type == UNSPEC) {
        status.s = FAILED;
        status.transferred_bytes = 0;
        return Status::OK();
    }
    auto &transport = transport_list_[task.type];
    auto &sub_batch = batch->sub_batch[task.type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("Transport not available" LOC_MARK);
    }
    return transport->getTransferStatus(sub_batch, task.sub_task_id, status);
}

Status TransferEngine::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    status_list.clear();
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        TransferStatus task_status;
        CHECK_STATUS(getTransferStatus(batch_id, task_id, task_status));
        status_list.push_back(task_status);
    }
    return Status::OK();
}

Status TransferEngine::getTransferStatus(BatchID batch_id,
                                         TransferStatus &overall_status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch *batch = (Batch *)(batch_id);
    overall_status.s = WAITING;
    overall_status.transferred_bytes = 0;
    size_t success_tasks = 0;
    size_t total_tasks = 0;
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        auto &task = batch->task_list[task_id];
        if (task.derived) continue;  // This task is performed by other tasks
        total_tasks++;
        if (task.type == UNSPEC) {
            overall_status.s = FAILED;
            continue;
        }
        auto &transport = transport_list_[task.type];
        auto &sub_batch = batch->sub_batch[task.type];
        if (!transport || !sub_batch) {
            return Status::InvalidArgument("Transport not available" LOC_MARK);
        }
        TransferStatus task_status;
        CHECK_STATUS(transport->getTransferStatus(sub_batch, task.sub_task_id,
                                                  task_status));
        if (task_status.s == COMPLETED) {
            success_tasks++;
            overall_status.transferred_bytes += task_status.transferred_bytes;
        } else {
            overall_status.s = task_status.s;
        }
    }
    if (success_tasks == total_tasks) overall_status.s = COMPLETED;
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
