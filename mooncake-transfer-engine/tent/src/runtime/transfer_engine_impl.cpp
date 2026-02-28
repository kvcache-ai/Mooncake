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

#include "tent/runtime/transfer_engine_impl.h"

#include <fstream>
#include <random>
#include <cstdlib>
#include <stdexcept>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/metastore/redis.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/segment_tracker.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/common/utils/ip.h"
#include "tent/common/utils/random.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/metrics/config_loader.h"

namespace mooncake {
namespace tent {

struct Batch {
    Batch() : max_size(0) { sub_batch.fill(nullptr); }

    ~Batch() {}

    std::array<Transport::SubBatchRef, kSupportedTransportTypes> sub_batch;
    std::vector<TaskInfo> task_list;
    size_t max_size;

    struct SubmitHook {
        size_t start_task_id{0};
        size_t end_task_id{0};  // [start, end)
        Notification notifi;
        bool fired{false};
        std::unordered_set<SegmentID> targets;
    };
    std::vector<SubmitHook> submit_hooks;
};

TransferEngineImpl::TransferEngineImpl()
    : conf_(std::make_shared<Config>()), available_(false) {
    ConfigHelper().loadFromEnv(*conf_);
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngineImpl::TransferEngineImpl(std::shared_ptr<Config> conf)
    : conf_(conf), available_(false) {
    // Load environment variables - they should override config file settings
    ConfigHelper().loadFromEnv(*conf_);
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngineImpl::~TransferEngineImpl() { deconstruct(); }

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

Status TransferEngineImpl::setupLocalSegment() {
    auto& manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    segment->name = local_segment_name_;
    segment->type = SegmentType::Memory;
    segment->machine_id = getMachineID();
    auto& detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.topology = *(topology_.get());
    detail.rpc_server_addr = buildIpAddrWithPort(hostname_, port_, ipv6_);
    local_segment_tracker_ = std::make_unique<SegmentTracker>(segment);
    return manager.synchronizeLocal();
}

Status TransferEngineImpl::construct() {
    auto metadata_type = conf_->get("metadata_type", "p2p");
    auto metadata_servers = conf_->get("metadata_servers", "");

    // Get Redis password from environment variable for security
    std::string redis_password;
    const char* env_password = std::getenv("MC_REDIS_PASSWORD");
    if (env_password && *env_password) {
        redis_password = env_password;
    }

    // Get Redis DB index from environment variable or config
    int redis_db_index_config = conf_->get("redis_db_index", 0);
    const char* env_db_index = std::getenv("MC_REDIS_DB_INDEX");
    if (env_db_index && *env_db_index) {
        try {
            redis_db_index_config = std::stoi(env_db_index);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Invalid REDIS_DB_INDEX environment variable: "
                         << env_db_index
                         << ", using config value: " << redis_db_index_config;
        }
    }

    setLogLevel(conf_->get("log_level", "info"));
    hostname_ = conf_->get("rpc_server_hostname", "");
    local_segment_name_ = conf_->get("local_segment_name", "");
    port_ = conf_->get("rpc_server_port", 0);
    merge_requests_ = conf_->get("merge_requests", true);
    if (!hostname_.empty())
        CHECK_STATUS(checkLocalIpAddress(hostname_, ipv6_));
    else
        CHECK_STATUS(discoverLocalIpAddress(hostname_, ipv6_));

    topology_ = std::make_shared<Topology>();
    auto loader = &Platform::getLoader(conf_);
    CHECK_STATUS(topology_->discover({loader}));

    // Validate redis_db_index range (0-255)
    uint8_t db_index = REDIS_DEFAULT_DB_INDEX;
    if (redis_db_index_config >= 0 &&
        redis_db_index_config <= REDIS_MAX_DB_INDEX) {
        db_index = static_cast<uint8_t>(redis_db_index_config);
    } else {
        LOG(WARNING) << "Invalid Redis DB index: " << redis_db_index_config
                     << ", using default "
                     << static_cast<int>(REDIS_DEFAULT_DB_INDEX);
    }

    metadata_ = std::make_shared<ControlService>(
        metadata_type, metadata_servers, redis_password, db_index, this);

    CHECK_STATUS(metadata_->start(port_, ipv6_));

    if (metadata_type == "p2p")
        local_segment_name_ = buildIpAddrWithPort(hostname_, port_, ipv6_);
    else if (local_segment_name_.empty())
        local_segment_name_ = randomSegmentName();

    CHECK_STATUS(setupLocalSegment());
    CHECK_STATUS(loadTransports());

    std::string transport_string;
    for (auto& transport : transport_list_) {
        if (transport) {
            auto status = transport->install(local_segment_name_, metadata_,
                                             topology_, conf_);
            if (!status.ok()) {
                LOG(WARNING) << "Transport " << transport->getName()
                             << " skipped: " << status.ToString();
                transport = nullptr;
                continue;
            }
            transport_string += transport->getName();
            transport_string += " ";
        }
    }

    staging_proxy_ = std::make_unique<ProxyManager>(this);

    // Initialize and start Metrics system
    auto metrics_config = MetricsConfigLoader::loadWithDefaults(conf_.get());
    if (metrics_config.enabled) {
        std::string validation_error;
        if (!MetricsConfigLoader::validateConfig(metrics_config,
                                                 &validation_error)) {
            LOG(WARNING) << "Invalid metrics configuration: "
                         << validation_error << ", Metrics system disabled";
        } else {
            // Initialize metrics
            auto status = TentMetrics::instance().initialize(metrics_config);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to initialize TENT metrics: "
                             << status.ToString();
            } else {
                LOG(INFO) << "TENT Metrics system initialized";
            }
        }
    } else {
        LOG(INFO) << "Metrics system disabled by configuration";
    }

    if (conf_->get("verbose", false)) {
        LOG(INFO) << "========== Transfer Engine Parameters ==========";
        LOG(INFO) << " - Segment Name:       " << local_segment_name_;
        LOG(INFO) << " - RPC Server Address: "
                  << buildIpAddrWithPort(hostname_, port_, ipv6_);
        LOG(INFO) << " - Metadata Type:      " << metadata_type;
        LOG(INFO) << " - Metadata Servers:   " << metadata_servers;
        LOG(INFO) << " - Loaded Transports:  " << transport_string;
        LOG(INFO) << "================================================";
    } else {
        LOG(INFO) << "Transfer Engine " << local_segment_name_
                  << " started successfully";
    }

    return Status::OK();
}

Status TransferEngineImpl::deconstruct() {
    // Metrics cleanup is handled automatically by TentMetrics destructor

    local_segment_tracker_->forEach([&](BufferDesc& desc) -> Status {
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            if (transport_list_[type])
                transport_list_[type]->removeMemoryBuffer(desc);
        }
        return Status::OK();
    });

    // Free all batches BEFORE destroying transports, so that
    // freeSubBatch() can properly return SubBatch/Slice objects
    // to each transport's Slab allocator.
    batch_set_.forEach([&](BatchSet& entry) {
        for (auto& batch : entry.active) {
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                auto& transport = transport_list_[type];
                auto& sub_batch = batch->sub_batch[type];
                if (!transport || !sub_batch) continue;
                transport->freeSubBatch(sub_batch);
            }
            Slab<Batch>::Get().deallocate(batch);
        }
        entry.active.clear();
        entry.freelist.clear();
    });

    // Now safe to destroy transports (workers join here)
    for (auto& transport : transport_list_) transport.reset();
    local_segment_tracker_.reset();
    metadata_->segmentManager().deleteLocal();
    metadata_.reset();
    return Status::OK();
}

const std::string TransferEngineImpl::getSegmentName() const {
    return local_segment_name_;
}

const std::string TransferEngineImpl::getRpcServerAddress() const {
    return hostname_;
}

uint16_t TransferEngineImpl::getRpcServerPort() const { return port_; }

Status TransferEngineImpl::exportLocalSegment(std::string& shared_handle) {
    return Status::NotImplemented(
        "exportLocalSegment not implemented" LOC_MARK);
}

Status TransferEngineImpl::importRemoteSegment(
    SegmentID& handle, const std::string& shared_handle) {
    return Status::NotImplemented(
        "importRemoteSegment not implemented" LOC_MARK);
}

Status TransferEngineImpl::openSegment(SegmentID& handle,
                                       const std::string& segment_name) {
    if (segment_name.empty() || segment_name == local_segment_name_) {
        handle = LOCAL_SEGMENT_ID;
        return Status::OK();
    }
    return metadata_->segmentManager().openRemote(handle, segment_name);
}

Status TransferEngineImpl::closeSegment(SegmentID handle) {
    if (handle == LOCAL_SEGMENT_ID) return Status::OK();
    return metadata_->segmentManager().closeRemote(handle);
}

Status TransferEngineImpl::getSegmentInfo(SegmentID handle, SegmentInfo& info) {
    SegmentDesc* desc = nullptr;
    if (handle == LOCAL_SEGMENT_ID) {
        desc = metadata_->segmentManager().getLocal().get();
    } else {
        CHECK_STATUS(metadata_->segmentManager().getRemoteCached(desc, handle));
    }
    if (desc->type == SegmentType::File) {
        info.type = SegmentInfo::File;
        auto& detail = std::get<FileSegmentDesc>(desc->detail);
        for (auto& entry : detail.buffers) {
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = entry.offset,
                                    .length = entry.length,
                                    .location = kWildcardLocation});
        }
    } else {
        info.type = SegmentInfo::Memory;
        auto& detail = std::get<MemorySegmentDesc>(desc->detail);
        for (auto& entry : detail.buffers) {
            if (entry.internal) continue;
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = (uint64_t)entry.addr,
                                    .length = entry.length,
                                    .location = entry.location});
        }
    }
    return Status::OK();
}

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               Location location) {
    return allocateLocalMemory(addr, size, location, false);
}

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               Location location,
                                               bool internal) {
    // Decide transport type based on location
    MemoryOptions options;
    options.location = location;
    options.internal = internal;
    if (location == kWildcardLocation ||
        LocationParser(location).type() == "cpu") {
        if (transport_list_[SHM])
            options.type = SHM;
        else if (transport_list_[RDMA])
            options.type = RDMA;
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

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               MemoryOptions& options) {
    if (options.type == UNSPEC) {
        if (transport_list_[RDMA])
            options.type = RDMA;
        else if (transport_list_[TCP])
            options.type = TCP;
        else
            return Status::InvalidArgument(
                "Not supported type in memory options" LOC_MARK);
    }
    auto& transport = transport_list_[options.type];
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

Status TransferEngineImpl::freeLocalMemory(void* addr) {
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

Status TransferEngineImpl::registerLocalMemory(void* addr, size_t size,
                                               Permission permission) {
    MemoryOptions options;
    options.perm = permission;
    return registerLocalMemory({addr}, {size}, options);
}

Status TransferEngineImpl::registerLocalMemory(std::vector<void*> addr_list,
                                               std::vector<size_t> size_list,
                                               Permission permission) {
    MemoryOptions options;
    options.perm = permission;
    return registerLocalMemory(addr_list, size_list, options);
}

std::vector<TransportType> TransferEngineImpl::getSupportedTransports(
    TransportType request_type) {
    std::vector<TransportType> result;
    if (transport_list_[MNNVL]) result.push_back(MNNVL);
    if (transport_list_[NVLINK]) result.push_back(NVLINK);
    if (transport_list_[RDMA]) result.push_back(RDMA);
    if (transport_list_[AscendDirect]) result.push_back(AscendDirect);
    if (transport_list_[SHM]) result.push_back(SHM);
    if (transport_list_[TCP]) result.push_back(TCP);
    if (transport_list_[GDS]) result.push_back(GDS);
    return result;
}

Status TransferEngineImpl::registerLocalMemory(std::vector<void*> addr_list,
                                               std::vector<size_t> size_list,
                                               MemoryOptions& options) {
    if (addr_list.size() != size_list.size()) {
        return Status::InvalidArgument(
            "Mismatched addresses and sizes in registerLocalMemory" LOC_MARK);
    }
    auto transports = getSupportedTransports(options.type);

    // Build BufferDescs: warm-up → NUMA probe → fill location
    std::vector<BufferDesc> desc_list;
    desc_list.reserve(addr_list.size());
    for (size_t i = 0; i < addr_list.size(); ++i) {
        BufferDesc desc;
        desc.addr = (uint64_t)addr_list[i];
        desc.length = size_list[i];

        // MR warm-up: pin pages via temp ibv_reg_mr, benefits both
        // subsequent RDMA registration and NUMA probing
        bool pages_pinned = false;
        for (auto type : transports) {
            if (transport_list_[type]->warmupMemory(addr_list[i],
                                                    size_list[i])) {
                pages_pinned = true;
                break;
            }
        }

        // NUMA probe: skip prefault if warm-up already pinned pages
        auto entries = Platform::getLoader().getLocation(
            addr_list[i], size_list[i], pages_pinned);
        if (entries.size() == 1) {
            desc.location = entries[0].location;
        } else {
            desc.location = entries[0].location;
            for (auto& entry : entries)
                desc.regions.push_back(Region{entry.len, entry.location});
        }
        desc.ref_count = 1;
        if (options.location != kWildcardLocation)
            desc.location = options.location;
        if (options.internal) desc.internal = options.internal;
        desc_list.push_back(std::move(desc));
    }

    auto status = local_segment_tracker_->addInBatch(
        desc_list, [&](std::vector<BufferDesc>& descs) -> Status {
            for (auto type : transports) {
                auto s = transport_list_[type]->addMemoryBuffer(descs, options);
                if (!s.ok()) LOG(WARNING) << s.ToString();
            }
            return Status::OK();
        });
    if (!status.ok()) return status;
    // Synchronize local segment to metadata server so remote peers can see the
    // new buffers
    return metadata_->segmentManager().synchronizeLocal();
}

// WARNING: before exiting TE, make sure that all local memory are
// unregistered, otherwise the CUDA may halt!
Status TransferEngineImpl::unregisterLocalMemory(void* addr, size_t size) {
    bool removed = false;
    auto status = local_segment_tracker_->remove(
        (uint64_t)addr, size, [&](BufferDesc& desc) -> Status {
            removed = true;
            for (auto type : desc.transports) {
                auto status = transport_list_[type]->removeMemoryBuffer(desc);
                if (!status.ok()) LOG(WARNING) << status.ToString();
            }
            return Status::OK();
        });
    if (!status.ok()) return status;
    if (!removed) return Status::OK();
    return metadata_->segmentManager().synchronizeLocal();
}

Status TransferEngineImpl::unregisterLocalMemory(
    std::vector<void*> addr_list, std::vector<size_t> size_list) {
    if (!size_list.empty() && addr_list.size() != size_list.size()) {
        return Status::InvalidArgument(
            "Mismatched addresses and sizes in unregisterLocalMemory" LOC_MARK);
    }
    bool removed_any = false;
    for (size_t i = 0; i < addr_list.size(); ++i) {
        bool removed = false;
        auto status = local_segment_tracker_->remove(
            (uint64_t)addr_list[i], size_list.empty() ? 0 : size_list[i],
            [&](BufferDesc& desc) -> Status {
                removed = true;
                for (auto type : desc.transports) {
                    auto s = transport_list_[type]->removeMemoryBuffer(desc);
                    if (!s.ok()) LOG(WARNING) << s.ToString();
                }
                return Status::OK();
            });
        if (!status.ok()) return status;
        if (removed) removed_any = true;
    }
    if (!removed_any) return Status::OK();
    return metadata_->segmentManager().synchronizeLocal();
}

BatchID TransferEngineImpl::allocateBatch(size_t batch_size) {
    Batch* batch = Slab<Batch>::Get().allocate();
    if (!batch) return (BatchID)0;
    batch->max_size = batch_size;
    batch_set_.get().active.insert(batch);
    return (BatchID)batch;
}

Status TransferEngineImpl::freeBatch(BatchID batch_id) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    batch_set_.get().freelist.push_back(batch);
    lazyFreeBatch();
    return Status::OK();
}

Status TransferEngineImpl::lazyFreeBatch() {
    auto& batch_set = batch_set_.get();
    for (auto it = batch_set.freelist.begin();
         it != batch_set.freelist.end();) {
        auto& batch = *it;
        TransferStatus overall_status;
        CHECK_STATUS(getTransferStatus((BatchID)batch, overall_status));
        if (overall_status.s == PENDING) {
            it++;
            continue;
        }
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            auto& transport = transport_list_[type];
            auto& sub_batch = batch->sub_batch[type];
            if (transport && sub_batch) transport->freeSubBatch(sub_batch);
        }
        batch_set.active.erase(batch);
        Slab<Batch>::Get().deallocate(batch);
        it = batch_set.freelist.erase(it);
    }
    return Status::OK();
}

static bool checkAvailability(const std::shared_ptr<Transport>& xport,
                              MemoryType local) {
    if (local == MTYPE_CPU) return xport && xport->capabilities().dram_to_file;
    if (local == MTYPE_CUDA) return xport && xport->capabilities().gpu_to_file;
    return false;
}

static bool checkAvailability(const std::shared_ptr<Transport>& xport,
                              MemoryType local, MemoryType remote) {
    if (local == MTYPE_CPU && remote == MTYPE_CPU)
        return xport && xport->capabilities().dram_to_dram;
    if (local == MTYPE_CUDA && remote == MTYPE_CUDA)
        return xport && xport->capabilities().gpu_to_gpu;
    if (local == MTYPE_CPU && remote == MTYPE_CUDA)
        return xport && xport->capabilities().dram_to_gpu;
    if (local == MTYPE_CUDA && remote == MTYPE_CPU)
        return xport && xport->capabilities().gpu_to_dram;
    return false;
}

static MemoryType getTypeEnum(const std::string& type) {
    if (type == "cpu" || type == "*") return MTYPE_CPU;
    if (type == "cuda") return MTYPE_CUDA;
    if (type == "npu") return MTYPE_CUDA;
    return MTYPE_UNKNOWN;
}

TransportType TransferEngineImpl::getTransportType(const Request& request,
                                                   int priority) {
    SegmentDesc* desc;
    if (request.target_id == LOCAL_SEGMENT_ID) {
        desc = metadata_->segmentManager().getLocal().get();
    } else {
        auto status = metadata_->segmentManager().getRemoteCached(
            desc, request.target_id);
        if (!status.ok()) return UNSPEC;
    }
    auto local_mtype = Platform::getLoader().getMemoryType(request.source);
    if (desc->type == SegmentType::File) {
        if (checkAvailability(transport_list_[GDS], local_mtype)) {
            if (priority-- == 0) return GDS;
        }
        if (checkAvailability(transport_list_[IOURING], local_mtype)) {
            if (priority-- == 0) return IOURING;
        }
        return UNSPEC;
    } else {
        auto entry = desc->findBuffer(request.target_offset, request.length);
        if (!entry) return UNSPEC;
        bool same_machine =
            (desc->machine_id ==
             metadata_->segmentManager().getLocal()->machine_id);
        auto remote_mtype = getTypeEnum(LocationParser(entry->location).type());
        for (auto type : entry->transports) {
            if ((type == NVLINK || type == SHM) && !same_machine) continue;
            if (checkAvailability(transport_list_[type], local_mtype,
                                  remote_mtype)) {
                if (priority-- == 0) return type;
            }
        }
        return UNSPEC;
    }
}

std::string printRequest(const Request& request) {
    std::stringstream ss;
    ss << "opcode " << request.opcode << " source " << request.source
       << " target_id " << request.target_id << " target_offset "
       << (void*)request.target_offset << " length " << request.length;
    return ss.str();
}

struct MergeResult {
    std::vector<Request> request_list;
    std::map<size_t, size_t> task_lookup;
};

MergeResult mergeRequests(const std::vector<Request>& requests, bool do_merge) {
    MergeResult result;
    if (requests.empty()) return result;
    if (!do_merge) {
        size_t idx = 0;
        for (auto& req : requests) {
            result.request_list.push_back(req);
            result.task_lookup[idx] = idx;
            idx++;
        }
        return result;
    }

    struct Item {
        Request req;
        size_t orig_idx;
    };

    std::vector<Item> items;
    items.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); i++)
        items.push_back({requests[i], i});

    std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) {
        if (a.req.opcode != b.req.opcode) return a.req.opcode < b.req.opcode;
        if (a.req.target_id != b.req.target_id)
            return a.req.target_id < b.req.target_id;
        if (a.req.target_offset != b.req.target_offset)
            return a.req.target_offset < b.req.target_offset;
        return a.req.source < b.req.source;
    });

    for (const auto& item : items) {
        if (result.request_list.empty()) {
            result.request_list.push_back(item.req);
            result.task_lookup[item.orig_idx] = result.request_list.size() - 1;
        } else {
            Request& last = result.request_list.back();
            char* last_src_end = static_cast<char*>(last.source) + last.length;
            char* curr_src = static_cast<char*>(item.req.source);
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

void TransferEngineImpl::findStagingPolicy(const Request& request,
                                           std::vector<std::string>& policy) {
    SegmentDesc* desc;
    if (request.target_id == LOCAL_SEGMENT_ID) return;
    auto status =
        metadata_->segmentManager().getRemoteCached(desc, request.target_id);
    if (!status.ok()) return;
    auto entry = desc->findBuffer(request.target_offset, request.length);
    if (!entry) return;
    auto local =
        Platform::getLoader().getLocation(request.source, 1)[0].location;
    auto remote = entry->location;
    auto local_mtype = getTypeEnum(LocationParser(local).type());
    auto remote_mtype = getTypeEnum(LocationParser(remote).type());
    auto server_addr = desc->getMemory().rpc_server_addr;
    policy.clear();
    // case 1: rdma without gpu direct
    if (transport_list_[RDMA] && transport_list_[NVLINK]) {
        auto& xport = transport_list_[RDMA];
        auto& caps = xport->capabilities();
        if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CUDA &&
            !caps.gpu_to_gpu) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        } else if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CPU &&
                   !caps.gpu_to_dram) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back("");  // no remote stage
        } else if (local_mtype == MTYPE_CPU && remote_mtype == MTYPE_CUDA &&
                   !caps.dram_to_gpu) {
            policy.push_back(server_addr);
            policy.push_back("");  // no local stage
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        }
    }
    // case 2: pure mnnvl
    if (transport_list_[MNNVL] && transport_list_[NVLINK]) {
        auto& xport = transport_list_[RDMA];
        auto& caps = xport->capabilities();
        if (local_mtype == MTYPE_CPU && remote_mtype == MTYPE_CPU &&
            !caps.dram_to_dram) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local, Topology::MEM_CUDA));
            policy.push_back("");  // remote stage
        } else if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CPU &&
                   !caps.gpu_to_dram) {
            policy.push_back(server_addr);
            policy.push_back("");  // no local stage
            policy.push_back(desc->getMemory().topology.findNearMem(
                remote, Topology::MEM_CUDA));
        }
    }
}

TransportType TransferEngineImpl::resolveTransport(const Request& req,
                                                   int priority,
                                                   bool invalidate_on_fail) {
    auto type = getTransportType(req, priority);
    if (type == UNSPEC && invalidate_on_fail) {
        metadata_->segmentManager().invalidateRemote(req.target_id);
        type = getTransportType(req, priority);
    }
    return type;
}

Status TransferEngineImpl::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);

    std::vector<Request> classified_request_list[kSupportedTransportTypes];
    std::vector<size_t> task_id_list[kSupportedTransportTypes];
    std::unordered_map<size_t, TaskInfo> merged_task_id_map;

    size_t start_task_id = batch->task_list.size();
    batch->task_list.insert(batch->task_list.end(), request_list.size(),
                            TaskInfo{});

    // Record start time for metrics tracking
    auto submit_time = std::chrono::steady_clock::now();

    auto merged = mergeRequests(request_list, merge_requests_);
    std::unordered_map<TransportType, size_t> next_sub_task_id;
    for (auto& kv : merged.task_lookup) {
        size_t task_id = start_task_id + kv.first;
        size_t merged_task_id = kv.second;
        auto& task = batch->task_list[task_id];
        auto& merged_request = merged.request_list[merged_task_id];
        if (merged_task_id_map.count(merged_task_id)) {
            task = merged_task_id_map[merged_task_id];
            task.derived = true;
            if (task.type != UNSPEC) task_id_list[task.type].push_back(task_id);
            continue;
        }

        task.xport_priority = 0;
        task.status = PENDING;
        task.request = merged_request;
        task.staging = false;
        task.start_time =
            submit_time;  // Record start time for latency tracking
        task.type = resolveTransport(merged_request, 0);
        if (task.type == UNSPEC) {
            LOG(WARNING) << "Unable to find registered buffer for request: "
                         << printRequest(merged_request);
            merged_task_id_map[merged_task_id] = task;
            continue;
        }

        if (task.type == TCP) {
            std::vector<std::string> staging_params;
            findStagingPolicy(merged_request, staging_params);
            if (!staging_params.empty() && staging_proxy_) {
                task.staging = true;
                staging_proxy_->submit(&task, staging_params);
                continue;
            }
        }

        if (!batch->sub_batch[task.type]) {
            auto& transport = transport_list_[task.type];
            auto status = transport->allocateSubBatch(
                batch->sub_batch[task.type], batch->max_size);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to allocate SubBatch " << task.type
                             << ":" << status.ToString();
                merged_task_id_map[merged_task_id] = task;
                continue;
            }
        }

        if (!next_sub_task_id.count(task.type))
            next_sub_task_id[task.type] = batch->sub_batch[task.type]->size();
        size_t sub_task_id = next_sub_task_id[task.type];
        next_sub_task_id[task.type]++;

        classified_request_list[task.type].push_back(merged_request);
        task.sub_task_id = sub_task_id;
        task.derived = false;
        task_id_list[task.type].push_back(task_id);
        merged_task_id_map[merged_task_id] = task;
    }

    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (classified_request_list[type].empty()) continue;
        auto& transport = transport_list_[type];
        auto& sub_batch = batch->sub_batch[type];
        auto status = transport->submitTransferTasks(
            sub_batch, classified_request_list[type]);
        if (!status.ok()) {
            // LOG(WARNING) << "Failed to submit SubBatch " << type << ":"
            //              << status.ToString();
            for (auto& task_id : task_id_list[type])
                batch->task_list[task_id].type = UNSPEC;
        }
    }

    return Status::OK();
}

Status TransferEngineImpl::maybeFireSubmitHooks(Batch* batch, bool check) {
    for (auto& hook : batch->submit_hooks) {
        if (hook.fired) continue;
        bool all_completed = true;
        if (check) {
            for (size_t tid = hook.start_task_id; tid < hook.end_task_id;
                 ++tid) {
                auto& t = batch->task_list[tid];
                if (t.status == PENDING) {
                    all_completed = false;
                    break;
                }
                if (t.status != COMPLETED) {
                    all_completed = false;
                    break;
                }
            }
        }
        if (!all_completed) continue;
        Status last = Status::OK();
        for (auto target_id : hook.targets) {
            last = sendNotification(target_id, hook.notifi);
            if (!last.ok()) {
                LOG(WARNING) << "sendNotification failed: " << last.ToString();
                break;
            }
        }
        if (last.ok()) hook.fired = true;
    }
    return Status::OK();
}

Status TransferEngineImpl::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list,
    const Notification& notifi) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    const size_t start_task_id = batch->task_list.size();
    CHECK_STATUS(submitTransfer(batch_id, request_list));
    const size_t end_task_id = start_task_id + request_list.size();
    Batch::SubmitHook hook;
    hook.start_task_id = start_task_id;
    hook.end_task_id = end_task_id;
    hook.notifi = notifi;
    hook.fired = false;
    for (const auto& request : request_list)
        hook.targets.insert(request.target_id);
    batch->submit_hooks.emplace_back(std::move(hook));
    return Status::OK();
}

Status TransferEngineImpl::resubmitTransferTask(Batch* batch, size_t task_id) {
    auto& task = batch->task_list[task_id];
    if (task.staging)
        task.staging = false;
    else
        task.xport_priority++;
    auto type = resolveTransport(task.request, task.xport_priority);
    if (type == UNSPEC)
        return Status::InvalidEntry("All available transports are failed");

    auto& transport = transport_list_[type];
    if (!batch->sub_batch[type])
        CHECK_STATUS(transport->allocateSubBatch(batch->sub_batch[type],
                                                 batch->max_size));
    auto& sub_batch = batch->sub_batch[type];
    task.sub_task_id = sub_batch->size();
    task.type = type;
    return transport->submitTransferTasks(sub_batch, {task.request});
}

Status TransferEngineImpl::sendNotification(SegmentID target_id,
                                            const Notification& notifi) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto& transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->sendNotification(target_id, notifi);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngineImpl::receiveNotification(
    std::vector<Notification>& notifi_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto& transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->receiveNotification(notifi_list);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngineImpl::getTransferStatus(BatchID batch_id, size_t task_id,
                                             TransferStatus& task_status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    if (task_id >= batch->task_list.size())
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    auto& task = batch->task_list[task_id];
    auto prev_status = task.status;
    if (task.staging) {
        CHECK_STATUS(staging_proxy_->getStatus(&task, task_status));
    } else {
        if (task.type == UNSPEC) {
            if (resubmitTransferTask(batch, task_id).ok())
                task_status.s = PENDING;
            else
                task_status.s = FAILED;
            task_status.transferred_bytes = 0;
            return Status::OK();
        }
        auto& transport = transport_list_[task.type];
        auto& sub_batch = batch->sub_batch[task.type];
        if (!transport || !sub_batch) {
            return Status::InvalidArgument("Transport not available" LOC_MARK);
        }
        CHECK_STATUS(transport->getTransferStatus(sub_batch, task.sub_task_id,
                                                  task_status));
    }
    if (task_status.s == FAILED && resubmitTransferTask(batch, task_id).ok()) {
        task_status.s = PENDING;
        task_status.transferred_bytes = 0;
    }
    batch->task_list[task_id].status = task_status.s;

    // Record metrics when task transitions to terminal state
    recordTaskCompletionMetrics(batch->task_list[task_id], prev_status,
                                task_status.s);

    if (task_status.s == COMPLETED) CHECK_STATUS(maybeFireSubmitHooks(batch));
    return Status::OK();
}

Status TransferEngineImpl::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus>& status_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    status_list.clear();
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        TransferStatus task_status;
        CHECK_STATUS(getTransferStatus(batch_id, task_id, task_status));
        status_list.push_back(task_status);
    }
    return Status::OK();
}

Status TransferEngineImpl::getTransferStatus(BatchID batch_id,
                                             TransferStatus& overall_status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    overall_status.s = PENDING;
    overall_status.transferred_bytes = 0;
    size_t success_tasks = 0;
    size_t total_tasks = 0;
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        auto& task = batch->task_list[task_id];
        if (task.derived) continue;  // This task is performed by other tasks
        total_tasks++;
        TransferStatus task_status;
        if (task.status != PENDING) {
            if (task.status == COMPLETED) {
                success_tasks++;
                overall_status.transferred_bytes += task.request.length;
            } else {
                overall_status.s = task.status;
            }
            continue;
        }
        auto prev_status = task.status;
        if (task.staging) {
            CHECK_STATUS(staging_proxy_->getStatus(&task, task_status));
        } else {
            if (task.type == UNSPEC) {
                if (!resubmitTransferTask(batch, task_id).ok())
                    overall_status.s = FAILED;
                continue;
            }
            auto& transport = transport_list_[task.type];
            auto& sub_batch = batch->sub_batch[task.type];
            if (!transport || !sub_batch) {
                return Status::InvalidArgument(
                    "Transport not available" LOC_MARK);
            }
            CHECK_STATUS(transport->getTransferStatus(
                sub_batch, task.sub_task_id, task_status));
        }
        if (task_status.s == FAILED &&
            resubmitTransferTask(batch, task_id).ok()) {
            task_status.s = PENDING;
            task_status.transferred_bytes = 0;
        }
        if (task_status.s == COMPLETED) {
            success_tasks++;
            overall_status.transferred_bytes += task_status.transferred_bytes;
        } else {
            overall_status.s = task_status.s;
        }
        // memorize task result
        task.status = task_status.s;

        // Record metrics when task transitions to terminal state
        recordTaskCompletionMetrics(batch->task_list[task_id], prev_status,
                                    task_status.s);
    }
    if (success_tasks == total_tasks) overall_status.s = COMPLETED;
    CHECK_STATUS(maybeFireSubmitHooks(batch, overall_status.s == COMPLETED));
    return Status::OK();
}

Status TransferEngineImpl::waitTransferCompletion(BatchID batch_id) {
    TransferStatus xfer_status;
    while (true) {
        CHECK_STATUS(getTransferStatus(batch_id, xfer_status));
        if (xfer_status.s != PENDING) {
            freeBatch(batch_id);
            return xfer_status.s == COMPLETED
                       ? Status::OK()
                       : Status::InternalError(
                             "Transfer failed: " +
                             std::to_string((int)xfer_status.s));
        }
    }
}

Status TransferEngineImpl::transferSync(
    const std::vector<Request>& request_list) {
    auto batch_id = allocateBatch(request_list.size());
    CHECK_STATUS(submitTransfer(batch_id, request_list));
    while (true) {
        TransferStatus xfer_status;
        CHECK_STATUS(getTransferStatus(batch_id, xfer_status));
        if (xfer_status.s == COMPLETED) break;
        if (xfer_status.s != PENDING) {
            CHECK_STATUS(freeBatch(batch_id));
            return Status::InternalError(
                "Transfer via stage buffer failed" LOC_MARK);
        }
    }
    CHECK_STATUS(freeBatch(batch_id));
    return Status::OK();
}

uint64_t TransferEngineImpl::lockStageBuffer(const std::string& location) {
    uint64_t addr = 0;
    auto status = staging_proxy_->pinStageBuffer(location, addr);
    if (!status.ok()) LOG(ERROR) << status.ToString();
    return addr;
}

Status TransferEngineImpl::unlockStageBuffer(uint64_t addr) {
    return staging_proxy_->unpinStageBuffer(addr);
}

void TransferEngineImpl::recordTaskCompletionMetrics(
    TaskInfo& task, TransferStatusEnum prev_status,
    TransferStatusEnum new_status) {
    if (prev_status == PENDING && new_status != PENDING && !task.derived) {
        auto start_time = task.start_time;
        if (start_time.time_since_epoch().count() > 0) {
            auto end_time = std::chrono::steady_clock::now();
            double latency_seconds =
                std::chrono::duration<double>(end_time - start_time).count();
            if (new_status == COMPLETED) {
                if (task.request.opcode == Request::READ) {
                    TentMetrics::instance().recordReadCompleted(
                        task.request.length, latency_seconds);
                } else {
                    TentMetrics::instance().recordWriteCompleted(
                        task.request.length, latency_seconds);
                }
            } else if (new_status == FAILED) {
                if (task.request.opcode == Request::READ) {
                    TentMetrics::instance().recordReadFailed(
                        task.request.length);
                } else {
                    TentMetrics::instance().recordWriteFailed(
                        task.request.length);
                }
            }
            // Reset start_time to prevent duplicate recording
            task.start_time = std::chrono::steady_clock::time_point{};
        }
    }
}

}  // namespace tent
}  // namespace mooncake
