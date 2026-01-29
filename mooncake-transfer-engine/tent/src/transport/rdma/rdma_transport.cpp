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

#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/ibv_loader.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "tent/common/status.h"
#include "tent/transport/rdma/buffers.h"
#include "tent/transport/rdma/endpoint_store.h"
#include "tent/transport/rdma/workers.h"
#include "tent/common/utils/string_builder.h"
#include "tent/runtime/topology.h"
#include "tent/common/utils/random.h"

#define SET_DEVICE(key, param) \
    param = conf->get("transports/rdma/device/" #key, param)

#define SET_ENDPOINT(key, param) \
    param = conf->get("transports/rdma/endpoint/" #key, param)

#define SET_WORKERS(key, param) \
    param = conf->get("transports/rdma/workers/" #key, param)

namespace mooncake {
namespace tent {
static void convertConfToRdmaParams(std::shared_ptr<Config> conf,
                                    std::shared_ptr<RdmaParams> params) {
    SET_DEVICE(num_cq_list, params->device.num_cq_list);
    SET_DEVICE(num_comp_channels, params->device.num_comp_channels);
    SET_DEVICE(port, params->device.port);
    SET_DEVICE(gid_index, params->device.gid_index);
    SET_DEVICE(max_cqe, params->device.max_cqe);

    SET_ENDPOINT(endpoint_store_cap, params->endpoint.endpoint_store_cap);
    SET_ENDPOINT(qp_mul_factor, params->endpoint.qp_mul_factor);
    SET_ENDPOINT(max_sge, params->endpoint.max_sge);
    SET_ENDPOINT(max_qp_wr, params->endpoint.max_qp_wr);
    SET_ENDPOINT(max_inline_bytes, params->endpoint.max_inline_bytes);
    SET_ENDPOINT(pkey_index, params->endpoint.pkey_index);
    SET_ENDPOINT(hop_limit, params->endpoint.hop_limit);
    SET_ENDPOINT(flow_label, params->endpoint.flow_label);
    SET_ENDPOINT(traffic_class, params->endpoint.traffic_class);
    SET_ENDPOINT(service_level, params->endpoint.service_level);
    SET_ENDPOINT(src_path_bits, params->endpoint.src_path_bits);
    SET_ENDPOINT(static_rate, params->endpoint.static_rate);
    SET_ENDPOINT(rq_psn, params->endpoint.rq_psn);
    SET_ENDPOINT(max_dest_rd_atomic, params->endpoint.max_dest_rd_atomic);
    SET_ENDPOINT(min_rnr_timer, params->endpoint.min_rnr_timer);
    SET_ENDPOINT(sq_psn, params->endpoint.sq_psn);
    SET_ENDPOINT(send_timeout, params->endpoint.send_timeout);
    SET_ENDPOINT(send_retry_count, params->endpoint.send_retry_count);
    SET_ENDPOINT(send_rnr_count, params->endpoint.send_rnr_count);
    SET_ENDPOINT(max_rd_atomic, params->endpoint.max_rd_atomic);

    size_t mtu_val = conf->get("transports/rdma/endpoint/path_mtu", 4096);
    if (mtu_val == 4096)
        params->endpoint.path_mtu = IBV_MTU_4096;
    else if (mtu_val == 2048)
        params->endpoint.path_mtu = IBV_MTU_2048;
    else if (mtu_val == 1024)
        params->endpoint.path_mtu = IBV_MTU_1024;
    else
        params->endpoint.path_mtu = IBV_MTU_512;

    SET_WORKERS(num_workers, params->workers.num_workers);
    SET_WORKERS(max_retry_count, params->workers.max_retry_count);
    SET_WORKERS(block_size, params->workers.block_size);
    SET_WORKERS(grace_period_ns, params->workers.grace_period_ns);
    SET_WORKERS(rail_topo_path, params->workers.rail_topo_path);

    params->verbose = conf->get("verbose", false);
}

static bool isGpuDirectRdmaSupported() {
    if (getenv("MC_DISABLE_GPU_DIRECT_RDMA")) {
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

RdmaTransport::RdmaTransport() : installed_(false) {}

RdmaTransport::~RdmaTransport() { uninstall(); }

// Check if RDMA relaxed ordering is supported
static bool hasIbvRegMrIova2() {
    return IbvLoader::Instance().symbols().ibv_reg_mr_iova2 != nullptr;
}

// Determine whether RELAXED_ORDERING is enabled
static bool shouldEnableRelaxedOrdering() {
    const char* env_val = std::getenv("MC_IB_PCI_RELAXED_ORDERING");
    int mode = 0;  // Default: disabled for backward compatibility
    if (env_val) {
        // Validate the input is a number
        char* end;
        long val = std::strtol(env_val, &end, 10);
        
        // Check if conversion was successful and value is in valid range
        if (*end != '\0' || val < 0 || val > 2) {
            LOG(WARNING) << "[RDMA TENT] Invalid MC_IB_PCI_RELAXED_ORDERING value: "
                         << env_val << ". Expected 0, 1, or 2. Using default (0).";
            mode = 0;
        } else {
            mode = static_cast<int>(val);
        }
    }
    
    if (mode == 0) {
        // Explicitly disabled
        return false;
    }
    
    // Mode 1 or 2 (auto): enabled if supported
    bool supported = hasIbvRegMrIova2();
    if (supported) {
        LOG(INFO) << "[RDMA TENT] Relaxed ordering is supported; "
                  << "IBV_ACCESS_RELAXED_ORDERING will be enabled for "
                  << "optimal GPUDirect RDMA performance.";
    } else {
        LOG(INFO) << "[RDMA TENT] Relaxed ordering is NOT supported "
                  << "(ibv_reg_mr_iova2 missing). "
                  << "Falling back to strict ordering.";
    }
    return supported;
}

Status RdmaTransport::install(std::string& local_segment_name,
                              std::shared_ptr<ControlService> metadata,
                              std::shared_ptr<Topology> local_topology,
                              std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "RDMA transport has been installed" LOC_MARK);
    }

    if (!IbvLoader::Instance().available()) {
        return Status::InvalidArgument("RDMA transport not available" LOC_MARK);
    }

    if (local_topology == nullptr ||
        !local_topology->getNicCount(Topology::NIC_RDMA)) {
        return Status::DeviceNotFound(
            "No RDMA device found in topology" LOC_MARK);
    }

    conf_ = conf;
    params_ = std::make_shared<RdmaParams>();
    convertConfToRdmaParams(conf_, params_);
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    local_buffer_manager_.setTopology(local_topology);
    
    // Enable relaxed ordering for optimal GPUDirect RDMA performance
    bool relaxed_ordering_enabled = shouldEnableRelaxedOrdering();
    local_buffer_manager_.setRelaxedOrderingEnabled(relaxed_ordering_enabled);
    
    context_set_.clear();
    for (size_t i = 0; i < local_topology_->getNicCount(); ++i) {
        auto entry = local_topology_->getNicEntry(i);
        if (entry->type != Topology::NIC_RDMA) continue;
        auto context = std::make_shared<RdmaContext>(*this);
        int ret = context->construct(entry->name, params_);
        if (ret) {
            LOG(WARNING) << "Disable RDMA device " << entry->name << " because "
                         << "of initialization failure";
            continue;
        }
        context_name_lookup_[entry->name] = context_set_.size();
        context_set_.push_back(context);
        local_buffer_manager_.addDevice(context.get());
    }
    const bool context_empty = context_set_.empty();
    const bool topology_empty = local_topology_->empty();
    if (context_empty || topology_empty) {
        const char* error_message = "No RDMA device initialized successfully";
        uninstall();
        return Status::DeviceNotFound(std::string(error_message) + LOC_MARK);
    }

    if (conf_->get("verbose", false)) {
        local_topology_->print();
    }
    setupLocalSegment();

    metadata_->setBootstrapRdmaCallback(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2));

    workers_ = std::make_unique<Workers>(this);
    workers_->start();

    installed_ = true;
    caps.dram_to_dram = true;
    if (isGpuDirectRdmaSupported()) {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    }
    return Status::OK();
}

Status RdmaTransport::uninstall() {
    if (installed_) {
        workers_.reset();
        metadata_.reset();
        local_buffer_manager_.clear();
        context_set_.clear();
        context_name_lookup_.clear();
        installed_ = false;
    }
    return Status::OK();
}

Status RdmaTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto rdma_batch = Slab<RdmaSubBatch>::Get().allocate();
    if (!rdma_batch)
        return Status::InternalError(
            "Unable to allocate RDMA sub-batch" LOC_MARK);
    batch = rdma_batch;
    rdma_batch->task_list.reserve(max_size);
    rdma_batch->max_size = max_size;
    return Status::OK();
}

Status RdmaTransport::freeSubBatch(SubBatchRef& batch) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    for (auto slice : rdma_batch->slice_chain) {
        while (slice) {
            auto next = slice->next;
            RdmaSliceStorage::Get().deallocate(slice);
            slice = next;
        }
    }
    Slab<RdmaSubBatch>::Get().deallocate(rdma_batch);
    batch = nullptr;
    return Status::OK();
}

static inline uint64_t roundup(uint64_t a, uint64_t b) {
    return (a % b == 0) ? a : (a / b + 1) * b;
}

Status RdmaTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    if (request_list.size() + rdma_batch->task_list.size() >
        rdma_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    const size_t default_block_size = params_->workers.block_size;
    const int num_workers = params_->workers.num_workers;
    const int num_devices = (size_t)local_topology_->getNicCount();
    std::vector<RdmaSliceList> slice_lists(num_workers);
    std::vector<RdmaSlice*> slice_tails(num_workers, nullptr);
    auto enqueue_ts = getCurrentTimeInNano();

    static std::atomic<int> g_caller_threads(0);
    thread_local int tl_caller_id = g_caller_threads.fetch_add(1);
    bool enable_spray =
        g_caller_threads.load(std::memory_order_relaxed) <= num_workers;
    int submit_slices = 0;
    for (auto& request : request_list) {
        auto opcode = request.opcode;
        auto type = Platform::getLoader().getMemoryType(request.source);
        size_t max_slice_count = 64;
        if (type == MTYPE_CUDA || opcode == Request::WRITE)
            max_slice_count = 32;
        rdma_batch->task_list.push_back(RdmaTask{});
        auto& task = rdma_batch->task_list.back();
        task.request = request;
        task.num_slices = 0;
        task.status_word = PENDING;
        task.transferred_bytes = 0;

        const double merge_ratio = 0.25;
        uint64_t base_block = default_block_size;
        uint64_t num_slices = (request.length + base_block - 1) / base_block;
        num_slices = std::max<uint64_t>(
            1, std::min<uint64_t>(num_slices, max_slice_count));

        if (num_slices > 1) {
            uint64_t tail = request.length % base_block;
            if (tail > 0 &&
                tail < static_cast<uint64_t>(base_block * merge_ratio)) {
                num_slices = std::max<uint64_t>(1, num_slices - 1);
            }
        }

        uint64_t block_size = roundup(
            (request.length + num_slices - 1) / num_slices, default_block_size);

        num_slices = std::max<uint64_t>(
            1, std::min<uint64_t>(num_slices, max_slice_count));

        uint64_t offset = 0;
        for (uint64_t slice_idx = 0; slice_idx < num_slices; ++slice_idx) {
            uint64_t length =
                std::min<uint64_t>(request.length - offset, block_size);
            auto slice = RdmaSliceStorage::Get().allocate();
            slice->source_addr = (char*)request.source + offset;
            slice->target_addr = request.target_offset + offset;
            slice->length = length;
            slice->task = &task;
            slice->retry_count = 0;
            slice->ep_weak_ptr = nullptr;
            slice->word = PENDING;
            slice->next = nullptr;
            slice->enqueue_ts = enqueue_ts;
            task.num_slices++;
            offset += length;
            int part_id =
                ((enable_spray ? submit_slices : static_cast<int>(slice_idx)) /
                 num_devices) %
                num_workers;
            auto& list = slice_lists[part_id];
            auto& tail = slice_tails[part_id];
            list.num_slices++;
            submit_slices++;
            if (list.first) {
                tail->next = slice;
                tail = slice;
            } else {
                list.first = tail = slice;
            }
        }
    }

    for (int i = 0; i < num_workers; ++i) {
        if (slice_lists[i].first) {
            rdma_batch->slice_chain.push_back(slice_lists[i].first);
            workers_->submit(slice_lists[i], (tl_caller_id + i) % num_workers);
        }
    }
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                        TransferStatus& status) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch*>(batch);
    if (task_id < 0 || task_id >= (int)rdma_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    }
    auto& task = rdma_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status RdmaTransport::addMemoryBuffer(BufferDesc& desc,
                                      const MemoryOptions& options) {
    CHECK_STATUS(local_buffer_manager_.addBuffer(desc, options));
    desc.transports.push_back(TransportType::RDMA);
    return Status::OK();
}

Status RdmaTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                      const MemoryOptions& options) {
    CHECK_STATUS(local_buffer_manager_.addBuffer(desc_list, options));
    for (auto& desc : desc_list) {
        desc.transports.push_back(TransportType::RDMA);
    }
    return Status::OK();
}

Status RdmaTransport::removeMemoryBuffer(BufferDesc& desc) {
    return local_buffer_manager_.removeBuffer(desc);
}

Status RdmaTransport::setupLocalSegment() {
    auto& manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    assert(segment);
    auto& detail = std::get<MemorySegmentDesc>(segment->detail);
    for (auto& context : context_set_) {
        if (context->status() != RdmaContext::DEVICE_ENABLED) continue;
        DeviceDesc device_desc;
        device_desc.name = context->name();
        device_desc.lid = context->lid();
        device_desc.gid = context->gid();
        detail.devices.push_back(device_desc);
    }
    return manager.synchronizeLocal();
}

int RdmaTransport::onSetupRdmaConnections(const BootstrapDesc& peer_desc,
                                          BootstrapDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() || !context_name_lookup_.count(local_nic_name)) {
        std::stringstream ss;
        ss << "No device found in local segment: " << local_nic_name;
        LOG(ERROR) << ss.str();
        local_desc.reply_msg = ss.str();
        return -1;
    }
    auto index = context_name_lookup_[local_nic_name];
    auto context = context_set_[index];
    if (context->status() == RdmaContext::DEVICE_DISABLED) {
        std::stringstream ss;
        ss << "Device is down: " << peer_desc.local_nic_path;
        LOG(ERROR) << ss.str();
        local_desc.reply_msg = ss.str();
        return -1;
    }
    auto endpoint =
        context->endpointStore()->getOrInsert(peer_desc.local_nic_path);
    if (!endpoint) {
        std::stringstream ss;
        ss << "Cannot allocate endpoint: " << peer_desc.local_nic_path;
        LOG(ERROR) << ss.str();
        local_desc.reply_msg = ss.str();
        return -1;
    }
    auto status = endpoint->accept(peer_desc, local_desc);
    if (!status.ok()) {
        LOG(ERROR) << status.ToString();
        local_desc.reply_msg = status.ToString();
        return -1;
    }
    return 0;
}

}  // namespace tent
}  // namespace mooncake
