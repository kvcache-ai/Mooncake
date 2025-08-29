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

#include "v1/transport/rdma/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "v1/common/status.h"
#include "v1/memory/location.h"
#include "v1/transport/rdma/buffers.h"
#include "v1/transport/rdma/endpoint_store.h"
#include "v1/transport/rdma/workers.h"
#include "v1/utility/string_builder.h"
#include "v1/utility/topology.h"
#include "v1/utility/random.h"

#define SET_DEVICE(key, param) \
    param = conf->get("transports/rdma/device/" #key, param)

#define SET_ENDPOINT(key, param) \
    param = conf->get("transports/rdma/endpoint/" #key, param)

#define SET_WORKERS(key, param) \
    param = conf->get("transports/rdma/workers/" #key, param)

namespace mooncake {
namespace v1 {
static void convertConfToRdmaParams(std::shared_ptr<ConfigManager> conf,
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
}

RdmaTransport::RdmaTransport() : installed_(false) {}

RdmaTransport::~RdmaTransport() { uninstall(); }

Status RdmaTransport::install(std::string &local_segment_name,
                              std::shared_ptr<MetadataService> metadata,
                              std::shared_ptr<Topology> local_topology,
                              std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "RDMA transport has been installed" LOC_MARK);
    }

    if (local_topology == nullptr || local_topology->getDeviceList().empty()) {
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
    auto endpoint_store = std::make_shared<SIEVEEndpointStore>(
        params_->endpoint.endpoint_store_cap);
    auto hca_list = local_topology_->getDeviceList();
    for (auto &device_name : hca_list) {
        auto context = std::make_shared<RdmaContext>();
        int ret = context->construct(device_name, endpoint_store, params_);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
            continue;
        }
        context_name_lookup_[device_name] = context_set_.size();
        context_set_.push_back(context);
        local_buffer_manager_.addDevice(context.get());
    }
    if (local_topology_->empty()) {
        uninstall();
        return Status::DeviceNotFound(
            "No RDMA device detected in active" LOC_MARK);
    }

    device_quota_ = std::make_unique<DeviceQuota>();
    CHECK_STATUS(device_quota_->loadTopology(local_topology_));

    local_topology_->print();
    setupLocalSegment();

    metadata_->setBootstrapRdmaCallback(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2));

    workers_ = std::make_unique<Workers>(this);
    workers_->start();

    installed_ = true;
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

Status RdmaTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto rdma_batch = Slab<RdmaSubBatch>::Get().allocate();
    if (!rdma_batch)
        return Status::InternalError("Unable to allocate RDMA sub-batch");
    batch = rdma_batch;
    rdma_batch->task_list.reserve(max_size);
    rdma_batch->max_size = max_size;
    return Status::OK();
}

Status RdmaTransport::freeSubBatch(SubBatchRef &batch) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
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

Status RdmaTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" LOC_MARK);
    if (request_list.size() + rdma_batch->task_list.size() >
        rdma_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    const size_t block_size = params_->workers.block_size;
    const int num_workers = params_->workers.num_workers;
    std::vector<RdmaSliceList> slice_lists(num_workers);
    std::vector<RdmaSlice *> slice_tails(num_workers, nullptr);
    int start_worker_id = SimpleRandom::Get().next(num_workers);
    auto local_segment = metadata_->segmentManager().getLocal().get();
    for (auto &request : request_list) {
        rdma_batch->task_list.push_back(RdmaTask{});
        auto &task = rdma_batch->task_list.back();
        task.request = request;
        task.num_slices = 0;
        task.status_word = WAITING;
        task.transferred_bytes = 0;

        std::vector<DeviceQuota::AllocPlan> plan_list;
        auto buffer = getBufferDesc(local_segment, (uint64_t)request.source,
                                    request.length);
        CHECK_STATUS(device_quota_->allocate(
            request.length, buffer ? buffer->location : "", plan_list));

        for (auto &plan : plan_list) {
            uint64_t remaining = plan.length;

            // 至少切成 num_workers 块，充分利用 QP
            uint64_t blocks = (remaining + block_size - 1) / block_size;
            uint64_t num_slices = std::min<uint64_t>(num_workers, blocks);
            uint64_t blocks_per_slice = (blocks + num_slices - 1) / num_slices;
            uint64_t slice_bytes = blocks_per_slice * block_size;
            for (uint64_t slice_idx = 0; slice_idx < num_slices; ++slice_idx) {
                uint64_t offset = slice_idx * slice_bytes;
                if (offset >= remaining) break;
                uint64_t cur_len = std::min(slice_bytes, remaining - offset);

                auto slice = RdmaSliceStorage::Get().allocate();
                slice->source_addr =
                    (char *)request.source + plan.offset + offset;
                slice->target_addr =
                    request.target_offset + plan.offset + offset;
                slice->length = cur_len;
                slice->task = &task;
                slice->retry_count = 0;
                slice->endpoint_quota = nullptr;
                slice->next = nullptr;
                slice->source_dev_id = plan.dev_id;
                task.num_slices++;

                int part_id = (slice_idx + start_worker_id) % num_workers;
                auto &list = slice_lists[part_id];
                auto &tail = slice_tails[part_id];
                list.num_slices++;
                if (list.first) {
                    tail->next = slice;
                    tail = slice;
                } else {
                    list.first = tail = slice;
                }
            }
        }
    }

    for (int part_id = 0; part_id < num_workers; ++part_id) {
        if (slice_lists[part_id].first) {
            rdma_batch->slice_chain.push_back(slice_lists[part_id].first);
            workers_->submit(slice_lists[part_id], part_id);
        }
    }
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                        TransferStatus &status) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)rdma_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    }
    auto &task = rdma_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status RdmaTransport::addMemoryBuffer(BufferDesc &desc,
                                      const MemoryOptions &options) {
    return local_buffer_manager_.addBuffer(desc, options);
}

Status RdmaTransport::removeMemoryBuffer(BufferDesc &desc) {
    return local_buffer_manager_.removeBuffer(desc);
}

Status RdmaTransport::setupLocalSegment() {
    auto &manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    assert(segment);
    auto &detail = std::get<MemorySegmentDesc>(segment->detail);
    for (auto &context : context_set_) {
        DeviceDesc device_desc;
        device_desc.name = context->name();
        device_desc.lid = context->lid();
        device_desc.gid = context->gid();
        detail.devices.push_back(device_desc);
    }
    return manager.synchronizeLocal();
}

int RdmaTransport::onSetupRdmaConnections(const BootstrapDesc &peer_desc,
                                          BootstrapDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() || !context_name_lookup_.count(local_nic_name)) {
        local_desc.reply_msg =
            "Unable to find RDMA device " + peer_desc.peer_nic_path;
        return ERR_ENDPOINT;
    }

    auto index = context_name_lookup_[local_nic_name];
    auto context = context_set_[index];
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) {
        local_desc.reply_msg = "Unable to create endpoint object";
        return ERR_ENDPOINT;
    }

    auto peer_nic_path_ = peer_desc.local_nic_path;
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        return ERR_ENDPOINT;
    }

    local_desc.local_nic_path =
        MakeNicPath(local_segment_name_, context->name());
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = endpoint->qpNum();
    SegmentDescRef segment_desc;
    auto status =
        metadata_->segmentManager().getRemote(segment_desc, peer_server_name);
    if (status.ok() && segment_desc->type == SegmentType::Memory) {
        auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
        for (auto &nic : detail.devices)
            if (nic.name == peer_nic_name)
                return endpoint->configurePeer(
                    nic.gid, nic.lid, peer_desc.qp_num, &local_desc.reply_msg);
    }
    local_desc.reply_msg = "Unable to find RDMA device " + peer_nic_path_ +
                           ": " + status.ToString();
    return ERR_ENDPOINT;
}

}  // namespace v1
}  // namespace mooncake
