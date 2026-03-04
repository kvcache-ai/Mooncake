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

#include "transport/rdma_transport/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <future>
#include <set>
#include <thread>

#include <dlfcn.h>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "topology.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {

static bool MCIbRelaxedOrderingEnabled = false;
static int MCIbRelaxedOrderingMode = 2;

// Mode definition for MC_IB_PCI_RELAXED_ORDERING env.
// 0 - disabled, 1 - enabled if supported, 2 - auto (default, same as 1 today).
static int getIbRelaxedOrderingMode() {
    int val = globalConfig().ib_pci_relaxed_ordering_mode;
    if (val < 0 || val > 2) {
        return 2;
    }
    return val;
}

// Determine whether RELAXED_ORDERING is enabled and possible
// This function checks for ibv_reg_mr_iova2 symbol which is available
// in IBVERBS_1.8 and above. The feature is only supported in IBVERBS_1.8+.
bool has_ibv_reg_mr_iova2(void) {
    void *sym = dlsym(RTLD_DEFAULT, "ibv_reg_mr_iova2");
    return sym != NULL;
}

RdmaTransport::RdmaTransport() {
    MCIbRelaxedOrderingMode = getIbRelaxedOrderingMode();
    if (MCIbRelaxedOrderingMode == 0) {
        LOG(INFO) << "[RDMA] Relaxed ordering disabled via "
                  << "MC_IB_PCI_RELAXED_ORDERING=0. "
                  << "Falling back to strict ordering.";
        MCIbRelaxedOrderingEnabled = false;
        return;
    }

    MCIbRelaxedOrderingEnabled = has_ibv_reg_mr_iova2();
    if (MCIbRelaxedOrderingEnabled) {
        LOG(INFO) << "[RDMA] Relaxed ordering is supported on this host; "
                     "IBV_ACCESS_RELAXED_ORDERING will be requested for "
                     "registered memory regions.";
    } else {
        LOG(INFO) << "[RDMA] Relaxed ordering is NOT supported ("
                  << "ibv_reg_mr_iova2 missing or unavailable). "
                  << "Falling back to strict ordering.";
    }
}

RdmaTransport::~RdmaTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

int RdmaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "RdmaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    auto ret = initializeRdmaResources();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot initialize RDMA resources";
        return ret;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        return ret;
    }

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot publish segments";
        return ret;
    }

    return 0;
}

int RdmaTransport::preTouchMemory(void *addr, size_t length) {
    if (context_list_.size() == 0) {
        // At least one context is required for pre-touch.
        return 0;
    }

    auto hwc = std::thread::hardware_concurrency();
    auto num_threads = hwc > 64 ? 16 : std::min(hwc, 8u);
    if (length > (size_t)globalConfig().max_mr_size) {
        length = (size_t)globalConfig().max_mr_size;
    }
    size_t block_size = length / num_threads;
    if (block_size == 0) {
        return 0;
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<int> thread_results(num_threads, 0);

    for (size_t thread_i = 0; thread_i < num_threads; ++thread_i) {
        void *block_addr = static_cast<char *>(addr) + thread_i * block_size;
        threads.emplace_back([this, thread_i, block_addr, block_size,
                              &thread_results]() {
            int ret = context_list_[0]->preTouchMemory(block_addr, block_size);
            thread_results[thread_i] = ret;
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i < num_threads; ++i) {
        if (thread_results[i] != 0) {
            return thread_results[i];
        }
    }

    return 0;
}

int RdmaTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &name,
                                       bool remote_accessible,
                                       bool update_metadata) {
    return registerLocalMemoryInternal(addr, length, name, remote_accessible,
                                       update_metadata, false);
}

int RdmaTransport::registerLocalMemoryInternal(void *addr, size_t length,
                                               const std::string &name,
                                               bool remote_accessible,
                                               bool update_metadata,
                                               bool force_sequential) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    const int kBaseAccessRights = IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ;

    static int access_rights = kBaseAccessRights;
    if (MCIbRelaxedOrderingEnabled) {
        access_rights |= IBV_ACCESS_RELAXED_ORDERING;
    }
    bool do_pre_touch = context_list_.size() > 0 &&
                        std::thread::hardware_concurrency() >= 4 &&
                        length >= (size_t)4 * 1024 * 1024 * 1024;
    if (do_pre_touch) {
        // Parallel Pre-touch the memory to speedup the registration process.
        int ret = preTouchMemory(addr, length);
        if (ret != 0) {
            return ret;
        }
    }

    /* Parallel register when:
    1. parallel_reg_mr is enabled via MC_ENABLE_PARALLEL_REG_MR;
    2. parallel_reg_mr not set and multiple contexts exist and memory has been
    pre-touched
    Note: If memory hasn't been touched, parallel register can be
    slower. Details in: https://github.com/kvcache-ai/Mooncake/issues/848
    Note: force_sequential is used by batch operations to avoid nested
    parallelism.
    */
    int use_parallel_reg = 0;
    if (!force_sequential) {
        use_parallel_reg = globalConfig().parallel_reg_mr;
        if (use_parallel_reg == -1) {
            use_parallel_reg = context_list_.size() > 1 && do_pre_touch;
        }
    }

    auto reg_start = std::chrono::steady_clock::now();

    if (use_parallel_reg) {
        std::vector<std::thread> reg_threads;
        reg_threads.reserve(context_list_.size());
        std::vector<int> ret_codes(context_list_.size(), 0);
        const int ar = access_rights;  // Local copy for lambda capture

        for (size_t i = 0; i < context_list_.size(); ++i) {
            reg_threads.emplace_back([this, &ret_codes, i, addr, length, ar]() {
                ret_codes[i] =
                    context_list_[i]->registerMemoryRegion(addr, length, ar);
            });
        }

        for (auto &thread : reg_threads) {
            thread.join();
        }

        for (size_t i = 0; i < ret_codes.size(); ++i) {
            if (ret_codes[i] != 0) {
                LOG(ERROR) << "Failed to register memory region with context "
                           << i;
                return ret_codes[i];
            }
        }
    } else {
        for (size_t i = 0; i < context_list_.size(); ++i) {
            int ret = context_list_[i]->registerMemoryRegion(addr, length,
                                                             access_rights);
            if (ret) {
                LOG(ERROR) << "Failed to register memory region with context "
                           << i;
                return ret;
            }
        }
    }

    auto reg_end = std::chrono::steady_clock::now();
    auto reg_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(reg_end -
                                                              reg_start)
            .count();

    if (globalConfig().trace) {
        LOG(INFO) << "registerMemoryRegion: addr=" << addr
                  << ", length=" << length
                  << ", contexts=" << context_list_.size()
                  << ", parallel=" << (use_parallel_reg ? "true" : "false")
                  << ", duration=" << reg_duration_ms << "ms";
    }

    // Collect keys from all contexts
    for (auto &context : context_list_) {
        buffer_desc.lkey.push_back(context->lkey(addr));
        buffer_desc.rkey.push_back(context->rkey(addr));
    }

    // Get the memory location automatically after registered MR(pinned),
    // when the name is kWildcardLocation("*").
    if (name == kWildcardLocation) {
        bool only_first_page = true;
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length, only_first_page);
        if (entries.empty()) return -1;
        buffer_desc.name = entries[0].location;
    } else {
        buffer_desc.name = name;
    }

    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
    int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (rc) return rc;
    return 0;
}

int RdmaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return unregisterLocalMemoryInternal(addr, update_metadata, false);
}

int RdmaTransport::unregisterLocalMemoryInternal(void *addr,
                                                 bool update_metadata,
                                                 bool force_sequential) {
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;

    // force_sequential is used by batch operations to avoid nested parallelism
    int use_parallel_unreg = 0;
    if (!force_sequential) {
        use_parallel_unreg = globalConfig().parallel_reg_mr;
        if (use_parallel_unreg == -1) {
            use_parallel_unreg = context_list_.size() > 1;
        }
    }

    if (use_parallel_unreg) {
        std::vector<std::thread> unreg_threads;
        unreg_threads.reserve(context_list_.size());
        std::vector<int> ret_codes(context_list_.size(), 0);

        for (size_t i = 0; i < context_list_.size(); ++i) {
            unreg_threads.emplace_back([this, &ret_codes, i, addr]() {
                ret_codes[i] = context_list_[i]->unregisterMemoryRegion(addr);
            });
        }

        for (auto &thread : unreg_threads) {
            thread.join();
        }

        for (size_t i = 0; i < ret_codes.size(); ++i) {
            if (ret_codes[i] != 0) {
                LOG(ERROR) << "Failed to unregister memory region with context "
                           << i;
                return ret_codes[i];
            }
        }
    } else {
        for (size_t i = 0; i < context_list_.size(); ++i) {
            int ret = context_list_[i]->unregisterMemoryRegion(addr);
            if (ret) {
                LOG(ERROR) << "Failed to unregister memory region with context "
                           << i;
                return ret;
            }
        }
    }

    return 0;
}

int RdmaTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "rdma";
    for (auto &entry : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = entry->deviceName();
        device_desc.lid = entry->lid();
        device_desc.gid = entry->gid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_.get());
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int RdmaTransport::registerLocalMemoryBatch(
    const std::vector<RdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
#if !defined(WITH_NVIDIA_PEERMEM) && defined(USE_CUDA)
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) {
            LOG(WARNING) << "RdmaTransport: Failed to register memory: addr "
                         << buffer.addr << " length " << buffer.length;
        }
    }
#else
    std::vector<std::future<int>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                // Use force_sequential=true to avoid nested parallelism
                return registerLocalMemoryInternal(buffer.addr, buffer.length,
                                                   location, true, false, true);
            }));
    }

    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "RdmaTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }
#endif

    return metadata_->updateLocalSegmentDesc();
}

int RdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    std::vector<std::future<int>> results;
    for (auto &addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                // Use force_sequential=true to avoid nested parallelism
                return unregisterLocalMemoryInternal(addr, false, true);
            }));
    }

    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "RdmaTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

Status RdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "RdmaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "RdmaTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask *> task_list;
    for (auto &task : batch_desc.task_list) task_list.push_back(&task);
    return submitTransferTask(task_list);
}

Status RdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    assert(local_segment_desc.get());
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit;
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;
    uint64_t nr_slices;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        nr_slices = 0;
        assert(task.request);
        auto &request = *task.request;

        auto request_buffer_id = -1, request_device_id = -1;
        if (selectDevice(local_segment_desc.get(), (uint64_t)request.source,
                         request.length, request_buffer_id,
                         request_device_id)) {
            request_buffer_id = -1;
            request_device_id = -1;
        }

        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice *slice = getSliceCache().allocate();
            assert(slice);
            if (!slice->from_cache) {
                nr_slices++;
            }

            bool merge_final_slice =
                request.length - offset <= kBlockSize + kFragmentSize;

            slice->source_addr = (char *)request.source + offset;
            slice->length =
                merge_final_slice ? request.length - offset : kBlockSize;
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);

            int buffer_id = -1, device_id = -1,
                retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            if (request_buffer_id >= 0 && request_device_id >= 0) {
                found_device = true;
                buffer_id = request_buffer_id;
                device_id = request_device_id;
            }
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                assert(device_id >= 0 &&
                       static_cast<size_t>(device_id) < context_list_.size());
                auto &context = context_list_[device_id];
                assert(context.get());
                if (!context->active()) continue;
                assert(buffer_id >= 0 &&
                       static_cast<size_t>(buffer_id) <
                           local_segment_desc->buffers.size());
                assert(local_segment_desc->buffers[buffer_id].lkey.size() ==
                       context_list_.size());
                found_device = true;
                break;
            }
            if (!found_device) {
                auto source_addr = slice->source_addr;
                for (auto &entry : slices_to_post)
                    for (auto s : entry.second) getSliceCache().deallocate(s);
                LOG(ERROR)
                    << "Memory region not registered by any active device(s): "
                    << source_addr;
                return Status::AddressNotRegistered(
                    "Memory region not registered by any active device(s): " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            } else {
                auto &context = context_list_[device_id];
                if (!context->active()) {
                    LOG(ERROR) << "Device " << device_id << " is not active";
                    return Status::InvalidArgument("Device " +
                                                   std::to_string(device_id) +
                                                   " is not active");
                }
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                __sync_fetch_and_add(&task.slice_count, 1);
            }

            if (nr_slices >= kSubmitWatermark) {
                for (auto &entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);
                slices_to_post.clear();
                nr_slices = 0;
            }

            if (merge_final_slice) {
                break;
            }
        }
    }

    for (auto &entry : slices_to_post)
        if (!entry.second.empty()) entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(BatchID batch_id,
                                        std::vector<TransferStatus> &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.resize(task_count);
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        auto &task = batch_desc.task_list[task_id];
        status[task_id].transferred_bytes = task.transferred_bytes;
        uint64_t success_slice_count = task.success_slice_count;
        uint64_t failed_slice_count = task.failed_slice_count;
        if (success_slice_count + failed_slice_count == task.slice_count) {
            if (failed_slice_count)
                status[task_id].s = TransferStatusEnum::FAILED;
            else
                status[task_id].s = TransferStatusEnum::COMPLETED;
            task.is_finished = true;
        } else {
            status[task_id].s = TransferStatusEnum::WAITING;
        }
    }
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "RdmaTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count)
            status.s = TransferStatusEnum::FAILED;
        else
            status.s = TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

RdmaTransport::SegmentID RdmaTransport::getSegmentID(
    const std::string &segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<RdmaContext> context;
    int index = 0;
    for (auto &entry : local_topology_->getHcaList()) {
        if (entry == local_nic_name) {
            context = context_list_[index];
            break;
        }
        index++;
    }
    if (!context) return ERR_INVALID_ARGUMENT;

#ifdef CONFIG_ERDMA
    if (context->deleteEndpoint(peer_desc.local_nic_path)) return ERR_ENDPOINT;
#endif
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    return endpoint->setupConnectionsByPassive(peer_desc, local_desc);
}

int RdmaTransport::initializeRdmaResources() {
    auto hca_list = local_topology_->getHcaList();
    for (auto &device_name : hca_list) {
        auto context = std::make_shared<RdmaContext>(*this, device_name);
        auto &config = globalConfig();
        int ret = context->construct(config.num_cq_per_ctx,
                                     config.num_comp_channels_per_ctx,
                                     config.port, config.gid_index,
                                     config.max_cqe, config.max_ep_per_ctx);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
        } else {
            context_list_.push_back(context);
        }
    }
    if (local_topology_->empty()) {
        LOG(ERROR) << "RdmaTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }
    return 0;
}

int RdmaTransport::startHandshakeDaemon(std::string &local_server_name) {
    // Register delete endpoint callback to handle peer endpoint deletion
    // notifications
    metadata_->registerDeleteEndpointCallback(std::bind(
        &RdmaTransport::onDeleteEndpoint, this, std::placeholders::_1));

    return metadata_->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int RdmaTransport::onDeleteEndpoint(const DeleteEndpointDesc &peer_desc) {
    // Find local NIC name from target_nic_path (which is our local NIC path)
    auto local_nic_name = getNicNameFromNicPath(peer_desc.target_nic_path);
    if (local_nic_name.empty()) {
        LOG(WARNING) << "Invalid target NIC path in delete endpoint request: "
                     << peer_desc.target_nic_path;
        return ERR_INVALID_ARGUMENT;
    }

    // Find the corresponding RdmaContext
    for (auto &context : context_list_) {
        if (context->deviceName() == local_nic_name) {
            // Delete the endpoint only if peer_endpoint_id matches, to avoid
            // deleting a newer replacement endpoint for the same nic_path
            int ret = context->deleteEndpoint(peer_desc.deleted_nic_path,
                                              peer_desc.endpoint_id);
            if (ret) {
                LOG(WARNING)
                    << "onDeleteEndpoint: endpoint not deleted "
                    << "(not found or endpoint_id mismatch), "
                    << "error code: " << ret
                    << "deleted_nic_path=" << peer_desc.deleted_nic_path
                    << ", local_nic_path=" << peer_desc.target_nic_path
                    << ", peer_endpoint_id=" << peer_desc.endpoint_id;
            } else {
                LOG(INFO) << "onDeleteEndpoint: deleted endpoint, "
                          << "deleted_nic_path=" << peer_desc.deleted_nic_path
                          << ", local_nic_path=" << peer_desc.target_nic_path
                          << ", peer_endpoint_id=" << peer_desc.endpoint_id;
            }
            return 0;
        }
    }

    LOG(WARNING) << "onDeleteEndpoint: target_nic_path not found, "
                 << "target_nic_path=" << peer_desc.target_nic_path
                 << ", peer_nic_path=" << peer_desc.deleted_nic_path;
    return ERR_DEVICE_NOT_FOUND;
}

// According to the request desc, offset and length information, find proper
// buffer_id and device_id as output.
// Return 0 if successful, ERR_ADDRESS_NOT_REGISTERED otherwise.
int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, std::string_view hint,
                                int &buffer_id, int &device_id,
                                int retry_count) {
    if (desc == nullptr) return ERR_ADDRESS_NOT_REGISTERED;
    const auto &buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto &buffer = buffers[buffer_id];

        // Check if offset is within buffer range
        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;
        }

        device_id =
            hint.empty()
                ? desc->topology.selectDevice(buffer.name, retry_count)
                : desc->topology.selectDevice(buffer.name, hint, retry_count);
        if (device_id >= 0) return 0;
        device_id = hint.empty() ? desc->topology.selectDevice(
                                       kWildcardLocation, retry_count)
                                 : desc->topology.selectDevice(
                                       kWildcardLocation, hint, retry_count);
        if (device_id >= 0) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, int &buffer_id, int &device_id,
                                int retry_count) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_count);
}
}  // namespace mooncake
