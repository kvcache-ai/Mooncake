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

#include "transport/efa_transport/efa_transport.h"

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
#include "transport/efa_transport/efa_context.h"
#include "transport/efa_transport/efa_endpoint.h"

namespace mooncake {

EfaTransport::EfaTransport() {
    LOG(INFO) << "[EFA] AWS Elastic Fabric Adapter transport initialized";
}

EfaTransport::~EfaTransport() {
    stopWorkerThreads();
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

void EfaTransport::startWorkerThreads() {
    if (worker_running_) return;

    worker_running_ = true;
    // One poller thread per context for responsive CQ draining under load
    size_t num_threads = context_list_.size();
    for (size_t i = 0; i < num_threads; i++) {
        worker_threads_.emplace_back(&EfaTransport::workerThreadFunc, this, i);
    }
    LOG(INFO) << "EfaTransport: Started " << num_threads
              << " CQ polling worker threads";
}

void EfaTransport::stopWorkerThreads() {
    if (!worker_running_) return;

    worker_running_ = false;
    for (auto &thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();
    LOG(INFO) << "EfaTransport: Stopped CQ polling worker threads";
}

void EfaTransport::workerThreadFunc(int thread_id) {
    const int kPollBatchSize = 64;

    while (worker_running_) {
        bool did_work = false;

        // Poll CQs from all contexts
        for (size_t ctx_idx = thread_id; ctx_idx < context_list_.size();
             ctx_idx += worker_threads_.size()) {
            auto &context = context_list_[ctx_idx];
            if (!context || !context->active()) continue;

            for (size_t cq_idx = 0; cq_idx < context->cqCount(); cq_idx++) {
                int completed = context->pollCq(kPollBatchSize, cq_idx);
                if (completed > 0) {
                    did_work = true;
                }
            }
        }

        // If no work was done, yield CPU briefly
        if (!did_work) {
            std::this_thread::yield();
        }
    }
}

int EfaTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "EfaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    auto ret = initializeEfaResources();
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot initialize EFA resources";
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
        LOG(ERROR) << "EfaTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot publish segments";
        return ret;
    }

    // Start CQ polling worker threads
    startWorkerThreads();

    return 0;
}

int EfaTransport::preTouchMemory(void *addr, size_t length) {
    if (context_list_.size() == 0) {
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

int EfaTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &name,
                                      bool remote_accessible,
                                      bool update_metadata) {
    return registerLocalMemoryInternal(addr, length, name, remote_accessible,
                                       update_metadata, false);
}

int EfaTransport::registerLocalMemoryInternal(void *addr, size_t length,
                                              const std::string &name,
                                              bool remote_accessible,
                                              bool update_metadata,
                                              bool force_sequential) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    const int kBaseAccessRights = IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ;

    int access_rights = kBaseAccessRights;

    bool do_pre_touch = context_list_.size() > 0 &&
                        std::thread::hardware_concurrency() >= 4 &&
                        length >= (size_t)4 * 1024 * 1024 * 1024;
    if (do_pre_touch) {
        int ret = preTouchMemory(addr, length);
        if (ret != 0) {
            return ret;
        }
    }

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
        const int ar = access_rights;

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
                LOG(ERROR)
                    << "Failed to register memory region with EFA context "
                    << i;
                return ret_codes[i];
            }
        }
    } else {
        for (size_t i = 0; i < context_list_.size(); ++i) {
            int ret = context_list_[i]->registerMemoryRegion(addr, length,
                                                             access_rights);
            if (ret) {
                LOG(ERROR)
                    << "Failed to register memory region with EFA context "
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
        LOG(INFO) << "EFA registerMemoryRegion: addr=" << addr
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

int EfaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return unregisterLocalMemoryInternal(addr, update_metadata, false);
}

int EfaTransport::unregisterLocalMemoryInternal(void *addr,
                                                bool update_metadata,
                                                bool force_sequential) {
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;

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
                LOG(ERROR)
                    << "Failed to unregister memory region with EFA context "
                    << i;
                return ret_codes[i];
            }
        }
    } else {
        for (size_t i = 0; i < context_list_.size(); ++i) {
            int ret = context_list_[i]->unregisterMemoryRegion(addr);
            if (ret) {
                LOG(ERROR)
                    << "Failed to unregister memory region with EFA context "
                    << i;
                return ret;
            }
        }
    }

    return 0;
}

int EfaTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "efa";
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

int EfaTransport::registerLocalMemoryBatch(
    const std::vector<EfaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    std::vector<std::future<int>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemoryInternal(buffer.addr, buffer.length,
                                                   location, true, false, true);
            }));
    }

    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "EfaTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }

    return metadata_->updateLocalSegmentDesc();
}

int EfaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    std::vector<std::future<int>> results;
    for (auto &addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemoryInternal(addr, false, true);
            }));
    }

    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "EfaTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

Status EfaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "EfaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "EfaTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask *> task_list;
    for (auto &task : batch_desc.task_list) task_list.push_back(&task);
    return submitTransferTask(task_list);
}

Status EfaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<EfaContext>, std::vector<Slice *>>
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
                LOG(ERROR) << "Memory region not registered by any active EFA "
                              "device(s): "
                           << source_addr;
                return Status::AddressNotRegistered(
                    "Memory region not registered by any active EFA "
                    "device(s): " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            } else {
                auto &context = context_list_[device_id];
                if (!context->active()) {
                    LOG(ERROR)
                        << "EFA Device " << device_id << " is not active";
                    return Status::InvalidArgument("EFA Device " +
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

Status EfaTransport::getTransferStatus(BatchID batch_id,
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

Status EfaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "EfaTransport::getTransportStatus invalid argument, batch id: " +
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

EfaTransport::SegmentID EfaTransport::getSegmentID(
    const std::string &segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int EfaTransport::onSetupEfaConnections(const HandShakeDesc &peer_desc,
                                        HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    // Find context by device name instead of using hca_list index, since
    // context_list_ only contains EFA devices and may have different
    // indexing than the full hca_list.
    std::shared_ptr<EfaContext> context;
    for (auto &entry : context_list_) {
        if (entry->deviceName() == local_nic_name) {
            context = entry;
            break;
        }
    }
    if (!context) return ERR_INVALID_ARGUMENT;

    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    return endpoint->setupConnectionsByPassive(peer_desc, local_desc);
}

int EfaTransport::initializeEfaResources() {
    auto hca_list = local_topology_->getHcaList();

    // Filter for EFA devices (names typically start with "rdmap" on AWS)
    std::vector<std::string> efa_devices;
    std::vector<std::string> non_efa_devices;
    for (auto &device_name : hca_list) {
        if (device_name.find("rdmap") != std::string::npos ||
            device_name.find("efa") != std::string::npos) {
            efa_devices.push_back(device_name);
        } else {
            non_efa_devices.push_back(device_name);
        }
    }

    if (efa_devices.empty()) {
        LOG(WARNING) << "EfaTransport: No EFA devices found, falling back to "
                        "all devices";
        efa_devices = hca_list;
        non_efa_devices.clear();
    }

    // Disable non-EFA devices (e.g. ibp* IB devices) in the topology so that
    // topology device indices stay aligned with context_list_ indices.
    // Without this, selectDevice() can return an index from the full topology
    // (which includes non-EFA devices), causing out-of-bounds access on
    // context_list_ which only contains EFA devices.
    for (auto &device_name : non_efa_devices) {
        local_topology_->disableDevice(device_name);
        LOG(INFO) << "EfaTransport: Disabled non-EFA device " << device_name
                  << " in topology";
    }

    for (auto &device_name : efa_devices) {
        auto context = std::make_shared<EfaContext>(*this, device_name);
        auto &config = globalConfig();
        int ret = context->construct(config.num_cq_per_ctx,
                                     config.num_comp_channels_per_ctx,
                                     config.port, config.gid_index,
                                     config.max_cqe, config.max_ep_per_ctx);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "EfaTransport: Disable device " << device_name;
        } else {
            context_list_.push_back(context);
            LOG(INFO) << "EfaTransport: Initialized EFA device " << device_name;
        }
    }
    if (context_list_.empty()) {
        LOG(ERROR) << "EfaTransport: No available EFA devices";
        return ERR_DEVICE_NOT_FOUND;
    }
    return 0;
}

int EfaTransport::startHandshakeDaemon(std::string &local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&EfaTransport::onSetupEfaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int EfaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                               size_t length, std::string_view hint,
                               int &buffer_id, int &device_id,
                               int retry_count) {
    if (desc == nullptr) return ERR_ADDRESS_NOT_REGISTERED;
    const auto &buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto &buffer = buffers[buffer_id];

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

int EfaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                               size_t length, int &buffer_id, int &device_id,
                               int retry_count) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_count);
}

}  // namespace mooncake
