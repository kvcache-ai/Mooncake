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

#include "transport/cxi_transport/cxi_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <future>
#include <set>
#include <thread>

#include <dlfcn.h>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "topology.h"
#include "transport/cxi_transport/cxi_context.h"
#include "transport/cxi_transport/cxi_endpoint.h"

namespace mooncake {

NicReplicaPolicy CxiTransport::getReplicaPolicy() {
    return NicReplicaPolicy::NUMA_AWARE;
}

CxiTransport::CxiTransport() {
    LOG(INFO) << "[CXI] CXI Slingshot transport initialized";
}

CxiTransport::~CxiTransport() {
    stopWorkerThreads();
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

void CxiTransport::startWorkerThreads() {
    if (worker_running_) return;

    worker_running_ = true;
    // One poller thread per context for responsive CQ draining under load
    size_t num_threads = context_list_.size();
    for (size_t i = 0; i < num_threads; i++) {
        worker_threads_.emplace_back(&CxiTransport::workerThreadFunc, this, i);
    }
    LOG(INFO) << "CxiTransport: Started " << num_threads
              << " CQ polling worker threads";
}

void CxiTransport::stopWorkerThreads() {
    if (!worker_running_) return;

    worker_running_ = false;
    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();
    LOG(INFO) << "CxiTransport: Stopped CQ polling worker threads";
}

void CxiTransport::workerThreadFunc(int thread_id) {
    const int kPollBatchSize = 64;

    while (worker_running_) {
        bool did_work = false;

        // Poll CQs from all contexts
        for (size_t ctx_idx = thread_id; ctx_idx < context_list_.size();
             ctx_idx += worker_threads_.size()) {
            auto& context = context_list_[ctx_idx];
            if (!context || !context->active()) continue;

            for (size_t cq_idx = 0; cq_idx < context->cqCount(); cq_idx++) {
                int completed = context->pollCq(kPollBatchSize, cq_idx);
                if (completed > 0) {
                    did_work = true;
                }
            }
        }

        // Under the shared-endpoint model there is no per-peer QP to evict:
        // a new peer costs one AV entry (~bytes), not an fi_endpoint slot.
        // Stale peers are reclaimed when submitPostSend() drops a peer whose
        // handshake failed.

        // If no work was done, yield CPU briefly
        if (!did_work) {
            std::this_thread::yield();
        }
    }
}

int CxiTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "CxiTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    auto ret = initializeCxiResources();
    if (ret) {
        LOG(ERROR) << "CxiTransport: cannot initialize CXI resources";
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
        LOG(ERROR) << "CxiTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "CxiTransport: cannot publish segments";
        return ret;
    }

    // Start CQ polling worker threads
    startWorkerThreads();

    return 0;
}

int CxiTransport::preTouchMemory(void* addr, size_t length) {
    if (context_list_.size() == 0) {
        return 0;
    }

    auto hwc = std::thread::hardware_concurrency();
    auto num_threads = hwc > 64 ? 16 : std::min(hwc, 8u);
    size_t block_size = length / num_threads;
    if (block_size == 0) {
        return 0;
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<int> thread_results(num_threads, 0);

    for (size_t thread_i = 0; thread_i < num_threads; ++thread_i) {
        void* block_addr = static_cast<char*>(addr) + thread_i * block_size;
        threads.emplace_back([this, thread_i, block_addr, block_size,
                              &thread_results]() {
            int ret = context_list_[0]->preTouchMemory(block_addr, block_size);
            thread_results[thread_i] = ret;
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i < num_threads; ++i) {
        if (thread_results[i] != 0) {
            return thread_results[i];
        }
    }

    return 0;
}

int CxiTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& name,
                                      bool remote_accessible,
                                      bool update_metadata) {
    return registerLocalMemoryInternal(addr, length, name, remote_accessible,
                                       update_metadata, false);
}

int CxiTransport::registerLocalMemoryInternal(void* addr, size_t length,
                                              const std::string& name,
                                              bool remote_accessible,
                                              bool update_metadata,
                                              bool force_sequential) {
    (void)remote_accessible;
    const int kBaseAccessRights =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
        IBV_ACCESS_REMOTE_READ;  // this is not used, ignore

    int access_rights = kBaseAccessRights;
    size_t max_mr = (size_t)globalConfig().max_mr_size;

    // max_mr must be set, slingshot handles mempages differently compared to
    // efa
    size_t chunk_limit = max_mr;
    LOG(INFO) << "Auto-split params: max_mr_size=" << max_mr
              << ", chunk_limit=" << chunk_limit;

    // Determine chunk boundaries
    std::vector<std::pair<void*, size_t>> chunks;
    if (length > chunk_limit) {
        size_t offset = 0;
        while (offset < length) {
            size_t chunk_len = std::min(chunk_limit, length - offset);
            chunks.emplace_back(static_cast<char*>(addr) + offset, chunk_len);
            offset += chunk_len;
        }
        LOG(WARNING) << "Auto-splitting buffer " << addr << " (" << length
                     << " bytes) into " << chunks.size()
                     << " chunks of <= " << chunk_limit << " bytes each";
    } else {
        chunks.emplace_back(addr, length);
    }

    // Resolve location name once (based on original buffer)
    std::string resolved_name;
    if (name == kWildcardLocation) {
        bool only_first_page = true;
        const std::vector<MemoryLocationEntry> entries = getMemoryLocation(
            addr, length, only_first_page);  // check only first page
        if (entries.empty()) return -1;
        resolved_name = entries[0].location;
    } else {
        resolved_name = name;
    }

    size_t num_nics = context_list_.size();
    size_t num_chunks = chunks.size();

    auto policy = getReplicaPolicy();

    std::vector<std::vector<size_t>> nic_assignments(num_chunks);

    std::string nic = local_topology_->getHcaList().at(
        local_topology_->selectDevice(resolved_name));
    LOG(INFO) << "for this allocation, selected NIC " << nic;
    int id = -1;
    for (size_t i = 0; i < context_list_.size(); i++) {
        auto& entry = context_list_[i];
        if (entry->deviceName() == nic) {
            id = i;
            break;
        }
    }
    if (id == -1) {
        LOG(ERROR) << "possible mismatch between context list devices and "
                      "topology HCA names!";
        return -1;
    }
    int numa_node = id;

    // implicit assumption here, the address and relative chunks all lie in the
    // same numa node
    for (size_t ci = 0; ci < num_chunks; ci++) {
        // based on replication policy, assign chunk to nic
        switch (policy) {
            case NUMA_AWARE:
                nic_assignments[ci].push_back(numa_node);
                break;
            case REPLICATE_ALL:
                for (size_t nic_idx = 0; nic_idx < num_nics; nic_idx++)
                    nic_assignments[ci].push_back(nic_idx);
                break;
            default:
                nic_assignments[ci].push_back(
                    0);  // register everything to nic 0
                break;
        }
    }

    auto rollbackChunks = [&](size_t up_to_ci) {
        for (size_t ri = 0; ri <= up_to_ci; ++ri) {
            for (size_t nic_idx : nic_assignments[ri]) {
                context_list_[nic_idx]->unregisterMemoryRegion(
                    chunks[ri].first);
            }
        }
    };

    // Register each chunk on its assigned NICs
    for (size_t ci = 0; ci < chunks.size(); ++ci) {
        void* chunk_addr = chunks[ci].first;
        size_t chunk_len = chunks[ci].second;

        const auto& assigned_nics = nic_assignments[ci];

        // preTouchMemory does a CPU-side store to each page, which segfaults
        // on GPU VRAM (cudaMalloc'd pointers). Restrict it to host memory.
        bool is_host_mem = resolved_name.rfind("cpu", 0) == 0;
        bool do_pre_touch = is_host_mem && context_list_.size() > 0 &&
                            std::thread::hardware_concurrency() >= 4 &&
                            chunk_len >= (size_t)4 * 1024 * 1024 * 1024;
        if (do_pre_touch) {
            int ret = preTouchMemory(chunk_addr, chunk_len);
            if (ret != 0) {
                if (ci > 0) rollbackChunks(ci - 1);
                return ret;
            }
        }

        int use_parallel_reg = 0;
        if (!force_sequential) {
            use_parallel_reg = globalConfig().parallel_reg_mr;
            if (use_parallel_reg == -1) {
                use_parallel_reg = assigned_nics.size() > 1 && do_pre_touch;
            }
        }

        auto reg_start = std::chrono::steady_clock::now();

        if (use_parallel_reg) {
            std::vector<std::thread> reg_threads;
            reg_threads.reserve(assigned_nics.size());
            std::vector<int> ret_codes(assigned_nics.size(), 0);
            const int ar = access_rights;

            for (size_t j = 0; j < assigned_nics.size(); ++j) {
                size_t nic_idx = assigned_nics[j];
                reg_threads.emplace_back([this, &ret_codes, j, nic_idx,
                                          chunk_addr, chunk_len, ar]() {
                    ret_codes[j] = context_list_[nic_idx]->registerMemoryRegion(
                        chunk_addr, chunk_len, ar);
                });
            }

            for (auto& thread : reg_threads) {
                thread.join();
            }

            for (size_t j = 0; j < ret_codes.size(); ++j) {
                if (ret_codes[j] != 0) {
                    LOG(ERROR)
                        << "Failed to register memory region chunk " << ci
                        << " with CXI context " << assigned_nics[j];
                    rollbackChunks(ci);
                    return ret_codes[j];
                }
            }
        } else {
            for (size_t nic_idx : assigned_nics) {
                int ret = context_list_[nic_idx]->registerMemoryRegion(
                    chunk_addr, chunk_len, access_rights);
                if (ret) {
                    LOG(ERROR) << "Failed to register memory region chunk "
                               << ci << " with CXI context " << nic_idx;
                    rollbackChunks(ci);
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
            LOG(INFO) << "CXI registerMemoryRegion: chunk " << ci
                      << ", addr=" << chunk_addr << ", length=" << chunk_len
                      << ", nics=" << assigned_nics.size() << "/"
                      << context_list_.size()
                      << ", parallel=" << (use_parallel_reg ? "true" : "false")
                      << ", duration=" << reg_duration_ms << "ms";
        }

        LOG(WARNING) << "Chunk " << ci << "/" << chunks.size()
                     << " registered on " << assigned_nics.size() << " NICs"
                     << ", addr=" << chunk_addr << ", length=" << chunk_len
                     << ", duration=" << reg_duration_ms << "ms";

        // Collect keys: assigned NICs have valid keys, others get 0
        BufferDesc buffer_desc;
        for (auto& context : context_list_) {
            buffer_desc.lkey.push_back(context->lkey(chunk_addr));
            buffer_desc.rkey.push_back(context->rkey(chunk_addr));
        }

        buffer_desc.name = resolved_name;
        buffer_desc.addr = (uint64_t)chunk_addr;
        buffer_desc.length = chunk_len;
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
        if (rc) {
            rollbackChunks(ci);
            return rc;
        }
    }

    // Track chunks and NIC assignments for unregistration
    if (chunks.size() > 1) {
        std::lock_guard<std::mutex> lock(chunk_map_mutex_);
        std::vector<ChunkRegistration> regs;
        regs.reserve(chunks.size());
        for (size_t ci = 0; ci < chunks.size(); ++ci) {
            regs.push_back({(uint64_t)chunks[ci].first, nic_assignments[ci]});
        }
        chunk_map_[(uint64_t)addr] = std::move(regs);
    }

    return 0;
}

int CxiTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return unregisterLocalMemoryInternal(addr, update_metadata, false);
}

int CxiTransport::unregisterLocalMemoryInternal(void* addr,
                                                bool update_metadata,
                                                bool force_sequential) {
    // Check if this buffer was split into chunks (per-NIC partition)
    std::vector<ChunkRegistration> chunk_regs;
    {
        std::lock_guard<std::mutex> lock(chunk_map_mutex_);
        auto it = chunk_map_.find((uint64_t)addr);
        if (it != chunk_map_.end()) {
            chunk_regs = std::move(it->second);
            chunk_map_.erase(it);
        }
    }

    if (!chunk_regs.empty()) {
        // Unregister each chunk from its assigned NICs only
        for (auto& reg : chunk_regs) {
            void* ca = (void*)reg.addr;
            int rc = metadata_->removeLocalMemoryBuffer(ca, update_metadata);
            if (rc) {
                LOG(ERROR) << "Failed to remove chunk metadata at " << ca;
                return rc;
            }

            for (size_t nic_idx : reg.nic_indices) {
                int ret = context_list_[nic_idx]->unregisterMemoryRegion(ca);
                if (ret) {
                    LOG(ERROR) << "Failed to unregister chunk " << ca
                               << " with CXI context " << nic_idx;
                    return ret;
                }
            }
        }
        return 0;
    }

    // Non-chunked buffer: original path
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

        for (auto& thread : unreg_threads) {
            thread.join();
        }

        for (size_t i = 0; i < ret_codes.size(); ++i) {
            if (ret_codes[i] != 0) {
                LOG(ERROR)
                    << "Failed to unregister memory region with CXI context "
                    << i;
                return ret_codes[i];
            }
        }
    } else {
        for (size_t i = 0; i < context_list_.size(); ++i) {
            int ret = context_list_[i]->unregisterMemoryRegion(addr);
            if (ret) {
                LOG(ERROR)
                    << "Failed to unregister memory region with CXI context "
                    << i;
                return ret;
            }
        }
    }

    return 0;
}

int CxiTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "cxi";
    for (auto& entry : context_list_) {
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

int CxiTransport::registerLocalMemoryBatch(
    const std::vector<CxiTransport::BufferEntry>& buffer_list,
    const std::string& location) {
    std::vector<std::future<int>> results;
    for (auto& buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemoryInternal(buffer.addr, buffer.length,
                                                   location, true, false, true);
            }));
    }

    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "CxiTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }

    return metadata_->updateLocalSegmentDesc();
}

int CxiTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    std::vector<std::future<int>> results;
    for (auto& addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemoryInternal(addr, false, true);
            }));
    }

    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "CxiTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

int CxiTransport::warmupSegment(const std::string& segment_name) {
    if (!metadata_) {
        LOG(ERROR) << "CxiTransport::warmupSegment: metadata_ is null";
        return ERR_INVALID_ARGUMENT;
    }
    if (segment_name.empty() || segment_name == local_server_name_) {
        // Loopback / empty name — nothing to pre-connect.
        return 0;
    }

    auto desc = metadata_->getSegmentDescByName(segment_name);
    if (!desc) {
        LOG(ERROR) << "CxiTransport::warmupSegment: segment '" << segment_name
                   << "' not found in metadata (did you openSegment() first?)";
        return ERR_INVALID_ARGUMENT;
    }
    if (desc->devices.empty()) {
        LOG(WARNING) << "CxiTransport::warmupSegment: segment '" << segment_name
                     << "' has no devices";
        return 0;
    }

    // Build peer_nic_path list: "<segment_name>@<device_name>" for each NIC.
    std::vector<std::string> peer_paths;
    peer_paths.reserve(desc->devices.size());
    for (const auto& dev : desc->devices) {
        peer_paths.emplace_back(segment_name + "@" + dev.name);
    }

    auto t0 = std::chrono::steady_clock::now();
    size_t n_pairs = context_list_.size() * peer_paths.size();

    // Idempotent short-circuit: if every (local_ctx, peer_nic) pair already
    // has a connected endpoint, skip the whole async dispatch. Matters for
    // callers that invoke warmupSegment per request loop — without this the
    // 256-thread fan-out runs every time even though there is no work to do.
    size_t already_ready = 0;
    for (auto& ctx : context_list_) {
        for (const auto& path : peer_paths) {
            auto ep = ctx->peekEndpoint(path);
            if (ep && ep->connected()) ++already_ready;
        }
    }
    if (already_ready == n_pairs) {
        VLOG(1) << "CxiTransport::warmupSegment('" << segment_name << "'): all "
                << n_pairs << " endpoints already connected, "
                << "skipping";
        return 0;
    }

    // Warm up every (local_ctx, peer_nic) pair concurrently.  Under the
    // shared-endpoint model each warmup is just a handshake RPC +
    // fi_av_insert (no fi_endpoint, no fi_enable), so the critical path is
    // max(handshake RTT), not sum.  We still dispatch with std::async for
    // concurrency, but total wall time is typically ms-level.
    std::vector<std::future<int>> futs;
    futs.reserve(n_pairs);
    for (auto& ctx : context_list_) {
        for (const auto& path : peer_paths) {
            futs.emplace_back(
                std::async(std::launch::async, [ctx, path]() -> int {
                    auto ep = ctx->endpoint(path);
                    if (!ep) {
                        LOG(WARNING) << "warmupSegment: endpoint() returned "
                                        "null for "
                                     << path;
                        return -1;
                    }
                    if (ep->connected()) return 0;
                    int rc = ep->setupConnectionsByActive();
                    if (rc != 0) {
                        // Handshake failed: drop the peer handle so the AV
                        // slot is freed and the next warmup retry starts
                        // clean.  Cheap under the shared-endpoint model —
                        // no fi_endpoint teardown required.
                        ctx->deleteEndpoint(path);
                    }
                    return rc;
                }));
        }
    }
    int ok = 0, fail = 0;
    for (auto& f : futs) {
        int rc = f.get();
        if (rc == 0)
            ++ok;
        else
            ++fail;
    }
    auto elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    LOG(INFO) << "CxiTransport::warmupSegment('" << segment_name << "'): " << ok
              << "/" << n_pairs << " endpoints connected (" << fail
              << " failed) in " << elapsed << "s (" << context_list_.size()
              << " local NICs x " << peer_paths.size() << " peer NICs)";
    return fail == 0 ? 0 : ERR_ENDPOINT;
}

Status CxiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "CxiTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "CxiTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask*> task_list;
    for (auto& task : batch_desc.task_list) task_list.push_back(&task);
    return submitTransferTask(task_list);
}

Status CxiTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::unordered_map<std::shared_ptr<CxiContext>, std::vector<Slice*>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    assert(local_segment_desc.get());
    const int kMaxRetryCount = globalConfig().retry_cnt;

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;

        if (request.length == 0) continue;

        // Find which buffer and preferred device covers this request
        int request_buffer_id = -1, request_device_id = -1;
        if (selectDevice(local_segment_desc.get(), (uint64_t)request.source,
                         request.length, request_buffer_id,
                         request_device_id)) {
            request_buffer_id = -1;
            request_device_id = -1;
        }

        if (request_buffer_id >= 0 && request_device_id >= 0) {
            // One slice per request.  Round-robin NIC selection is
            // handled by selectDevice above.
            auto& context = context_list_[request_device_id];
            if (!context || !context->active()) {
                LOG(ERROR) << "CXI Device " << request_device_id
                           << " is not active";
                return Status::InvalidArgument(
                    "CXI Device " + std::to_string(request_device_id) +
                    " is not active");
            }

            Slice* slice = getSliceCache().allocate();
            assert(slice);
            slice->peer_nic_path.clear();
            slice->rdma.dest_rkey = 0;  // will be set later

            slice->source_addr = (char*)request.source;
            slice->length = request.length;
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);

            slice->rdma.source_lkey =
                local_segment_desc->buffers[request_buffer_id]
                    .lkey[request_device_id];
            slices_to_post[context].push_back(slice);
            __sync_fetch_and_add(&task.total_bytes, slice->length);
            __sync_fetch_and_add(&task.slice_count, 1);
        } else {
            // FALLBACK: device not found via initial selectDevice.
            // Try per-slice retry with increasing retry_cnt to find any
            // available device (handles edge cases like multi-buffer spans).
            int buffer_id = -1, device_id = -1;
            int retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)request.source, request.length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                if (device_id >= 0 &&
                    static_cast<size_t>(device_id) < context_list_.size() &&
                    context_list_[device_id] &&
                    context_list_[device_id]->active()) {
                    found_device = true;
                    break;
                }
            }
            if (!found_device) {
                LOG(ERROR) << "Memory region not registered by any active CXI "
                              "device(s): "
                           << request.source;
                for (auto& entry : slices_to_post)
                    for (auto s : entry.second) s->markFailed();
                return Status::AddressNotRegistered(
                    "Memory region not registered by any active CXI "
                    "device(s): " +
                    std::to_string(
                        reinterpret_cast<uintptr_t>(request.source)));
            }

            // Found a device via retry — create single slice
            Slice* slice = getSliceCache().allocate();
            assert(slice);
            slice->peer_nic_path.clear();
            slice->rdma.dest_rkey = 0;

            slice->source_addr = (char*)request.source;
            slice->length = request.length;
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);

            auto& context = context_list_[device_id];
            slice->rdma.source_lkey =
                local_segment_desc->buffers[buffer_id].lkey[device_id];
            slices_to_post[context].push_back(slice);
            __sync_fetch_and_add(&task.total_bytes, slice->length);
            __sync_fetch_and_add(&task.slice_count, 1);
        }
    }

    for (auto& entry : slices_to_post)
        if (!entry.second.empty()) entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status CxiTransport::getTransferStatus(BatchID batch_id,
                                       std::vector<TransferStatus>& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.resize(task_count);
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        auto& task = batch_desc.task_list[task_id];
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

Status CxiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "CxiTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
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

CxiTransport::SegmentID CxiTransport::getSegmentID(
    const std::string& segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int CxiTransport::onSetupCxiConnections(const HandShakeDesc& peer_desc,
                                        HandShakeDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    // Find context by device name instead of using hca_list index, since
    // context_list_ only contains CXI devices and may have different
    // indexing than the full hca_list.
    std::shared_ptr<CxiContext> context;
    for (auto& entry : context_list_) {
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

int CxiTransport::initializeCxiResources() {
    auto hca_list = local_topology_->getHcaList();

    // Filter for CXI devices (names typically start with "rdmap" on AWS)
    std::vector<std::string> efa_devices;
    std::vector<std::string> non_efa_devices;
    for (auto& device_name : hca_list) {
        if (device_name.find("cxi") != std::string::npos) {
            efa_devices.push_back(device_name);
        } else {
            non_efa_devices.push_back(device_name);
        }
    }

    if (efa_devices.empty()) {
        LOG(WARNING) << "CxiTransport: No CXI devices found, falling back to "
                        "all devices";
        efa_devices = hca_list;
        non_efa_devices.clear();
    }

    // Disable non-CXI devices
    for (auto& device_name : non_efa_devices) {
        local_topology_->disableDevice(device_name);
        LOG(INFO) << "CxiTransport: Disabled non-CXI device " << device_name
                  << " in topology";
    }

    for (auto& device_name : efa_devices) {
        auto context = std::make_shared<CxiContext>(*this, device_name);
        auto& config = globalConfig();
        int ret = context->construct(config.num_cq_per_ctx, config.max_cqe,
                                     config.max_ep_per_ctx);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "CxiTransport: Disable device " << device_name;
        } else {
            context_list_.push_back(context);
            LOG(INFO) << "CxiTransport: Initialized CXI device " << device_name;
        }
    }
    if (context_list_.empty()) {
        LOG(ERROR) << "CxiTransport: No available CXI devices";
        return ERR_DEVICE_NOT_FOUND;
    }

    return 0;
}

int CxiTransport::startHandshakeDaemon(std::string& local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&CxiTransport::onSetupCxiConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int CxiTransport::selectDevice(SegmentDesc* desc, uint64_t offset,
                               size_t length, std::string_view hint,
                               int& buffer_id, int& device_id,
                               int retry_count) {
    if (desc == nullptr) return ERR_ADDRESS_NOT_REGISTERED;
    const auto& buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto& buffer = buffers[buffer_id];

        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;
        }

        int num_devices = static_cast<int>(desc->devices.size());
        // this must be <=, because the first attempt will be random, so to
        // guarantee that the device is found we need num_device+1 attempts
        for (int attempt = 0; attempt <= num_devices; ++attempt) {
            int try_count = retry_count + attempt;
            device_id =
                hint.empty()
                    ? desc->topology.selectDevice(buffer.name, try_count)
                    : desc->topology.selectDevice(buffer.name, hint, try_count);
            if (device_id >= 0 &&
                static_cast<size_t>(device_id) < buffer.rkey.size() &&
                buffer.rkey[device_id] != 0) {
                return 0;
            }
            device_id = hint.empty() ? desc->topology.selectDevice(
                                           kWildcardLocation, try_count)
                                     : desc->topology.selectDevice(
                                           kWildcardLocation, hint, try_count);
            if (device_id >= 0 &&
                static_cast<size_t>(device_id) < buffer.rkey.size() &&
                buffer.rkey[device_id] != 0) {
                return 0;
            }
        }
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int CxiTransport::selectDevice(SegmentDesc* desc, uint64_t offset,
                               size_t length, int& buffer_id, int& device_id,
                               int retry_count) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_count);
}

}  // namespace mooncake
