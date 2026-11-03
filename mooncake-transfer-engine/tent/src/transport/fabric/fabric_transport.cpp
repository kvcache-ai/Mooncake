// Copyright 2026 KVCache.AI
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

#include "tent/transport/fabric/fabric_transport.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <thread>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {

namespace {

constexpr int kFabricAttrKey = static_cast<int>(TransportType::FABRIC);

// NUMA node of a CPU buffer location ("cpu:N"), or -1.
int cpuNumaNode(const std::string& location) {
    LocationParser parser(location);
    return parser.type() == "cpu" ? parser.index() : -1;
}

// The NICs of `nics` attached to `numa_node`, or all of them if there are
// none or the node is unknown. Posting from (and targeting) NICs on the
// buffer's socket keeps DMA off the inter-socket link.
template <typename NumaOf>
std::vector<int> numaLocalNics(const std::vector<int>& nics, int numa_node,
                               NumaOf numa_of) {
    std::vector<int> local;
    if (numa_node >= 0) {
        for (int nic : nics)
            if (numa_of(nic) == numa_node) local.push_back(nic);
    }
    return local.empty() ? nics : local;
}

bool hasTransport(const BufferDesc& desc, TransportType type) {
    return std::find(desc.transports.begin(), desc.transports.end(), type) !=
           desc.transports.end();
}

// Kernel page size backing addr, from /proc/self/smaps. The EFA PTE budget
// is counted in these pages.
uint64_t detectPageSize(uint64_t addr) {
    const uint64_t fallback = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
    std::ifstream smaps("/proc/self/smaps");
    std::string line;
    bool in_range = false;
    while (std::getline(smaps, line)) {
        uint64_t start = 0, end = 0;
        char dash = 0;
        std::istringstream header(line);
        if (header >> std::hex >> start >> dash >> end && dash == '-') {
            in_range = addr >= start && addr < end;
            continue;
        }
        if (in_range && line.rfind("KernelPageSize:", 0) == 0) {
            uint64_t kb = 0;
            std::istringstream(line.substr(15)) >> kb;
            return kb ? kb * 1024 : fallback;
        }
    }
    return fallback;
}

}  // namespace

void FabricLocalBuffer::release() {
    if (released.exchange(true)) return;
    for (auto& chunk : chunks) {
        for (size_t i = 0; i < chunk.mr.size(); ++i) {
            if (chunk.mr[i]) contexts[i]->deregisterMemory(chunk.mr[i]);
            chunk.mr[i] = nullptr;
        }
    }
}

FabricTransport::FabricTransport() {}

FabricTransport::~FabricTransport() { uninstall(); }

Status FabricTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    if (installed_) {
        return Status::InvalidArgument(
            "Fabric transport has been installed" LOC_MARK);
    }
    if (!metadata) {
        return Status::InvalidArgument("Fabric metadata is null" LOC_MARK);
    }
    if (conf) {
        params_.provider =
            conf->get("transports/fabric/provider", params_.provider);
        params_.fabric_name =
            conf->get("transports/fabric/fabric_name", params_.fabric_name);
        params_.devices =
            conf->getArray<std::string>("transports/fabric/devices");
        params_.slice_size =
            conf->get("transports/fabric/slice_size", params_.slice_size);
        params_.idle_sleep_us =
            conf->get("transports/fabric/idle_sleep_us", params_.idle_sleep_us);
        params_.post_timeout_ms = conf->get("transports/fabric/post_timeout_ms",
                                            params_.post_timeout_ms);
        params_.quiesce_timeout_ms = conf->get(
            "transports/fabric/quiesce_timeout_ms", params_.quiesce_timeout_ms);
        params_.op_timeout_ms =
            conf->get("transports/fabric/op_timeout_ms", params_.op_timeout_ms);
        params_.max_posted_ops = conf->get("transports/fabric/max_posted_ops",
                                           params_.max_posted_ops);
        params_.max_pte_entries = conf->get("transports/fabric/max_pte_entries",
                                            params_.max_pte_entries);
        params_.max_register_threads =
            conf->get("transports/fabric/max_register_threads",
                      params_.max_register_threads);
        chunk_limit_ = conf->get("transports/fabric/max_mr_size", chunk_limit_);
    }

    std::vector<const FabricProfile*> candidates;
    if (params_.provider == "auto") {
        candidates = autoFabricProfiles();
    } else if (auto* profile = findFabricProfile(params_.provider)) {
        candidates.push_back(profile);
    } else {
        return Status::InvalidArgument("Unknown fabric provider " +
                                       params_.provider + LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;

    Status status = Status::DeviceNotFound("No fabric provider" LOC_MARK);
    for (auto* profile : candidates) {
#if !defined(USE_CUDA) && !defined(USE_HIP)
        // Without device memory support, keep the EFA provider from probing
        // GPU HMEM interfaces it cannot use.
        if (std::string(profile->name) == "efa") setenv("FI_HMEM", "system", 0);
#endif
        profile_ = profile;
        status = openContexts();
        if (status.ok()) break;
        VLOG(1) << "Fabric provider " << profile->name
                << " unavailable: " << status.ToString();
    }
    if (!status.ok()) {
        profile_ = nullptr;
        metadata_.reset();
        return status;
    }

    status = publishPeerAttr();
    if (!status.ok()) {
        closeContexts();
        profile_ = nullptr;
        metadata_.reset();
        return status;
    }

    caps.dram_to_dram = true;
    shutting_down_.store(false, std::memory_order_release);
    installed_ = true;
    LOG(INFO) << "Fabric transport installed: provider=" << profile_->name
              << " nics=" << contexts_.size() << " slice_size=" << slice_size_
              << (virt_addr_ ? " va" : " offset");
    return Status::OK();
}

Status FabricTransport::openContexts() {
    std::vector<FabricDomainInfo> domains;
    CHECK_STATUS(discoverFabricDomains(*profile_, params_, domains));
    for (auto& domain : domains) {
        auto context = std::make_unique<FabricContext>();
        const std::string name = domain.name;
        Status status = context->open(*profile_, domain, params_);
        if (!status.ok()) {
            LOG(WARNING) << "Skip fabric domain " << name << ": "
                         << status.ToString();
            continue;
        }
        contexts_.push_back(std::move(context));
    }
    freeFabricDomains(domains);
    if (contexts_.empty()) {
        return Status::DeviceNotFound(
            "No fabric domain could be opened" LOC_MARK);
    }
    virt_addr_ = contexts_[0]->virtAddr();
    slice_size_ = params_.slice_size;
    for (auto& context : contexts_) {
        if (context->virtAddr() != virt_addr_) {
            closeContexts();
            return Status::InvalidArgument(
                "Fabric domains disagree on FI_MR_VIRT_ADDR" LOC_MARK);
        }
        if (context->maxMsgSize())
            slice_size_ = std::min(slice_size_, context->maxMsgSize());
    }
    if (slice_size_ == 0) slice_size_ = 512 * 1024;
    return Status::OK();
}

void FabricTransport::closeContexts() {
    // Workers must be gone before MRs are closed, and MRs before domains.
    for (auto& context : contexts_) context->stop();
    {
        std::unique_lock<std::shared_mutex> guard(buffers_mutex_);
        for (auto& entry : buffers_) entry.second->release();
        buffers_.clear();
        for (auto& weak : retired_) {
            if (auto buffer = weak.lock()) buffer->release();
        }
        retired_.clear();
    }
    {
        std::lock_guard<std::mutex> guard(peers_mutex_);
        peers_.clear();
    }
    for (auto& context : contexts_) context->close();
    contexts_.clear();
}

Status FabricTransport::publishPeerAttr() {
    FabricPeerAttr attr;
    attr.provider = profile_->name;
    attr.virt_addr = virt_addr_;
    for (auto& context : contexts_) {
        attr.nics.push_back(
            {context->name(), context->address(), context->numaNode()});
    }
    const std::string encoded = encodeFabricPeerAttr(attr);
    CHECK_STATUS(metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& desc) -> Status {
            if (desc.type != SegmentType::Memory) {
                return Status::InvalidMetadataType(
                    "Fabric requires a local memory segment" LOC_MARK);
            }
            std::get<MemorySegmentDesc>(desc.detail)
                .transport_attrs[kFabricAttrKey] = encoded;
            return Status::OK();
        }));
    return metadata_->segmentManager().synchronizeLocal();
}

Status FabricTransport::unpublishPeerAttr() {
    CHECK_STATUS(metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& desc) -> Status {
            if (desc.type == SegmentType::Memory) {
                std::get<MemorySegmentDesc>(desc.detail)
                    .transport_attrs.erase(kFabricAttrKey);
            }
            return Status::OK();
        }));
    return metadata_->segmentManager().synchronizeLocal();
}

Status FabricTransport::uninstall() {
    if (!installed_) return Status::OK();
    quiesce();
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    if (!installed_) return Status::OK();
    Status status = unpublishPeerAttr();
    if (!status.ok())
        LOG(WARNING) << "Fabric metadata cleanup failed: " << status.ToString();
    closeContexts();
    metadata_.reset();
    profile_ = nullptr;
    installed_ = false;
    return Status::OK();
}

Status FabricTransport::quiesce() {
    {
        std::lock_guard<std::mutex> guard(lifecycle_mutex_);
        shutting_down_.store(true, std::memory_order_release);
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(params_.quiesce_timeout_ms);
    while (true) {
        uint64_t inflight = 0;
        for (auto& context : contexts_) inflight += context->inflight();
        if (inflight == 0) return Status::OK();
        if (std::chrono::steady_clock::now() >= deadline) {
            LOG(WARNING) << "Fabric quiesce timed out with " << inflight
                         << " ops in flight";
            return Status::OK();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

Status FabricTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto fabric_batch = Slab<FabricSubBatch>::Get().allocate();
    if (!fabric_batch)
        return Status::InternalError("Unable to allocate fabric sub-batch");
    fabric_batch->task_list.reserve(max_size);
    fabric_batch->max_size = max_size;
    batch = fabric_batch;
    return Status::OK();
}

Status FabricTransport::freeSubBatch(SubBatchRef& batch) {
    auto fabric_batch = dynamic_cast<FabricSubBatch*>(batch);
    if (!fabric_batch)
        return Status::InvalidArgument("Invalid fabric sub-batch" LOC_MARK);
    // Tasks still in flight are kept alive by their ops.
    for (auto* task : fabric_batch->task_list) task->unref();
    fabric_batch->task_list.clear();
    Slab<FabricSubBatch>::Get().deallocate(fabric_batch);
    batch = nullptr;
    return Status::OK();
}

std::shared_ptr<FabricLocalBuffer> FabricTransport::findLocalBuffer(
    uint64_t addr, uint64_t length) {
    std::shared_lock<std::shared_mutex> guard(buffers_mutex_);
    auto it = buffers_.upper_bound(addr);
    if (it == buffers_.begin()) return nullptr;
    --it;
    const auto& buffer = it->second;
    if (addr + length < addr || addr + length > buffer->addr + buffer->length)
        return nullptr;
    return buffer;
}

Status FabricTransport::resolveTarget(const Request& request, Target& target) {
    SegmentDescRef pin;
    return metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* segment) -> Status {
            if (!segment || segment->type != SegmentType::Memory) {
                return Status::NeedsRefreshCache(
                    "Fabric target is not a memory segment" LOC_MARK);
            }
            BufferDesc* buffer =
                segment->findBuffer(request.target_offset, request.length);
            if (!buffer || !hasTransport(*buffer, TransportType::FABRIC)) {
                return Status::NeedsRefreshCache(
                    "Fabric target buffer is not registered" LOC_MARK);
            }

            std::lock_guard<std::mutex> guard(peers_mutex_);
            auto& peer = peers_[request.target_id];
            if (!peer || peer->snapshot.get() != segment) {
                const auto& attrs = segment->getMemory().transport_attrs;
                auto it = attrs.find(kFabricAttrKey);
                if (it == attrs.end()) {
                    peer.reset();
                    return Status::NeedsRefreshCache(
                        "Fabric peer has no endpoint metadata" LOC_MARK);
                }
                auto entry = std::make_shared<PeerEntry>();
                Status status = decodeFabricPeerAttr(it->second, entry->attr);
                if (!status.ok()) {
                    peer.reset();
                    return status;
                }
                if (entry->attr.provider != profile_->name ||
                    entry->attr.virt_addr != virt_addr_) {
                    peer.reset();
                    return Status::InvalidArgument(
                        "Fabric peer uses provider " + entry->attr.provider +
                        ", local is " + profile_->name + LOC_MARK);
                }
                entry->snapshot = pin;
                entry->fi_addrs.assign(
                    contexts_.size(),
                    std::vector<fi_addr_t>(entry->attr.nics.size(),
                                           FI_ADDR_UNSPEC));
                peer = std::move(entry);
            }

            auto& remote = peer->buffers[buffer->addr];
            if (!remote) {
                auto it = buffer->transport_attrs.find(TransportType::FABRIC);
                if (it == buffer->transport_attrs.end()) {
                    peer->buffers.erase(buffer->addr);
                    return Status::NeedsRefreshCache(
                        "Fabric buffer has no key metadata" LOC_MARK);
                }
                auto decoded = std::make_shared<PeerBuffer>();
                Status status = decodeFabricBufferAttr(
                    it->second, buffer->length, decoded->attr);
                for (const auto& chunk : decoded->attr.chunks) {
                    for (int nic : chunk.nics) {
                        if (nic < 0 || (size_t)nic >= peer->attr.nics.size())
                            status = Status::MalformedJson(
                                "Fabric buffer refers to an unknown "
                                "NIC" LOC_MARK);
                    }
                    decoded->ranges.push_back({chunk.offset, chunk.length});
                    if (chunk.nics.empty())
                        status = Status::MalformedJson(
                            "Fabric buffer chunk has no NIC" LOC_MARK);
                }
                if (status.ok()) {
                    const int numa_node = cpuNumaNode(buffer->location);
                    for (const auto& chunk : decoded->attr.chunks) {
                        decoded->preferred.push_back(
                            numaLocalNics(chunk.nics, numa_node, [&](int nic) {
                                return peer->attr.nics[nic].numa_node;
                            }));
                    }
                }
                if (!status.ok()) {
                    peer->buffers.erase(buffer->addr);
                    return status;
                }
                remote = std::move(decoded);
            }
            target.peer = peer;
            target.buffer = remote;
            target.buffer_addr = buffer->addr;
            return Status::OK();
        });
}

Status FabricTransport::peerAddress(PeerEntry& peer, size_t local_nic,
                                    size_t remote_nic, fi_addr_t& addr) {
    {
        std::lock_guard<std::mutex> guard(peers_mutex_);
        addr = peer.fi_addrs[local_nic][remote_nic];
    }
    if (addr != FI_ADDR_UNSPEC) return Status::OK();
    CHECK_STATUS(contexts_[local_nic]->resolvePeer(
        peer.attr.nics[remote_nic].address, addr));
    std::lock_guard<std::mutex> guard(peers_mutex_);
    peer.fi_addrs[local_nic][remote_nic] = addr;
    return Status::OK();
}

Status FabricTransport::planRequest(const Request& request, FabricTask* task,
                                    std::vector<std::vector<FabricOp*>>& ops) {
    if (request.opcode != Request::READ && request.opcode != Request::WRITE) {
        return Status::InvalidArgument("Invalid fabric opcode" LOC_MARK);
    }
    const uint64_t source = reinterpret_cast<uint64_t>(request.source);
    auto local = findLocalBuffer(source, request.length);
    if (!local) {
        return Status::AddressNotRegistered(
            "Fabric source buffer is not registered" LOC_MARK);
    }
    Target target;
    CHECK_STATUS(resolveTarget(request, target));

    std::vector<FabricSpan> spans;
    if (!cutFabricSpans(request.length, source - local->addr, local->ranges,
                        request.target_offset - target.buffer_addr,
                        target.buffer->ranges, slice_size_, spans)) {
        return Status::InvalidArgument(
            "Fabric request is outside the registered chunks" LOC_MARK);
    }

    task->local_buffer = local;
    task->local_active = &local->active;
    for (const auto& span : spans) {
        const auto& lchunk = local->chunks[span.local_chunk];
        const auto& rchunk = target.buffer->attr.chunks[span.remote_chunk];
        const uint64_t cursor = cursor_.fetch_add(1, std::memory_order_relaxed);
        const int local_nic =
            lchunk.post_nics[cursor % lchunk.post_nics.size()];
        // Prefer the same NIC index on the peer (rails line up on
        // homogeneous hosts) if it is local to the remote buffer; otherwise
        // stripe over the NICs that are.
        const auto& remote_nics = target.buffer->preferred[span.remote_chunk];
        int remote_nic = local_nic;
        if (std::find(remote_nics.begin(), remote_nics.end(), remote_nic) ==
            remote_nics.end())
            remote_nic = remote_nics[cursor % remote_nics.size()];
        uint64_t key = 0;
        if (!rchunk.keyFor(remote_nic, key)) {
            return Status::InternalError(
                "Fabric remote chunk lost its key" LOC_MARK);
        }
        fi_addr_t peer = FI_ADDR_UNSPEC;
        CHECK_STATUS(peerAddress(*target.peer, local_nic, remote_nic, peer));

        const uint64_t remote_offset =
            request.target_offset - target.buffer_addr + span.offset;
        FabricOp* op = allocateFabricOp();
        if (!op) return Status::InternalError("Unable to allocate fabric op");
        op->task = task;
        op->local = reinterpret_cast<char*>(request.source) + span.offset;
        op->desc = lchunk.desc[local_nic];
        op->remote_addr = virt_addr_ ? request.target_offset + span.offset
                                     : remote_offset - rchunk.offset;
        op->key = key;
        op->length = span.length;
        op->peer = peer;
        op->is_write = request.opcode == Request::WRITE;
        ops[local_nic].push_back(op);
    }
    return Status::OK();
}

Status FabricTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto fabric_batch = dynamic_cast<FabricSubBatch*>(batch);
    if (!fabric_batch)
        return Status::InvalidArgument("Invalid fabric sub-batch" LOC_MARK);
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    if (!installed_ || shutting_down_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "Fabric transport is shutting down" LOC_MARK);
    }
    if (request_list.size() + fabric_batch->task_list.size() >
        fabric_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Plan everything first so a failure leaves the sub-batch untouched.
    std::vector<FabricTask*> tasks;
    std::vector<size_t> op_counts;
    std::vector<std::vector<FabricOp*>> ops(contexts_.size());
    auto rollback = [&]() {
        for (auto& list : ops)
            for (auto* op : list) freeFabricOp(op);
        for (auto* task : tasks) task->unref();
    };
    for (const auto& request : request_list) {
        FabricTask* task = allocateFabricTask();
        if (!task) {
            rollback();
            return Status::InternalError("Unable to allocate fabric task");
        }
        tasks.push_back(task);
        task->length = request.length;
        task->progress_batch_id = batch->progress_batch_id;
        task->notify_progress = batch->notify_progress;
        size_t before = 0;
        for (auto& list : ops) before += list.size();
        Status status =
            request.length ? planRequest(request, task, ops) : Status::OK();
        if (!status.ok()) {
            rollback();
            return status;
        }
        size_t after = 0;
        for (auto& list : ops) after += list.size();
        op_counts.push_back(after - before);
    }

    const uint64_t now = getCurrentTimeInNano();
    for (size_t i = 0; i < tasks.size(); ++i) {
        FabricTask* task = tasks[i];
        const size_t count = op_counts[i];
        if (count == 0) {
            task->status.store(TransferStatusEnum::COMPLETED,
                               std::memory_order_release);
        } else {
            task->pending.store(count, std::memory_order_relaxed);
            task->refs.fetch_add(count, std::memory_order_relaxed);
            task->local_active->fetch_add(1, std::memory_order_acq_rel);
        }
        fabric_batch->task_list.push_back(task);
    }
    for (size_t nic = 0; nic < ops.size(); ++nic) {
        for (auto* op : ops[nic]) op->enqueue_ns = now;
        contexts_[nic]->submit(ops[nic]);
    }
    return Status::OK();
}

Status FabricTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto fabric_batch = dynamic_cast<FabricSubBatch*>(batch);
    if (!fabric_batch || task_id < 0 ||
        task_id >= (int)fabric_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto* task = fabric_batch->task_list[task_id];
    status.s = task->status.load(std::memory_order_acquire);
    status.transferred_bytes =
        task->transferred.load(std::memory_order_acquire);
    return Status::OK();
}

Status FabricTransport::registerBuffer(
    uint64_t addr, uint64_t length,
    std::shared_ptr<FabricLocalBuffer>& buffer) {
    uint64_t page_size = 0;
    uint64_t chunk_limit = chunk_limit_;
    uint64_t pte_budget = 0;
    if (profile_->limit_mr_by_pte && params_.max_pte_entries) {
        page_size = detectPageSize(addr);
        pte_budget = params_.max_pte_entries;
        const uint64_t pte_limit = pte_budget * page_size;
        chunk_limit =
            chunk_limit ? std::min(chunk_limit, pte_limit) : pte_limit;
    }
    auto ranges = planFabricChunks(length, chunk_limit);
    std::vector<std::vector<int>> assignment;
    CHECK_STATUS(assignFabricChunkNics(ranges, contexts_.size(), page_size,
                                       pte_budget, assignment));
    if (ranges.size() > 1) {
        LOG(INFO) << "Fabric buffer " << (void*)addr << " (" << length
                  << " bytes) split into " << ranges.size()
                  << " chunks of <= " << chunk_limit << " bytes";
    }

    buffer = std::make_shared<FabricLocalBuffer>();
    buffer->addr = addr;
    buffer->length = length;
    buffer->ranges = ranges;
    for (auto& context : contexts_) buffer->contexts.push_back(context.get());
    struct Job {
        size_t chunk;
        int nic;
    };
    std::vector<Job> jobs;
    for (size_t c = 0; c < ranges.size(); ++c) {
        FabricLocalBuffer::Chunk chunk;
        chunk.range = ranges[c];
        chunk.nics = assignment[c];
        chunk.post_nics = chunk.nics;
        chunk.mr.assign(contexts_.size(), nullptr);
        chunk.desc.assign(contexts_.size(), nullptr);
        chunk.key.assign(contexts_.size(), 0);
        buffer->chunks.push_back(std::move(chunk));
        for (int nic : assignment[c]) jobs.push_back({c, nic});
    }

    std::vector<Status> results(jobs.size(), Status::OK());
    auto run = [&](size_t j) {
        auto& chunk = buffer->chunks[jobs[j].chunk];
        const int nic = jobs[j].nic;
        results[j] = contexts_[nic]->registerMemory(
            reinterpret_cast<void*>(addr + chunk.range.offset),
            chunk.range.length, chunk.mr[nic], chunk.desc[nic], chunk.key[nic]);
    };
    const size_t threads = std::min(
        jobs.size(), std::max<size_t>(1, params_.max_register_threads));
    if (threads <= 1) {
        for (size_t j = 0; j < jobs.size(); ++j) run(j);
    } else {
        std::atomic<size_t> next{0};
        std::vector<std::thread> workers;
        for (size_t t = 0; t < threads; ++t) {
            workers.emplace_back([&] {
                for (size_t j = next++; j < jobs.size(); j = next++) run(j);
            });
        }
        for (auto& worker : workers) worker.join();
    }
    for (auto& result : results) {
        if (!result.ok()) {
            buffer->release();
            buffer.reset();
            return result;
        }
    }
    return Status::OK();
}

Status FabricTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    if (!installed_ || shutting_down_.load(std::memory_order_acquire)) {
        return Status::TooManyRequests(
            "Fabric transport is shutting down" LOC_MARK);
    }
    // Device memory needs FI_HMEM and comes in a follow-up.
    if (Platform::getLoader().getMemoryType(
            reinterpret_cast<void*>(desc.addr)) != MTYPE_CPU) {
        return Status::OK();
    }

    std::shared_ptr<FabricLocalBuffer> buffer;
    {
        std::shared_lock<std::shared_mutex> guard(buffers_mutex_);
        auto it = buffers_.find(desc.addr);
        if (it != buffers_.end() && it->second->length == desc.length)
            buffer = it->second;
    }
    if (!buffer) {
        CHECK_STATUS(registerBuffer(desc.addr, desc.length, buffer));
        int numa_node = cpuNumaNode(desc.location);
        if (numa_node < 0) {
            auto located = Platform::getLoader().getLocation(
                reinterpret_cast<void*>(desc.addr), 1);
            if (!located.empty()) numa_node = cpuNumaNode(located[0].location);
        }
        for (auto& chunk : buffer->chunks) {
            chunk.post_nics = numaLocalNics(
                chunk.nics, numa_node,
                [&](int nic) { return contexts_[nic]->numaNode(); });
        }
        std::unique_lock<std::shared_mutex> guard(buffers_mutex_);
        auto& slot = buffers_[desc.addr];
        if (slot) retired_.push_back(slot);
        slot = buffer;
    }
    if (options.perm == kLocalReadWrite) return Status::OK();

    FabricBufferAttr attr;
    for (const auto& chunk : buffer->chunks) {
        FabricChunkAttr entry;
        entry.offset = chunk.range.offset;
        entry.length = chunk.range.length;
        for (int nic : chunk.nics) {
            entry.nics.push_back(nic);
            entry.keys.push_back(chunk.key[nic]);
        }
        attr.chunks.push_back(std::move(entry));
    }
    buffer->published = true;
    desc.transport_attrs[TransportType::FABRIC] = encodeFabricBufferAttr(attr);
    if (!hasTransport(desc, TransportType::FABRIC))
        desc.transports.push_back(TransportType::FABRIC);
    return Status::OK();
}

Status FabricTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                        const MemoryOptions& options) {
    // A failed buffer simply is not advertised for fabric; keep going so the
    // others still are.
    Status first = Status::OK();
    for (auto& desc : desc_list) {
        Status status = addMemoryBuffer(desc, options);
        if (!status.ok() && first.ok()) first = status;
    }
    return first;
}

Status FabricTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::shared_ptr<FabricLocalBuffer> buffer;
    {
        std::unique_lock<std::shared_mutex> guard(buffers_mutex_);
        auto it = buffers_.find(desc.addr);
        if (it == buffers_.end()) return Status::OK();
        buffer = std::move(it->second);
        buffers_.erase(it);
        // In-flight tasks may still hold the buffer; its MRs close when the
        // last one finishes, or at uninstall.
        retired_.erase(
            std::remove_if(retired_.begin(), retired_.end(),
                           [](const std::weak_ptr<FabricLocalBuffer>& weak) {
                               return weak.expired();
                           }),
            retired_.end());
        retired_.push_back(buffer);
    }
    desc.transport_attrs.erase(TransportType::FABRIC);
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::FABRIC),
        desc.transports.end());
    // The provider may still read or write the buffer for tasks that have
    // not completed, so the caller must not reuse it before they do.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(params_.quiesce_timeout_ms);
    while (uint64_t active = buffer->active.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return Status::InternalError("Fabric buffer unregistered with " +
                                         std::to_string(active) +
                                         " transfers still in flight" LOC_MARK);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return Status::OK();
}

bool FabricTransport::tracksLocalBuffer(const BufferDesc& desc) const {
    std::shared_lock<std::shared_mutex> guard(buffers_mutex_);
    return buffers_.count(desc.addr) > 0;
}

}  // namespace tent
}  // namespace mooncake
