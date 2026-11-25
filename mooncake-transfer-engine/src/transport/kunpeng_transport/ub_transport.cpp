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

#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <future>
#include <unordered_set>
#include <utility>
#include "buffer_range_index.h"
#include "config.h"
#include "memory_location.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transport/kunpeng_transport/ub_endpoint.h"
#include "transport/kunpeng_transport/urma/urma_endpoint.h"

namespace mooncake {
UbTransport::UbTransport(UB_ENDPOINT_TYPE endpoint_type)
    : endpoint_type_(endpoint_type) {
    const char* mode = getenv("MC_UB_TRANS_MODE");
    rm_mode_ = (mode != nullptr &&
                (strcmp(mode, "rm") == 0 || strcmp(mode, "RM") == 0 ||
                 strcmp(mode, "1") == 0));
    if (rm_mode_) {
        LOG(INFO) << "UbTransport: using RM (Reliable Message) mode "
                     "(UBoE compatible)";
    }
}

UbTransport::~UbTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    // Contexts hold URMA resources and need the module alive; release them
    // before pairing the urma_init() done in init().
    context_list_.clear();
    uninit(this);
}

int UbTransport::install(std::string& local_server_name,
                         std::shared_ptr<TransferMetadata> meta,
                         std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "UbTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }
    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;
    auto ret = initializeUbResources(this);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot initialize Ub resources";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: initialize Ub resources done";

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "Transfer engine allocate local segment done";

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot start handshake daemon";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: start handshake daemon done";

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot publish segments";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: publish segments done";

    return 0;
}

int UbTransport::registerLocalMemory(void* addr, size_t length,
                                     const std::string& name,
                                     bool remote_accessible,
                                     bool update_metadata) {
    (void)remote_accessible;

    // Mooncake#3503: UrmaContext previously clamped registrations to
    // max_seg_size while publishing the full BufferDesc.length. Split buffers
    // larger than max_seg_size into <= max_seg_size chunks, register each as
    // its own URMA segment, and publish one BufferDesc per chunk.
    size_t chunk_limit = static_cast<size_t>(globalConfig().max_seg_size);
    // Only buffers that actually need splitting pay for the smaps lookup;
    // chunk seams of HugeTLB-backed buffers must land on huge page boundaries.
    if (chunk_limit > 0 && length > chunk_limit) {
        const size_t buffer_page_size = detectBufferPageSize(addr);
        static const size_t base_page_size =
            static_cast<size_t>(sysconf(_SC_PAGESIZE));
        if (buffer_page_size > base_page_size) {
            const size_t aligned_limit =
                chunk_limit / buffer_page_size * buffer_page_size;
            if (aligned_limit == 0) {
                LOG(ERROR) << "UbTransport: Effective max_seg_size "
                           << chunk_limit
                           << " is smaller than the HugeTLB page size "
                           << buffer_page_size
                           << "; cannot form a page-aligned URMA segment chunk";
                return ERR_INVALID_ARGUMENT;
            }
            chunk_limit = aligned_limit;
        }
    }

    std::vector<std::pair<void*, size_t>> chunks;
    if (chunk_limit > 0 && length > chunk_limit) {
        for (size_t offset = 0; offset < length;) {
            size_t chunk_len = std::min(chunk_limit, length - offset);
            chunks.emplace_back(static_cast<char*>(addr) + offset, chunk_len);
            offset += chunk_len;
        }
        LOG(WARNING) << "UbTransport: Auto-splitting buffer " << addr << " ("
                     << length << " bytes) into " << chunks.size()
                     << " chunks of <= " << chunk_limit
                     << " bytes each (max_seg_size; Mooncake#3503)";
    } else {
        chunks.emplace_back(addr, length);
    }

    // Resolve wildcard location after chunk 0 pins/faults the buffer (mirrors
    // RdmaTransport::registerLocalMemoryInternal).
    std::string resolved_name =
        (name == kWildcardLocation) ? std::string() : name;

    auto unregisterChunkMRs = [&](void* chunk_addr, size_t ctx_count) {
        const size_t limit = std::min(ctx_count, context_list_.size());
        for (size_t i = 0; i < limit; ++i) {
            int ret = context_list_[i]->unregisterMemoryRegion(
                reinterpret_cast<uint64_t>(chunk_addr));
            if (ret) {
                LOG(WARNING) << "UbTransport rollback: failed to unregister "
                                "chunk segment at "
                             << chunk_addr << " (ret=" << ret << ")";
            }
        }
    };

    auto rollbackChunks = [&](size_t committed) {
        size_t n = std::min(committed, chunks.size());
        for (size_t ri = 0; ri < n; ++ri) {
            int rc = metadata_->removeLocalMemoryBuffer(
                chunks[ri].first, /*update_metadata=*/false);
            if (rc) {
                LOG(WARNING) << "UbTransport rollback: failed to remove "
                                "metadata for chunk at "
                             << chunks[ri].first << " (ret=" << rc << ")";
            }
            unregisterChunkMRs(chunks[ri].first, context_list_.size());
        }
    };

    for (size_t ci = 0; ci < chunks.size(); ++ci) {
        void* chunk_addr = chunks[ci].first;
        size_t chunk_len = chunks[ci].second;

        BufferDesc buffer_desc;
        size_t registered_ctxs = 0;
        for (size_t ctx_idx = 0; ctx_idx < context_list_.size(); ++ctx_idx) {
            auto& context = context_list_[ctx_idx];
            int ret = context->registerMemoryRegion(
                reinterpret_cast<uint64_t>(chunk_addr), chunk_len);
            if (ret) {
                LOG(ERROR) << "UbTransport: cannot register LocalMemory (chunk "
                           << ci << ")";
                unregisterChunkMRs(chunk_addr, registered_ctxs);
                rollbackChunks(ci);
                return ret;
            }
            ++registered_ctxs;
            ret = context->buildLocalBufferDesc(
                reinterpret_cast<uint64_t>(chunk_addr), buffer_desc);
            if (ret) {
                LOG(ERROR) << "UbTransport: build buffer description failed "
                              "(chunk "
                           << ci << ")";
                unregisterChunkMRs(chunk_addr, registered_ctxs);
                rollbackChunks(ci);
                return ret;
            }
        }

        if (ci == 0 && name == kWildcardLocation) {
            bool only_first_page = true;
            const std::vector<MemoryLocationEntry> entries =
                getMemoryLocation(addr, length, only_first_page);
            if (entries.empty()) {
                unregisterChunkMRs(chunk_addr, context_list_.size());
                return -1;
            }
            resolved_name = entries[0].location;
        }

        buffer_desc.name = resolved_name;
        buffer_desc.addr = reinterpret_cast<uint64_t>(chunk_addr);
        buffer_desc.length = chunk_len;
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc,
                                                 /*update_metadata=*/false);
        if (rc) {
            unregisterChunkMRs(chunk_addr, context_list_.size());
            rollbackChunks(ci);
            return rc;
        }
    }

    if (update_metadata) {
        int rc = metadata_->updateLocalSegmentDesc();
        if (rc) {
            rollbackChunks(chunks.size());
            return rc;
        }
    }

    if (chunks.size() > 1) {
        std::lock_guard<std::mutex> lock(chunk_map_mutex_);
        std::vector<uint64_t> chunk_addrs;
        chunk_addrs.reserve(chunks.size());
        for (const auto& c : chunks) {
            chunk_addrs.push_back(reinterpret_cast<uint64_t>(c.first));
        }
        chunk_map_[reinterpret_cast<uint64_t>(addr)] = std::move(chunk_addrs);
    }

    return 0;
}

int UbTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    bool found_in_chunk_map = false;
    std::vector<uint64_t> chunk_addrs;
    {
        std::lock_guard<std::mutex> lock(chunk_map_mutex_);
        auto it = chunk_map_.find(reinterpret_cast<uint64_t>(addr));
        if (it != chunk_map_.end()) {
            found_in_chunk_map = true;
            chunk_addrs = it->second;
        }
    }
    if (found_in_chunk_map) {
        int first_err = 0;
        std::vector<uint64_t> remaining_chunks;
        for (uint64_t ca : chunk_addrs) {
            int rc = metadata_->removeLocalMemoryBuffer(
                reinterpret_cast<void*>(ca), /*update_metadata=*/false);
            // On retry after a partial MR/publish failure, already-removed
            // chunks return ERR_ADDRESS_NOT_REGISTERED; keep going so any
            // surviving URMA segments can still be unregistered.
            if (rc && rc != ERR_ADDRESS_NOT_REGISTERED && !first_err) {
                first_err = rc;
            }
        }
        for (uint64_t ca : chunk_addrs) {
            bool chunk_failed = false;
            for (auto& context : context_list_) {
                int ret = context->unregisterMemoryRegion(ca);
                if (ret) {
                    LOG(ERROR) << "UbTransport: failed to unregister chunk "
                                  "segment at "
                               << reinterpret_cast<void*>(ca);
                    chunk_failed = true;
                    if (!first_err) first_err = ret;
                }
            }
            if (chunk_failed) {
                remaining_chunks.push_back(ca);
            }
        }
        int pub_err = 0;
        if (update_metadata) {
            pub_err = metadata_->updateLocalSegmentDesc();
            if (pub_err && !first_err) first_err = pub_err;
        }
        {
            std::lock_guard<std::mutex> lock(chunk_map_mutex_);
            auto it = chunk_map_.find(reinterpret_cast<uint64_t>(addr));
            if (it != chunk_map_.end()) {
                if (!remaining_chunks.empty()) {
                    it->second = std::move(remaining_chunks);
                } else if (pub_err) {
                    // All MRs are gone; keep an empty list so a retry only
                    // republishes metadata without re-touching MRs.
                    it->second.clear();
                } else {
                    chunk_map_.erase(it);
                }
            }
        }
        return first_err;
    }

    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;
    int first_err = 0;
    for (auto& context : context_list_) {
        int ret = context->unregisterMemoryRegion((uint64_t)addr);
        if (ret && !first_err) first_err = ret;
    }
    if (first_err) {
        // The BufferDesc is already gone, so a plain retry would stop at
        // removeLocalMemoryBuffer() and leak the surviving URMA segment.
        // Track it as a single-chunk entry so the retry goes through the
        // chunked path above, which tolerates the missing metadata.
        std::lock_guard<std::mutex> lock(chunk_map_mutex_);
        chunk_map_[reinterpret_cast<uint64_t>(addr)] = {
            reinterpret_cast<uint64_t>(addr)};
    }
    return first_err;
}

int UbTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    std::vector<std::future<int>> results;
    results.reserve(buffer_list.size());
    for (auto& buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemory(buffer.addr, buffer.length, location,
                                           true, false);
            }));
    }

    int first_error = 0;
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        int ret = results[i].get();
        if (ret) {
            LOG(WARNING) << "UbTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
            if (!first_error) first_error = ret;
        }
    }
    if (first_error) return first_error;

    return metadata_->updateLocalSegmentDesc();
}

int UbTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    std::vector<std::future<int>> results;
    results.reserve(addr_list.size());
    for (auto& addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemory(addr, false);
            }));
    }

    int first_error = 0;
    for (size_t i = 0; i < addr_list.size(); ++i) {
        int ret = results[i].get();
        if (ret) {
            LOG(WARNING) << "UbTransport: Failed to unregister memory: addr "
                         << addr_list[i];
            if (!first_error) first_error = ret;
        }
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

Status UbTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UbTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "UbTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask*> task_list;
    task_list.reserve(entries.size());
    for (auto& request : entries) {
        auto& task = batch_desc.task_list[task_id];
        ++task_id;
        task.batch_id = batch_id;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<TransferRequest*>(&request);
#else
        task.request = &request;
#endif
        task_list.push_back(&task);
    }
    return submitTransferTask(task_list);
}

static const BufferRangeIndex* ubUniqueCoverageIndex(
    const TransferMetadata::SegmentDesc* desc) {
    if (!desc) return nullptr;
    const BufferRangeIndex& index = desc->buffer_range_index;
    if (index.size() != desc->buffers.size() || index.overlaps())
        return nullptr;
    return &index;
}

// Remaining bytes from `address` to the end of the registered UB chunk that
// covers it, or 0 if none does. `last_buffer_idx` caches the previous hit so
// multi-slice requests across a seam avoid rescanning `buffers` per slice.
static uint64_t ubBytesUntilBufferEnd(
    const TransferMetadata::SegmentDesc* segment_desc, uint64_t address,
    bool require_remote_seg, int* last_buffer_idx = nullptr) {
    if (!segment_desc || segment_desc->buffers.empty()) return 0;
    const auto& buffers = segment_desc->buffers;

    auto is_valid_buffer = [&](int i) {
        if (i < 0 || static_cast<size_t>(i) >= buffers.size()) return false;
        const auto& buffer = buffers[static_cast<size_t>(i)];
        if (buffer.tseg.empty()) return false;
        if (!require_remote_seg && buffer.l_seg_index.empty()) return false;
        return bufferCoversPoint(buffer.addr, buffer.length, address);
    };

    auto remaining_in = [&](int i) {
        const auto& buffer = buffers[static_cast<size_t>(i)];
        return buffer.length - (address - buffer.addr);
    };

    if (const BufferRangeIndex* index = ubUniqueCoverageIndex(segment_desc)) {
        if (last_buffer_idx && is_valid_buffer(*last_buffer_idx))
            return remaining_in(*last_buffer_idx);
        const int found = index->findCovering(address, 0);
        if (!is_valid_buffer(found)) return 0;
        if (last_buffer_idx) *last_buffer_idx = found;
        return remaining_in(found);
    }

    uint64_t max_remaining = 0;
    for (int i = 0; i < static_cast<int>(buffers.size()); ++i) {
        if (!is_valid_buffer(i)) continue;
        const uint64_t remaining = remaining_in(i);
        if (remaining > max_remaining) {
            max_remaining = remaining;
            if (last_buffer_idx) *last_buffer_idx = i;
        }
    }
    return max_remaining;
}

// Calculate slice length capped at slice_size (with final-fragment merge) AND
// both source and target BufferDesc boundaries so a single URMA WR never
// straddles two registered segments on either side.
struct UbSliceLengthCalculator {
    const Transport::TransferRequest& request;
    size_t block_size;
    size_t fragment_size;
    const TransferMetadata::SegmentDesc* local_desc;
    const TransferMetadata::SegmentDesc* target_desc;
    int* src_buffer_idx;
    int* tgt_buffer_idx;

    template <typename OnTargetMiss>
    inline size_t calculate(uint64_t offset, OnTargetMiss&& on_target_miss) {
        const size_t remaining = request.length - offset;
        size_t slice_length =
            remaining <= block_size + fragment_size ? remaining : block_size;

        const uint64_t src_rem = ubBytesUntilBufferEnd(
            local_desc, reinterpret_cast<uint64_t>(request.source) + offset,
            /*require_remote_seg=*/false, src_buffer_idx);
        if (src_rem > 0)
            slice_length = std::min<uint64_t>(slice_length, src_rem);

        uint64_t tgt_rem =
            ubBytesUntilBufferEnd(target_desc, request.target_offset + offset,
                                  /*require_remote_seg=*/true, tgt_buffer_idx);
        if (tgt_rem == 0 && on_target_miss()) {
            tgt_rem = ubBytesUntilBufferEnd(
                target_desc, request.target_offset + offset,
                /*require_remote_seg=*/true, tgt_buffer_idx);
        }
        if (tgt_rem > 0)
            slice_length = std::min<uint64_t>(slice_length, tgt_rem);

        return slice_length;
    }
};

Status UbTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::unordered_map<std::shared_ptr<UbContext>, std::vector<Slice*>>
        slices_to_post;
    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        target_segment_descs;
    std::unordered_set<SegmentID> refreshed_targets;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit;
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;
    uint64_t nr_slices = 0;

    auto fail_unposted_slices = [&]() {
        for (auto& entry : slices_to_post)
            for (auto* slice : entry.second) slice->markFailed();
        slices_to_post.clear();
    };

    int last_local_buffer_id = -1;
    int last_remote_buffer_id = -1;
    SegmentID last_remote_target_id = static_cast<SegmentID>(-1);

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        if (request.length == 0) continue;

        auto target_desc_it = target_segment_descs.find(request.target_id);
        if (target_desc_it == target_segment_descs.end()) {
            target_desc_it =
                target_segment_descs
                    .emplace(request.target_id,
                             metadata_->getSegmentDescByID(request.target_id))
                    .first;
        }
        const auto& target_segment_desc = target_desc_it->second;
        if (request.target_id != last_remote_target_id) {
            last_remote_buffer_id = -1;
            last_remote_target_id = request.target_id;
        }

        auto request_buffer_id = -1, request_device_id = -1;
        if (request.advise_retry_cnt == 0 &&
            selectDevice(local_segment_desc.get(), (uint64_t)request.source,
                         request.length, request_buffer_id,
                         request_device_id)) {
            request_buffer_id = -1;
            request_device_id = -1;
        }
        if (request_device_id >= 0 &&
            (static_cast<size_t>(request_device_id) >= context_list_.size() ||
             !context_list_[request_device_id] ||
             !context_list_[request_device_id]->active() ||
             request_buffer_id < 0 ||
             static_cast<size_t>(request_buffer_id) >=
                 local_segment_desc->buffers.size() ||
             static_cast<size_t>(request_device_id) >=
                 local_segment_desc->buffers[request_buffer_id].tseg.size() ||
             static_cast<size_t>(request_device_id) >=
                 local_segment_desc->buffers[request_buffer_id]
                     .l_seg_index.size())) {
            request_buffer_id = -1;
            request_device_id = -1;
        }

        UbSliceLengthCalculator slice_calc{request,
                                           kBlockSize,
                                           kFragmentSize,
                                           local_segment_desc.get(),
                                           target_segment_desc.get(),
                                           &last_local_buffer_id,
                                           &last_remote_buffer_id};
        auto on_target_miss = [&]() -> bool {
            if (request.target_id == LOCAL_SEGMENT_ID ||
                refreshed_targets.count(request.target_id)) {
                return false;
            }
            refreshed_targets.insert(request.target_id);
            auto fresh = metadata_->getSegmentDescByID(request.target_id,
                                                       /*force_update=*/true);
            if (!fresh) return false;
            target_desc_it->second = std::move(fresh);
            slice_calc.target_desc = target_desc_it->second.get();
            last_remote_buffer_id = -1;
            return true;
        };

        for (uint64_t offset = 0; offset < request.length;) {
            size_t slice_length = slice_calc.calculate(offset, on_target_miss);
            if (slice_length == 0) slice_length = request.length - offset;

            char* source_addr = static_cast<char*>(request.source) + offset;

            int buffer_id = request_buffer_id;
            int device_id = request_device_id;
            int retry_cnt = request.advise_retry_cnt;
            bool found_device = (buffer_id >= 0 && device_id >= 0);
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 reinterpret_cast<uint64_t>(source_addr),
                                 slice_length, buffer_id, device_id,
                                 retry_cnt++))
                    continue;
                if (device_id < 0 ||
                    static_cast<size_t>(device_id) >= context_list_.size())
                    continue;
                auto& context = context_list_[device_id];
                if (!context || !context->active()) continue;
                if (buffer_id < 0 || static_cast<size_t>(buffer_id) >=
                                         local_segment_desc->buffers.size())
                    continue;
                const auto& local_buf = local_segment_desc->buffers[buffer_id];
                if (static_cast<size_t>(device_id) >= local_buf.tseg.size() ||
                    static_cast<size_t>(device_id) >=
                        local_buf.l_seg_index.size())
                    continue;
                found_device = true;
                break;
            }
            if (!found_device) {
                // Do not deallocate slices already queued in slices_to_post
                // here: every slice is also recorded in its owning
                // TransferTask::slice_list right after allocation, and
                // ~TransferTask() returns everything in slice_list to the
                // cache exactly once. Mark unposted slices failed so waits and
                // batch teardown do not observe stuck PENDING slices.
                fail_unposted_slices();
                LOG(ERROR)
                    << "UbTransport: Address not registered by any device(s) "
                    << static_cast<void*>(source_addr);
                return Status::AddressNotRegistered(
                    "UbTransport: not registered by any device(s), "
                    "address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }

            auto& context = context_list_[device_id];
            auto local_tseg_index =
                local_segment_desc->buffers[buffer_id].l_seg_index[device_id];
            void* l_seg = context->localSegWithIndex(local_tseg_index);
            if (!l_seg) {
                fail_unposted_slices();
                LOG(ERROR) << "UbTransport: Local segment no longer registered "
                              "for address "
                           << static_cast<void*>(source_addr);
                return Status::AddressNotRegistered(
                    "UbTransport: local segment unregistered, address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }

            Slice* slice = getSliceCache().allocate();
            assert(slice);
            if (!slice->from_cache) {
                nr_slices++;
            }
            slice->source_addr = source_addr;
            slice->length = slice_length;
            slice->opcode = request.opcode;
            slice->ub.dest_addr = request.target_offset + offset;
            slice->ub.retry_cnt = 0;
            slice->ub.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->ts = 0;
            slice->status = Slice::PENDING;
            task.slice_list.push_back(slice);

            slice->ub.l_seg = l_seg;
            slices_to_post[context].push_back(slice);
            task.total_bytes += slice->length;
            __sync_fetch_and_add(&task.slice_count, 1);
            if (nr_slices >= kSubmitWatermark) {
                for (auto& entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);
                slices_to_post.clear();
                nr_slices = 0;
            }

            offset += slice_length;
        }
    }
    for (auto& entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status UbTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                      TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UbTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    // Completion counters publish the preceding byte updates.
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count)
            status.s = FAILED;
        else
            status.s = COMPLETED;
        task.is_finished = true;
    } else {
        status.s = WAITING;
    }
    return Status::OK();
}

Transport::SegmentID UbTransport::getSegmentID(
    const std::string& segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int UbTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ub";
    for (auto& context : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = context->deviceName();
        device_desc.eid = context->getEid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_);
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int UbTransport::onSetupConnections(const HandShakeDesc& peer_desc,
                                    HandShakeDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<UbContext> context;
    int index = 0;
    for (auto& entry : local_topology_->getHcaList()) {
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

int UbTransport::startHandshakeDaemon(std::string& local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&UbTransport::onSetupConnections, this, std::placeholders::_1,
                  std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              int& buffer_id, int& device_id, int retry_cnt) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_cnt);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              std::string_view hint, int& buffer_id,
                              int& device_id, int retry_cnt) {
    if (desc == nullptr) {
        LOG(ERROR) << "UbTransport Get Segment Desc failed";
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    const auto& buffers = desc->buffers;
    auto try_buffer = [&](int candidate) -> bool {
        if (candidate < 0 || static_cast<size_t>(candidate) >= buffers.size())
            return false;
        const auto& buffer = buffers[static_cast<size_t>(candidate)];
        if (buffer.tseg.empty() ||
            !bufferCoversRange(buffer.addr, buffer.length, offset, length)) {
            return false;
        }
        int selected =
            hint.empty()
                ? desc->topology.selectDevice(buffer.name, retry_cnt)
                : desc->topology.selectDevice(buffer.name, hint, retry_cnt);
        if (selected < 0) {
            selected = hint.empty() ? desc->topology.selectDevice(
                                          kWildcardLocation, retry_cnt)
                                    : desc->topology.selectDevice(
                                          kWildcardLocation, hint, retry_cnt);
        }
        if (selected < 0) return false;
        buffer_id = candidate;
        device_id = selected;
        return true;
    };

    if (const BufferRangeIndex* index = ubUniqueCoverageIndex(desc)) {
        const int found = index->findCovering(offset, length);
        return try_buffer(found) ? 0 : ERR_ADDRESS_NOT_REGISTERED;
    }

    for (int candidate = 0; candidate < static_cast<int>(buffers.size());
         ++candidate) {
        if (try_buffer(candidate)) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int UbTransport::initializeUbResources(UbTransport* t) {
    auto ret = init(t);
    if (ret != 0) {
        LOG(ERROR) << "Failed to init, ret = " << ret;
        return -1;
    }

    std::vector<std::string> hca_list;
    // Try to get device list from topology
    if (t->local_topology_) {
        hca_list = t->local_topology_->getHcaList();
    }

    // If no devices from topology, use mock device
    if (hca_list.empty()) {
        hca_list.push_back("mock_urma_device");
        LOG(INFO) << "Using mock_urma_device for testing";
    }

    for (auto& device_name : hca_list) {
        auto& config = globalConfig();
        auto max_endpoints = config.max_ep_per_ctx;
        auto context = buildContext(t, device_name, max_endpoints);
        ret = context->doConstruct(config);
        if (ret) {
            if (t->local_topology_) {
                t->local_topology_->disableDevice(device_name);
            }
            LOG(WARNING) << "Disable device " << device_name;
        } else {
            t->context_list_.push_back(context);
            LOG(INFO) << "device " << context->deviceName() << " add to list";
        }
    }

    if (t->context_list_.empty()) {
        LOG(ERROR) << "UbTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }

    LOG(INFO) << "ub resources init success";
    return 0;
}

int UbTransport::init(UbTransport* transport) {
    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        if (!UrmaContext::init()) {
            LOG(ERROR) << "UrmaContext init failed";
            return -1;
        }
        transport->urma_initialized_ = true;
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return -1;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
        return -1;
    }
    return 0;
}

void UbTransport::uninit(UbTransport* transport) {
    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        // Idempotent: install() failure paths and the destructor both land
        // here, while urma_init() happens exactly once per transport.
        if (!transport->urma_initialized_) return;
        transport->urma_initialized_ = false;
        if (!UrmaContext::uninit()) {
            LOG(ERROR) << "UrmaContext uninit failed";
        }
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
    }
}

std::shared_ptr<UbContext> UbTransport::buildContext(
    UbTransport* t, const std::string& device_name, int max_endpoints) {
    if (t->endpoint_type_ == URMA_ENDPOINT) {
        auto context =
            std::make_shared<UrmaContext>(*t, device_name, max_endpoints);
        if (!context) {
            LOG(ERROR) << "UrmaContext build failed";
            return nullptr;
        }
        return context;
    } else if (t->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return nullptr;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << t->endpoint_type_;
        return nullptr;
    }
}
}  // namespace mooncake
