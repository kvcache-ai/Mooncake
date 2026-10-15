// Shutdown handoff: export of a quiesced master and restore into a new one.
//
// Restore outline (before the master serves):
//   0. Read and check the snapshot (handoff::ReadSnapshot). Then, under
//      client_mutex_ and master_state_mutex_ held exclusively, mount every
//      restored memory segment with its original identity and owner, an
//      empty real OffsetBufferAllocator, allocation disabled, and its
//      endpoint in the read gate (invalid_replica_endpoints_).
//   1. Read the object chunks in parallel and install the objects with
//      placeholder buffers (InstallRestoredObjects), remembering the range
//      and replica of every memory replica per segment.
//   2. Per segment, in parallel: sort and check the ranges together with the
//      quarantined ones (CheckRestoredRanges), import them into the real
//      allocator and swap each placeholder buffer for the imported one,
//      bound to the segment registration.
//   3. Finalize: replica id floor, weight metadata, tenant quota, metrics,
//      quarantine deadlines, fresh liveness timestamps.
// A remount by the owner with the same segment identity then only opens the
// segment (ConfirmPendingRestoredSegmentsLocked); a mismatch drops it
// (FindHandoffMismatchesLocked / DropPendingRestoredSegmentLocked).
//
// Only memory replicas are carried. LOCAL_DISK, DISK, NoF and DFS replicas
// are left out; an object without a memory replica is not exported.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "handoff/handoff_format.h"
#include "master_service.h"

namespace mooncake {

namespace {

using SteadyClock = std::chrono::steady_clock;

uint64_t NsSince(SteadyClock::time_point start) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            SteadyClock::now() - start)
            .count());
}

uint64_t ProcStatusKiB(const char* field) {
    std::ifstream f("/proc/self/status");
    std::string line;
    const std::string prefix = std::string(field) + ":";
    while (std::getline(f, line)) {
        if (line.rfind(prefix, 0) == 0) {
            return std::strtoull(line.c_str() + prefix.size(), nullptr, 10);
        }
    }
    return 0;
}

}  // namespace

std::string MasterService::HandoffRestoreStats::ToJson() const {
    std::ostringstream o;
    o << "{\"objects\":" << objects << ",\"replicas\":" << replicas
      << ",\"segments\":" << segments << ",\"chunks\":" << chunks
      << ",\"quarantine_ranges\":" << quarantine_ranges
      << ",\"workers\":" << workers << ",\"manifest_ns\":" << manifest_ns
      << ",\"mount_ns\":" << mount_ns
      << ",\"insert_wall_ns\":" << insert_wall_ns
      << ",\"rebuild_wall_ns\":" << rebuild_wall_ns
      << ",\"finalize_ns\":" << finalize_ns << ",\"total_ns\":" << total_ns
      << ",\"rss_kib_start\":" << rss_kib_start
      << ",\"rss_kib_end\":" << rss_kib_end
      << ",\"hwm_kib_end\":" << hwm_kib_end << "}";
    return o.str();
}

// ------------------------------------------------------------------ export

std::vector<handoff::HandoffSegment> MasterService::ExportHandoffSegments() {
    std::vector<handoff::HandoffSegment> out;
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> all;
    segment_access.GetAllSegments(all);
    for (const auto& [segment, owner] : all) {
        // DRAINING and DRAINED segments are exported too: their owner lists
        // them in its next remount, so leaving them out would remount them
        // with an empty allocator, allocatable at once, while a late write
        // may still land in their in-flight ranges. GRACEFULLY_UNMOUNTING
        // segments are left out on purpose: their owner has already removed
        // them from its own list, so it never remounts them. UNMOUNTING ones
        // are on their way out.
        SegmentStatus status = SegmentStatus::UNDEFINED;
        if (segment_access.GetSegmentStatusById(segment.id, status) !=
                ErrorCode::OK ||
            (status != SegmentStatus::OK && status != SegmentStatus::DRAINING &&
             status != SegmentStatus::DRAINED)) {
            continue;
        }
        if (owner == UUID{0, 0} || segment.protocol == "cxl" ||
            segment.base == 0 || segment.size == 0 ||
            !segment_access.GetAllocator(segment.id)) {
            continue;
        }
        out.push_back(
            handoff::HandoffSegment::FromSegment(segment, owner, status));
    }
    return out;
}

void MasterService::ExportHandoffShard(
    size_t shard_idx, const handoff::SegmentLocator& locator,
    std::vector<StandbyObjectEntry>& objects,
    std::vector<handoff::QuarantineRange>& quarantine,
    handoff::WriterStats& stats) const {
    MetadataShardAccessorRO shard(this, shard_idx);
    size_t count = 0;
    for (const auto& [tenant_id, tenant_state] : shard->tenants) {
        (void)tenant_id;
        count += tenant_state.metadata.size();
    }
    objects.reserve(objects.size() + count);
    for (const auto& [tenant_id, tenant_state] : shard->tenants) {
        for (const auto& [key, record] : tenant_state.metadata) {
            const auto& metadata = record.metadata;
            // An in-flight put (PutStart without PutEnd) is never exported;
            // its ranges are quarantined below.
            const bool processing = record.state.is_processing;
            StandbyObjectEntry entry;
            for (const auto& replica : metadata.GetAllReplicas()) {
                if (!replica.is_memory_replica()) {
                    // LOCAL_DISK, DISK, NoF and DFS replicas are not carried.
                    ++stats.dropped_non_memory_replicas;
                    continue;
                }
                if (replica.has_invalid_mem_handle()) {
                    // Its segment is gone; the range may already belong to a
                    // newer mount, so it is neither exported nor quarantined.
                    ++stats.dropped_unknown_segment_replicas;
                    continue;
                }
                auto descriptor = replica.get_descriptor();
                const auto& buffer =
                    descriptor.get_memory_descriptor().buffer_descriptor;
                const auto segment_index =
                    locator.Find(buffer.transport_endpoint_,
                                 buffer.buffer_address_, buffer.size_);
                if (!segment_index) {
                    ++stats.dropped_unknown_segment_replicas;
                    continue;
                }
                if (processing || !replica.is_completed() ||
                    buffer.size_ != metadata.TotalLength()) {
                    // The remaining time of an in-flight write is unknown:
                    // hold the range for the full release timeout.
                    quarantine.push_back({*segment_index,
                                          buffer.buffer_address_, buffer.size_,
                                          HandoffFullHoldMs()});
                    ++stats.dropped_incomplete_replicas;
                    ++stats.quarantine_ranges;
                    stats.quarantine_bytes += buffer.size_;
                    continue;
                }
                entry.metadata.replicas.push_back(std::move(descriptor));
            }
            if (entry.metadata.replicas.empty()) {
                if (processing) {
                    ++stats.skipped_processing;
                } else {
                    ++stats.skipped_no_exportable_replica;
                }
                continue;
            }
            entry.tenant_id = tenant_id.value();
            entry.key = key;
            entry.metadata.client_id = metadata.client_id;
            entry.metadata.size = metadata.TotalLength();
            entry.metadata.group_id = metadata.group_id;
            entry.metadata.data_type = metadata.data_type;
            entry.metadata.hard_pinned = metadata.IsHardPinned();
            entry.metadata.session_id = metadata.session_id;
            ++stats.objects;
            stats.replicas += entry.metadata.replicas.size();
            objects.push_back(std::move(entry));
        }
    }
}

void MasterService::ExportHandoffPendingQuarantine(
    const std::vector<handoff::HandoffSegment>& segments,
    std::vector<handoff::QuarantineRange>& quarantine,
    handoff::WriterStats& stats) const {
    std::unordered_map<UUID, uint32_t, boost::hash<UUID>> index_by_id;
    for (uint32_t i = 0; i < segments.size(); ++i) {
        index_by_id.emplace(*segments[i].info.segment_id, i);
    }
    const auto now = SteadyClock::now();
    std::lock_guard<std::mutex> lock(handoff_quarantine_.mutex);
    for (const auto& held : handoff_quarantine_.buffers) {
        const auto it = index_by_id.find(held.segment_id);
        // A dropped segment's ranges may belong to a newer mount already.
        if (it == index_by_id.end() || !held.buffer ||
            !held.buffer->isAllocatorValid() || held.deadline <= now) {
            continue;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                held.deadline - now)
                .count();
        quarantine.push_back(
            {it->second, reinterpret_cast<uint64_t>(held.buffer->data()),
             held.buffer->size(), static_cast<uint64_t>(remaining) + 1});
        ++stats.quarantine_ranges;
        stats.quarantine_bytes += held.buffer->size();
    }
}

void MasterService::ExportHandoffDiscardedQuarantine(
    const handoff::SegmentLocator& locator,
    std::vector<handoff::QuarantineRange>& quarantine,
    handoff::WriterStats& stats) {
    const auto now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> lock(discarded_replicas_mutex_);
    for (const auto& item : discarded_replicas_) {
        if (item.isExpired(now)) {
            continue;  // this master would release it at its next sweep
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(item.ttl() -
                                                                  now)
                .count();
        for (const auto& replica : item.replicas()) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            if (replica.has_invalid_mem_handle()) {
                // Its segment was unmounted; the range may already belong to
                // a newer mount (same rule as for live replicas).
                ++stats.dropped_discarded_ranges;
                continue;
            }
            const auto descriptor = replica.get_descriptor();
            const auto& buffer =
                descriptor.get_memory_descriptor().buffer_descriptor;
            const auto segment_index =
                locator.Find(buffer.transport_endpoint_, buffer.buffer_address_,
                             buffer.size_);
            if (!segment_index) {
                // On a segment that is not exported (e.g. gracefully
                // unmounting): its owner never remounts it, so the range is
                // never allocated by the next master.
                ++stats.dropped_discarded_ranges;
                continue;
            }
            quarantine.push_back({*segment_index, buffer.buffer_address_,
                                  buffer.size_,
                                  static_cast<uint64_t>(remaining) + 1});
            ++stats.discarded_ranges;
            ++stats.quarantine_ranges;
            stats.quarantine_bytes += buffer.size_;
        }
    }
}

WeightMetadataSnapshot MasterService::ExportHandoffWeightMetadata() const {
    return weight_manager_.ExportSnapshot();
}

uint64_t MasterService::HandoffObjectCount() const {
    uint64_t count = 0;
    for (size_t s = 0; s < kNumShards; ++s) {
        MetadataShardAccessorRO shard(this, s);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            (void)tenant_id;
            count += tenant_state.metadata.size();
        }
    }
    return count;
}

// ----------------------------------------------------------------- restore

tl::expected<MasterService::HandoffRestoreStats, std::string>
MasterService::RestoreFromHandoff(const std::string& dir,
                                  const HandoffRestoreOptions& options) {
    if (!HandoffMasterIsEmpty()) {
        // Never restore into a master that already holds state.
        return tl::make_unexpected("master is not empty");
    }
    auto result = RestoreFromHandoffLocked(dir, options);
    if (!result) {
        LOG(ERROR) << "handoff_restore=failed, dir=" << dir
                   << ", error=" << result.error();
        return result;
    }
    LOG(INFO) << "handoff_restore=ok, dir=" << dir
              << ", stats=" << result->ToJson();
    return result;
}

bool MasterService::HandoffMasterIsEmpty() {
    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> mounted;
        segment_access.GetAllSegments(mounted);
        if (!mounted.empty()) return false;
    }
    if (!client_liveness_records_.empty() ||
        !pending_restored_segments_.empty()) {
        return false;
    }
    for (size_t s = 0; s < kNumShards; ++s) {
        MetadataShardAccessorRO shard(this, s);
        if (!shard->tenants.empty()) return false;
    }
    return true;
}

size_t MasterService::HandoffPendingSegmentCount() const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    return pending_restored_segments_.size();
}

size_t MasterService::HandoffQuarantineBufferCount() const {
    std::lock_guard<std::mutex> lock(handoff_quarantine_.mutex);
    return handoff_quarantine_.buffers.size();
}

tl::expected<MasterService::HandoffRestoreStats, std::string>
MasterService::RestoreFromHandoffLocked(const std::string& dir,
                                        const HandoffRestoreOptions& options) {
    const auto total_start = SteadyClock::now();
    HandoffRestoreStats stats;
    stats.rss_kib_start = ProcStatusKiB("VmRSS");
    const size_t workers = std::max<size_t>(1, options.workers);
    stats.workers = workers;

    // ---- Phase 0a: read and check the snapshot (no locks needed yet).
    auto phase_start = SteadyClock::now();
    if (enable_ha_ || enable_dfs_) {
        return tl::make_unexpected(
            "handoff restore requires a standalone master without DFS");
    }
    if (memory_allocator_type_ != BufferAllocatorType::OFFSET) {
        return tl::make_unexpected(
            "handoff restore supports only the offset allocator");
    }
    auto contents = handoff::ReadSnapshot(dir);
    if (!contents) {
        return tl::make_unexpected(contents.error());
    }
    const auto& manifest = contents->manifest;
    const auto& batch = contents->batch;
    const auto& segments = contents->segments;
    const auto& quarantine_ranges = contents->quarantine;
    if (manifest.multi_tenant != enable_multi_tenants_) {
        return tl::make_unexpected("multi-tenant mode mismatch");
    }
    const auto read_blob = [&dir](const std::string& key, uint64_t bytes,
                                  uint32_t crc32c) {
        return handoff::ReadVerifiedBlob(dir, key, bytes, crc32c);
    };
    const size_t nseg = segments.size();
    const handoff::SegmentLocator locator(segments);
    stats.segments = nseg;
    stats.chunks = batch.object_chunks.size();
    stats.quarantine_ranges = quarantine_ranges.size();
    stats.manifest_ns = NsSince(phase_start);

    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    std::unique_lock<std::shared_mutex> state_lock(master_state_mutex_);

    // ---- Phase 0b: mount restored segments, closed for reads and writes.
    phase_start = SteadyClock::now();
    std::vector<std::shared_ptr<OffsetBufferAllocator>> allocators(nseg);
    std::vector<std::shared_ptr<BufferAllocatorBase>> placeholders(nseg);
    std::vector<std::function<void(AllocatedBuffer&)>> binders(nseg);
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>
        records;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (size_t i = 0; i < nseg; ++i) {
            const auto& restored = segments[i];
            const Segment segment = restored.ToSegment();
            auto& record = records[restored.Owner()];
            if (!record) {
                record = std::make_shared<ClientLivenessRecord>(
                    ClientLivenessRecord::Clock::now());
            }
            allocators[i] = std::make_shared<OffsetBufferAllocator>(
                segment.name, segment.base, segment.size, segment.te_endpoint,
                ReplicaType::MEMORY);
            const auto mounted = segment_access.MountRestoredSegment(
                segment, restored.Owner(), record, allocators[i],
                restored.attributes.status);
            if (mounted != ErrorCode::OK) {
                return tl::make_unexpected("mount of restored segment " +
                                           segment.name +
                                           " failed: " + toString(mounted));
            }
            binders[i] = segment_access.MakeBufferBinder(segment.id);
            if (!binders[i]) {
                return tl::make_unexpected("no registration for segment " +
                                           segment.name);
            }
            placeholders[i] = std::make_shared<DummyBufferAllocator>(
                segment.name, segment.te_endpoint);
            pending_restored_segments_[segment.id] = {restored.Owner(),
                                                      segment};
            invalid_replica_endpoints_.insert(segment.te_endpoint);
            invalid_replica_endpoints_.insert(segment.name);
        }
    }
    for (auto& [client_id, record] : records) {
        client_liveness_records_.emplace(client_id, record);
        MasterMetricManager::instance().client_liveness_record_created();
    }
    stats.mount_ns = NsSince(phase_start);

    // ---- Phase 1: read chunks and install objects with placeholders.
    struct WorkerState {
        std::vector<std::vector<RestoredRange>> ranges;  // per segment
        ReplicaID max_id{0};
        uint64_t objects{0};
        uint64_t replicas{0};
        std::string error;
    };
    std::vector<WorkerState> states(workers);
    for (auto& st : states) {
        st.ranges.resize(nseg);
    }
    // Read leases: the old master granted none after the export started, so
    // a lease it handed out ends by export + kv_lease_ttl (both masters run
    // with the same --default_kv_lease_ttl). Restored objects keep that
    // floor, so a reader still inside its lease is not undercut by an
    // eviction, remove or upsert right after a short roll.
    const auto lease_floor =
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(manifest.created_at_ms)) +
        std::chrono::milliseconds(default_kv_lease_ttl_);
    const auto finish = [&lease_floor](ObjectMetadata& metadata) {
        metadata.lease_->ExtendTo(lease_floor);
        // Every restored object has a completed memory replica.
        metadata.memory_cache_total_accounted = true;
    };
    std::atomic<size_t> next_chunk{0};
    phase_start = SteadyClock::now();
    auto inserted = RunRestoreWorkers(
        workers, [&](size_t w) -> tl::expected<bool, ErrorCode> {
            auto& st = states[w];
            const auto fail = [&st](std::string message) {
                st.error = std::move(message);
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            };
            const size_t chunk_index =
                next_chunk.fetch_add(1, std::memory_order_relaxed);
            if (chunk_index >= batch.object_chunks.size()) {
                return false;
            }
            const auto& chunk = batch.object_chunks[chunk_index];
            auto bytes = read_blob(chunk.key, chunk.stored_size, chunk.crc32c);
            if (!bytes) {
                return fail(bytes.error());
            }
            auto decoded = DecodeBatchOpLogSnapshotObjectChunk(
                *bytes, chunk.chunk_index, chunk.object_count);
            if (!decoded) {
                return fail("chunk " + std::to_string(chunk_index) + ": " +
                            decoded.error());
            }
            std::vector<uint8_t>().swap(*bytes);

            std::vector<RestoredObject> prepared;
            prepared.reserve(decoded->objects.size());
            for (const auto& entry : decoded->objects) {
                auto [scoped_tenant_id, user_key] =
                    TenantId::ParseScopedKey(entry.key);
                TenantId tenant_id(entry.tenant_id);
                if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
                    tenant_id = std::move(scoped_tenant_id);
                }
                if (!tenant_id.IsValid() || user_key.empty()) {
                    return fail("invalid tenant or key");
                }
                const auto& meta = entry.metadata;
                if (meta.replicas.empty()) {
                    return fail("object without replicas");
                }
                RestoredObject object;
                object.entry = &entry;
                object.replicas.reserve(meta.replicas.size());
                for (size_t r = 0; r < meta.replicas.size(); ++r) {
                    const auto& desc = meta.replicas[r];
                    if (desc.id == 0 ||
                        desc.id == std::numeric_limits<ReplicaID>::max()) {
                        return fail("invalid replica id");
                    }
                    for (size_t q = 0; q < r; ++q) {
                        if (meta.replicas[q].id == desc.id) {
                            return fail("duplicate replica id");
                        }
                    }
                    if (desc.status != ReplicaStatus::COMPLETE ||
                        !desc.is_memory_replica()) {
                        return fail("unexpected replica");
                    }
                    const auto& buffer =
                        desc.get_memory_descriptor().buffer_descriptor;
                    if (buffer.size_ != meta.size ||
                        buffer.protocol_ == "cxl") {
                        return fail("invalid memory descriptor");
                    }
                    const auto si =
                        locator.Find(buffer.transport_endpoint_,
                                     buffer.buffer_address_, buffer.size_);
                    if (!si) {
                        return fail(
                            "memory descriptor outside every restored "
                            "segment");
                    }
                    st.max_id = std::max(st.max_id, desc.id);
                    object.replicas.push_back(
                        Replica(desc.id,
                                std::make_unique<AllocatedBuffer>(
                                    placeholders[*si], buffer),
                                desc.status));
                    object.memory_segments.push_back(*si);
                }
                st.replicas += object.replicas.size();
                object.tenant_id = std::move(tenant_id);
                object.user_key = std::move(user_key);
                prepared.push_back(std::move(object));
            }
            auto installed =
                InstallRestoredObjects(prepared, nullptr, st.ranges, finish);
            if (!installed) {
                return fail("duplicate object in chunk " +
                            std::to_string(chunk_index));
            }
            st.objects += prepared.size();
            return true;
        });
    stats.insert_wall_ns = NsSince(phase_start);
    if (!inserted) {
        for (const auto& st : states) {
            if (!st.error.empty()) return tl::make_unexpected(st.error);
        }
        return tl::make_unexpected("object install failed");
    }
    ReplicaID max_live_id = 0;
    std::vector<std::vector<RestoredRange>> ranges(nseg);
    for (auto& st : states) {
        max_live_id = std::max(max_live_id, st.max_id);
        stats.objects += st.objects;
        stats.replicas += st.replicas;
        for (size_t si = 0; si < nseg; ++si) {
            ranges[si].insert(ranges[si].end(), st.ranges[si].begin(),
                              st.ranges[si].end());
        }
        st = {};
    }
    if (stats.objects != manifest.object_count ||
        stats.replicas != manifest.replica_count) {
        return tl::make_unexpected("object or replica count mismatch");
    }
    if (max_live_id != 0 && max_live_id >= manifest.next_replica_id) {
        return tl::make_unexpected("replica id above next_replica_id");
    }

    // ---- Phase 2: per-segment real allocator rebuild.
    phase_start = SteadyClock::now();
    // Quarantined ranges hold no replica; they are imported like live ones
    // and kept as allocations until their deadline.
    std::vector<std::unordered_map<uint64_t, uint64_t>> hold_ms_by_address(
        nseg);
    for (const auto& range : quarantine_ranges) {
        ranges[range.segment_index].push_back(
            {range.address, range.size, nullptr});
        hold_ms_by_address[range.segment_index][range.address] = range.hold_ms;
    }
    std::vector<StandbySegmentInfo> segment_infos;
    segment_infos.reserve(nseg);
    for (const auto& segment : segments) {
        segment_infos.push_back(segment.info);
    }
    if (auto checked = CheckRestoredRanges(segment_infos, ranges, workers);
        !checked) {
        return tl::make_unexpected("overlapping or excess ranges");
    }
    std::vector<std::string> segment_errors(nseg);
    // Per segment: (hold_ms, buffer) of every quarantined range.
    std::vector<
        std::vector<std::pair<uint64_t, std::unique_ptr<AllocatedBuffer>>>>
        quarantined_buffers(nseg);
    std::vector<uint64_t> swapped(nseg, 0);
    std::atomic<size_t> next_segment{0};
    (void)RunRestoreWorkers(
        std::min(workers, std::max<size_t>(1, nseg)),
        [&](size_t) -> tl::expected<bool, ErrorCode> {
            const size_t si =
                next_segment.fetch_add(1, std::memory_order_relaxed);
            if (si >= nseg) {
                return false;
            }
            const auto& refs = ranges[si];
            const uint64_t base = segments[si].Base();
            std::vector<LiveAllocation> allocations;
            allocations.reserve(refs.size());
            for (const auto& ref : refs) {
                allocations.push_back({ref.address - base, ref.size});
            }
            bool swap_failed = false;
            const auto& bind = binders[si];
            const bool imported = ImportOffsetBufferAllocations(
                allocators[si], allocations,
                [&](size_t index, std::unique_ptr<AllocatedBuffer> buffer) {
                    Replica* replica = refs[index].replica;
                    if (replica == nullptr) {
                        // Ranges are disjoint (checked above), so the
                        // address identifies the quarantined range.
                        quarantined_buffers[si].emplace_back(
                            hold_ms_by_address[si].at(refs[index].address),
                            std::move(buffer));
                        return;
                    }
                    bind(*buffer);
                    if (replica->replace_memory_buffer(std::move(buffer))) {
                        ++swapped[si];
                    } else {
                        swap_failed = true;
                    }
                });
            if (!imported || swap_failed) {
                segment_errors[si] = "allocator import failed for segment " +
                                     segments[si].info.segment_name;
            }
            return true;
        });
    stats.rebuild_wall_ns = NsSince(phase_start);
    for (size_t si = 0; si < nseg; ++si) {
        if (!segment_errors[si].empty()) {
            return tl::make_unexpected(segment_errors[si]);
        }
    }
    ranges.clear();

    // ---- Phase 3: finalize.
    phase_start = SteadyClock::now();
    // Every placeholder buffer must have been replaced by an imported one.
    uint64_t total_swapped = 0;
    for (const uint64_t n : swapped) total_swapped += n;
    if (total_swapped != stats.replicas) {
        return tl::make_unexpected("not every restored replica was rebound");
    }
    placeholders.clear();
    {
        const ReplicaID desired_next_id =
            static_cast<ReplicaID>(manifest.next_replica_id);
        ReplicaID current_next_id = Replica::next_id_.load();
        while (current_next_id < desired_next_id &&
               !Replica::next_id_.compare_exchange_weak(current_next_id,
                                                        desired_next_id)) {
        }
    }
    if (contents->weight_metadata &&
        !weight_manager_.RestoreSnapshot(*contents->weight_metadata)) {
        return tl::make_unexpected("weight metadata restore failed");
    }
    if (enable_multi_tenants_) {
        RebuildTenantQuotaUsageFromMetadata();
    }
    MasterMetricManager::instance().inc_mem_cache_nums(
        static_cast<int64_t>(stats.objects));
    {
        // Each range is held for the rest of the time the old master would
        // have held it, counted from now (the gap only adds to it).
        const auto quarantine_now = SteadyClock::now();
        std::lock_guard<std::mutex> lock(handoff_quarantine_.mutex);
        for (size_t si = 0; si < nseg; ++si) {
            for (auto& [hold_ms, buffer] : quarantined_buffers[si]) {
                handoff_quarantine_.buffers.push_back(
                    {*segments[si].info.segment_id,
                     quarantine_now + std::chrono::milliseconds(hold_ms),
                     std::move(buffer)});
            }
        }
    }
    // Liveness clocks start now, when serving starts, not when the restore
    // started.
    const auto liveness_now = ClientLivenessRecord::Clock::now();
    for (auto& [client_id, record] : records) {
        (void)client_id;
        (void)record->Observe(liveness_now);
    }
    stats.finalize_ns = NsSince(phase_start);
    stats.total_ns = NsSince(total_start);
    stats.rss_kib_end = ProcStatusKiB("VmRSS");
    stats.hwm_kib_end = ProcStatusKiB("VmHWM");
    return stats;
}

// ----------------------------------------------------- remount integration

std::vector<UUID> MasterService::FindHandoffMismatchesLocked(
    const std::vector<Segment>& segments, const UUID& client_id,
    bool is_remount) const {
    std::vector<UUID> mismatched;
    for (const auto& [pending_id, pending] : pending_restored_segments_) {
        const Segment& restored = pending.segment;
        bool stale = false;
        bool listed_by_owner = false;
        for (const auto& segment : segments) {
            if (pending.owner != client_id) {
                // Another store process presents the transport endpoint the
                // restored segment was reachable at: the restored owner is
                // gone (store restarted during the roll).
                if (segment.te_endpoint == restored.te_endpoint) {
                    stale = true;
                }
                continue;
            }
            if (segment.id == pending_id) {
                listed_by_owner = true;
                if (!(segment == restored)) {
                    stale = true;  // same id, different base/size/endpoint
                }
            } else if (segment.te_endpoint == restored.te_endpoint &&
                       segment.base < restored.base + restored.size &&
                       restored.base < segment.base + segment.size) {
                // The owner registered this memory again under a new id.
                stale = true;
            }
        }
        if (is_remount && pending.owner == client_id && !listed_by_owner) {
            stale = true;  // the owner no longer has this segment
        }
        if (stale) {
            mismatched.push_back(pending_id);
        }
    }
    return mismatched;
}

void MasterService::DropPendingRestoredSegmentLocked(const UUID& segment_id,
                                                     const char* reason) {
    const auto it = pending_restored_segments_.find(segment_id);
    if (it == pending_restored_segments_.end()) {
        return;
    }
    const PendingRestoredSegment pending = it->second;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        size_t capacity = 0;
        if (segment_access.PrepareUnmountSegment(segment_id, capacity) ==
            ErrorCode::OK) {
            // Invalidates every restored buffer of the segment at once; the
            // objects become unreadable now and are erased by the cleanup
            // worker.
            (void)segment_access.CommitUnmountSegment(segment_id, pending.owner,
                                                      capacity);
        }
    }
    ForgetPendingRestoredSegmentLocked(it);
    replica_cleanup_worker_.Schedule();
    handoff_segments_dropped_.fetch_add(1, std::memory_order_relaxed);
    LOG(WARNING) << "handoff_segment=dropped, segment_name="
                 << pending.segment.name
                 << ", endpoint=" << pending.segment.te_endpoint
                 << ", owner=" << pending.owner << ", reason=" << reason;
}

void MasterService::ForgetPendingRestoredSegmentLocked(
    std::unordered_map<UUID, PendingRestoredSegment,
                       boost::hash<UUID>>::iterator it) {
    const UUID segment_id = it->first;
    const PendingRestoredSegment pending = it->second;
    pending_restored_segments_.erase(it);
    bool alias_still_pending = false;
    for (const auto& [other_id, other] : pending_restored_segments_) {
        (void)other_id;
        if (other.segment.te_endpoint == pending.segment.te_endpoint ||
            other.segment.name == pending.segment.name ||
            other.segment.te_endpoint == pending.segment.name ||
            other.segment.name == pending.segment.te_endpoint) {
            alias_still_pending = true;
            break;
        }
    }
    if (!alias_still_pending) {
        invalid_replica_endpoints_.erase(pending.segment.te_endpoint);
        invalid_replica_endpoints_.erase(pending.segment.name);
    }
    std::vector<HandoffQuarantinedBuffer> released;
    {
        std::lock_guard<std::mutex> lock(handoff_quarantine_.mutex);
        auto& buffers = handoff_quarantine_.buffers;
        for (auto b = buffers.begin(); b != buffers.end();) {
            if (b->segment_id == segment_id) {
                released.push_back(std::move(*b));
                b = buffers.erase(b);
            } else {
                ++b;
            }
        }
    }
}

size_t MasterService::ReconcileHandoffPendingSegments() {
    {
        std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
        if (pending_restored_segments_.empty()) return 0;
    }
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    std::unique_lock<std::shared_mutex> state_lock(master_state_mutex_);
    std::vector<UUID> gone;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& [segment_id, pending] : pending_restored_segments_) {
            (void)pending;
            // Unmounted (e.g. by the TTL offboarding), or unmounted and
            // mounted again as a fresh, empty segment.
            if (!segment_access.IsRestoredSegmentPending(segment_id)) {
                gone.push_back(segment_id);
            }
        }
    }
    for (const auto& segment_id : gone) {
        const auto it = pending_restored_segments_.find(segment_id);
        if (it == pending_restored_segments_.end()) continue;
        LOG(WARNING) << "handoff_segment=forgotten, segment_name="
                     << it->second.segment.name
                     << ", endpoint=" << it->second.segment.te_endpoint
                     << ", owner=" << it->second.owner
                     << ", reason=unmounted_or_remounted_fresh";
        ForgetPendingRestoredSegmentLocked(it);
        handoff_segments_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
    return gone.size();
}

void MasterService::ConfirmPendingRestoredSegmentsLocked(
    const std::vector<Segment>& segments, const UUID& client_id) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    for (const auto& segment : segments) {
        const auto it = pending_restored_segments_.find(segment.id);
        if (it == pending_restored_segments_.end() ||
            it->second.owner != client_id || !(it->second.segment == segment)) {
            continue;
        }
        // An OK segment becomes allocatable; a DRAINING / DRAINED one keeps
        // its status (its drain job did not survive the roll; an operator
        // starts a new one or sets it back to OK).
        (void)segment_access.ConfirmRestoredSegment(segment.id);
        pending_restored_segments_.erase(it);
        handoff_segments_confirmed_.fetch_add(1, std::memory_order_relaxed);
        LOG(INFO) << "handoff_segment=confirmed, segment_name=" << segment.name
                  << ", owner=" << client_id;
    }
}

void MasterService::ReleaseExpiredHandoffQuarantine(
    std::chrono::steady_clock::time_point now) {
    std::vector<HandoffQuarantinedBuffer> released;
    {
        std::lock_guard<std::mutex> lock(handoff_quarantine_.mutex);
        std::vector<HandoffQuarantinedBuffer> kept;
        for (auto& held : handoff_quarantine_.buffers) {
            if (held.deadline <= now) {
                released.push_back(std::move(held));
            } else {
                kept.push_back(std::move(held));
            }
        }
        handoff_quarantine_.buffers.swap(kept);
        if (released.empty()) {
            return;
        }
    }
    LOG(INFO) << "handoff_quarantine=released, ranges=" << released.size();
}

}  // namespace mooncake
