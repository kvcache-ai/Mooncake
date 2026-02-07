#include "oplog_applier.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include "etcd_oplog_store.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"
#include "oplog_manager.h"

namespace mooncake {

OpLogApplier::OpLogApplier(MetadataStore* metadata_store,
                           const std::string& cluster_id)
    : metadata_store_(metadata_store),
      cluster_id_(cluster_id),
      expected_sequence_id_(1) {
    if (metadata_store_ == nullptr) {
        LOG(FATAL) << "OpLogApplier: metadata_store cannot be null";
    }

    // Validate cluster_id if provided (required for etcd operations).
    // Normalize by stripping trailing slashes for validation.
    std::string normalized = cluster_id_;
    while (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    if (!normalized.empty() && !IsValidClusterIdComponent(normalized)) {
        LOG(FATAL)
            << "Invalid cluster_id for OpLogApplier: '" << cluster_id_
            << "' (normalized: '" << normalized
            << "'). Allowed chars: [A-Za-z0-9_.-], max_len=128, no slashes.";
    }
}

EtcdOpLogStore* OpLogApplier::GetEtcdOpLogStore() const {
#ifdef STORE_USE_ETCD
    if (cluster_id_.empty()) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(etcd_oplog_store_mutex_);
    if (!etcd_oplog_store_) {
        // Reader: do not start `/latest` batch update thread.
        etcd_oplog_store_ = std::make_unique<EtcdOpLogStore>(
            cluster_id_, /*enable_latest_seq_batch_update=*/false);
    }
    return etcd_oplog_store_.get();
#else
    return nullptr;
#endif
}

bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // Basic DoS protection: validate key/payload sizes before parsing/applying.
    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLogApplier: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    // Verify checksum to detect data corruption or tampering.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR)
            << "OpLogApplier: checksum mismatch, sequence_id="
            << entry.sequence_id << ", key=" << entry.object_key
            << ". Possible data corruption or tampering. Discarding entry.";
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return false;
    }

    // Global ordering only.
    //
    // IMPORTANT:
    // - Watch callbacks / retries may deliver duplicate or already-applied
    // entries.
    // - Those must be treated as no-op, not as "out-of-order pending",
    // otherwise
    //   pending_entries_ can grow and the applier may appear stuck.
    const uint64_t expected = expected_sequence_id_.load();
    if (IsSequenceOlder(entry.sequence_id, expected)) {
        // Late arrival of a previously-skipped gap entry: apply only if it's a
        // delete/revoke.
        bool was_skipped = false;
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            auto it = skipped_sequence_ids_.find(entry.sequence_id);
            if (it != skipped_sequence_ids_.end()) {
                was_skipped = true;
                skipped_sequence_ids_.erase(it);
            }
        }
        if (was_skipped) {
            if (entry.op_type == OpType::REMOVE ||
                entry.op_type == OpType::PUT_REVOKE) {
                // Safe: ensure we don't keep stale metadata.
                if (entry.op_type == OpType::REMOVE) {
                    ApplyRemove(entry);
                } else {
                    ApplyPutRevoke(entry);
                }
                return true;
            }
            // PUT_END (or others): discard to avoid resurrecting stale state.
            VLOG(1) << "OpLogApplier: discard late skipped entry, op_type="
                    << static_cast<int>(entry.op_type)
                    << ", sequence_id=" << entry.sequence_id
                    << ", key=" << entry.object_key;
            return true;
        }

        VLOG(2) << "OpLogApplier: skip already-applied entry, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key;
        return true;  // consumed (no-op)
    }
    if (IsSequenceNewer(entry.sequence_id, expected)) {
        // Future entry - store into pending, wait for the gap to be filled.
        std::lock_guard<std::mutex> lock(pending_mutex_);

        if (pending_entries_.size() >=
            static_cast<size_t>(kMaxPendingEntries)) {
            LOG(ERROR) << "OpLogApplier: too many pending entries ("
                       << pending_entries_.size()
                       << "), discarding entry sequence_id="
                       << entry.sequence_id << ", key=" << entry.object_key;
            return false;
        }

        pending_entries_[entry.sequence_id] = entry;
        VLOG(1) << "OpLogApplier: future entry buffered, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key
                << ", pending_entries=" << pending_entries_.size();
        return false;
    }

    // Apply the operation based on type
    switch (entry.op_type) {
        case OpType::PUT_END:
            ApplyPutEnd(entry);
            break;
        case OpType::PUT_REVOKE:
            ApplyPutRevoke(entry);
            break;
        case OpType::REMOVE:
            ApplyRemove(entry);
            break;
        default:
            LOG(ERROR) << "OpLogApplier: unsupported op_type="
                       << static_cast<int>(entry.op_type)
                       << ", sequence_id=" << entry.sequence_id
                       << ", key=" << entry.object_key;
            return false;
    }

    // Update expected sequence ID
    expected_sequence_id_.store(entry.sequence_id + 1);

    // Update metrics
    HAMetricManager::instance().inc_oplog_applied_entries();
    HAMetricManager::instance().set_oplog_applied_sequence_id(
        static_cast<int64_t>(entry.sequence_id));

    // Try to process pending entries
    ProcessPendingEntries();

    return true;
}

size_t OpLogApplier::ApplyOpLogEntries(const std::vector<OpLogEntry>& entries) {
    size_t applied_count = 0;
    for (const auto& entry : entries) {
        if (ApplyOpLogEntry(entry)) {
            applied_count++;
        }
    }
    return applied_count;
}

uint64_t OpLogApplier::GetKeySequenceId(const std::string& key) const {
    // Global sequence_id is used for ordering.
    (void)key;  // Suppress unused parameter warning
    return 0;
}

uint64_t OpLogApplier::GetExpectedSequenceId() const {
    return expected_sequence_id_.load();
}

void OpLogApplier::Recover(uint64_t last_applied_sequence_id) {
    expected_sequence_id_.store(last_applied_sequence_id + 1);
    LOG(INFO) << "OpLogApplier: recovered from sequence_id="
              << last_applied_sequence_id << ", expected_sequence_id set to="
              << expected_sequence_id_.load();
}

size_t OpLogApplier::ProcessPendingEntries() {
    // Check for missing sequence IDs, possibly skip after timeout, and/or
    // request them.
    uint64_t missing_seq_to_request = 0;
    uint64_t skipped_count = 0;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto now = std::chrono::steady_clock::now();
        for (;;) {
            if (pending_entries_.empty()) {
                break;
            }
            const uint64_t first_pending_seq = pending_entries_.begin()->first;
            const uint64_t expected = expected_sequence_id_.load();
            if (IsSequenceOlderOrEqual(first_pending_seq, expected)) {
                break;
            }

            // There's a gap: expected is missing.
            const uint64_t missing_seq = expected;
            auto it = missing_sequence_ids_.find(missing_seq);
            if (it == missing_sequence_ids_.end()) {
                missing_sequence_ids_[missing_seq] = now;
                ScheduleWaitForMissingEntries(missing_seq);
                break;
            }

            const auto waited =
                std::chrono::duration_cast<std::chrono::seconds>(now -
                                                                 it->second);

            // Skip after timeout to avoid global stall (user requested
            // behavior).
            if (waited.count() >= kMissingEntrySkipSeconds) {
                skipped_sequence_ids_[missing_seq] = now;
                missing_sequence_ids_.erase(missing_seq);
                expected_sequence_id_.store(missing_seq + 1);
                skipped_count++;
                HAMetricManager::instance().inc_oplog_skipped_entries();
                LOG(WARNING)
                    << "OpLogApplier: skipped missing entry seq=" << missing_seq
                    << " after " << waited.count() << "s timeout";
                continue;  // may skip multiple consecutive gaps
            }

            // Best-effort request from etcd (before skip triggers).
            if (waited.count() >= kMissingEntryRequestSeconds) {
                missing_seq_to_request = missing_seq;
                break;
            }
            break;
        }
    }

    // Request missing OpLog if needed (outside the lock to avoid deadlock)
    bool retrieved_missing = false;
    if (missing_seq_to_request > 0) {
        retrieved_missing = RequestMissingOpLog(missing_seq_to_request);
        if (retrieved_missing) {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            missing_sequence_ids_.erase(missing_seq_to_request);
        }
    }

    size_t processed_count = 0;
    for (;;) {
        OpLogEntry entry_copy;
        bool has_entry = false;

        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            if (pending_entries_.empty()) {
                break;
            }

            auto it = pending_entries_.begin();
            const uint64_t expected = expected_sequence_id_.load();
            if (!IsSequenceEqual(it->first, expected)) {
                break;  // still waiting for earlier sequence_id
            }

            entry_copy = it->second;
            pending_entries_.erase(it);
            has_entry = true;
        }

        if (!has_entry) {
            break;
        }

        // Apply outside lock.
        switch (entry_copy.op_type) {
            case OpType::PUT_END:
                ApplyPutEnd(entry_copy);
                break;
            case OpType::PUT_REVOKE:
                ApplyPutRevoke(entry_copy);
                break;
            case OpType::REMOVE:
                ApplyRemove(entry_copy);
                break;
            default:
                LOG(ERROR)
                    << "OpLogApplier: unsupported op_type in pending entry";
                break;
        }

        expected_sequence_id_.store(entry_copy.sequence_id + 1);

        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            missing_sequence_ids_.erase(entry_copy.sequence_id);
        }

        processed_count++;
    }

    // Clean up old missing sequence IDs (older than 1 minute)
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto now = std::chrono::steady_clock::now();
        for (auto it = missing_sequence_ids_.begin();
             it != missing_sequence_ids_.end();) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(
                now - it->second);
            if (age.count() > 60) {
                LOG(WARNING)
                    << "OpLogApplier: giving up on missing sequence_id="
                    << it->first << " after " << age.count() << " seconds";
                it = missing_sequence_ids_.erase(it);
            } else {
                ++it;
            }
        }

        // Clean up old skipped sequence IDs too (avoid unbounded growth).
        for (auto it = skipped_sequence_ids_.begin();
             it != skipped_sequence_ids_.end();) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(
                now - it->second);
            if (age.count() > 60) {
                it = skipped_sequence_ids_.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (skipped_count > 0) {
        LOG(WARNING) << "OpLogApplier: skipped " << skipped_count
                     << " missing sequence_id(s) after timeout, "
                        "expected_sequence_id now="
                     << expected_sequence_id_.load();
    }

    if (processed_count > 0) {
        LOG(INFO) << "OpLogApplier: processed " << processed_count
                  << " pending entries, expected_sequence_id now="
                  << expected_sequence_id_.load();
    }

    // Update pending entries metric
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        HAMetricManager::instance().set_oplog_pending_entries(
            static_cast<int64_t>(pending_entries_.size()));
    }

    return processed_count;
}

OpLogApplier::GapResolveResult OpLogApplier::TryResolveGapsOnceForPromotion(
    size_t max_ids) {
    GapResolveResult r;
#ifdef STORE_USE_ETCD
    EtcdOpLogStore* store = GetEtcdOpLogStore();
    if (store == nullptr) {
        return r;
    }

    std::vector<uint64_t> gap_ids;
    gap_ids.reserve(max_ids);
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (const auto& kv : missing_sequence_ids_) {
            if (gap_ids.size() >= max_ids) break;
            gap_ids.push_back(kv.first);
        }
        for (const auto& kv : skipped_sequence_ids_) {
            if (gap_ids.size() >= max_ids) break;
            gap_ids.push_back(kv.first);
        }
    }

    if (gap_ids.empty()) {
        return r;
    }

    std::sort(gap_ids.begin(), gap_ids.end());
    gap_ids.erase(std::unique(gap_ids.begin(), gap_ids.end()), gap_ids.end());

    r.attempted = gap_ids.size();
    std::vector<uint64_t> successfully_processed;
    for (uint64_t seq : gap_ids) {
        OpLogEntry e;
        ErrorCode err = store->ReadOpLog(seq, e);
        if (err != ErrorCode::OK) {
            // Log failed gap for monitoring, but don't clear it so it can be
            // retried later.
            LOG(WARNING) << "Promotion gap resolve: failed to fetch seq=" << seq
                         << ", err=" << static_cast<int>(err);
            continue;
        }
        r.fetched++;

        // Apply policy: only delete/revoke; drop PUT_END.
        if (e.op_type == OpType::REMOVE) {
            ApplyRemove(e);
            r.applied_deletes++;
            successfully_processed.push_back(seq);
        } else if (e.op_type == OpType::PUT_REVOKE) {
            ApplyPutRevoke(e);
            r.applied_deletes++;
            successfully_processed.push_back(seq);
        } else {
            // PUT_END or others: mark as processed (dropped) so we don't retry.
            successfully_processed.push_back(seq);
        }
    }

    // Only clear gaps we successfully fetched and processed.
    // Failed gaps remain in missing_sequence_ids_/skipped_sequence_ids_ for
    // potential retry or monitoring.
    if (!successfully_processed.empty()) {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (uint64_t seq : successfully_processed) {
            missing_sequence_ids_.erase(seq);
            skipped_sequence_ids_.erase(seq);
        }
    }
    return r;
#else
    (void)max_ids;
    return r;
#endif
}

bool OpLogApplier::CheckSequenceOrder(const OpLogEntry& entry) {
    // Only check global sequence order.
    // Use IsSequenceEqual for wrap-around safety (though equality check doesn't
    // need special handling, we use it for consistency).
    return IsSequenceEqual(entry.sequence_id, expected_sequence_id_.load());
}

void OpLogApplier::ApplyPutEnd(const OpLogEntry& entry) {
    // Payload contains serialized metadata (replicas, size, etc.) in JSON
    // format. Deserialize the payload immediately and store structured
    // metadata. This allows Standby to serve requests immediately after
    // promotion.

    if (entry.payload.empty()) {
        // No payload - create empty metadata (legacy compatibility)
        LOG(WARNING) << "OpLogApplier: PUT_END without payload, key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id;
        StandbyObjectMetadata empty_metadata;
        empty_metadata.last_sequence_id = entry.sequence_id;
        if (!metadata_store_->PutMetadata(entry.object_key, empty_metadata)) {
            LOG(ERROR) << "OpLogApplier: failed to PutMetadata key="
                       << entry.object_key
                       << ", sequence_id=" << entry.sequence_id;
        }
        return;
    }

    // Deserialize payload using struct_pack (msgpack binary format)
    MetadataPayload payload;
    bool parse_success = false;
    auto result = struct_pack::deserialize_to(payload, entry.payload);
    if (result == struct_pack::errc::ok) {
        parse_success = true;
    } else {
        LOG(ERROR) << "OpLogApplier: failed to deserialize payload for key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id
                   << ", payload_size=" << entry.payload.size()
                   << ", error_code=" << static_cast<int>(result);
    }

    if (!parse_success) {
        // Fallback to empty metadata if parsing fails
        StandbyObjectMetadata empty_metadata;
        empty_metadata.last_sequence_id = entry.sequence_id;
        metadata_store_->PutMetadata(entry.object_key, empty_metadata);
        return;
    }

    // Convert to StandbyObjectMetadata and store
    StandbyObjectMetadata metadata =
        payload.ToStandbyMetadata(entry.sequence_id);

    if (!metadata_store_->PutMetadata(entry.object_key, metadata)) {
        LOG(ERROR) << "OpLogApplier: failed to PutMetadata key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_END, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id
                << ", replicas=" << metadata.replicas.size()
                << ", size=" << metadata.size;
    }
}

void OpLogApplier::ApplyPutRevoke(const OpLogEntry& entry) {
    // PUT_REVOKE means the object should be removed from metadata store
    // (but the key itself may still exist if there are other replicas).
    // Current implementation removes the entire key; if we later support
    // partial replica revocation this logic will need to be refined.
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key="
                     << entry.object_key
                     << " in PUT_REVOKE, sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_REVOKE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

void OpLogApplier::ApplyRemove(const OpLogEntry& entry) {
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied REMOVE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

bool OpLogApplier::RequestMissingOpLog(uint64_t missing_seq_id) {
#ifdef STORE_USE_ETCD
    HAMetricManager::instance().inc_oplog_gap_resolve_attempts();

    EtcdOpLogStore* oplog_store = GetEtcdOpLogStore();
    if (oplog_store == nullptr) {
        LOG(WARNING)
            << "OpLogApplier: cannot request missing OpLog, cluster_id not set";
        return false;
    }

    OpLogEntry entry;
    ErrorCode err = oplog_store->ReadOpLog(missing_seq_id, entry);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        LOG(INFO) << "OpLogApplier: missing OpLog entry not found in etcd, "
                     "sequence_id="
                  << missing_seq_id;
        return false;
    }
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "OpLogApplier: failed to read missing OpLog from etcd, "
                      "sequence_id="
                   << missing_seq_id << ", error=" << static_cast<int>(err);
        return false;
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLogApplier: missing entry size rejected, sequence_id="
                   << missing_seq_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    // Verify checksum before adding to pending entries.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR) << "OpLogApplier: checksum mismatch for retrieved missing "
                      "entry, sequence_id="
                   << missing_seq_id << ", key=" << entry.object_key
                   << ". Possible data corruption. Discarding entry.";
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return false;
    }

    // Successfully retrieved the missing OpLog entry
    LOG(INFO) << "OpLogApplier: retrieved missing OpLog entry, sequence_id="
              << missing_seq_id
              << ", op_type=" << static_cast<int>(entry.op_type)
              << ", key=" << entry.object_key;
    HAMetricManager::instance().inc_oplog_gap_resolve_success();

    // Add to pending entries
    // Note: We don't call ProcessPendingEntries() here to avoid potential
    // recursion. The caller (ProcessPendingEntries itself) will process the
    // entry in the next loop.
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_entries_[entry.sequence_id] = entry;
    }

    return true;
#else
    LOG(WARNING) << "OpLogApplier: STORE_USE_ETCD not enabled, cannot request "
                    "missing OpLog";
    return false;
#endif
}

void OpLogApplier::ScheduleWaitForMissingEntries(uint64_t missing_seq_id) {
    // This method is called when we first detect a missing sequence_id.
    // The actual waiting and requesting is handled in ProcessPendingEntries().
    // We just log it here for tracking.
    VLOG(1) << "OpLogApplier: scheduling wait for missing sequence_id="
            << missing_seq_id << ", will request after "
            << kMissingEntryRequestSeconds << " seconds";
}

}  // namespace mooncake
