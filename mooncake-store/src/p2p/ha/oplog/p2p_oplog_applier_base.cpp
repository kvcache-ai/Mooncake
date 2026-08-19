#include "p2p/ha/oplog/p2p_oplog_applier_base.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include "p2p/ha/oplog/oplog_store.h"
#include "p2p/p2p_ha_metric_manager.h"
#include "metadata_store.h"
#include "p2p/ha/oplog/oplog_manager.h"

namespace mooncake {

P2POpLogApplierBase::P2POpLogApplierBase(MetadataStore* metadata_store,
                           const std::string& cluster_id,
                           OpLogStore* oplog_store)
    : metadata_store_(metadata_store),
      cluster_id_(cluster_id),
      oplog_store_(oplog_store),
      expected_sequence_id_(1) {
    if (metadata_store_ == nullptr) {
        LOG(FATAL) << "P2POpLogApplierBase: metadata_store cannot be null";
    }
    P2PHAMetricManager::instance().set_standby_degraded(false);
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for P2POpLogApplierBase: '" << cluster_id_
                   << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128.";
    }
}

bool P2POpLogApplierBase::ApplyOpLogEntry(const OpLogEntry& entry) {
    if (!healthy_.load()) {
        return false;
    }

    // Basic DoS protection: validate key/payload sizes before parsing/applying.
    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "P2POpLogApplierBase: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return HandleApplyFailure(entry, "entry size rejected");
    }

    // Verify checksum to detect data corruption or tampering.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR)
            << "P2POpLogApplierBase: checksum mismatch, sequence_id="
            << entry.sequence_id << ", key=" << entry.object_key
            << ". Possible data corruption or tampering. Discarding entry.";
        P2PHAMetricManager::instance().inc_oplog_checksum_failures();
        return HandleApplyFailure(entry, "checksum mismatch");
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
        // Late arrival of a previously-skipped gap entry: apply only if it
        // cannot resurrect old state.
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
            if (IsLateSkippedDeleteLikeOpLogEntry(entry)) {
                if (!ApplyLateSkippedDeleteLikeOpLogEntry(entry)) {
                    LOG(ERROR)
                        << "P2POpLogApplierBase: failed to apply late skipped "
                           "delete-like entry"
                        << ", op_type=" << static_cast<int>(entry.op_type)
                        << ", sequence_id=" << entry.sequence_id
                        << ", key=" << entry.object_key;
                    return HandleApplyFailure(
                        entry, "late skipped delete-like apply failed");
                }
                return true;
            }
            // PUT_END / add-like entries: discard to avoid resurrecting stale
            // state.
            if (entry.op_type == OpType::PUT_END) {
                P2PHAMetricManager::instance().inc_oplog_dropped_put_end();
            }
            VLOG(1) << "P2POpLogApplierBase: discard late skipped entry, op_type="
                    << static_cast<int>(entry.op_type)
                    << ", sequence_id=" << entry.sequence_id
                    << ", key=" << entry.object_key;
            return true;
        }

        VLOG(2) << "P2POpLogApplierBase: skip already-applied entry, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key;
        return true;  // consumed (no-op)
    }
    if (IsSequenceNewer(entry.sequence_id, expected)) {
        // Future entry - store into pending, wait for the gap to be filled.
        std::lock_guard<std::mutex> lock(pending_mutex_);

        if (pending_entries_.size() >=
            static_cast<size_t>(kMaxPendingEntries)) {
            LOG(ERROR) << "P2POpLogApplierBase: too many pending entries ("
                       << pending_entries_.size()
                       << "), discarding entry sequence_id="
                       << entry.sequence_id << ", key=" << entry.object_key;
            return false;
        }

        pending_entries_[entry.sequence_id] = entry;
        P2PHAMetricManager::instance().set_oplog_pending_entries(
            static_cast<int64_t>(pending_entries_.size()));
        VLOG(1) << "P2POpLogApplierBase: future entry buffered, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key
                << ", pending_entries=" << pending_entries_.size();
        return false;
    }

    if (!ApplyOpLogEntryInternal(entry)) {
        LOG(ERROR) << "P2POpLogApplierBase: unsupported or failed op_type="
                   << static_cast<int>(entry.op_type)
                   << ", sequence_id=" << entry.sequence_id
                   << ", key=" << entry.object_key;
        return HandleApplyFailure(entry, "operation apply failed");
    }

    // Update expected sequence ID
    expected_sequence_id_.store(entry.sequence_id + 1);

    // Update metrics
    P2PHAMetricManager::instance().inc_oplog_applied_entries();
    P2PHAMetricManager::instance().set_oplog_applied_sequence_id(
        static_cast<int64_t>(entry.sequence_id));

    // Try to process pending entries
    ProcessPendingEntries();

    return true;
}

bool P2POpLogApplierBase::ApplyCustomOpLogEntry(const OpLogEntry&) { return false; }

bool P2POpLogApplierBase::ApplyOpLogEntryInternal(const OpLogEntry& entry) {
    switch (entry.op_type) {
        case OpType::PUT_END:
            ApplyPutEnd(entry);
            return true;
        case OpType::PUT_REVOKE:
            ApplyPutRevoke(entry);
            return true;
        case OpType::REMOVE:
            ApplyRemove(entry);
            return true;
        default:
            return ApplyCustomOpLogEntry(entry);
    }
}

bool P2POpLogApplierBase::IsBestEffortOpLogEntry(const OpLogEntry&) const {
    return false;
}

bool P2POpLogApplierBase::IsLateSkippedDeleteLikeOpLogEntry(
    const OpLogEntry& entry) const {
    return entry.op_type == OpType::REMOVE ||
           entry.op_type == OpType::PUT_REVOKE;
}

std::string P2POpLogApplierBase::GetFailureReason() const {
    std::lock_guard<std::mutex> lock(failure_mutex_);
    return failure_reason_;
}

bool P2POpLogApplierBase::HandleApplyFailure(const OpLogEntry& entry,
                                      const char* reason) {
    if (IsBestEffortOpLogEntry(entry)) {
        P2PHAMetricManager::instance().inc_oplog_best_effort_apply_skipped();
        LOG(ERROR) << "P2POpLogApplierBase: skipping best-effort entry after apply "
                      "failure"
                   << ", sequence_id=" << entry.sequence_id
                   << ", op_type=" << static_cast<int>(entry.op_type)
                   << ", reason=" << reason;
        const uint64_t expected = expected_sequence_id_.load();
        if (IsSequenceNewer(entry.sequence_id, expected)) {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            confirmed_missing_sequence_ids_.insert(entry.sequence_id);
            return false;
        }
        if (IsSequenceOlder(entry.sequence_id, expected)) {
            return true;
        }
        expected_sequence_id_.store(entry.sequence_id + 1);
        ProcessPendingEntries();
        return true;
    }

    {
        std::lock_guard<std::mutex> lock(failure_mutex_);
        failure_reason_ = reason;
    }
    failed_op_type_.store(static_cast<int>(entry.op_type));
    failed_sequence_id_.store(entry.sequence_id);
    healthy_.store(false);
    P2PHAMetricManager::instance().inc_oplog_apply_failures();
    P2PHAMetricManager::instance().set_standby_degraded(true);
    LOG(ERROR) << "P2POpLogApplierBase: critical apply failure"
               << ", sequence_id=" << entry.sequence_id
               << ", op_type=" << static_cast<int>(entry.op_type)
               << ", reason=" << reason;
    return false;
}

size_t P2POpLogApplierBase::ApplyOpLogEntries(const std::vector<OpLogEntry>& entries) {
    size_t applied_count = 0;
    for (const auto& entry : entries) {
        if (ApplyOpLogEntry(entry)) {
            applied_count++;
        }
    }
    return applied_count;
}

uint64_t P2POpLogApplierBase::GetExpectedSequenceId() const {
    return expected_sequence_id_.load();
}

void P2POpLogApplierBase::Recover(uint64_t last_applied_sequence_id) {
    expected_sequence_id_.store(last_applied_sequence_id + 1);
    LOG(INFO) << "P2POpLogApplierBase: recovered from sequence_id="
              << last_applied_sequence_id << ", expected_sequence_id set to="
              << expected_sequence_id_.load();
}

void P2POpLogApplierBase::ConfirmMissingSequenceIds(
    const std::vector<uint64_t>& missing_sequence_ids) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    const uint64_t expected = expected_sequence_id_.load();
    for (uint64_t sequence_id : missing_sequence_ids) {
        if (!IsSequenceOlder(sequence_id, expected)) {
            confirmed_missing_sequence_ids_.insert(sequence_id);
        }
    }
}

size_t P2POpLogApplierBase::ProcessPendingEntries() {
    if (!healthy_.load()) {
        return 0;
    }
    // Check for missing sequence IDs, possibly skip after timeout, and/or
    // request them.
    uint64_t missing_seq_to_request = 0;
    uint64_t skipped_count = 0;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto now = std::chrono::steady_clock::now();
        for (;;) {
            const uint64_t expected = expected_sequence_id_.load();
            auto confirmed_it = confirmed_missing_sequence_ids_.find(expected);
            if (confirmed_it != confirmed_missing_sequence_ids_.end()) {
                confirmed_missing_sequence_ids_.erase(confirmed_it);
                skipped_sequence_ids_[expected] = now;
                missing_sequence_ids_.erase(expected);
                expected_sequence_id_.store(expected + 1);
                skipped_count++;
                P2PHAMetricManager::instance().inc_oplog_skipped_entries();
                LOG(WARNING) << "P2POpLogApplierBase: skipped confirmed missing entry "
                                "seq="
                             << expected;
                continue;
            }

            if (pending_entries_.empty()) {
                break;
            }
            const uint64_t first_pending_seq = pending_entries_.begin()->first;
            if (IsSequenceOlderOrEqual(first_pending_seq, expected)) {
                break;
            }

            // There's a gap: expected is missing.
            const uint64_t missing_seq = expected;
            auto it = missing_sequence_ids_.find(missing_seq);
            if (it == missing_sequence_ids_.end()) {
                missing_sequence_ids_[missing_seq] = now;
                VLOG(1) << "P2POpLogApplierBase: scheduling wait for missing "
                           "sequence_id="
                        << missing_seq << ", will request after "
                        << kMissingEntryRequestSeconds << " seconds";
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
                P2PHAMetricManager::instance().inc_oplog_skipped_entries();
                LOG(WARNING)
                    << "P2POpLogApplierBase: skipped missing entry seq=" << missing_seq
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
        if (!ApplyOpLogEntryInternal(entry_copy)) {
            if (IsBestEffortOpLogEntry(entry_copy)) {
                P2PHAMetricManager::instance()
                    .inc_oplog_best_effort_apply_skipped();
                LOG(ERROR)
                    << "P2POpLogApplierBase: skipping failed best-effort pending entry"
                    << ", sequence_id=" << entry_copy.sequence_id
                    << ", op_type=" << static_cast<int>(entry_copy.op_type);
                expected_sequence_id_.store(entry_copy.sequence_id + 1);
                processed_count++;
                continue;
            }
            LOG(ERROR) << "P2POpLogApplierBase: failed to apply pending entry"
                       << ", sequence_id=" << entry_copy.sequence_id
                       << ", op_type=" << static_cast<int>(entry_copy.op_type);
            HandleApplyFailure(entry_copy, "pending operation apply failed");
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_entries_.emplace(entry_copy.sequence_id,
                                     std::move(entry_copy));
            break;
        }

        expected_sequence_id_.store(entry_copy.sequence_id + 1);
        P2PHAMetricManager::instance().inc_oplog_applied_entries();
        P2PHAMetricManager::instance().set_oplog_applied_sequence_id(
            static_cast<int64_t>(entry_copy.sequence_id));

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
                    << "P2POpLogApplierBase: giving up on missing sequence_id="
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
        LOG(WARNING) << "P2POpLogApplierBase: skipped " << skipped_count
                     << " missing sequence_id(s) after timeout, "
                        "expected_sequence_id now="
                     << expected_sequence_id_.load();
    }

    if (processed_count > 0) {
        LOG(INFO) << "P2POpLogApplierBase: processed " << processed_count
                  << " pending entries, expected_sequence_id now="
                  << expected_sequence_id_.load();
    }

    // Update pending entries metric
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        P2PHAMetricManager::instance().set_oplog_pending_entries(
            static_cast<int64_t>(pending_entries_.size()));
    }

    return processed_count;
}

P2POpLogApplierBase::GapResolveResult P2POpLogApplierBase::TryResolveGapsOnceForPromotion(
    size_t max_ids) {
    GapResolveResult r;
    if (oplog_store_ == nullptr) {
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
        ErrorCode err = oplog_store_->ReadOpLog(seq, e);
        if (err != ErrorCode::OK) {
            // Log failed gap for monitoring, but don't clear it so it can be
            // retried later.
            LOG(WARNING) << "Promotion gap resolve: failed to fetch seq=" << seq
                         << ", err=" << static_cast<int>(err);
            continue;
        }
        r.fetched++;

        // Apply policy: only delete-like entries; drop PUT_END/add-like ops.
        if (IsLateSkippedDeleteLikeOpLogEntry(e)) {
            if (!ApplyLateSkippedDeleteLikeOpLogEntry(e)) {
                LOG(ERROR) << "Promotion gap resolve: failed to apply "
                              "delete-like seq="
                           << seq
                           << ", op_type=" << static_cast<int>(e.op_type);
                (void)HandleApplyFailure(
                    e, "promotion gap delete-like apply failed");
                continue;
            }
            r.applied_deletes++;
            successfully_processed.push_back(seq);
        } else {
            // PUT_END / add-like entries: mark as processed (dropped) so we
            // don't retry.
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
}

bool P2POpLogApplierBase::ApplyLateSkippedDeleteLikeOpLogEntry(
    const OpLogEntry& entry) {
    if (entry.op_type == OpType::REMOVE) {
        ApplyRemove(entry);
        return true;
    }
    if (entry.op_type == OpType::PUT_REVOKE) {
        ApplyPutRevoke(entry);
        return true;
    }
    return ApplyCustomOpLogEntry(entry);
}

bool P2POpLogApplierBase::CheckSequenceOrder(const OpLogEntry& entry) {
    // Only check global sequence order.
    // Use IsSequenceEqual for wrap-around safety (though equality check doesn't
    // need special handling, we use it for consistency).
    return IsSequenceEqual(entry.sequence_id, expected_sequence_id_.load());
}

void P2POpLogApplierBase::ApplyPutEnd(const OpLogEntry& entry) {
    // Payload contains serialized metadata (replicas, size, etc.) in JSON
    // format. Deserialize the payload immediately and store structured
    // metadata. This allows Standby to serve requests immediately after
    // promotion.

    if (entry.payload.empty()) {
        // No payload - create empty metadata (legacy compatibility)
        LOG(WARNING) << "P2POpLogApplierBase: PUT_END without payload, key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id;
        StandbyObjectMetadata empty_metadata;
        empty_metadata.last_sequence_id = entry.sequence_id;
        if (!metadata_store_->PutMetadata(entry.object_key, empty_metadata)) {
            LOG(ERROR) << "P2POpLogApplierBase: failed to PutMetadata key="
                       << entry.object_key
                       << ", sequence_id=" << entry.sequence_id;
        }
        return;
    }

    // Deserialize payload using struct_pack (msgpack binary format)
    MetadataPayload payload;
    auto result = struct_pack::deserialize_to(payload, entry.payload);
    if (result != struct_pack::errc::ok) {
        LOG(ERROR) << "P2POpLogApplierBase: failed to deserialize payload for key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id
                   << ", payload_size=" << entry.payload.size()
                   << ", error_code=" << static_cast<int>(result);
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
        LOG(ERROR) << "P2POpLogApplierBase: failed to PutMetadata key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
    } else {
        VLOG(1) << "P2POpLogApplierBase: applied PUT_END, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id
                << ", replicas=" << metadata.replicas.size()
                << ", size=" << metadata.size;
    }
}

void P2POpLogApplierBase::ApplyPutRevoke(const OpLogEntry& entry) {
    // PUT_REVOKE means the object should be removed from metadata store
    // (but the key itself may still exist if there are other replicas).
    // Current implementation removes the entire key; if we later support
    // partial replica revocation this logic will need to be refined.
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "P2POpLogApplierBase: failed to Remove key="
                     << entry.object_key
                     << " in PUT_REVOKE, sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "P2POpLogApplierBase: applied PUT_REVOKE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

void P2POpLogApplierBase::ApplyRemove(const OpLogEntry& entry) {
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "P2POpLogApplierBase: failed to Remove key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "P2POpLogApplierBase: applied REMOVE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

bool P2POpLogApplierBase::RequestMissingOpLog(uint64_t missing_seq_id) {
    P2PHAMetricManager::instance().inc_oplog_gap_resolve_attempts();

    if (oplog_store_ == nullptr) {
        LOG(WARNING)
            << "P2POpLogApplierBase: cannot request missing OpLog, no store set";
        return false;
    }

    OpLogEntry entry;
    ErrorCode err = oplog_store_->ReadOpLog(missing_seq_id, entry);
    if (err == ErrorCode::OPLOG_ENTRY_NOT_FOUND) {
        LOG(INFO) << "P2POpLogApplierBase: missing OpLog entry not found in store, "
                     "sequence_id="
                  << missing_seq_id;
        return false;
    }
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "P2POpLogApplierBase: failed to read missing OpLog from store, "
                      "sequence_id="
                   << missing_seq_id << ", error=" << static_cast<int>(err);
        return false;
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "P2POpLogApplierBase: missing entry size rejected, sequence_id="
                   << missing_seq_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    // Verify checksum before adding to pending entries.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR) << "P2POpLogApplierBase: checksum mismatch for retrieved missing "
                      "entry, sequence_id="
                   << missing_seq_id << ", key=" << entry.object_key
                   << ". Possible data corruption. Discarding entry.";
        P2PHAMetricManager::instance().inc_oplog_checksum_failures();
        return false;
    }

    // Successfully retrieved the missing OpLog entry
    LOG(INFO) << "P2POpLogApplierBase: retrieved missing OpLog entry, sequence_id="
              << missing_seq_id
              << ", op_type=" << static_cast<int>(entry.op_type)
              << ", key=" << entry.object_key;
    P2PHAMetricManager::instance().inc_oplog_gap_resolve_success();

    // Add to pending entries
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_entries_[entry.sequence_id] = entry;
    }

    return true;
}

}  // namespace mooncake
