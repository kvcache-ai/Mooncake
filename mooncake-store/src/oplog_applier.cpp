#include "oplog_applier.h"

#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <chrono>

#include "etcd_oplog_store.h"
#include "metadata_store.h"

namespace mooncake {

OpLogApplier::OpLogApplier(MetadataStore* metadata_store,
                           const std::string& cluster_id)
    : metadata_store_(metadata_store),
      cluster_id_(cluster_id),
      expected_sequence_id_(1) {
    if (metadata_store_ == nullptr) {
        LOG(FATAL) << "OpLogApplier: metadata_store cannot be null";
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
        etcd_oplog_store_ =
            std::make_unique<EtcdOpLogStore>(cluster_id_, /*enable_latest_seq_batch_update=*/false);
    }
    return etcd_oplog_store_.get();
#else
    return nullptr;
#endif
}

bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // Check global sequence order (key_sequence_id is no longer used)
    if (entry.sequence_id != expected_sequence_id_) {
        // Global sequence violation - add to pending entries
        std::lock_guard<std::mutex> lock(pending_mutex_);
        
        // Check if we've exceeded max pending entries
        if (pending_entries_.size() >= static_cast<size_t>(kMaxPendingEntries)) {
            LOG(ERROR) << "OpLogApplier: too many pending entries ("
                       << pending_entries_.size() << "), discarding entry sequence_id="
                       << entry.sequence_id << ", key=" << entry.object_key;
            return false;
        }
        
        pending_entries_[entry.sequence_id] = entry;
        VLOG(1) << "OpLogApplier: sequence order violation, sequence_id="
                << entry.sequence_id << ", expected=" << expected_sequence_id_
                << ", key=" << entry.object_key
                << ", added to pending entries (total: " << pending_entries_.size() << ")";
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
    expected_sequence_id_ = entry.sequence_id + 1;

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
    // Deprecated: key_sequence_id is no longer tracked.
    // Global sequence_id is used for ordering.
    (void)key;  // Suppress unused parameter warning
    return 0;
}

uint64_t OpLogApplier::GetExpectedSequenceId() const {
    return expected_sequence_id_;
}

void OpLogApplier::Recover(uint64_t last_applied_sequence_id) {
    expected_sequence_id_ = last_applied_sequence_id + 1;
    LOG(INFO) << "OpLogApplier: recovered from sequence_id="
              << last_applied_sequence_id
              << ", expected_sequence_id set to=" << expected_sequence_id_;
}

size_t OpLogApplier::ProcessPendingEntries() {
    // Check for missing sequence IDs and request them if needed (before acquiring lock)
    uint64_t missing_seq_to_request = 0;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        if (!pending_entries_.empty()) {
            uint64_t first_pending_seq = pending_entries_.begin()->first;
            if (first_pending_seq > expected_sequence_id_) {
                // There's a gap - we're missing entries between expected_sequence_id_ and first_pending_seq
                uint64_t missing_seq = expected_sequence_id_;
                auto missing_it = missing_sequence_ids_.find(missing_seq);
                auto now = std::chrono::steady_clock::now();
                
                if (missing_it == missing_sequence_ids_.end()) {
                    // First time we see this missing sequence - record the time
                    missing_sequence_ids_[missing_seq] = now;
                    ScheduleWaitForMissingEntries(missing_seq);
                } else {
                    // Check if we've waited long enough
                    auto wait_duration = std::chrono::duration_cast<std::chrono::seconds>(
                        now - missing_it->second);
                    if (wait_duration.count() >= kMissingEntryWaitSeconds) {
                        // Mark for requesting (we'll do it after releasing the lock)
                        missing_seq_to_request = missing_seq;
                    }
                }
            }
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
            if (it->first != expected_sequence_id_) {
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
                LOG(ERROR) << "OpLogApplier: unsupported op_type in pending entry";
                break;
        }

        expected_sequence_id_ = entry_copy.sequence_id + 1;

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
        for (auto it = missing_sequence_ids_.begin(); it != missing_sequence_ids_.end();) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second);
            if (age.count() > 60) {
                LOG(WARNING) << "OpLogApplier: giving up on missing sequence_id="
                             << it->first << " after " << age.count() << " seconds";
                it = missing_sequence_ids_.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (processed_count > 0) {
        LOG(INFO) << "OpLogApplier: processed " << processed_count
                  << " pending entries, expected_sequence_id now="
                  << expected_sequence_id_;
    }

    return processed_count;
}

bool OpLogApplier::CheckSequenceOrder(const OpLogEntry& entry) {
    // Only check global sequence order.
    // key_sequence_id is no longer used for ordering.
    return entry.sequence_id == expected_sequence_id_;
}

void OpLogApplier::ApplyPutEnd(const OpLogEntry& entry) {
    // Payload contains serialized metadata (replicas, size, lease) in JSON format.
    // Deserialize the payload immediately and store structured metadata.
    // This allows Standby to serve requests immediately after promotion.
    
    if (entry.payload.empty()) {
        // No payload - create empty metadata (legacy compatibility)
        LOG(WARNING) << "OpLogApplier: PUT_END without payload, key=" << entry.object_key
                     << ", sequence_id=" << entry.sequence_id;
        StandbyObjectMetadata empty_metadata;
        empty_metadata.last_sequence_id = entry.sequence_id;
        if (!metadata_store_->PutMetadata(entry.object_key, empty_metadata)) {
            LOG(ERROR) << "OpLogApplier: failed to PutMetadata key=" << entry.object_key
                       << ", sequence_id=" << entry.sequence_id;
        }
        return;
    }
    
    // Deserialize payload to MetadataPayload
    MetadataPayload payload;
    bool parse_success = false;
    try {
        struct_json::from_json(payload, entry.payload);
        parse_success = true;
    } catch (const std::exception& e) {
        LOG(ERROR) << "OpLogApplier: failed to parse payload for key=" << entry.object_key
                   << ", sequence_id=" << entry.sequence_id
                   << ", error=" << e.what();
    }
    
    if (!parse_success) {
        // Fallback to empty metadata if parsing fails
        StandbyObjectMetadata empty_metadata;
        empty_metadata.last_sequence_id = entry.sequence_id;
        metadata_store_->PutMetadata(entry.object_key, empty_metadata);
        return;
    }
    
    // Convert to StandbyObjectMetadata and store
    StandbyObjectMetadata metadata = payload.ToStandbyMetadata(entry.sequence_id);
    
    if (!metadata_store_->PutMetadata(entry.object_key, metadata)) {
        LOG(ERROR) << "OpLogApplier: failed to PutMetadata key=" << entry.object_key
                   << ", sequence_id=" << entry.sequence_id;
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_END, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id
                << ", replicas=" << metadata.replicas.size()
                << ", size=" << metadata.size;
    }
}

void OpLogApplier::ApplyPutRevoke(const OpLogEntry& entry) {
    // PUT_REVOKE means the object should be removed from metadata store
    // (but the key itself may still exist if there are other replicas)
    // For now, we treat it as a remove operation
    // In the future, we may need to handle partial replica removal
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key=" << entry.object_key
                     << " in PUT_REVOKE, sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_REVOKE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

void OpLogApplier::ApplyRemove(const OpLogEntry& entry) {
    if (!metadata_store_->Remove(entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key=" << entry.object_key
                     << ", sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied REMOVE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

bool OpLogApplier::RequestMissingOpLog(uint64_t missing_seq_id) {
#ifdef STORE_USE_ETCD
    EtcdOpLogStore* oplog_store = GetEtcdOpLogStore();
    if (oplog_store == nullptr) {
        LOG(WARNING) << "OpLogApplier: cannot request missing OpLog, cluster_id not set";
        return false;
    }

    OpLogEntry entry;
    ErrorCode err = oplog_store->ReadOpLog(missing_seq_id, entry);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        LOG(INFO) << "OpLogApplier: missing OpLog entry not found in etcd, sequence_id="
                  << missing_seq_id;
        return false;
    }
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "OpLogApplier: failed to read missing OpLog from etcd, sequence_id="
                   << missing_seq_id << ", error=" << static_cast<int>(err);
        return false;
    }

    // Successfully retrieved the missing OpLog entry
    LOG(INFO) << "OpLogApplier: retrieved missing OpLog entry, sequence_id="
              << missing_seq_id << ", op_type=" << static_cast<int>(entry.op_type)
              << ", key=" << entry.object_key;

    // Add to pending entries
    // Note: We don't call ProcessPendingEntries() here to avoid potential recursion.
    // The caller (ProcessPendingEntries itself) will process the entry in the next loop.
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_entries_[entry.sequence_id] = entry;
    }

    return true;
#else
    LOG(WARNING) << "OpLogApplier: STORE_USE_ETCD not enabled, cannot request missing OpLog";
    return false;
#endif
}

void OpLogApplier::ScheduleWaitForMissingEntries(uint64_t missing_seq_id) {
    // This method is called when we first detect a missing sequence_id.
    // The actual waiting and requesting is handled in ProcessPendingEntries().
    // We just log it here for tracking.
    VLOG(1) << "OpLogApplier: scheduling wait for missing sequence_id=" << missing_seq_id
            << ", will request after " << kMissingEntryWaitSeconds << " seconds";
}

}  // namespace mooncake

