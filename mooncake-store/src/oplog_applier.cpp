#include "oplog_applier.h"

#include <glog/logging.h>

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
        etcd_oplog_store_ = std::make_unique<EtcdOpLogStore>(cluster_id_);
    }
    return etcd_oplog_store_.get();
#else
    return nullptr;
#endif
}

bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // Check sequence order
    bool order_valid = CheckSequenceOrder(entry);
    
    // If key-level sequence order violation detected, delete the key
    if (!order_valid) {
        // Check if it's a key-level violation (not just global sequence violation)
        bool is_key_violation = false;
        uint64_t current_key_seq_id = 0;
        {
            std::lock_guard<std::mutex> lock(key_sequence_mutex_);
            auto it = key_sequence_map_.find(entry.object_key);
            if (it != key_sequence_map_.end()) {
                current_key_seq_id = it->second;
                // Key exists and key_sequence_id is not greater
                // Also check that global sequence is correct (only handle key-level violations)
                if (entry.sequence_id == expected_sequence_id_ &&
                    entry.key_sequence_id <= current_key_seq_id) {
                    is_key_violation = true;
                }
            }
        }
        
        if (is_key_violation) {
            // Key-level sequence violation: delete the key and its metadata
            LOG(WARNING) << "OpLogApplier: key sequence order violation detected, "
                        << "deleting key=" << entry.object_key
                        << ", new key_sequence_id=" << entry.key_sequence_id
                        << ", current key_sequence_id=" << current_key_seq_id;
            
            // Delete from metadata store
            metadata_store_->Remove(entry.object_key);
            
            // Delete from key_sequence_map_
            {
                std::lock_guard<std::mutex> lock(key_sequence_mutex_);
                key_sequence_map_.erase(entry.object_key);
            }
            
            // Now the key is deleted, we can continue to process this entry
            // since global sequence is correct (we checked above)
            order_valid = true;
        }
    }
    
    if (!order_valid) {
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
        LOG(WARNING) << "OpLogApplier: global sequence order violation, sequence_id="
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

    // Update key sequence ID
    {
        std::lock_guard<std::mutex> lock(key_sequence_mutex_);
        key_sequence_map_[entry.object_key] = entry.key_sequence_id;
    }

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
    std::lock_guard<std::mutex> lock(key_sequence_mutex_);
    auto it = key_sequence_map_.find(key);
    if (it != key_sequence_map_.end()) {
        return it->second;
    }
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
    
    // Now process pending entries
    std::lock_guard<std::mutex> lock(pending_mutex_);
    size_t processed_count = 0;

    // Process entries in order
    while (!pending_entries_.empty()) {
        auto it = pending_entries_.begin();
        const OpLogEntry& entry = it->second;

        // Check if this entry can be applied now
        if (entry.sequence_id == expected_sequence_id_) {
            // Release lock before applying (to avoid deadlock)
            OpLogEntry entry_copy = entry;
            pending_entries_.erase(it);

            // Apply based on type
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
                    continue;
            }

            // Update expected sequence ID
            expected_sequence_id_ = entry_copy.sequence_id + 1;

            // Update key sequence ID
            {
                std::lock_guard<std::mutex> key_lock(key_sequence_mutex_);
                key_sequence_map_[entry_copy.object_key] = entry_copy.key_sequence_id;
            }

            // Remove from missing list if it was there
            missing_sequence_ids_.erase(entry_copy.sequence_id);

            processed_count++;
        } else {
            // Cannot process more entries yet
            break;
        }
    }

    // Clean up old missing sequence IDs (older than 1 minute)
    auto now = std::chrono::steady_clock::now();
    for (auto it = missing_sequence_ids_.begin(); it != missing_sequence_ids_.end();) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second);
        if (age.count() > 60) {
            // Too old, remove it
            LOG(WARNING) << "OpLogApplier: giving up on missing sequence_id="
                        << it->first << " after " << age.count() << " seconds";
            it = missing_sequence_ids_.erase(it);
        } else {
            ++it;
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
    // Check global sequence order
    if (entry.sequence_id != expected_sequence_id_) {
        return false;
    }

    // Check per-key sequence order
    {
        std::lock_guard<std::mutex> lock(key_sequence_mutex_);
        auto it = key_sequence_map_.find(entry.object_key);
        if (it != key_sequence_map_.end()) {
            // Key exists - check that new key_sequence_id is greater
            if (entry.key_sequence_id <= it->second) {
                LOG(WARNING) << "OpLogApplier: key sequence order violation, key="
                             << entry.object_key
                             << ", new key_sequence_id=" << entry.key_sequence_id
                             << ", current key_sequence_id=" << it->second;
                return false;
            }
        }
        // If key doesn't exist, any key_sequence_id is valid (should be >= 1)
        if (entry.key_sequence_id < 1) {
            LOG(WARNING) << "OpLogApplier: invalid key_sequence_id="
                         << entry.key_sequence_id << " for new key="
                         << entry.object_key;
            return false;
        }
    }

    return true;
}

void OpLogApplier::ApplyPutEnd(const OpLogEntry& entry) {
    // For now, payload is empty in current implementation.
    // In the future, payload may contain serialized metadata.
    // For now, we just mark the key as existing.
    if (!metadata_store_->Put(entry.object_key, entry.payload)) {
        LOG(ERROR) << "OpLogApplier: failed to Put key=" << entry.object_key
                   << ", sequence_id=" << entry.sequence_id;
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_END, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
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

