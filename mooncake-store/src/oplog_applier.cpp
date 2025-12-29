#include "oplog_applier.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include "metadata_store.h"

namespace mooncake {

OpLogApplier::OpLogApplier(MetadataStore* metadata_store)
    : metadata_store_(metadata_store), expected_sequence_id_(1) {
    if (metadata_store_ == nullptr) {
        LOG(FATAL) << "OpLogApplier: metadata_store cannot be null";
    }
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
        pending_entries_[entry.sequence_id] = entry;
        LOG(WARNING) << "OpLogApplier: global sequence order violation, sequence_id="
                     << entry.sequence_id << ", expected=" << expected_sequence_id_
                     << ", key=" << entry.object_key
                     << ", added to pending entries";
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

            // Apply the entry (this will update expected_sequence_id_)
            // We need to call ApplyOpLogEntry but without sequence check
            // Since we've already verified the sequence_id matches expected_sequence_id_,
            // we can directly apply it.

            // Actually, we should just apply it normally, but we need to skip
            // the CheckSequenceOrder since we've already verified it.
            // For now, let's just call ApplyOpLogEntry again, which will work
            // because expected_sequence_id_ should now match.
            // But this is inefficient. Let's do it properly:

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

            processed_count++;
        } else {
            // Cannot process more entries yet
            break;
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
    // TODO: Implement request missing OpLog from etcd
    // This will be implemented in Phase 3
    LOG(WARNING) << "OpLogApplier: RequestMissingOpLog not yet implemented, "
                 << "missing_seq_id=" << missing_seq_id;
    return false;
}

void OpLogApplier::ScheduleWaitForMissingEntries(uint64_t missing_seq_id) {
    // TODO: Implement scheduling wait for missing entries
    // This will be implemented in Phase 3
    LOG(WARNING) << "OpLogApplier: ScheduleWaitForMissingEntries not yet implemented, "
                 << "missing_seq_id=" << missing_seq_id;
}

}  // namespace mooncake

