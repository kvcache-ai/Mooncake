#include "oplog_manager.h"

#include <algorithm>
#include <chrono>
#include <xxhash.h>
#include <glog/logging.h>

#include "etcd_oplog_store.h"

namespace mooncake {

OpLogManager::OpLogManager() = default;

void OpLogManager::SetEtcdOpLogStore(
    std::shared_ptr<EtcdOpLogStore> etcd_oplog_store) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    etcd_oplog_store_ = etcd_oplog_store;
}

uint64_t OpLogManager::Append(OpType type, const std::string& key,
                              const std::string& payload) {
    OpLogEntry entry;
    entry.op_type = type;
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = NowMs();
    entry.checksum = ComputeChecksum(entry.payload);
    entry.prefix_hash = ComputePrefixHash(entry.object_key);

    std::unique_lock<std::shared_mutex> lock(mutex_);
    entry.sequence_id = ++last_seq_id_;
    
    // Note: We use global sequence_id for ordering guarantee.
    // key_sequence_id is set to sequence_id for backward compatibility,
    // but the actual ordering is based on global sequence_id.
    entry.key_sequence_id = entry.sequence_id;

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }

    buffer_.emplace_back(entry);  // Copy entry to buffer
    
    // Write to etcd if EtcdOpLogStore is set
    if (etcd_oplog_store_) {
        // Release lock before writing to etcd to avoid blocking
        // We use the original entry (before it was copied to buffer)
        lock.unlock();
        ErrorCode err = etcd_oplog_store_->WriteOpLog(entry);
        if (err != ErrorCode::OK) {
            // Log error but don't fail the operation
            // The entry is already in the memory buffer
            LOG(WARNING) << "Failed to write OpLog to etcd, sequence_id="
                         << entry.sequence_id
                         << ", but entry is in memory buffer";
        }
    }
    
    return last_seq_id_;
}

uint64_t OpLogManager::GetLastSequenceId() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return last_seq_id_;
}

void OpLogManager::SetInitialSequenceId(uint64_t sequence_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (last_seq_id_ == 0 && buffer_.empty()) {
        // Only allow setting initial sequence_id if OpLogManager is empty
        last_seq_id_ = sequence_id;
        first_seq_id_ = sequence_id + 1;  // first_seq_id_ should be > last_seq_id_ when empty
        LOG(INFO) << "OpLogManager initial sequence_id set to " << sequence_id;
    } else {
        LOG(WARNING) << "Cannot set initial sequence_id: OpLogManager is not empty "
                     << "(last_seq_id_=" << last_seq_id_ << ", buffer_size=" << buffer_.size() << ")";
    }
}

size_t OpLogManager::GetEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return buffer_.size();
}

uint64_t OpLogManager::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

uint32_t OpLogManager::ComputeChecksum(const std::string& data) {
    // Use xxHash XXH32 for a fast, deterministic 32-bit checksum.
    // Requires linking against xxHash (e.g., libxxhash) and including <xxhash.h>.
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

uint32_t OpLogManager::ComputePrefixHash(const std::string& key) {
    if (key.empty()) {
        return 0;
    }
    // Use XXH32 for consistency with ComputeChecksum and better performance.
    // XXH32 provides faster hashing and lower collision rate than std::hash.
    // Computing hash for the entire key ensures better distribution and fewer collisions.
    return static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
}

}  // namespace mooncake


