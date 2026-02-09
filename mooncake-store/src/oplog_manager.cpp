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
    const uint64_t seq = entry.sequence_id;  // save before potential unlock

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }

    buffer_.emplace_back(entry);  // Copy entry to buffer

    // Write to etcd if EtcdOpLogStore is set.
    // Strategy: PUT_END is async (sync=false) — only pushes to batch queue
    // (microsecond-level), safe to hold mutex_.
    // REMOVE / PUT_REVOKE are sync (sync=true) — blocks until etcd confirms
    // persistence; must release mutex_ to avoid blocking other Append calls
    // during the wait.  The caller relies on sync semantics to know the
    // entry is durable before freeing/reusing associated memory.
    if (etcd_oplog_store_) {
        bool sync = (type != OpType::PUT_END);
        if (sync) {
            // Release lock before the blocking wait to avoid holding
            // mutex_ for the entire etcd round-trip.
            lock.unlock();
        }
        ErrorCode err = etcd_oplog_store_->WriteOpLog(entry, sync);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to write OpLog to etcd, sequence_id=" << seq
                         << ", but entry is in memory buffer";
        }
    }

    return seq;
}

OpLogEntry OpLogManager::AllocateEntry(OpType type, const std::string& key,
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

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }
    buffer_.emplace_back(entry);
    return entry;
}

ErrorCode OpLogManager::PersistEntryToEtcd(const OpLogEntry& entry) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto store = etcd_oplog_store_;
    lock.unlock();
    if (!store) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    // Strategy 2+: PUT_END is Async, REMOVE (and others) are Sync
    bool sync = (entry.op_type != OpType::PUT_END);
    return store->WriteOpLog(entry, sync);
}

tl::expected<uint64_t, ErrorCode> OpLogManager::AppendAndPersist(
    OpType type, const std::string& key, const std::string& payload) {
    // Seq pre-allocation semantics: allocate first, then persist.
    OpLogEntry entry = AllocateEntry(type, key, payload);
    ErrorCode err = PersistEntryToEtcd(entry);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return entry.sequence_id;
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
        first_seq_id_ = sequence_id +
                        1;  // first_seq_id_ should be > last_seq_id_ when empty
        LOG(INFO) << "OpLogManager initial sequence_id set to " << sequence_id;
    } else {
        LOG(WARNING)
            << "Cannot set initial sequence_id: OpLogManager is not empty "
            << "(last_seq_id_=" << last_seq_id_
            << ", buffer_size=" << buffer_.size() << ")";
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
    // Requires linking against xxHash (e.g., libxxhash) and including
    // <xxhash.h>.
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

uint32_t OpLogManager::ComputePrefixHash(const std::string& key) {
    if (key.empty()) {
        return 0;
    }
    // Use XXH32 for consistency with ComputeChecksum and better performance.
    // XXH32 provides faster hashing and lower collision rate than std::hash.
    // Computing hash for the entire key ensures better distribution and fewer
    // collisions.
    return static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
}

bool OpLogManager::VerifyChecksum(const OpLogEntry& entry) {
    uint32_t computed = ComputeChecksum(entry.payload);
    return computed == entry.checksum;
}

bool OpLogManager::ValidateEntrySize(const OpLogEntry& entry,
                                     std::string* reason) {
    if (entry.object_key.size() > kMaxObjectKeySize) {
        if (reason) {
            *reason = "object_key too large: size=" +
                      std::to_string(entry.object_key.size());
        }
        return false;
    }
    if (entry.payload.size() > kMaxPayloadSize) {
        if (reason) {
            *reason = "payload too large: size=" +
                      std::to_string(entry.payload.size());
        }
        return false;
    }
    return true;
}

}  // namespace mooncake
