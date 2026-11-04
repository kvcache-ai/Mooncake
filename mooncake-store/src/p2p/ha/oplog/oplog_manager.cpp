#include "p2p/ha/oplog/oplog_manager.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <xxhash.h>
#include <glog/logging.h>

#include "p2p/ha/oplog/oplog_store.h"

namespace mooncake {

OpLogManager::OpLogManager() = default;

void OpLogManager::SetOpLogStore(std::shared_ptr<OpLogStore> oplog_store) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    oplog_store_ = oplog_store;
}

void OpLogManager::RequestStopWriting() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto store = oplog_store_;
    lock.unlock();
    if (store) {
        store->RequestStopWriting();
    }
}

void OpLogManager::StopWriting() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto store = oplog_store_;
    lock.unlock();
    if (store) {
        store->StopWriting();
    }
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
    if (oplog_store_) {
        bool sync = (type != OpType::PUT_END);
        if (sync) {
            // Release lock before the blocking wait to avoid holding
            // mutex_ for the entire etcd round-trip.
            lock.unlock();
        }
        ErrorCode err = oplog_store_->WriteOpLog(entry, sync);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to write OpLog to store, sequence_id="
                         << seq << ", but entry is in memory buffer";
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

ErrorCode OpLogManager::PersistEntry(const OpLogEntry& entry, bool sync) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto store = oplog_store_;
    lock.unlock();
    if (!store) {
        return ErrorCode::INTERNAL_ERROR;
    }
    return store->WriteOpLog(entry, sync);
}

tl::expected<uint64_t, ErrorCode> OpLogManager::AppendAndPersist(
    OpType type, const std::string& key, const std::string& payload,
    bool sync) {
    // Seq pre-allocation semantics: allocate first, then persist.
    OpLogEntry entry = AllocateEntry(type, key, payload);
    ErrorCode err = PersistEntry(entry, sync);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return entry.sequence_id;
}

uint64_t OpLogManager::GetLastSequenceId() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return last_seq_id_;
}

tl::expected<uint64_t, ErrorCode> OpLogManager::GetLocalCommittedSequenceId()
    const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto store = oplog_store_;
    lock.unlock();
    if (!store) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    uint64_t sequence_id = 0;
    ErrorCode err = store->GetLocalCommittedSequenceId(sequence_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return sequence_id;
}

tl::expected<std::vector<OpLogEntry>, ErrorCode>
OpLogManager::GetBufferedEntriesAfter(uint64_t sequence_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (sequence_id >= last_seq_id_) {
        return std::vector<OpLogEntry>();
    }
    if (buffer_.empty() || sequence_id + 1 < first_seq_id_) {
        LOG(WARNING) << "OpLogManager: requested buffered OpLog range is no "
                        "longer available"
                     << ", sequence_id=" << sequence_id
                     << ", first_seq_id=" << first_seq_id_
                     << ", last_seq_id=" << last_seq_id_;
        return tl::make_unexpected(ErrorCode::OPLOG_ENTRY_NOT_FOUND);
    }

    std::vector<OpLogEntry> entries;
    entries.reserve(last_seq_id_ - sequence_id);
    for (const auto& entry : buffer_) {
        if (entry.sequence_id > sequence_id) {
            entries.push_back(entry);
        }
    }
    return entries;
}

ErrorCode OpLogManager::SetInitialSequenceId(uint64_t sequence_id) {
    std::shared_ptr<OpLogStore> store;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        store = oplog_store_;
    }
    uint64_t persisted_latest = 0;
    if (store) {
        // Do not hold the manager lock over I/O: RequestStopWriting must
        // remain responsive if leadership is lost during initialization.
        uint64_t local_committed = 0;
        auto err = store->GetLocalCommittedSequenceId(local_committed);
        if (err != ErrorCode::OK) {
            return err;
        }
        err = store->GetLatestSequenceId(persisted_latest);
        if (err != ErrorCode::OK) {
            return err;
        }
        if (local_committed != persisted_latest) {
            LOG(ERROR) << "Cannot initialize OpLog sequence: latest changed "
                          "since writer initialization"
                       << ", local_committed=" << local_committed
                       << ", persisted_latest=" << persisted_latest;
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (last_seq_id_ != 0 || !buffer_.empty() || store != oplog_store_) {
        LOG(ERROR)
            << "Cannot initialize OpLog sequence: manager was used or store "
               "changed during initialization "
            << "(last_seq_id_=" << last_seq_id_
            << ", buffer_size=" << buffer_.size() << ")";
        return ErrorCode::INVALID_PARAMS;
    }
    const auto initial_sequence_id = std::max(sequence_id, persisted_latest);
    if (initial_sequence_id == std::numeric_limits<uint64_t>::max()) {
        LOG(ERROR) << "Cannot initialize OpLog sequence: sequence exhausted"
                   << ", initial_sequence_id=" << initial_sequence_id;
        return ErrorCode::INVALID_PARAMS;
    }
    last_seq_id_ = initial_sequence_id;
    first_seq_id_ = initial_sequence_id + 1;
    LOG(INFO) << "OpLogManager initial sequence_id set to "
              << initial_sequence_id << ", restored_sequence_id=" << sequence_id
              << ", persisted_latest=" << persisted_latest;
    return ErrorCode::OK;
}

size_t OpLogManager::GetEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return buffer_.size();
}

ErrorCode OpLogManager::CleanupOpLogBefore(uint64_t before_sequence_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!oplog_store_) {
        return ErrorCode::OK;
    }
    return oplog_store_->CleanupOpLogBefore(before_sequence_id);
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
