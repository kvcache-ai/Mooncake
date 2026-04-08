// mooncake-store/tests/hot_standby_ut/mock_oplog_store.h
#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store.h"
#include "types.h"

namespace mooncake::test {

// In-memory OpLog store for unit tests.
class MockOpLogStore : public OpLogStore {
   public:
    MockOpLogStore() = default;

    ErrorCode Init() override { return ErrorCode::OK; }

    ErrorCode WriteOpLog(const OpLogEntry& entry,
                         bool /*sync*/ = true) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (write_error_ != ErrorCode::OK) {
            return write_error_;
        }
        entries_[entry.sequence_id] = entry;
        if (entry.sequence_id > latest_seq_id_) {
            latest_seq_id_ = entry.sequence_id;
        }
        return ErrorCode::OK;
    }

    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (read_error_ != ErrorCode::OK) {
            return read_error_;
        }
        auto it = entries_.find(sequence_id);
        if (it == entries_.end()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        entry = it->second;
        return ErrorCode::OK;
    }

    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (read_error_ != ErrorCode::OK) {
            return read_error_;
        }
        entries.clear();
        for (const auto& [seq, entry] : entries_) {
            if (seq > start_sequence_id) {
                entries.push_back(entry);
                if (entries.size() >= limit) break;
            }
        }
        return ErrorCode::OK;
    }

    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        sequence_id = latest_seq_id_;
        return ErrorCode::OK;
    }

    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (entries_.empty()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        sequence_id = entries_.rbegin()->first;
        return ErrorCode::OK;
    }

    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        latest_seq_id_ = sequence_id;
        return ErrorCode::OK;
    }

    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshots_[snapshot_id] = sequence_id;
        return ErrorCode::OK;
    }

    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = snapshots_.find(snapshot_id);
        if (it == snapshots_.end()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        sequence_id = it->second;
        return ErrorCode::OK;
    }

    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = entries_.begin(); it != entries_.end();) {
            if (it->first < before_sequence_id) {
                it = entries_.erase(it);
            } else {
                break;
            }
        }
        return ErrorCode::OK;
    }

    // === Test control methods ===

    void SetWriteError(ErrorCode err) { write_error_ = err; }
    void SetReadError(ErrorCode err) { read_error_ = err; }
    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.clear();
        snapshots_.clear();
        latest_seq_id_ = 0;
    }
    size_t EntryCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.size();
    }

   private:
    mutable std::mutex mutex_;
    std::map<uint64_t, OpLogEntry> entries_;
    std::map<std::string, uint64_t> snapshots_;
    uint64_t latest_seq_id_{0};
    ErrorCode write_error_{ErrorCode::OK};
    ErrorCode read_error_{ErrorCode::OK};
};

}  // namespace mooncake::test
