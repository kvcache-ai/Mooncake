// mooncake-store/include/localfs_oplog_store.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store.h"
#include "types.h"

namespace mooncake {

class LocalFsOpLogStore : public OpLogStore {
   public:
    explicit LocalFsOpLogStore(const std::string& cluster_id,
                               const std::string& root_dir,
                               bool enable_batch_write,
                               int poll_interval_ms = 1000);
    ~LocalFsOpLogStore();

    ErrorCode Init() override;
    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true) override;
    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override;
    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override;
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override;
    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override;
    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override;
    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override;
    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override;
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override;
    std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string& cluster_id) override;

   private:
    // Segment file format: 32-byte header + length-prefixed entries
    static constexpr char kSegmentMagic[4] = {'M', 'C', 'S', 'G'};
    static constexpr uint32_t kSegmentVersion = 1;
    static constexpr size_t kSegmentHeaderSize = 32;

    // Segment header layout (all little-endian):
    //   [0..3]   magic    "MCSG"
    //   [4..7]   version  uint32_t
    //   [8..15]  min_seq  uint64_t
    //   [16..23] max_seq  uint64_t
    //   [24..27] count    uint32_t
    //   [28..31] reserved uint32_t (0)
    struct SegmentHeader {
        char magic[4];
        uint32_t version;
        uint64_t min_seq;
        uint64_t max_seq;
        uint32_t entry_count;
        uint32_t reserved;
    };
    static_assert(sizeof(SegmentHeader) == kSegmentHeaderSize,
                  "SegmentHeader must be 32 bytes");

    // Segment file name info parsed from filename
    struct SegmentInfo {
        std::string filename;
        uint64_t min_seq;
        uint64_t max_seq;
    };

    // Group Commit batch entry
    struct BatchEntry {
        std::string serialized_value;
        uint64_t sequence_id;
        bool is_sync;
    };

    // Directory and path helpers
    std::string SegmentsDir() const;
    std::string SnapshotsDir() const;
    std::string LatestFilePath() const;
    std::string BuildSegmentFilename(uint64_t min_seq, uint64_t max_seq) const;
    std::string BuildSnapshotPath(const std::string& snapshot_id) const;

    // Segment I/O
    ErrorCode WriteSegmentFile(const std::vector<BatchEntry>& entries);
    ErrorCode ReadSegmentEntries(const std::string& filepath,
                                 std::vector<OpLogEntry>& entries);
    ErrorCode ReadSegmentHeader(const std::string& filepath,
                                SegmentHeader& header);
    std::vector<SegmentInfo> ListSegments() const;
    static bool ParseSegmentFilename(const std::string& filename,
                                     uint64_t& min_seq, uint64_t& max_seq);
    ErrorCode NormalizeBatchEntries(std::vector<BatchEntry>& entries) const;
    ErrorCode VerifyPersistedEntryMatches(uint64_t sequence_id,
                                          const std::string& serialized_value);
    ErrorCode RecoverPersistedState();

    // Atomic file write: write to .tmp, fsync, rename
    ErrorCode AtomicWriteFile(const std::string& target_path,
                              const std::string& content);
    ErrorCode AtomicWriteFile(const std::string& target_path, const void* data,
                              size_t size);

    // Cleanup temp files from previous crash
    void CleanupTempFiles();

    // Snapshot ID validation
    static bool ValidateSnapshotId(const std::string& snapshot_id);

    // Read a uint64 value from a single-value text file
    ErrorCode ReadUint64FromFile(const std::string& filepath,
                                 uint64_t& value) const;

    // Batch write thread
    void BatchWriteThread();
    void FlushBatch();

    // Members
    std::string cluster_id_;
    std::string root_dir_;
    std::string cluster_dir_;  // root_dir_/cluster_id_
    bool enable_batch_write_;
    int poll_interval_ms_;

    // Group Commit state
    mutable std::mutex batch_mutex_;
    std::deque<BatchEntry> pending_batch_;
    std::condition_variable cv_batch_updated_;
    std::condition_variable cv_sync_completed_;
    std::atomic<bool> batch_write_running_{false};
    std::thread batch_write_thread_;
    std::atomic<uint64_t> last_persisted_seq_id_{0};

    // Batch write configs
    static constexpr size_t kBatchCountLimit = 100;
    static constexpr int kBatchTimeoutMs = 100;
    static constexpr int kSyncWaitTimeoutMs = 3000;
    static constexpr int kFlushRetryCount = 3;
    static constexpr int kFlushRetryIntervalMs = 50;
};

}  // namespace mooncake
