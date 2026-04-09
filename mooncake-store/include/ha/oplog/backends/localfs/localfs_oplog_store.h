#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace localfs {

class LocalFsOpLogStore final : public OpLogStore {
   public:
    LocalFsOpLogStore(std::string root_dir, ClusterNamespace cluster_namespace,
                      bool enable_batch_write);
    ~LocalFsOpLogStore() override;

    ErrorCode Init();

    tl::expected<OpLogSequenceId, ErrorCode> Append(
        const OpLogAppendRequest& request) override;

    tl::expected<OpLogPollResult, ErrorCode> PollFrom(
        OpLogSequenceId start_seq, size_t max_records,
        std::chrono::milliseconds timeout) override;

    tl::expected<OpLogSequenceId, ErrorCode> GetLatestSequence() override;

    ErrorCode CleanupBefore(OpLogSequenceId before_sequence_id) override;

   private:
    struct SegmentHeader {
        char magic[4];
        uint32_t version;
        uint64_t min_seq;
        uint64_t max_seq;
        uint32_t entry_count;
        uint32_t reserved;
    };
    static_assert(sizeof(SegmentHeader) == 32,
                  "LocalFsOpLogStore::SegmentHeader must be 32 bytes");

    struct SegmentInfo {
        std::string filename;
        OpLogSequenceId min_seq{0};
        OpLogSequenceId max_seq{0};
    };

    struct PendingEntry {
        std::string payload;
        OpLogSequenceId sequence_id{0};
    };

    static constexpr char kSegmentMagic[4] = {'M', 'C', 'S', 'G'};
    static constexpr uint32_t kSegmentVersion = 1;
    static constexpr size_t kSegmentHeaderSize = sizeof(SegmentHeader);
    static constexpr auto kPollRetrySleep = std::chrono::milliseconds(50);
    static constexpr auto kBatchTimeout = std::chrono::milliseconds(100);
    static constexpr auto kSyncWaitTimeout = std::chrono::seconds(3);
    static constexpr auto kFlushRetryInterval = std::chrono::milliseconds(50);
    static constexpr int kFlushRetryCount = 3;

    static bool ParseSegmentFilename(const std::string& filename,
                                     OpLogSequenceId& min_seq,
                                     OpLogSequenceId& max_seq);

    std::string ClusterDir() const;
    std::string SegmentsDir() const;
    std::string LatestFilePath() const;
    std::string BuildSegmentFilename(OpLogSequenceId min_seq,
                                     OpLogSequenceId max_seq) const;

    void CleanupTempFiles() const;
    ErrorCode RecoverPersistedState();
    ErrorCode AtomicWriteFile(const std::string& target_path, const void* data,
                              size_t size) const;
    ErrorCode AtomicWriteFile(const std::string& target_path,
                              const std::string& content) const;
    ErrorCode ReadUint64FromFile(const std::string& filepath,
                                 OpLogSequenceId& value) const;

    std::vector<SegmentInfo> ListSegments() const;
    ErrorCode NormalizePendingEntries(std::vector<PendingEntry>& entries) const;
    ErrorCode WriteSegmentFile(const std::vector<PendingEntry>& entries) const;
    ErrorCode ReadSegmentRecords(const std::string& filepath,
                                 std::vector<OpLogRecord>& records) const;
    ErrorCode ReadPayloadForSequence(OpLogSequenceId sequence_id,
                                     std::string& payload) const;
    ErrorCode VerifyPersistedPayloadMatches(OpLogSequenceId sequence_id,
                                            const std::string& payload) const;

    void BatchWriteLoop();
    ErrorCode FlushPendingBatch();

    std::string root_dir_;
    ClusterNamespace cluster_namespace_;
    bool enable_batch_write_{false};

    mutable std::mutex batch_mutex_;
    std::deque<PendingEntry> pending_batch_;
    std::condition_variable cv_batch_updated_;
    std::condition_variable cv_sync_completed_;
    std::atomic<bool> batch_write_running_{false};
    std::thread batch_write_thread_;
    std::atomic<OpLogSequenceId> last_persisted_seq_id_{0};
};

}  // namespace localfs
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
