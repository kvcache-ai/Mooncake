#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "storage/local/log_structured/index.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class WalRecordType : uint16_t {
    kPrepareValue = 1,
    kCommitValue = 2,
    kAbortValue = 3,
    kApplyTombstone = 4,
};

enum class WalError {
    kInvalidArgument,
    kOpenFailed,
    kIoError,
    kSyncFailed,
    kTruncateFailed,
    kCorruptRecord,
    kReplayFailed,
};

struct WalRecord {
    WalRecordType type{WalRecordType::kPrepareValue};
    uint64_t sequence{0};
    RecordIdentity identity;
    PhysicalRecord physical;

    bool operator==(const WalRecord&) const = default;
};

enum class WalScanTermination {
    kCleanEof,
    kIncompleteTail,
    kCorruptRecord,
};

struct WalScanResult {
    std::vector<WalRecord> records;
    uint64_t valid_bytes{0};
    WalScanTermination termination{WalScanTermination::kCleanEof};
};

struct WalWriterStats {
    uint64_t append_requests{0};
    uint64_t appended_records{0};
    uint64_t write_groups{0};
    uint64_t sync_groups{0};
    uint64_t grouped_requests{0};
    uint64_t max_group_requests{0};
    uint64_t appended_bytes{0};
};

class WalWriter {
   public:
    static tl::expected<std::unique_ptr<WalWriter>, WalError> Create(
        std::string path);
    static tl::expected<std::unique_ptr<WalWriter>, WalError> OpenForAppend(
        std::string path, uint64_t valid_bytes);

    ~WalWriter();

    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;

    tl::expected<void, WalError> Append(const WalRecord& record, bool sync);
    tl::expected<void, WalError> AppendBatch(
        const std::vector<WalRecord>& records, bool sync);
    tl::expected<void, WalError> Sync();

    uint64_t tail() const;
    WalWriterStats SnapshotStats() const;
    const std::string& path() const { return path_; }

    static void SetBeforeWriteHookForTest(std::function<void()> hook);

   private:
    struct PendingAppend {
        std::string encoded;
        size_t record_count{0};
        bool sync{false};
        bool leader{false};
        bool done{false};
        std::optional<WalError> error;
    };

    WalWriter(std::string path, int fd, uint64_t tail);

    tl::expected<void, WalError> Submit(PendingAppend& request);
    void RunWriter();
    void CompleteGroup(const std::vector<PendingAppend*>& group,
                       std::optional<WalError> error);

    std::string path_;
    int fd_;
    mutable std::mutex mutex_;
    std::condition_variable completion_cv_;
    std::deque<PendingAppend*> pending_;
    bool writer_active_{false};
    std::optional<WalError> terminal_error_;
    uint64_t tail_;
    WalWriterStats stats_;
};

tl::expected<WalScanResult, WalError> ScanWal(const std::string& path);
tl::expected<void, WalError> ReplayWal(const std::vector<WalRecord>& records,
                                       VersionIndex& index);

}  // namespace mooncake::logstructured
