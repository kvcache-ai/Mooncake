#pragma once

#include <cstdint>
#include <memory>
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
    tl::expected<void, WalError> Sync();

    uint64_t tail() const { return tail_; }
    const std::string& path() const { return path_; }

   private:
    WalWriter(std::string path, int fd, uint64_t tail);

    std::string path_;
    int fd_;
    uint64_t tail_;
};

tl::expected<WalScanResult, WalError> ScanWal(const std::string& path);
tl::expected<void, WalError> ReplayWal(const std::vector<WalRecord>& records,
                                       VersionIndex& index);

}  // namespace mooncake::logstructured
