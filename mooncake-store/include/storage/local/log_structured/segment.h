#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/local/log_structured/record_format.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class SegmentError {
    kInvalidArgument,
    kOpenFailed,
    kIoError,
    kSyncFailed,
    kTruncateFailed,
};

enum class ScanTermination {
    kCleanEof,
    kIncompleteTail,
    kCorruptRecord,
};

struct PhysicalRecord {
    uint64_t segment_id{0};
    uint64_t record_offset{0};
    uint64_t value_offset{0};
    uint64_t value_length{0};
    uint64_t total_length{0};

    bool operator==(const PhysicalRecord&) const = default;
};

struct ScannedRecord {
    RecordIdentity identity;
    RecordKind kind{RecordKind::kValue};
    uint64_t sequence{0};
    PhysicalRecord physical;
};

struct SegmentScanResult {
    std::vector<ScannedRecord> records;
    uint64_t valid_bytes{0};
    ScanTermination termination{ScanTermination::kCleanEof};
    DecodeError decode_error{DecodeError::kNeedMoreData};
};

class SegmentWriter {
   public:
    static tl::expected<std::unique_ptr<SegmentWriter>, SegmentError> Create(
        std::string path, uint64_t segment_id);

    static tl::expected<std::unique_ptr<SegmentWriter>, SegmentError>
    OpenForAppend(std::string path, uint64_t segment_id, uint64_t valid_bytes);

    ~SegmentWriter();

    SegmentWriter(const SegmentWriter&) = delete;
    SegmentWriter& operator=(const SegmentWriter&) = delete;

    tl::expected<PhysicalRecord, SegmentError> Append(
        const RecordIdentity& identity, std::string_view value, RecordKind kind,
        uint64_t sequence, bool sync);

    tl::expected<void, SegmentError> Sync();

    uint64_t tail() const { return tail_; }
    uint64_t segment_id() const { return segment_id_; }
    const std::string& path() const { return path_; }

   private:
    static tl::expected<std::unique_ptr<SegmentWriter>, SegmentError> Open(
        std::string path, uint64_t segment_id, int flags, uint64_t tail);

    SegmentWriter(std::string path, uint64_t segment_id, int fd, uint64_t tail);

    std::string path_;
    uint64_t segment_id_;
    int fd_;
    uint64_t tail_;
};

tl::expected<SegmentScanResult, SegmentError> ScanSegment(
    const std::string& path, uint64_t segment_id);

tl::expected<DecodedRecord, SegmentError> ReadRecord(
    const std::string& path, const PhysicalRecord& physical);

tl::expected<void, SegmentError> TruncateSegment(const std::string& path,
                                                 uint64_t valid_bytes);

}  // namespace mooncake::logstructured
