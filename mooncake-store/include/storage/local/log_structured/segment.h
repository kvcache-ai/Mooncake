#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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

struct SegmentAppendRequest {
    RecordIdentity identity;
    std::string_view value;
    RecordKind kind{RecordKind::kValue};
    uint64_t sequence{0};
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
    tl::expected<std::vector<PhysicalRecord>, SegmentError> AppendBatch(
        const std::vector<SegmentAppendRequest>& requests, bool sync,
        size_t parallelism);

    tl::expected<void, SegmentError> Sync();

    static void SetWriteFailurePredicateForTest(
        std::function<bool(std::string_view path, uint64_t offset,
                           size_t length)>
            predicate);

    uint64_t tail() const;
    uint64_t segment_id() const { return segment_id_; }
    const std::string& path() const { return path_; }

   private:
    static tl::expected<std::unique_ptr<SegmentWriter>, SegmentError> Open(
        std::string path, uint64_t segment_id, int flags, uint64_t tail);

    SegmentWriter(std::string path, uint64_t segment_id, int fd, uint64_t tail);

    std::string path_;
    uint64_t segment_id_;
    int fd_;
    mutable std::mutex append_mutex_;
    uint64_t tail_;
};

class SegmentReader {
   public:
    static tl::expected<std::shared_ptr<SegmentReader>, SegmentError> Open(
        std::string path, uint64_t segment_id);

    ~SegmentReader();

    SegmentReader(const SegmentReader&) = delete;
    SegmentReader& operator=(const SegmentReader&) = delete;

    tl::expected<DecodedRecord, SegmentError> Read(
        const PhysicalRecord& physical) const;
    tl::expected<void, SegmentError> ReadValue(const PhysicalRecord& physical,
                                               char* value,
                                               size_t value_size) const;

    uint64_t segment_id() const { return segment_id_; }
    const std::string& path() const { return path_; }

   private:
    SegmentReader(std::string path, uint64_t segment_id, int fd);

    std::string path_;
    uint64_t segment_id_;
    int fd_;
};

tl::expected<SegmentScanResult, SegmentError> ScanSegment(
    const std::string& path, uint64_t segment_id);

tl::expected<DecodedRecord, SegmentError> ReadRecord(
    const std::string& path, const PhysicalRecord& physical);

tl::expected<void, SegmentError> TruncateSegment(const std::string& path,
                                                 uint64_t valid_bytes);

}  // namespace mooncake::logstructured
