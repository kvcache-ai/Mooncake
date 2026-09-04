#include "storage/local/log_structured/segment.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <type_traits>

#include "crc32c.h"
#include "storage/local/log_structured/uring_batch_io.h"

namespace mooncake::logstructured {
namespace {

constexpr size_t kScanBufferSize = 64 * 1024;
constexpr size_t kFooterLengthOffset = 0;
constexpr size_t kFooterPayloadChecksumOffset = 8;
constexpr size_t kFooterChecksumOffset = 12;
constexpr size_t kFooterMagicOffset = 16;
constexpr size_t kFooterChecksumInputSize = kFooterChecksumOffset;

std::mutex write_failure_mutex;
std::function<bool(std::string_view, uint64_t, size_t)> write_failure_predicate;

bool ShouldFailWrite(std::string_view path, uint64_t offset, size_t length) {
    std::lock_guard lock(write_failure_mutex);
    return write_failure_predicate &&
           write_failure_predicate(path, offset, length);
}

template <typename T>
T ReadLittleEndian(const char* input) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<Unsigned>(static_cast<unsigned char>(input[i]))
                 << (i * 8);
    }
    return static_cast<T>(value);
}

bool PwriteAll(int fd, const char* data, size_t length, uint64_t offset) {
    size_t written = 0;
    while (written < length) {
        const ssize_t result =
            pwrite(fd, data + written, length - written, offset + written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        written += static_cast<size_t>(result);
    }
    return true;
}

bool PreadAll(int fd, char* data, size_t length, uint64_t offset) {
    size_t read_bytes = 0;
    while (read_bytes < length) {
        const ssize_t result = pread(fd, data + read_bytes, length - read_bytes,
                                     offset + read_bytes);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        read_bytes += static_cast<size_t>(result);
    }
    return true;
}

}  // namespace

SegmentWriter::SegmentWriter(std::string path, uint64_t segment_id, int fd,
                             uint64_t tail)
    : path_(std::move(path)), segment_id_(segment_id), fd_(fd), tail_(tail) {}

SegmentWriter::~SegmentWriter() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

void SegmentWriter::SetWriteFailurePredicateForTest(
    std::function<bool(std::string_view, uint64_t, size_t)> predicate) {
    std::lock_guard lock(write_failure_mutex);
    write_failure_predicate = std::move(predicate);
}

tl::expected<std::unique_ptr<SegmentWriter>, SegmentError>
SegmentWriter::Create(std::string path, uint64_t segment_id) {
    return Open(std::move(path), segment_id,
                O_CREAT | O_TRUNC | O_RDWR | O_CLOEXEC, 0);
}

tl::expected<std::unique_ptr<SegmentWriter>, SegmentError> SegmentWriter::Open(
    std::string path, uint64_t segment_id, int flags, uint64_t tail) {
    const int fd = open(path.c_str(), flags, 0644);
    if (fd < 0) {
        return tl::unexpected(SegmentError::kOpenFailed);
    }
    return std::unique_ptr<SegmentWriter>(
        new SegmentWriter(std::move(path), segment_id, fd, tail));
}

tl::expected<std::unique_ptr<SegmentWriter>, SegmentError>
SegmentWriter::OpenForAppend(std::string path, uint64_t segment_id,
                             uint64_t valid_bytes) {
    const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        return tl::unexpected(SegmentError::kOpenFailed);
    }
    struct stat file_stat{};
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size < 0 ||
        valid_bytes > static_cast<uint64_t>(file_stat.st_size)) {
        close(fd);
        return tl::unexpected(SegmentError::kInvalidArgument);
    }
    if (ftruncate(fd, static_cast<off_t>(valid_bytes)) != 0) {
        close(fd);
        return tl::unexpected(SegmentError::kTruncateFailed);
    }
    return std::unique_ptr<SegmentWriter>(
        new SegmentWriter(std::move(path), segment_id, fd, valid_bytes));
}

tl::expected<PhysicalRecord, SegmentError> SegmentWriter::Append(
    const RecordIdentity& identity, std::string_view value, RecordKind kind,
    uint64_t sequence, bool sync) {
    std::lock_guard lock(append_mutex_);
    auto envelope = EncodeRecordEnvelope(identity, value, kind, sequence);
    if (!envelope) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }
    if (tail_ > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                    envelope->total_length) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }

    uint64_t cursor = tail_;
    const std::array<std::string_view, 4> payload_parts = {
        std::string_view(envelope->header.data(), envelope->header.size()),
        identity.tenant_id, identity.object_key, value};
    for (const auto part : payload_parts) {
        if (!part.empty() &&
            (ShouldFailWrite(path_, cursor, part.size()) ||
             !PwriteAll(fd_, part.data(), part.size(), cursor))) {
            return tl::unexpected(SegmentError::kIoError);
        }
        cursor += part.size();
    }
    if (envelope->padding_length > 0) {
        static constexpr std::array<char, kRecordAlignment> kZeroPadding{};
        if (ShouldFailWrite(path_, cursor, envelope->padding_length) ||
            !PwriteAll(fd_, kZeroPadding.data(), envelope->padding_length,
                       cursor)) {
            return tl::unexpected(SegmentError::kIoError);
        }
        cursor += envelope->padding_length;
    }
    if (ShouldFailWrite(path_, cursor, envelope->footer.size()) ||
        !PwriteAll(fd_, envelope->footer.data(), envelope->footer.size(),
                   cursor)) {
        return tl::unexpected(SegmentError::kIoError);
    }
    if (sync && fdatasync(fd_) != 0) {
        return tl::unexpected(SegmentError::kSyncFailed);
    }

    PhysicalRecord physical{
        .segment_id = segment_id_,
        .record_offset = tail_,
        .value_offset = tail_ + kRecordHeaderSize + identity.tenant_id.size() +
                        identity.object_key.size(),
        .value_length = value.size(),
        .total_length = envelope->total_length,
    };
    tail_ += envelope->total_length;
    return physical;
}

tl::expected<std::vector<PhysicalRecord>, SegmentError>
SegmentWriter::AppendBatch(const std::vector<SegmentAppendRequest>& requests,
                           bool sync, size_t parallelism) {
    std::lock_guard lock(append_mutex_);
    if (requests.empty() || parallelism == 0) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }

    std::vector<PhysicalRecord> physical_records;
    physical_records.reserve(requests.size());
    uint64_t next_offset = tail_;
    for (const auto& request : requests) {
        const uint64_t record_size = AlignedRecordSize(
            static_cast<uint32_t>(request.identity.tenant_id.size()),
            static_cast<uint32_t>(request.identity.object_key.size()),
            request.value.size());
        if (record_size == 0 ||
            next_offset >
                static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                    record_size) {
            return tl::unexpected(SegmentError::kInvalidArgument);
        }
        physical_records.push_back(
            {.segment_id = segment_id_,
             .record_offset = next_offset,
             .value_offset = next_offset + kRecordHeaderSize +
                             request.identity.tenant_id.size() +
                             request.identity.object_key.size(),
             .value_length = request.value.size(),
             .total_length = record_size});
        next_offset += record_size;
    }

    std::vector<std::string> encoded_records(requests.size());
    std::atomic<size_t> next_index{0};
    std::atomic<bool> encode_failed{false};
    const size_t worker_count = std::min(parallelism, requests.size());
    auto encode_records = [&] {
        while (!encode_failed.load(std::memory_order_relaxed)) {
            const size_t index =
                next_index.fetch_add(1, std::memory_order_relaxed);
            if (index >= requests.size()) break;
            const auto& request = requests[index];
            auto encoded = EncodeRecord(request.identity, request.value,
                                        request.kind, request.sequence);
            if (!encoded) {
                encode_failed.store(true, std::memory_order_relaxed);
                break;
            }
            encoded_records[index] = std::move(*encoded);
        }
    };
    if (worker_count == 1) {
        encode_records();
    } else {
        std::vector<std::future<void>> workers;
        workers.reserve(worker_count);
        for (size_t worker = 0; worker < worker_count; ++worker) {
            workers.push_back(std::async(std::launch::async, encode_records));
        }
        for (auto& worker : workers) worker.get();
    }

    if (encode_failed.load(std::memory_order_relaxed)) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }

    std::vector<UringWriteRequest> write_requests;
    write_requests.reserve(requests.size());
    bool injected_failure = false;
    for (size_t index = 0; index < requests.size(); ++index) {
        const uint64_t offset = physical_records[index].record_offset;
        if (ShouldFailWrite(path_, offset, encoded_records[index].size())) {
            injected_failure = true;
            break;
        }
        write_requests.push_back({.data = encoded_records[index].data(),
                                  .length = encoded_records[index].size(),
                                  .offset = offset});
    }

    auto write_result = injected_failure
                            ? UringBatchWriteResult::kIoError
                            : UringBatchWrite(fd_, write_requests, parallelism);
    if (write_result == UringBatchWriteResult::kUnavailable) {
        write_result = UringBatchWriteResult::kSuccess;
        for (const auto& request : write_requests) {
            if (!PwriteAll(fd_, request.data, request.length, request.offset)) {
                write_result = UringBatchWriteResult::kIoError;
                break;
            }
        }
    }
    if (write_result != UringBatchWriteResult::kSuccess) {
        if (ftruncate(fd_, static_cast<off_t>(tail_)) != 0) {
            return tl::unexpected(SegmentError::kTruncateFailed);
        }
        return tl::unexpected(SegmentError::kIoError);
    }
    if (sync && fdatasync(fd_) != 0) {
        return tl::unexpected(SegmentError::kSyncFailed);
    }
    tail_ = next_offset;
    return physical_records;
}

tl::expected<void, SegmentError> SegmentWriter::Sync() {
    std::lock_guard lock(append_mutex_);
    if (fdatasync(fd_) != 0) {
        return tl::unexpected(SegmentError::kSyncFailed);
    }
    return {};
}

uint64_t SegmentWriter::tail() const {
    std::lock_guard lock(append_mutex_);
    return tail_;
}

tl::expected<SegmentScanResult, SegmentError> ScanSegment(
    const std::string& path, uint64_t segment_id) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return tl::unexpected(SegmentError::kOpenFailed);
    }

    struct FdGuard {
        int fd;
        ~FdGuard() { close(fd); }
    } fd_guard{fd};

    struct stat file_stat{};
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size < 0) {
        return tl::unexpected(SegmentError::kIoError);
    }
    const uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);

    SegmentScanResult result;
    uint64_t offset = 0;
    std::array<char, kRecordHeaderSize> header_bytes{};
    std::array<char, kRecordFooterSize> footer_bytes{};
    std::array<char, kScanBufferSize> scan_buffer{};

    while (offset < file_size) {
        const uint64_t remaining = file_size - offset;
        if (remaining < kRecordHeaderSize) {
            result.valid_bytes = offset;
            result.termination = ScanTermination::kIncompleteTail;
            return result;
        }
        if (!PreadAll(fd, header_bytes.data(), header_bytes.size(), offset)) {
            return tl::unexpected(SegmentError::kIoError);
        }
        auto header_result = DecodeRecordHeader(header_bytes);
        if (!header_result) {
            result.valid_bytes = offset;
            result.termination = ScanTermination::kCorruptRecord;
            result.decode_error = header_result.error();
            return result;
        }
        const RecordHeader& header = header_result.value();
        if (header.total_length > remaining) {
            result.valid_bytes = offset;
            result.termination = ScanTermination::kIncompleteTail;
            return result;
        }
        if (header.total_length > std::numeric_limits<off_t>::max()) {
            return tl::unexpected(SegmentError::kInvalidArgument);
        }

        const uint64_t footer_offset =
            offset + header.total_length - kRecordFooterSize;
        if (!PreadAll(fd, footer_bytes.data(), footer_bytes.size(),
                      footer_offset)) {
            return tl::unexpected(SegmentError::kIoError);
        }
        if (ReadLittleEndian<uint64_t>(footer_bytes.data() +
                                       kFooterLengthOffset) !=
                header.total_length ||
            ReadLittleEndian<uint64_t>(footer_bytes.data() +
                                       kFooterMagicOffset) !=
                kRecordCommitMagic ||
            Crc32cValue(footer_bytes.data(), kFooterChecksumInputSize) !=
                ReadLittleEndian<uint32_t>(footer_bytes.data() +
                                           kFooterChecksumOffset)) {
            result.valid_bytes = offset;
            result.termination = ScanTermination::kCorruptRecord;
            result.decode_error = DecodeError::kFooterMismatch;
            return result;
        }

        RecordIdentity identity;
        identity.incarnation = header.incarnation;
        identity.tenant_id.resize(header.tenant_length);
        identity.object_key.resize(header.key_length);
        uint64_t cursor = offset + kRecordHeaderSize;
        if (!identity.tenant_id.empty() &&
            !PreadAll(fd, identity.tenant_id.data(), identity.tenant_id.size(),
                      cursor)) {
            return tl::unexpected(SegmentError::kIoError);
        }
        cursor += identity.tenant_id.size();
        if (!identity.object_key.empty() &&
            !PreadAll(fd, identity.object_key.data(),
                      identity.object_key.size(), cursor)) {
            return tl::unexpected(SegmentError::kIoError);
        }
        cursor += identity.object_key.size();

        Crc32c payload_crc;
        payload_crc.Extend(identity.tenant_id.data(),
                           identity.tenant_id.size());
        payload_crc.Extend(identity.object_key.data(),
                           identity.object_key.size());
        uint64_t value_remaining = header.value_length;
        uint64_t value_cursor = cursor;
        while (value_remaining > 0) {
            const size_t chunk = static_cast<size_t>(
                std::min<uint64_t>(value_remaining, scan_buffer.size()));
            if (!PreadAll(fd, scan_buffer.data(), chunk, value_cursor)) {
                return tl::unexpected(SegmentError::kIoError);
            }
            payload_crc.Extend(scan_buffer.data(), chunk);
            value_remaining -= chunk;
            value_cursor += chunk;
        }
        if (payload_crc.Final() !=
            ReadLittleEndian<uint32_t>(footer_bytes.data() +
                                       kFooterPayloadChecksumOffset)) {
            result.valid_bytes = offset;
            result.termination = ScanTermination::kCorruptRecord;
            result.decode_error = DecodeError::kPayloadChecksumMismatch;
            return result;
        }

        result.records.push_back(ScannedRecord{
            .identity = std::move(identity),
            .kind = header.kind,
            .sequence = header.sequence,
            .physical =
                PhysicalRecord{
                    .segment_id = segment_id,
                    .record_offset = offset,
                    .value_offset = cursor,
                    .value_length = header.value_length,
                    .total_length = header.total_length,
                },
        });
        offset += header.total_length;
    }

    result.valid_bytes = offset;
    result.termination = ScanTermination::kCleanEof;
    return result;
}

tl::expected<DecodedRecord, SegmentError> ReadRecord(
    const std::string& path, const PhysicalRecord& physical) {
    if (physical.total_length < kRecordHeaderSize + kRecordFooterSize ||
        physical.total_length >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
        physical.record_offset >
            static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                physical.total_length) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return tl::unexpected(SegmentError::kOpenFailed);
    }
    struct FdGuard {
        int fd;
        ~FdGuard() { close(fd); }
    } fd_guard{fd};

    std::string encoded(static_cast<size_t>(physical.total_length), '\0');
    if (!PreadAll(fd, encoded.data(), encoded.size(), physical.record_offset)) {
        return tl::unexpected(SegmentError::kIoError);
    }
    auto decoded = DecodeRecord(encoded);
    if (!decoded) {
        return tl::unexpected(SegmentError::kIoError);
    }
    const uint64_t expected_value_offset = physical.record_offset +
                                           kRecordHeaderSize +
                                           decoded->identity.tenant_id.size() +
                                           decoded->identity.object_key.size();
    if (decoded->total_length != physical.total_length ||
        decoded->value.size() != physical.value_length ||
        expected_value_offset != physical.value_offset) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }
    return decoded.value();
}

tl::expected<void, SegmentError> TruncateSegment(const std::string& path,
                                                 uint64_t valid_bytes) {
    if (valid_bytes > std::numeric_limits<off_t>::max()) {
        return tl::unexpected(SegmentError::kInvalidArgument);
    }
    if (truncate(path.c_str(), static_cast<off_t>(valid_bytes)) != 0) {
        return tl::unexpected(SegmentError::kTruncateFailed);
    }
    return {};
}

}  // namespace mooncake::logstructured
