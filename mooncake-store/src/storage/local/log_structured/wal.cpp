#include "storage/local/log_structured/wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <span>
#include <type_traits>

#include "crc32c.h"

namespace mooncake::logstructured {
namespace {

constexpr uint32_t kWalMagic = 0x4C57434D;
constexpr uint16_t kWalVersion = 1;
constexpr uint64_t kWalCommitMagic = 0x54494D4D4F43574DULL;
constexpr size_t kWalHeaderSize = 96;
constexpr size_t kWalFooterSize = 16;
constexpr size_t kWalAlignment = 8;
constexpr size_t kMagicOffset = 0;
constexpr size_t kVersionOffset = 4;
constexpr size_t kTypeOffset = 6;
constexpr size_t kTotalLengthOffset = 8;
constexpr size_t kSequenceOffset = 16;
constexpr size_t kIncarnationHighOffset = 24;
constexpr size_t kIncarnationLowOffset = 32;
constexpr size_t kSegmentIdOffset = 40;
constexpr size_t kRecordOffset = 48;
constexpr size_t kValueOffset = 56;
constexpr size_t kValueLengthOffset = 64;
constexpr size_t kPhysicalLengthOffset = 72;
constexpr size_t kTenantLengthOffset = 80;
constexpr size_t kKeyLengthOffset = 84;
constexpr size_t kHeaderChecksumOffset = 88;
constexpr size_t kHeaderChecksumInputSize = kHeaderChecksumOffset;
constexpr size_t kFooterPayloadChecksumOffset = 0;
constexpr size_t kFooterChecksumOffset = 4;
constexpr size_t kFooterMagicOffset = 8;
constexpr size_t kFooterChecksumInputSize = kFooterChecksumOffset;

template <typename T>
void WriteLittleEndian(char* output, T value) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned unsigned_value = static_cast<Unsigned>(value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        output[i] = static_cast<char>((unsigned_value >> (i * 8)) & 0xff);
    }
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

bool IsKnownType(WalRecordType type) {
    return type == WalRecordType::kPrepareValue ||
           type == WalRecordType::kCommitValue ||
           type == WalRecordType::kAbortValue ||
           type == WalRecordType::kApplyTombstone ||
           type == WalRecordType::kCheckpoint;
}

bool HasPhysicalRecord(const PhysicalRecord& physical) {
    return physical.total_length != 0;
}

bool IsValidRecord(const WalRecord& record) {
    if (!IsKnownType(record.type) ||
        record.identity.tenant_id.size() > kMaxTenantLength ||
        record.identity.object_key.size() > kMaxKeyLength) {
        return false;
    }
    if (record.type == WalRecordType::kPrepareValue) {
        return HasPhysicalRecord(record.physical);
    }
    if (record.type == WalRecordType::kCheckpoint) {
        return record.identity.tenant_id.empty() &&
               record.identity.object_key.empty() &&
               record.identity.incarnation == ObjectIncarnation{} &&
               !HasPhysicalRecord(record.physical);
    }
    return !HasPhysicalRecord(record.physical);
}

uint64_t EncodedLength(const WalRecord& record) {
    const uint64_t payload_length =
        record.identity.tenant_id.size() + record.identity.object_key.size();
    const uint64_t unaligned = kWalHeaderSize + payload_length;
    return ((unaligned + kWalAlignment - 1) & ~(kWalAlignment - 1)) +
           kWalFooterSize;
}

bool PwriteAll(int fd, const char* data, size_t length, uint64_t offset) {
    size_t written = 0;
    while (written < length) {
        const ssize_t result =
            pwrite(fd, data + written, length - written, offset + written);
        if (result < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (result == 0) return false;
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
            if (errno == EINTR) continue;
            return false;
        }
        if (result == 0) return false;
        read_bytes += static_cast<size_t>(result);
    }
    return true;
}

tl::expected<std::string, WalError> EncodeWalRecord(const WalRecord& record) {
    if (!IsValidRecord(record)) {
        return tl::unexpected(WalError::kInvalidArgument);
    }
    const uint64_t total_length = EncodedLength(record);
    if (total_length >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return tl::unexpected(WalError::kInvalidArgument);
    }

    std::string encoded(static_cast<size_t>(total_length), '\0');
    char* header = encoded.data();
    WriteLittleEndian<uint32_t>(header + kMagicOffset, kWalMagic);
    WriteLittleEndian<uint16_t>(header + kVersionOffset, kWalVersion);
    WriteLittleEndian<uint16_t>(header + kTypeOffset,
                                static_cast<uint16_t>(record.type));
    WriteLittleEndian<uint64_t>(header + kTotalLengthOffset, total_length);
    WriteLittleEndian<uint64_t>(header + kSequenceOffset, record.sequence);
    WriteLittleEndian<uint64_t>(header + kIncarnationHighOffset,
                                record.identity.incarnation.high);
    WriteLittleEndian<uint64_t>(header + kIncarnationLowOffset,
                                record.identity.incarnation.low);
    WriteLittleEndian<uint64_t>(header + kSegmentIdOffset,
                                record.physical.segment_id);
    WriteLittleEndian<uint64_t>(header + kRecordOffset,
                                record.physical.record_offset);
    WriteLittleEndian<uint64_t>(header + kValueOffset,
                                record.physical.value_offset);
    WriteLittleEndian<uint64_t>(header + kValueLengthOffset,
                                record.physical.value_length);
    WriteLittleEndian<uint64_t>(header + kPhysicalLengthOffset,
                                record.physical.total_length);
    WriteLittleEndian<uint32_t>(header + kTenantLengthOffset,
                                record.identity.tenant_id.size());
    WriteLittleEndian<uint32_t>(header + kKeyLengthOffset,
                                record.identity.object_key.size());
    WriteLittleEndian<uint32_t>(header + kHeaderChecksumOffset,
                                Crc32cValue(header, kHeaderChecksumInputSize));

    size_t cursor = kWalHeaderSize;
    std::memcpy(encoded.data() + cursor, record.identity.tenant_id.data(),
                record.identity.tenant_id.size());
    cursor += record.identity.tenant_id.size();
    std::memcpy(encoded.data() + cursor, record.identity.object_key.data(),
                record.identity.object_key.size());

    Crc32c payload_crc;
    payload_crc.Extend(record.identity.tenant_id.data(),
                       record.identity.tenant_id.size());
    payload_crc.Extend(record.identity.object_key.data(),
                       record.identity.object_key.size());
    char* footer = encoded.data() + total_length - kWalFooterSize;
    WriteLittleEndian<uint32_t>(footer + kFooterPayloadChecksumOffset,
                                payload_crc.Final());
    WriteLittleEndian<uint32_t>(footer + kFooterChecksumOffset,
                                Crc32cValue(footer, kFooterChecksumInputSize));
    WriteLittleEndian<uint64_t>(footer + kFooterMagicOffset, kWalCommitMagic);
    return encoded;
}

tl::expected<WalRecord, WalError> DecodeWalRecord(std::span<const char> bytes) {
    if (bytes.size() < kWalHeaderSize ||
        ReadLittleEndian<uint32_t>(bytes.data() + kMagicOffset) != kWalMagic ||
        ReadLittleEndian<uint16_t>(bytes.data() + kVersionOffset) !=
            kWalVersion ||
        Crc32cValue(bytes.data(), kHeaderChecksumInputSize) !=
            ReadLittleEndian<uint32_t>(bytes.data() + kHeaderChecksumOffset)) {
        return tl::unexpected(WalError::kCorruptRecord);
    }

    const auto type = static_cast<WalRecordType>(
        ReadLittleEndian<uint16_t>(bytes.data() + kTypeOffset));
    const uint64_t total_length =
        ReadLittleEndian<uint64_t>(bytes.data() + kTotalLengthOffset);
    const uint32_t tenant_length =
        ReadLittleEndian<uint32_t>(bytes.data() + kTenantLengthOffset);
    const uint32_t key_length =
        ReadLittleEndian<uint32_t>(bytes.data() + kKeyLengthOffset);
    const uint64_t expected_length =
        ((kWalHeaderSize + static_cast<uint64_t>(tenant_length) + key_length +
          kWalAlignment - 1) &
         ~(kWalAlignment - 1)) +
        kWalFooterSize;
    if (!IsKnownType(type) || tenant_length > kMaxTenantLength ||
        key_length > kMaxKeyLength || total_length != expected_length ||
        total_length > bytes.size()) {
        return tl::unexpected(WalError::kCorruptRecord);
    }

    const char* footer = bytes.data() + total_length - kWalFooterSize;
    if (ReadLittleEndian<uint64_t>(footer + kFooterMagicOffset) !=
            kWalCommitMagic ||
        Crc32cValue(footer, kFooterChecksumInputSize) !=
            ReadLittleEndian<uint32_t>(footer + kFooterChecksumOffset)) {
        return tl::unexpected(WalError::kCorruptRecord);
    }

    WalRecord record;
    record.type = type;
    record.sequence =
        ReadLittleEndian<uint64_t>(bytes.data() + kSequenceOffset);
    record.identity.incarnation.high =
        ReadLittleEndian<uint64_t>(bytes.data() + kIncarnationHighOffset);
    record.identity.incarnation.low =
        ReadLittleEndian<uint64_t>(bytes.data() + kIncarnationLowOffset);
    record.physical.segment_id =
        ReadLittleEndian<uint64_t>(bytes.data() + kSegmentIdOffset);
    record.physical.record_offset =
        ReadLittleEndian<uint64_t>(bytes.data() + kRecordOffset);
    record.physical.value_offset =
        ReadLittleEndian<uint64_t>(bytes.data() + kValueOffset);
    record.physical.value_length =
        ReadLittleEndian<uint64_t>(bytes.data() + kValueLengthOffset);
    record.physical.total_length =
        ReadLittleEndian<uint64_t>(bytes.data() + kPhysicalLengthOffset);
    size_t cursor = kWalHeaderSize;
    record.identity.tenant_id.assign(bytes.data() + cursor, tenant_length);
    cursor += tenant_length;
    record.identity.object_key.assign(bytes.data() + cursor, key_length);

    Crc32c payload_crc;
    payload_crc.Extend(record.identity.tenant_id.data(),
                       record.identity.tenant_id.size());
    payload_crc.Extend(record.identity.object_key.data(),
                       record.identity.object_key.size());
    if (payload_crc.Final() !=
            ReadLittleEndian<uint32_t>(footer + kFooterPayloadChecksumOffset) ||
        !IsValidRecord(record)) {
        return tl::unexpected(WalError::kCorruptRecord);
    }
    return record;
}

}  // namespace

WalWriter::WalWriter(std::string path, int fd, uint64_t tail)
    : path_(std::move(path)), fd_(fd), tail_(tail) {}

WalWriter::~WalWriter() {
    if (fd_ >= 0) close(fd_);
}

tl::expected<std::unique_ptr<WalWriter>, WalError> WalWriter::Create(
    std::string path) {
    const int fd =
        open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) return tl::unexpected(WalError::kOpenFailed);
    return std::unique_ptr<WalWriter>(new WalWriter(std::move(path), fd, 0));
}

tl::expected<std::unique_ptr<WalWriter>, WalError> WalWriter::OpenForAppend(
    std::string path, uint64_t valid_bytes) {
    const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(WalError::kOpenFailed);
    struct stat file_stat{};
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size < 0 ||
        valid_bytes > static_cast<uint64_t>(file_stat.st_size) ||
        valid_bytes >
            static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        close(fd);
        return tl::unexpected(WalError::kInvalidArgument);
    }
    if (ftruncate(fd, static_cast<off_t>(valid_bytes)) != 0) {
        close(fd);
        return tl::unexpected(WalError::kTruncateFailed);
    }
    return std::unique_ptr<WalWriter>(
        new WalWriter(std::move(path), fd, valid_bytes));
}

tl::expected<void, WalError> WalWriter::Append(const WalRecord& record,
                                               bool sync) {
    auto encoded = EncodeWalRecord(record);
    if (!encoded ||
        tail_ > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                    encoded->size()) {
        return tl::unexpected(WalError::kInvalidArgument);
    }
    if (!PwriteAll(fd_, encoded->data(), encoded->size(), tail_)) {
        return tl::unexpected(WalError::kIoError);
    }
    if (sync && fdatasync(fd_) != 0) {
        return tl::unexpected(WalError::kSyncFailed);
    }
    tail_ += encoded->size();
    return {};
}

tl::expected<void, WalError> WalWriter::Sync() {
    if (fdatasync(fd_) != 0) return tl::unexpected(WalError::kSyncFailed);
    return {};
}

tl::expected<WalScanResult, WalError> ScanWal(const std::string& path) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(WalError::kOpenFailed);
    struct FdGuard {
        int fd;
        ~FdGuard() { close(fd); }
    } guard{fd};

    struct stat file_stat{};
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size < 0) {
        return tl::unexpected(WalError::kIoError);
    }
    const uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);
    WalScanResult result;
    std::array<char, kWalHeaderSize> header{};
    uint64_t offset = 0;
    while (offset < file_size) {
        const uint64_t remaining = file_size - offset;
        if (remaining < kWalHeaderSize) {
            result.valid_bytes = offset;
            result.termination = WalScanTermination::kIncompleteTail;
            return result;
        }
        if (!PreadAll(fd, header.data(), header.size(), offset)) {
            return tl::unexpected(WalError::kIoError);
        }
        if (ReadLittleEndian<uint32_t>(header.data() + kMagicOffset) !=
                kWalMagic ||
            ReadLittleEndian<uint16_t>(header.data() + kVersionOffset) !=
                kWalVersion ||
            Crc32cValue(header.data(), kHeaderChecksumInputSize) !=
                ReadLittleEndian<uint32_t>(header.data() +
                                           kHeaderChecksumOffset)) {
            result.valid_bytes = offset;
            result.termination = WalScanTermination::kCorruptRecord;
            return result;
        }
        const uint64_t total_length =
            ReadLittleEndian<uint64_t>(header.data() + kTotalLengthOffset);
        if (total_length < kWalHeaderSize + kWalFooterSize ||
            total_length > remaining ||
            total_length >
                static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            result.valid_bytes = offset;
            result.termination = total_length > remaining
                                     ? WalScanTermination::kIncompleteTail
                                     : WalScanTermination::kCorruptRecord;
            return result;
        }
        std::string encoded(static_cast<size_t>(total_length), '\0');
        if (!PreadAll(fd, encoded.data(), encoded.size(), offset)) {
            return tl::unexpected(WalError::kIoError);
        }
        auto decoded = DecodeWalRecord(encoded);
        if (!decoded) {
            result.valid_bytes = offset;
            result.termination = WalScanTermination::kCorruptRecord;
            return result;
        }
        result.records.push_back(std::move(decoded.value()));
        offset += total_length;
    }
    result.valid_bytes = offset;
    result.termination = WalScanTermination::kCleanEof;
    return result;
}

tl::expected<void, WalError> ReplayWal(const std::vector<WalRecord>& records,
                                       VersionIndex& index) {
    for (const auto& record : records) {
        bool success = false;
        switch (record.type) {
            case WalRecordType::kPrepareValue:
                success = index
                              .Prepare(record.identity, record.physical,
                                       record.sequence)
                              .has_value();
                break;
            case WalRecordType::kCommitValue:
                success =
                    index.Commit(record.identity, record.sequence).has_value();
                break;
            case WalRecordType::kAbortValue:
                success =
                    index.Abort(record.identity, record.sequence).has_value();
                break;
            case WalRecordType::kApplyTombstone:
                success = index.ApplyTombstone(record.identity, record.sequence)
                              .has_value();
                break;
            case WalRecordType::kCheckpoint:
                success = true;
                break;
        }
        if (!success) return tl::unexpected(WalError::kReplayFailed);
    }
    return {};
}

}  // namespace mooncake::logstructured
