#include "storage/local/log_structured/record_format.h"
#include "storage/local/log_structured/segment.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class TempDirectory {
   public:
    TempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake-log-segment-test-" + std::to_string(getpid()) + "-" +
                 std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~TempDirectory() { std::filesystem::remove_all(path_); }

    std::filesystem::path File(std::string_view name) const {
        return path_ / name;
    }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

RecordIdentity Identity(std::string key, uint64_t incarnation) {
    return RecordIdentity{
        .tenant_id = "tenant-a",
        .object_key = std::move(key),
        .incarnation = ObjectIncarnation{.high = 17, .low = incarnation},
    };
}

TEST(LogStructuredRecordTest, RoundTripsValueAndTombstone) {
    const auto identity = Identity("key-a", 3);
    auto encoded = EncodeRecord(identity, "value-a", RecordKind::kValue, 11);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = DecodeRecord(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->identity, identity);
    EXPECT_EQ(decoded->kind, RecordKind::kValue);
    EXPECT_EQ(decoded->sequence, uint64_t{11});
    EXPECT_EQ(decoded->value, "value-a");
    EXPECT_EQ(decoded->total_length, encoded->size());

    auto tombstone = EncodeRecord(identity, "", RecordKind::kTombstone, 12);
    ASSERT_TRUE(tombstone.has_value());
    auto decoded_tombstone = DecodeRecord(*tombstone);
    ASSERT_TRUE(decoded_tombstone.has_value());
    EXPECT_EQ(decoded_tombstone->kind, RecordKind::kTombstone);
    EXPECT_TRUE(decoded_tombstone->value.empty());
}

TEST(LogStructuredRecordTest, RejectsCorruptedPayload) {
    auto encoded =
        EncodeRecord(Identity("key-b", 4), "payload", RecordKind::kValue, 1);
    ASSERT_TRUE(encoded.has_value());
    (*encoded)[kRecordHeaderSize + std::string("tenant-a").size() +
               std::string("key-b").size()] ^= 0x01;

    auto decoded = DecodeRecord(*encoded);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), DecodeError::kPayloadChecksumMismatch);
}

TEST(LogStructuredRecordTest, RejectsCorruptedHeaderAndFooter) {
    auto encoded =
        EncodeRecord(Identity("key-c", 5), "payload", RecordKind::kValue, 7);
    ASSERT_TRUE(encoded.has_value());

    auto header_corruption = *encoded;
    header_corruption[16] ^= 0x01;
    auto decoded_header = DecodeRecord(header_corruption);
    ASSERT_FALSE(decoded_header.has_value());
    EXPECT_EQ(decoded_header.error(), DecodeError::kHeaderChecksumMismatch);

    auto footer_corruption = *encoded;
    footer_corruption.back() ^= 0x01;
    auto decoded_footer = DecodeRecord(footer_corruption);
    ASSERT_FALSE(decoded_footer.has_value());
    EXPECT_EQ(decoded_footer.error(), DecodeError::kFooterMismatch);
}

TEST(LogStructuredRecordTest, RejectsNonzeroReservedHeaderField) {
    auto encoded =
        EncodeRecord(Identity("key-d", 6), "payload", RecordKind::kValue, 8);
    ASSERT_TRUE(encoded.has_value());
    (*encoded)[60] = 1;

    auto decoded = DecodeRecord(*encoded);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), DecodeError::kInvalidFlags);
}

TEST(LogStructuredRecordTest, RejectsTombstoneWithValue) {
    auto encoded = EncodeRecord(Identity("key-d", 6), "not-empty",
                                RecordKind::kTombstone, 8);
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error(), DecodeError::kInvalidLength);
}

TEST(LogStructuredSegmentTest, AppendsAndScansRecords) {
    TempDirectory temp;
    const auto path = temp.File("segment-1.log");
    auto writer = SegmentWriter::Create(path.string(), 1);
    ASSERT_TRUE(writer.has_value());

    auto first = (*writer)->Append(Identity("a", 1), "first",
                                   RecordKind::kValue, 1, false);
    auto second = (*writer)->Append(Identity("b", 2), "second",
                                    RecordKind::kCompactionCopy, 2, true);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->record_offset, uint64_t{0});
    EXPECT_EQ(second->record_offset, first->total_length);
    writer.value().reset();

    auto scan = ScanSegment(path.string(), 1);
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, ScanTermination::kCleanEof);
    ASSERT_EQ(scan->records.size(), size_t{2});
    EXPECT_EQ(scan->records[0].identity, Identity("a", 1));
    EXPECT_EQ(scan->records[0].physical, *first);
    EXPECT_EQ(scan->records[1].identity, Identity("b", 2));
    EXPECT_EQ(scan->records[1].kind, RecordKind::kCompactionCopy);
    EXPECT_EQ(scan->valid_bytes, first->total_length + second->total_length);
}

TEST(LogStructuredSegmentTest, DetectsAndRepairsIncompleteTail) {
    TempDirectory temp;
    const auto path = temp.File("segment-2.log");
    auto writer = SegmentWriter::Create(path.string(), 2);
    ASSERT_TRUE(writer.has_value());
    auto record = (*writer)->Append(Identity("stable", 8), "complete",
                                    RecordKind::kValue, 1, true);
    ASSERT_TRUE(record.has_value());
    writer.value().reset();

    auto incomplete =
        EncodeRecord(Identity("torn", 9), "unfinished", RecordKind::kValue, 2);
    ASSERT_TRUE(incomplete.has_value());
    const int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    const size_t partial_size = incomplete->size() / 2;
    ASSERT_EQ(write(fd, incomplete->data(), partial_size),
              static_cast<ssize_t>(partial_size));
    ASSERT_EQ(close(fd), 0);

    auto scan = ScanSegment(path.string(), 2);
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, ScanTermination::kIncompleteTail);
    EXPECT_EQ(scan->valid_bytes, record->total_length);
    ASSERT_EQ(scan->records.size(), size_t{1});

    ASSERT_TRUE(TruncateSegment(path.string(), scan->valid_bytes).has_value());
    auto repaired = ScanSegment(path.string(), 2);
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(repaired->termination, ScanTermination::kCleanEof);
    EXPECT_EQ(repaired->records.size(), size_t{1});
}

TEST(LogStructuredSegmentTest, ReopensAtRecoveredTailAndOverwritesGarbage) {
    TempDirectory temp;
    const auto path = temp.File("segment-reopen.log");
    auto writer = SegmentWriter::Create(path.string(), 4);
    ASSERT_TRUE(writer.has_value());
    auto first = (*writer)->Append(Identity("first", 1), "stable",
                                   RecordKind::kValue, 1, true);
    ASSERT_TRUE(first.has_value());
    writer.value().reset();

    {
        std::ofstream output(path, std::ios::binary | std::ios::app);
        ASSERT_TRUE(output.good());
        output << "partial-record-garbage";
    }

    auto scan = ScanSegment(path.string(), 4);
    ASSERT_TRUE(scan.has_value());
    ASSERT_EQ(scan->termination, ScanTermination::kIncompleteTail);
    ASSERT_EQ(scan->valid_bytes, first->total_length);

    auto reopened =
        SegmentWriter::OpenForAppend(path.string(), 4, scan->valid_bytes);
    ASSERT_TRUE(reopened.has_value());
    auto second =
        (*reopened)->Append(Identity("second", 2), std::string(128 * 1024, 'x'),
                            RecordKind::kValue, 2, true);
    ASSERT_TRUE(second.has_value());
    reopened.value().reset();

    auto recovered = ScanSegment(path.string(), 4);
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(recovered->termination, ScanTermination::kCleanEof);
    ASSERT_EQ(recovered->records.size(), size_t{2});
    EXPECT_EQ(recovered->records[1].identity, Identity("second", 2));
    EXPECT_EQ(recovered->records[1].physical.value_length,
              uint64_t{128 * 1024});
}

TEST(LogStructuredSegmentTest, StopsAtCorruptedRecordWithoutTruncating) {
    TempDirectory temp;
    const auto path = temp.File("segment-3.log");
    auto writer = SegmentWriter::Create(path.string(), 3);
    ASSERT_TRUE(writer.has_value());
    auto first = (*writer)->Append(Identity("good", 1), "good-value",
                                   RecordKind::kValue, 1, false);
    auto second = (*writer)->Append(Identity("bad", 2), "bad-value",
                                    RecordKind::kValue, 2, true);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    writer.value().reset();

    const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    char byte = 0;
    ASSERT_EQ(pread(fd, &byte, 1, second->value_offset), 1);
    byte ^= 0x40;
    ASSERT_EQ(pwrite(fd, &byte, 1, second->value_offset), 1);
    ASSERT_EQ(close(fd), 0);

    auto scan = ScanSegment(path.string(), 3);
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, ScanTermination::kCorruptRecord);
    EXPECT_EQ(scan->decode_error, DecodeError::kPayloadChecksumMismatch);
    EXPECT_EQ(scan->valid_bytes, first->total_length);
    ASSERT_EQ(scan->records.size(), size_t{1});
    EXPECT_EQ(std::filesystem::file_size(path),
              first->total_length + second->total_length);
}

TEST(LogStructuredSegmentTest, InjectedWriteFailureLeavesRecoverableTail) {
    TempDirectory temp;
    const auto path = temp.File("segment-4.log");
    auto writer = SegmentWriter::Create(path.string(), 4);
    ASSERT_TRUE(writer.has_value());

    std::atomic<size_t> writes{0};
    SegmentWriter::SetWriteFailurePredicateForTest(
        [&](std::string_view, uint64_t, size_t) {
            return writes.fetch_add(1, std::memory_order_relaxed) == 1;
        });
    auto failed = (*writer)->Append(Identity("partial", 1), "value",
                                    RecordKind::kValue, 1, true);
    SegmentWriter::SetWriteFailurePredicateForTest({});
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), SegmentError::kIoError);
    writer.value().reset();

    auto scan = ScanSegment(path.string(), 4);
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, ScanTermination::kIncompleteTail);
    EXPECT_EQ(scan->valid_bytes, uint64_t{0});
    EXPECT_TRUE(scan->records.empty());

    auto recovered = SegmentWriter::OpenForAppend(path.string(), 4, 0);
    ASSERT_TRUE(recovered.has_value());
    ASSERT_TRUE((*recovered)
                    ->Append(Identity("recovered", 2), "value",
                             RecordKind::kValue, 2, true)
                    .has_value());
    recovered.value().reset();

    auto final_scan = ScanSegment(path.string(), 4);
    ASSERT_TRUE(final_scan.has_value());
    EXPECT_EQ(final_scan->termination, ScanTermination::kCleanEof);
    ASSERT_EQ(final_scan->records.size(), size_t{1});
    EXPECT_EQ(final_scan->records[0].identity, Identity("recovered", 2));
}

}  // namespace
}  // namespace mooncake::logstructured
