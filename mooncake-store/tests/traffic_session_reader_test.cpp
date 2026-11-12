#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

#include "session_reader.h"
#include "traffic/file_sink.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

std::vector<char> MakeRecord(std::uint32_t route_key, std::uint32_t seq_num,
                             const std::string& payload) {
    TrafficRecordHeader header{};
    header.total_len =
        static_cast<std::uint32_t>(sizeof(header) + payload.size());
    header.route_key = route_key;
    header.conn_id = 1;
    header.seq_num = seq_num;
    header.payload_len = static_cast<std::uint32_t>(payload.size());
    header.timestamp_ms = 1000 + seq_num;
    std::vector<char> record(header.total_len);
    std::memcpy(record.data(), &header, sizeof(header));
    std::memcpy(record.data() + sizeof(header), payload.data(), payload.size());
    return record;
}

class TempDir {
   public:
    TempDir()
        : path_(std::filesystem::temp_directory_path() /
                ("mooncake_session_reader_test_" + std::to_string(::getpid()) +
                 "_" + std::to_string(++counter_))) {
        std::filesystem::create_directories(path_);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    static inline int counter_ = 0;
    std::filesystem::path path_;
};

std::filesystem::path OnlySegment(const std::filesystem::path& session) {
    for (const auto& entry : std::filesystem::directory_iterator(session)) {
        if (entry.path().extension() == ".mcr") {
            return entry.path();
        }
    }
    return {};
}

void Patch(const std::filesystem::path& file, std::uint64_t offset,
           const void* data, std::size_t len) {
    std::FILE* handle = std::fopen(file.c_str(), "r+b");
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(std::fseek(handle, static_cast<long>(offset), SEEK_SET), 0);
    ASSERT_EQ(std::fwrite(data, 1, len, handle), len);
    std::fclose(handle);
}

class SessionReaderTest : public ::testing::Test {
   protected:
    std::string WriteSession() {
        FileSink sink(1024 * 1024, 300, 0, RedactionMode::kNone);
        EXPECT_TRUE(sink.OpenSession(dir_.path().string(), 1));
        auto first = MakeRecord(7, 1, "abcdef");
        auto second = MakeRecord(9, 2, "gh");
        struct iovec iov[2] = {{first.data(), first.size()},
                               {second.data(), second.size()}};
        EXPECT_TRUE(sink.WriteRecords(iov, 2, first.size() + second.size(), 2));
        sink.Close();
        return sink.session_dir();
    }

    TempDir dir_;
};

TEST_F(SessionReaderTest, ReadsRecordsAndCounters) {
    const std::string session = WriteSession();
    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(session, &error)) << error;

    Record record;
    ASSERT_EQ(reader.Next(&record, &error), SessionReader::NextResult::kRecord)
        << error;
    EXPECT_EQ(record.header.route_key, 7u);
    EXPECT_EQ(record.header.seq_num, 1u);
    EXPECT_EQ(std::string(record.payload.begin(), record.payload.end()),
              "abcdef");
    ASSERT_EQ(reader.Next(&record, &error), SessionReader::NextResult::kRecord)
        << error;
    EXPECT_EQ(record.header.route_key, 9u);
    EXPECT_EQ(std::string(record.payload.begin(), record.payload.end()), "gh");
    EXPECT_EQ(reader.Next(&record, &error), SessionReader::NextResult::kDone)
        << error;

    EXPECT_EQ(reader.records(), 2u);
    EXPECT_EQ(reader.payload_bytes(), 8u);
    // The fixture stamps record N at 1000 + N ms, so two records span 1 ms.
    EXPECT_EQ(reader.duration_ms(), 1);
    EXPECT_EQ(reader.block_count(), 1u);
    EXPECT_EQ(reader.segment_count(), 1u);
    EXPECT_EQ(reader.redaction(), 0);
    ASSERT_EQ(reader.routes().size(), 2u);
    EXPECT_EQ(reader.routes().at(7), 1u);
    EXPECT_EQ(reader.routes().at(9), 1u);
    EXPECT_EQ(reader.session_dir(), std::filesystem::path(session));
}

TEST_F(SessionReaderTest, FindsSingleSessionSubdirectory) {
    WriteSession();
    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(dir_.path().string(), &error)) << error;
    Record record;
    EXPECT_EQ(reader.Next(&record, &error), SessionReader::NextResult::kRecord)
        << error;
    EXPECT_EQ(reader.records(), 1u);
}

TEST_F(SessionReaderTest, RejectsDirectoryWithoutSegments) {
    SessionReader reader;
    std::string error;
    EXPECT_FALSE(reader.Open(dir_.path().string(), &error));
    EXPECT_NE(error.find("no segment-*.mcr files in"), std::string::npos)
        << error;
}

TEST_F(SessionReaderTest, RejectsParentWithSeveralSessions) {
    for (const char* name : {"session-a", "session-b"}) {
        const auto child = dir_.path() / name;
        std::filesystem::create_directories(child);
        FileSink sink(1024 * 1024, 300, 0, RedactionMode::kNone);
        ASSERT_TRUE(sink.OpenSession(child.string(), 1));
        auto record = MakeRecord(7, 1, "abcdef");
        struct iovec iov = {record.data(), record.size()};
        ASSERT_TRUE(sink.WriteRecords(&iov, 1, record.size(), 1));
        sink.Close();
    }

    SessionReader reader;
    std::string error;
    EXPECT_FALSE(reader.Open(dir_.path().string(), &error));
    EXPECT_NE(error.find("several sessions in"), std::string::npos) << error;
    EXPECT_NE(error.find("session-a"), std::string::npos) << error;
}

TEST_F(SessionReaderTest, DetectsCorruptedBlock) {
    const std::string session = WriteSession();
    const auto segment = OnlySegment(session);
    char spoiled = 0;
    Patch(segment, kTrafficFileHeaderSize + kTrafficBlockHeaderSize, &spoiled,
          1);

    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(session, &error)) << error;
    Record record;
    EXPECT_EQ(reader.Next(&record, &error), SessionReader::NextResult::kError);
    EXPECT_NE(error.find("block crc mismatch"), std::string::npos) << error;
}

TEST_F(SessionReaderTest, AcceptsSegmentWithoutFooter) {
    const std::string session = WriteSession();
    const auto segment = OnlySegment(session);
    const auto size = std::filesystem::file_size(segment);
    std::error_code ec;
    // Dropping the footer is what a killed recorder leaves behind; the blocks
    // it wrote are complete.
    std::filesystem::resize_file(segment, size - kTrafficFooterSize, ec);
    ASSERT_FALSE(ec) << ec.message();

    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(session, &error)) << error;
    EXPECT_FALSE(reader.incomplete());
    EXPECT_TRUE(reader.Finish(&error)) << error;
    EXPECT_TRUE(reader.incomplete());
    EXPECT_EQ(reader.records(), 2u);
    EXPECT_EQ(reader.payload_bytes(), 8u);
    EXPECT_EQ(reader.block_count(), 1u);
}

TEST_F(SessionReaderTest, AcceptsTornFinalBlock) {
    FileSink sink(1024 * 1024, 300, 0, RedactionMode::kNone);
    ASSERT_TRUE(sink.OpenSession(dir_.path().string(), 1));
    auto first = MakeRecord(7, 1, "abcdef");
    auto second = MakeRecord(9, 2, "gh");
    struct iovec first_iov[2] = {{first.data(), first.size()},
                                 {second.data(), second.size()}};
    ASSERT_TRUE(
        sink.WriteRecords(first_iov, 2, first.size() + second.size(), 2));
    auto third = MakeRecord(11, 3, "xyz");
    struct iovec second_iov[1] = {{third.data(), third.size()}};
    ASSERT_TRUE(sink.WriteRecords(second_iov, 1, third.size(), 1));
    sink.Close();

    const auto segment = OnlySegment(sink.session_dir());
    const auto size = std::filesystem::file_size(segment);
    std::error_code ec;
    // A process killed during a write leaves no footer and a short last block.
    // The block before it is intact, so the read stops at the cut.
    std::filesystem::resize_file(segment, size - kTrafficFooterSize - 10, ec);
    ASSERT_FALSE(ec) << ec.message();

    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(sink.session_dir(), &error)) << error;
    EXPECT_TRUE(reader.Finish(&error)) << error;
    EXPECT_TRUE(reader.incomplete());
    EXPECT_EQ(reader.records(), 2u);
    EXPECT_EQ(reader.block_count(), 1u);
}

TEST_F(SessionReaderTest, ReportsFooterCounterMismatch) {
    const std::string session = WriteSession();
    const auto segment = OnlySegment(session);
    const auto size = std::filesystem::file_size(segment);
    const std::uint64_t records_offset =
        size - kTrafficFooterSize + offsetof(TrafficFooter, record_count);
    const std::uint64_t wrong = 5;
    Patch(segment, records_offset, &wrong, sizeof(wrong));

    SessionReader reader;
    std::string error;
    ASSERT_TRUE(reader.Open(session, &error)) << error;
    // The records themselves are intact; the footer is only read once they have
    // been consumed.
    EXPECT_FALSE(reader.Finish(&error));
    EXPECT_NE(error.find("footer counters mismatch"), std::string::npos)
        << error;
}

}  // namespace
}  // namespace mooncake::traffic
