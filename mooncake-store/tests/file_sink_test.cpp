#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "crc32c.h"
#include "traffic/file_sink.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

class TempDir {
   public:
    TempDir() {
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake_file_sink_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(counter_++));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    std::filesystem::path path_;
    static inline int counter_ = 0;
};

std::vector<char> ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(in),
                             std::istreambuf_iterator<char>());
}

std::vector<char> MakeRecords(std::initializer_list<std::uint32_t> payloads) {
    std::vector<char> records;
    for (const std::uint32_t payload_len : payloads) {
        TrafficRecordHeader header;
        header.total_len = kTrafficRecordHeaderSize + payload_len;
        header.route_key = 7;
        header.conn_id = 9;
        header.seq_num = payload_len;
        header.payload_len = payload_len;
        header.timestamp_ms = 1'770'000'000'123;
        const char* header_bytes = reinterpret_cast<const char*>(&header);
        records.insert(records.end(), header_bytes,
                       header_bytes + sizeof(header));
        records.insert(records.end(), payload_len, 'x');
    }
    return records;
}

std::vector<std::filesystem::path> SegmentFiles(
    const std::string& session_dir) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(session_dir)) {
        if (entry.path().extension() == ".mcr") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

// The on-disk structs are packed, so a field sits at an arbitrary alignment.
// Reading it through the struct type would be an unaligned access; copying the
// bytes into a local of the field type keeps the test well defined and still
// checks the encoded value.
template <typename T>
T ReadValue(const std::vector<char>& bytes, std::size_t offset) {
    T value{};
    EXPECT_LE(offset + sizeof(T), bytes.size());
    std::memcpy(&value, bytes.data() + offset, sizeof(T));
    return value;
}

std::array<std::uint8_t, 16> ReadSessionId(const std::vector<char>& bytes) {
    std::array<std::uint8_t, 16> id{};
    if (bytes.size() >= 12 + id.size()) {
        std::memcpy(id.data(), bytes.data() + 12, id.size());
    }
    return id;
}

TEST(FileSinkTest, RoundTripsFileBytes) {
    TempDir dir;
    FileSink sink(1 << 20, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 123456));
    auto records = MakeRecords({16, 32});
    const std::size_t split = 10;
    struct iovec iov[2] = {
        {records.data(), split},
        {records.data() + split, records.size() - split},
    };
    ASSERT_TRUE(sink.WriteRecords(iov, 2, records.size(), 2));
    sink.Close();

    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(files.size(), 1u);
    const auto bytes = ReadFile(files.front());
    ASSERT_EQ(bytes.size(), kTrafficFileHeaderSize + kTrafficBlockHeaderSize +
                                records.size() + kTrafficFooterSize);

    // The first twelve bytes pin the on-disk encoding: the file magic, both
    // version fields, and the header size, all little-endian. They are checked
    // as bytes so the test does not read the file through the same structs the
    // writer used.
    const std::array<std::uint8_t, 12> expected_prefix = {
        'M', 'C', 'R', '1', 0x01, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
    };
    ASSERT_EQ(0, std::memcmp(bytes.data(), expected_prefix.data(),
                             expected_prefix.size()));

    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, 0), kTrafficFileMagic);
    EXPECT_EQ(ReadValue<std::uint16_t>(bytes, 4), kTrafficVersionMajor);
    EXPECT_EQ(ReadValue<std::uint16_t>(bytes, 6), kTrafficVersionMinor);
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, 8), kTrafficFileHeaderSize);
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, 28), 0u);
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, 36), 123456u);
    const auto session_id = ReadSessionId(bytes);
    EXPECT_TRUE(std::any_of(session_id.begin(), session_id.end(),
                            [](std::uint8_t b) { return b != 0; }));

    const std::size_t block_offset = kTrafficFileHeaderSize;
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, block_offset),
              kTrafficBlockMagic);
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, block_offset + 4),
              records.size());
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, block_offset + 8), 2u);
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, block_offset + 12),
              Crc32cValue(records.data(), records.size()));

    const std::size_t records_offset =
        kTrafficFileHeaderSize + kTrafficBlockHeaderSize;
    EXPECT_EQ(0, std::memcmp(bytes.data() + records_offset, records.data(),
                             records.size()));
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, records_offset + 24),
              1'770'000'000'123u);

    const std::size_t footer_offset = records_offset + records.size();
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, footer_offset),
              kTrafficFooterMagic);
    EXPECT_EQ(ReadValue<std::uint32_t>(bytes, footer_offset + 4), 1u);
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, footer_offset + 8), 2u);
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, footer_offset + 16),
              records.size());
    EXPECT_EQ(ReadValue<std::uint64_t>(bytes, footer_offset + 24), 123456u);
}

TEST(FileSinkTest, RotatesSegmentsAndReusesTheSessionId) {
    TempDir dir;
    const auto records = MakeRecords({64});
    const std::uint64_t segment_bytes = kTrafficFileHeaderSize +
                                        kTrafficBlockHeaderSize +
                                        records.size() + kTrafficFooterSize + 1;
    FileSink sink(segment_bytes, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 1000));
    for (int i = 0; i < 3; ++i) {
        struct iovec iov{const_cast<char*>(records.data()), records.size()};
        ASSERT_TRUE(sink.WriteRecords(&iov, 1, records.size(), 1));
    }
    sink.Close();

    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(files.size(), 3u);
    std::array<std::uint8_t, 16> session_id{};
    for (std::size_t i = 0; i < files.size(); ++i) {
        const auto bytes = ReadFile(files[i]);
        ASSERT_GE(bytes.size(), kTrafficFileHeaderSize + kTrafficFooterSize);
        EXPECT_EQ(ReadValue<std::uint64_t>(bytes, 28), i);
        const auto id = ReadSessionId(bytes);
        if (i == 0) {
            session_id = id;
        } else {
            EXPECT_EQ(id, session_id);
        }
        // Every rotated segment is finalized on its own.
        const std::size_t footer_offset = bytes.size() - kTrafficFooterSize;
        EXPECT_EQ(ReadValue<std::uint32_t>(bytes, footer_offset),
                  kTrafficFooterMagic);
        EXPECT_EQ(ReadValue<std::uint32_t>(bytes, footer_offset + 4), 1u);
    }
}

TEST(FileSinkTest, RotatesSegmentsOnTime) {
    TempDir dir;
    FileSink sink(1 << 20, 1, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 1100));
    std::array<char, 32> data{};
    struct iovec iov{data.data(), data.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    sink.Close();
    EXPECT_EQ(sink.segments(), 2u);
}

TEST(FileSinkTest, DoesNotRotateAboveTheClockRange) {
    TempDir dir;
    FileSink sink(1 << 20, std::numeric_limits<std::uint64_t>::max(), 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 1200));
    std::array<char, 32> data{};
    struct iovec iov{data.data(), data.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    sink.Close();
    // A millisecond product would overflow here and rotate immediately.
    EXPECT_EQ(sink.segments(), 1u);
}

TEST(FileSinkTest, EnforcesQuotaAndPermissions) {
    TempDir dir;
    FileSink sink(4096, 0, 1024);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 2000));
    std::array<char, 512> data{};
    struct iovec iov{data.data(), data.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    // Hitting the limit stops the session but still finalizes it, so the sink
    // reports the quota rather than a write failure.
    EXPECT_TRUE(sink.quota_reached());
    EXPECT_FALSE(sink.failed());
    sink.Close();

    struct stat st{};
    ASSERT_EQ(::stat(sink.session_dir().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0700);
    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(files.size(), 1u);
    ASSERT_EQ(::stat(files.front().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600);
}

TEST(FileSinkTest, StopsAtQuotaBeforeRotating) {
    TempDir dir;
    // Five 56-byte blocks leave the first segment at 424 bytes. The next block
    // would rotate, and the rotation alone costs the old footer plus a new
    // header and footer (104 bytes), so the session would end above 568 bytes
    // even though the block fits the remaining budget on its own.
    FileSink sink(500, 0, 568);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 4700));
    std::array<char, 56> data{};
    struct iovec iov{data.data(), data.size()};
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    }
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    EXPECT_TRUE(sink.quota_reached());
    EXPECT_FALSE(sink.failed());
    // Later writes stay silent: the session already ended at its size limit.
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    EXPECT_FALSE(sink.failed());
    sink.Close();
    EXPECT_EQ(sink.segments(), 1u);
    EXPECT_EQ(sink.bytes_written(), kTrafficFileHeaderSize +
                                        5 * kTrafficBlockHeaderSize +
                                        5 * data.size() + kTrafficFooterSize);
    EXPECT_LE(sink.bytes_written(), 568u);
}

TEST(FileSinkTest, GivesSessionsStartedTogetherDistinctIds) {
    TempDir dir;
    FileSink first(4096, 0, 0);
    FileSink second(4096, 0, 0);
    // Both sinks start at the same millisecond, and they open concurrently so
    // the directory choice is decided by the create call rather than by a check
    // that both could pass.
    const std::string output = dir.path().string();
    std::atomic<int> ready{0};
    const auto open = [&](FileSink& sink) {
        ready.fetch_add(1);
        while (ready.load() != 2) {
        }
        EXPECT_TRUE(sink.OpenSession(output, 5000));
    };
    std::thread a(open, std::ref(first));
    std::thread b(open, std::ref(second));
    a.join();
    b.join();

    const std::string first_dir = first.session_dir();
    const std::string second_dir = second.session_dir();
    ASSERT_FALSE(first_dir.empty());
    ASSERT_FALSE(second_dir.empty());
    EXPECT_NE(first_dir, second_dir);

    // Both sessions must have their own segment header; a missing file would
    // otherwise show up as "two different ids".
    const auto read_id = [](const std::string& session_dir) {
        const auto files = SegmentFiles(session_dir);
        EXPECT_EQ(files.size(), 1u);
        if (files.empty()) {
            return std::array<std::uint8_t, 16>{};
        }
        return ReadSessionId(ReadFile(files.front()));
    };
    const auto first_id = read_id(first_dir);
    const auto second_id = read_id(second_dir);
    EXPECT_NE(first_id, second_id);
    EXPECT_TRUE(std::any_of(first_id.begin(), first_id.end(),
                            [](std::uint8_t byte) { return byte != 0; }));
    first.Close();
    second.Close();
}

TEST(FileSinkTest, RejectsEmptyBlock) {
    TempDir dir;
    FileSink sink(4096, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 4500));
    EXPECT_FALSE(sink.WriteRecords(nullptr, 0, 0, 0));
    EXPECT_TRUE(sink.failed());
    sink.Close();
}

TEST(FileSinkTest, RejectsBlocksAboveTheFormatLimit) {
    TempDir dir;
    FileSink sink(4096, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 4600));
    // The buffer holds as many bytes as the iovec claims, so the test cannot
    // turn a reordered length check into an out-of-bounds read.
    std::vector<char> data(static_cast<std::size_t>(kMaxBlockBytes) + 1, 'z');
    struct iovec iov{data.data(), data.size()};
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    EXPECT_TRUE(sink.failed());
    sink.Close();
}

TEST(FileSinkTest, RejectsInconsistentByteCount) {
    TempDir dir;
    FileSink sink(4096, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 2500));
    std::array<char, 16> data{};
    struct iovec iov{data.data(), data.size()};
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size() - 1, 1));
    EXPECT_TRUE(sink.failed());

    // A record header alone is 32 bytes, so 16 bytes cannot hold one record.
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 2501));
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    EXPECT_TRUE(sink.failed());
    sink.Close();
}

TEST(FileSinkTest, WritesBlocksWithMoreSegmentsThanWritevAccepts) {
    TempDir dir;
    FileSink sink(1 << 20, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 4700));
    // writev rejects more than IOV_MAX segments; the sink writes the block in
    // slices instead of turning a large iovec into a write failure.
    constexpr std::size_t kSegments = 1100;
    std::string payload(8 * kSegments, 'y');
    std::vector<struct iovec> iov(kSegments);
    for (std::size_t i = 0; i < kSegments; ++i) {
        iov[i].iov_base = payload.data() + i * 8;
        iov[i].iov_len = 8;
    }
    ASSERT_TRUE(sink.WriteRecords(iov.data(), static_cast<int>(iov.size()),
                                  payload.size(), 1));
    sink.Close();
    EXPECT_FALSE(sink.failed());

    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(files.size(), 1u);
    const auto bytes = ReadFile(files.front());
    const std::size_t records_offset =
        kTrafficFileHeaderSize + kTrafficBlockHeaderSize;
    ASSERT_GE(bytes.size(), records_offset + payload.size());
    EXPECT_EQ(0, std::memcmp(bytes.data() + records_offset, payload.data(),
                             payload.size()));
}

TEST(FileSinkTest, RejectsWritesBeforeOpenAndAfterClose) {
    TempDir dir;
    FileSink sink(4096, 0, 0);
    // One record header is the smallest block the format allows.
    std::array<char, kTrafficRecordHeaderSize> data{};
    struct iovec iov{data.data(), data.size()};
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 3000));
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    sink.Close();
    EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
}

// Applies RLIMIT_FSIZE for its lifetime so a write fails the way a full disk
// does. The destructor restores the previous limit and signal action, also
// when an assertion returns from the test early.
class ScopedFileSizeLimit {
   public:
    explicit ScopedFileSizeLimit(std::uint64_t bytes) {
        // SIGXFSZ terminates the process, so the write has to see EFBIG.
        struct sigaction ignore{};
        ignore.sa_handler = SIG_IGN;
        ::sigaction(SIGXFSZ, &ignore, &previous_action_);
        if (::getrlimit(RLIMIT_FSIZE, &previous_) != 0) {
            return;
        }
        struct rlimit limited = previous_;
        limited.rlim_cur = static_cast<rlim_t>(bytes);
        applied_ = ::setrlimit(RLIMIT_FSIZE, &limited) == 0;
    }

    ~ScopedFileSizeLimit() {
        if (applied_) {
            ::setrlimit(RLIMIT_FSIZE, &previous_);
        }
        ::sigaction(SIGXFSZ, &previous_action_, nullptr);
    }

    bool applied() const { return applied_; }

   private:
    struct rlimit previous_{};
    struct sigaction previous_action_{};
    bool applied_ = false;
};

// A failed write can leave part of a block behind. The segment still has to
// end on a block boundary, otherwise a reader rejects the whole file and the
// blocks that did reach the disk are lost.
TEST(FileSinkTest, KeepsTheBlocksWrittenBeforeAFailure) {
    TempDir dir;
    // A zero segment size disables rotation, so the failure is a plain write.
    FileSink sink(0, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 5300));
    std::array<char, 64> data{};
    struct iovec iov{data.data(), data.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));

    {
        // 64 (header) + 2 * (16 + 64) + 40 (footer) leaves 20 spare bytes, so
        // the third block header and part of its payload fit before the write
        // fails.
        ScopedFileSizeLimit limit(264 + 20);
        ASSERT_TRUE(limit.applied());
        EXPECT_FALSE(sink.WriteRecords(&iov, 1, data.size(), 1));
        EXPECT_TRUE(sink.failed());
    }

    sink.Close();
    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(1u, files.size());
    const std::uint64_t complete =
        kTrafficFileHeaderSize + 2 * (kTrafficBlockHeaderSize + data.size());
    EXPECT_EQ(complete + kTrafficFooterSize,
              std::filesystem::file_size(files.front()));
    const auto bytes = ReadFile(files.front());
    ASSERT_GE(bytes.size(), kTrafficFooterSize);
    const std::size_t footer_offset = bytes.size() - kTrafficFooterSize;
    EXPECT_EQ(kTrafficFooterMagic,
              ReadValue<std::uint32_t>(bytes, footer_offset));
    EXPECT_EQ(2u, ReadValue<std::uint32_t>(bytes, footer_offset + 4));
}

TEST(FileSinkTest, KeepsTheSessionFailedWhenItCannotBeFinalized) {
    TempDir dir;
    FileSink sink(1 << 20, 0, 0);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), 7000));
    std::array<char, 32> data{};
    struct iovec iov{data.data(), data.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, data.size(), 1));
    const auto files = SegmentFiles(sink.session_dir());
    ASSERT_EQ(1u, files.size());
    const std::uint64_t size = std::filesystem::file_size(files.front());
    {
        // The footer cannot be appended, so the open session must not be
        // replaced by a new one that reports success.
        ScopedFileSizeLimit limit(size);
        ASSERT_TRUE(limit.applied());
        EXPECT_FALSE(sink.OpenSession(dir.path().string(), 7001));
    }
    EXPECT_TRUE(sink.failed());
}

}  // namespace
}  // namespace mooncake::traffic
