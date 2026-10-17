#include <glog/logging.h>
#include <gtest/gtest.h>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/uio.h>
#include <string_view>
#include <vector>
#include "file_interface.h"
#include "../src/uring_submit.h"

namespace mooncake {

class PosixFileTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PosixFileTest");
        FLAGS_logtostderr = 1;

        // Create and open a test file
        test_filename = "test_file.txt";
        test_fd = open(test_filename.c_str(), O_CREAT | O_RDWR, 0644);
        ASSERT_GE(test_fd, 0) << "Failed to open test file";
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        if (test_fd >= 0) {
            close(test_fd);
        }
        remove(test_filename.c_str());
    }

    std::string test_filename;
    int test_fd = -1;
};

#ifdef USE_URING
TEST(UringSubmitTest, ContinuesAfterPositiveShortSubmit) {
    unsigned pending = 8;
    std::vector<unsigned> requested;
    std::array<int, 2> returns{3, 5};
    size_t call = 0;

    auto result = detail::submit_all_pending(
        [&] { return pending; },
        [&](unsigned requested_count) {
            requested.push_back(requested_count);
            int submitted = returns[call++];
            pending -= static_cast<unsigned>(submitted);
            return submitted;
        },
        [] {});

    EXPECT_EQ(result.error, 0);
    EXPECT_EQ(result.submitted, 8U);
    EXPECT_EQ(result.pending, 0U);
    EXPECT_EQ(requested, (std::vector<unsigned>{8, 5}));
}

TEST(UringSubmitTest, RetriesTransientSubmissionErrors) {
    unsigned pending = 4;
    std::array<int, 3> returns{-EINTR, -EAGAIN, 4};
    size_t call = 0;
    unsigned yields = 0;

    auto result = detail::submit_all_pending(
        [&] { return pending; },
        [&](unsigned) {
            int ret = returns[call++];
            if (ret > 0) pending -= static_cast<unsigned>(ret);
            return ret;
        },
        [&] { ++yields; });

    EXPECT_EQ(result.error, 0);
    EXPECT_EQ(result.submitted, 4U);
    EXPECT_EQ(result.pending, 0U);
    EXPECT_EQ(yields, 2U);
}

TEST(UringSubmitTest, StopsAfterBoundedNoProgress) {
    unsigned pending = 4;
    unsigned calls = 0;
    auto submit = [&](unsigned) {
        ++calls;
        return -ENOMEM;
    };

    auto result =
        detail::submit_all_pending([&] { return pending; }, submit, [] {}, 2);

    EXPECT_EQ(result.error, -ENOMEM);
    EXPECT_EQ(result.submitted, 0U);
    EXPECT_EQ(result.pending, pending);
    EXPECT_EQ(calls, 3U);
}

// detail::single_sqe_write_max() for max_sectors_kb 128, and for 1280 (the
// cap).
constexpr size_t kLimitAt128K = 2 * 128 * 1024;
constexpr size_t kLimitAt1280K = detail::kSingleSqeWriteCap;

TEST(UringChunkTest, WriteUpToTheLimitGoesOutAsOneSqe) {
    // A 4 KiB header page plus a 128 KiB value with max_sectors_kb 128, the
    // same header plus a 1 MiB value with 1280, a write of exactly the
    // limit, and a sub-page write.
    EXPECT_EQ(detail::write_chunk_size(135168, 32, 4096, kLimitAt128K),
              135168U);
    EXPECT_EQ(detail::write_chunk_size(1052672, 32, 4096, kLimitAt1280K),
              1052672U);
    EXPECT_EQ(detail::write_chunk_size(kLimitAt128K, 32, 4096, kLimitAt128K),
              kLimitAt128K);
    EXPECT_EQ(detail::write_chunk_size(1000, 32, 4096, kLimitAt128K), 1000U);
}

TEST(UringChunkTest, LargerWriteKeepsQueueDepthSplit) {
    // One byte over the limit, the header page plus a 1 MiB value with
    // max_sectors_kb 128, the header page plus a 2 MiB or a 64 MiB value
    // with 1280, and any write to an unknown device (limit 0).
    EXPECT_EQ(
        detail::write_chunk_size(kLimitAt128K + 1, 32, 4096, kLimitAt128K),
        16384U);
    EXPECT_EQ(detail::write_chunk_size(1052672, 32, 4096, kLimitAt128K),
              65536U);
    EXPECT_EQ(detail::write_chunk_size(2101248, 32, 4096, kLimitAt1280K),
              131072U);
    EXPECT_EQ(detail::write_chunk_size(67112960, 32, 4096, kLimitAt1280K),
              4194304U);
    EXPECT_EQ(detail::write_chunk_size(135168, 32, 4096, 0), 8192U);
}

TEST(UringChunkTest, SingleSqeWriteMaxReadsTheDeviceQueue) {
    // A fake sysfs: disk 259:0 with max_sectors_kb 128 and its partition
    // 259:1, and disk 259:2 with 1280, linked from dev/block the way the
    // kernel links them. A partition has no queue of its own.
    namespace fs = std::filesystem;
    const std::string root =
        ::testing::TempDir() + "uring_fake_sysfs_" + std::to_string(getpid());
    fs::remove_all(root);
    fs::create_directories(root + "/devices/disk0/disk0p1");
    fs::create_directories(root + "/devices/disk0/queue");
    fs::create_directories(root + "/devices/disk1/queue");
    fs::create_directories(root + "/devices/disk2/queue");
    fs::create_directories(root + "/dev/block");
    std::ofstream(root + "/devices/disk0/queue/max_sectors_kb") << "128\n";
    std::ofstream(root + "/devices/disk1/queue/max_sectors_kb") << "1280\n";
    std::ofstream(root + "/devices/disk2/queue/max_sectors_kb") << "none\n";
    fs::create_directory_symlink("../../devices/disk0",
                                 root + "/dev/block/259:0");
    fs::create_directory_symlink("../../devices/disk0/disk0p1",
                                 root + "/dev/block/259:1");
    fs::create_directory_symlink("../../devices/disk1",
                                 root + "/dev/block/259:2");
    fs::create_directory_symlink("../../devices/disk2",
                                 root + "/dev/block/259:4");

    EXPECT_EQ(detail::single_sqe_write_max(makedev(259, 0), root),
              kLimitAt128K);
    EXPECT_EQ(detail::single_sqe_write_max(makedev(259, 1), root),
              kLimitAt128K);
    EXPECT_EQ(detail::single_sqe_write_max(makedev(259, 2), root),
              kLimitAt1280K);
    // An unreadable value, no such device, and no block device at all
    // (tmpfs, NFS, btrfs).
    EXPECT_EQ(detail::single_sqe_write_max(makedev(259, 4), root), 0U);
    EXPECT_EQ(detail::single_sqe_write_max(makedev(259, 3), root), 0U);
    EXPECT_EQ(detail::single_sqe_write_max(makedev(0, 42), root), 0U);
    fs::remove_all(root);
}

TEST(UringChunkTest, SplitChunkSizeIsQueueDepthSplit) {
    EXPECT_EQ(detail::split_chunk_size(135168, 32, 4096), 8192U);
    EXPECT_EQ(detail::split_chunk_size(2101248, 32, 4096), 131072U);
    EXPECT_EQ(detail::split_chunk_size(1000, 32, 4096), 4096U);
}
#endif

// Test basic file lifecycle
TEST_F(PosixFileTest, FileLifecycle) {
    PosixFile posix_file(test_filename, test_fd);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
    // Destructor will close the file
}

// Test basic write operation
TEST_F(PosixFileTest, BasicWrite) {
    PosixFile posix_file(test_filename, test_fd);

    std::string test_data = "Test write data";
    auto result = posix_file.write(test_data, test_data.size());

    ASSERT_TRUE(result) << "Write failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, test_data.size());
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test basic read operation
TEST_F(PosixFileTest, BasicRead) {
    // Clear file content
    ASSERT_EQ(ftruncate(test_fd, 0), 0) << "Failed to truncate file";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    // Write test data
    const char* test_data = "Test read data";
    ssize_t written = write(test_fd, test_data, strlen(test_data));
    ASSERT_EQ(written, static_cast<ssize_t>(strlen(test_data)))
        << "Write failed";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    PosixFile posix_file(test_filename, test_fd);

    std::string buffer;
    auto result = posix_file.read(
        buffer, strlen(test_data));  // Read up to test_data bytes

    ASSERT_TRUE(result) << "Read failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, strlen(test_data));
    EXPECT_EQ(buffer, test_data);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test vectorized write operation
TEST_F(PosixFileTest, VectorizedWrite) {
    PosixFile posix_file(test_filename, test_fd);

    std::string data1 = "First part ";
    std::string data2 = "Second part";

    iovec iov[2];
    iov[0].iov_base = const_cast<char*>(data1.data());
    iov[0].iov_len = data1.size();
    iov[1].iov_base = const_cast<char*>(data2.data());
    iov[1].iov_len = data2.size();

    auto result = posix_file.vector_write(iov, 2, 0);

    ASSERT_TRUE(result) << "Vector write failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, data1.size() + data2.size());
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// A short pwritev must be treated as a failed write. Otherwise the PosixFile
// destructor has no failure state and leaves a truncated object on disk.
TEST_F(PosixFileTest, ShortVectorizedWriteRemovesPartialFile) {
#if defined(RLIMIT_FSIZE) && defined(SIGXFSZ)
    const std::string partial_filename =
        "short_vector_write_" + std::to_string(getpid()) + ".tmp";

    pid_t child = fork();
    ASSERT_GE(child, 0) << "Failed to fork short-write test";

    if (child == 0) {
        if (signal(SIGXFSZ, SIG_IGN) == SIG_ERR) _exit(10);

        const struct rlimit limit = {4096, 4096};
        if (setrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(11);

        int fd =
            open(partial_filename.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) _exit(12);

        std::string data(8192, 'x');
        {
            PosixFile posix_file(partial_filename, fd);
            iovec iov{data.data(), data.size()};
            auto result = posix_file.vector_write(&iov, 1, 0);
            if (result || result.error() != ErrorCode::FILE_WRITE_FAIL)
                _exit(13);
        }

        if (access(partial_filename.c_str(), F_OK) == 0) _exit(14);
        if (errno != ENOENT) _exit(15);
        _exit(0);
    }

    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    unlink(partial_filename.c_str());
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    GTEST_SKIP() << "RLIMIT_FSIZE/SIGXFSZ is unavailable on this platform";
#endif
}

// Test vectorized read operation
TEST_F(PosixFileTest, VectorizedRead) {
    // Clear file content
    ASSERT_EQ(ftruncate(test_fd, 0), 0) << "Failed to truncate file";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    // Write test data
    const char* test_data = "Vectorized read test data";
    ssize_t written = write(test_fd, test_data, strlen(test_data));
    ASSERT_EQ(written, static_cast<ssize_t>(strlen(test_data)))
        << "Write failed";
    ASSERT_NE(lseek(test_fd, 0, SEEK_SET), -1) << "Seek failed";

    PosixFile posix_file(test_filename, test_fd);

    char buf1[11] = {0};  // "Vectorized" + null
    char buf2[16] = {0};  // " read test data" + null

    iovec iov[2];
    iov[0].iov_base = buf1;
    iov[0].iov_len = 10;  // Exact length of "Vectorized"
    iov[1].iov_base = buf2;
    iov[1].iov_len = 15;  // Exact length of " read test data"

    auto result = posix_file.vector_read(iov, 2, 0);

    ASSERT_TRUE(result) << "Vector read failed with error: "
                        << toString(result.error());
    EXPECT_EQ(*result, strlen(test_data));
    EXPECT_STREQ(buf1, "Vectorized");
    EXPECT_STREQ(buf2, " read test data");
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::OK);
}

// Test error cases
TEST_F(PosixFileTest, ErrorCases) {
    // Test invalid file descriptor
    PosixFile posix_file("invalid.txt", -1);
    EXPECT_EQ(posix_file.get_error_code(), ErrorCode::FILE_INVALID_HANDLE);

    // Test write to invalid file
    std::string test_data = "test";
    auto write_result = posix_file.write(test_data, test_data.size());
    EXPECT_FALSE(write_result);
    EXPECT_EQ(write_result.error(), ErrorCode::FILE_NOT_FOUND);

    // Test read from invalid file
    std::string buffer;
    auto read_result = posix_file.read(buffer, test_data.size());
    EXPECT_FALSE(read_result);
    EXPECT_EQ(read_result.error(), ErrorCode::FILE_NOT_FOUND);
}

// Test file locking
TEST_F(PosixFileTest, FileLocking) {
    PosixFile posix_file(test_filename, test_fd);

    {
        // Acquire write lock
        auto lock = posix_file.acquire_write_lock();
        EXPECT_TRUE(lock.is_locked());

        // Try to read while locked
        std::string buffer;
        auto result = posix_file.read(buffer, 10);
        EXPECT_FALSE(result);
    }

    {
        // Acquire read lock
        auto lock = posix_file.acquire_read_lock();
        EXPECT_TRUE(lock.is_locked());
    }
}

#ifdef USE_URING
TEST_F(PosixFileTest, UringBatchReadReportsPerRequestResultsAcrossQueueDepth) {
    constexpr size_t kBlockSize = 128;
    constexpr int kRequestCount = 40;
    std::vector<std::array<char, kBlockSize>> expected(kRequestCount);
    for (int i = 0; i < kRequestCount; ++i) {
        expected[i].fill(static_cast<char>('A' + i % 26));
        ASSERT_EQ(pwrite(test_fd, expected[i].data(), expected[i].size(),
                         static_cast<off_t>(i * kBlockSize)),
                  static_cast<ssize_t>(expected[i].size()));
    }

    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, false);
    std::vector<std::array<char, kBlockSize>> actual(kRequestCount);
    std::vector<UringFile::ReadDesc> descs;
    descs.reserve(kRequestCount);
    for (int i = 0; i < kRequestCount; ++i) {
        descs.push_back(
            UringFile::ReadDesc{actual[i].data(), actual[i].size(),
                                static_cast<off_t>(i * kBlockSize)});
    }

    auto result = uring_file.batch_read(descs.data(), descs.size());
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    for (int i = 0; i < kRequestCount; ++i) {
        EXPECT_TRUE(descs[i].completed);
        EXPECT_EQ(descs[i].error, ErrorCode::OK);
        EXPECT_EQ(descs[i].bytes_read, kBlockSize);
        EXPECT_EQ(actual[i], expected[i]);
    }
}

TEST_F(PosixFileTest, UringBatchReadReportsShortReadPerRequest) {
    constexpr std::string_view data = "abcdef";
    ASSERT_EQ(pwrite(test_fd, data.data(), data.size(), 0),
              static_cast<ssize_t>(data.size()));

    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, false);
    std::array<char, 3> complete{};
    std::array<char, 8> short_read{};
    std::array<UringFile::ReadDesc, 2> descs{
        UringFile::ReadDesc{complete.data(), complete.size(), 0},
        UringFile::ReadDesc{short_read.data(), short_read.size(), 4}};

    auto result = uring_file.batch_read(descs.data(), descs.size());
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_TRUE(descs[0].completed);
    EXPECT_EQ(descs[0].error, ErrorCode::OK);
    EXPECT_EQ(descs[0].bytes_read, complete.size());
    EXPECT_TRUE(descs[1].completed);
    EXPECT_EQ(descs[1].error, ErrorCode::OK);
    EXPECT_EQ(descs[1].bytes_read, 2U);
}

TEST_F(PosixFileTest, UringBatchReadRejectsMisalignedDirectIoDescriptors) {
    int uring_fd = dup(test_fd);
    ASSERT_GE(uring_fd, 0);
    UringFile uring_file(test_filename, uring_fd, 32, true);

    void* allocation = nullptr;
    ASSERT_EQ(posix_memalign(&allocation, 4096, 8192), 0);
    std::unique_ptr<void, decltype(&std::free)> buffer(allocation, &std::free);
    auto* aligned = static_cast<char*>(buffer.get());

    auto expect_invalid = [&](UringFile::ReadDesc desc) {
        auto result = uring_file.batch_read(&desc, 1);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::FILE_INVALID_BUFFER);
        EXPECT_FALSE(desc.completed);
    };

    expect_invalid(UringFile::ReadDesc{aligned + 1, 4096, 0});
    expect_invalid(UringFile::ReadDesc{aligned, 4095, 0});
    expect_invalid(UringFile::ReadDesc{aligned, 4096, 1});
}

TEST_F(PosixFileTest, UringBatchReadDrainsErrorsBeforeNextOperation) {
    std::array<char, 16> first{};
    std::array<char, 16> second{};
    {
        int invalid_fd = open("/dev/null", O_WRONLY | O_CLOEXEC);
        ASSERT_GE(invalid_fd, 0);
        UringFile invalid_file("/dev/null", invalid_fd, 32, false);
        std::array<UringFile::ReadDesc, 2> invalid_descs{
            UringFile::ReadDesc{first.data(), first.size(), 0},
            UringFile::ReadDesc{second.data(), second.size(), 0}};

        auto invalid_result =
            invalid_file.batch_read(invalid_descs.data(), invalid_descs.size());
        ASSERT_FALSE(invalid_result.has_value());
        EXPECT_EQ(invalid_result.error(), ErrorCode::FILE_READ_FAIL);
        EXPECT_TRUE(invalid_descs[0].completed);
        EXPECT_EQ(invalid_descs[0].error, ErrorCode::FILE_READ_FAIL);
        EXPECT_TRUE(invalid_descs[1].completed);
        EXPECT_EQ(invalid_descs[1].error, ErrorCode::FILE_READ_FAIL);
    }

    constexpr std::string_view data = "ring-remains-usable";
    ASSERT_EQ(pwrite(test_fd, data.data(), data.size(), 0),
              static_cast<ssize_t>(data.size()));
    int valid_fd = dup(test_fd);
    ASSERT_GE(valid_fd, 0);
    UringFile valid_file(test_filename, valid_fd, 32, false);
    std::vector<char> output(data.size());
    UringFile::ReadDesc desc{output.data(), output.size(), 0};

    auto valid_result = valid_file.batch_read(&desc, 1);
    ASSERT_TRUE(valid_result.has_value()) << toString(valid_result.error());
    EXPECT_TRUE(desc.completed);
    EXPECT_EQ(desc.error, ErrorCode::OK);
    EXPECT_EQ(desc.bytes_read, data.size());
    EXPECT_EQ(std::string_view(output.data(), output.size()), data);
}

TEST_F(PosixFileTest, UringVectorIoHandlesUnalignedDirectIo) {
    const std::string direct_filename = "direct_vector_test.bin";
    remove(direct_filename.c_str());

    int direct_fd = open(direct_filename.c_str(),
                         O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC, 0644);
    ASSERT_GE(direct_fd, 0) << strerror(errno);
    ASSERT_EQ(ftruncate(direct_fd, 16384), 0);

    UringFile uring_file(direct_filename, direct_fd, 32, true);

    // Neighboring data in the same 4KiB block must survive RMW writes.
    const std::string neighbor = "keep-me";
    iovec neighbor_iov;
    neighbor_iov.iov_base = const_cast<char*>(neighbor.data());
    neighbor_iov.iov_len = neighbor.size();
    auto neighbor_write = uring_file.vector_write(&neighbor_iov, 1, 0);
    ASSERT_TRUE(neighbor_write.has_value()) << toString(neighbor_write.error());

    char header[48] = {};
    std::memcpy(header, "HDR", 3);
    const std::string key = "object-key";
    const std::string payload = "payload-bytes-for-direct-io";

    iovec write_iov[3];
    write_iov[0].iov_base = header;
    write_iov[0].iov_len = sizeof(header);
    write_iov[1].iov_base = const_cast<char*>(key.data());
    write_iov[1].iov_len = key.size();
    write_iov[2].iov_base = const_cast<char*>(payload.data());
    write_iov[2].iov_len = payload.size();

    constexpr off_t kOffset = 513;
    auto write_result = uring_file.vector_write(write_iov, 3, kOffset);
    ASSERT_TRUE(write_result.has_value()) << toString(write_result.error());
    const size_t expected = sizeof(header) + key.size() + payload.size();
    EXPECT_EQ(*write_result, expected);

    char out_header[sizeof(header)] = {};
    std::string out_key(key.size(), '\0');
    std::string out_payload(payload.size(), '\0');
    iovec read_iov[3];
    read_iov[0].iov_base = out_header;
    read_iov[0].iov_len = sizeof(out_header);
    read_iov[1].iov_base = out_key.data();
    read_iov[1].iov_len = out_key.size();
    read_iov[2].iov_base = out_payload.data();
    read_iov[2].iov_len = out_payload.size();

    auto read_result = uring_file.vector_read(read_iov, 3, kOffset);
    ASSERT_TRUE(read_result.has_value()) << toString(read_result.error());
    EXPECT_EQ(*read_result, expected);
    EXPECT_EQ(std::string_view(out_header, 3), "HDR");
    EXPECT_EQ(out_key, key);
    EXPECT_EQ(out_payload, payload);

    std::string out_neighbor(neighbor.size(), '\0');
    iovec neighbor_read_iov;
    neighbor_read_iov.iov_base = out_neighbor.data();
    neighbor_read_iov.iov_len = out_neighbor.size();
    auto neighbor_read = uring_file.vector_read(&neighbor_read_iov, 1, 0);
    ASSERT_TRUE(neighbor_read.has_value()) << toString(neighbor_read.error());
    EXPECT_EQ(out_neighbor, neighbor);

    remove(direct_filename.c_str());
}

TEST_F(PosixFileTest, UringFileTakesTheWriteLimitOfItsDevice) {
    // O_DIRECT files get their device's limit (0 on tmpfs or overlayfs);
    // buffered files always split.
    struct stat st;
    ASSERT_EQ(fstat(test_fd, &st), 0);
    UringFile direct_file(test_filename, dup(test_fd), 32, true);
    EXPECT_EQ(direct_file.single_sqe_write_max(),
              detail::single_sqe_write_max(st.st_dev));
    UringFile buffered_file(test_filename, dup(test_fd), 32, false);
    EXPECT_EQ(buffered_file.single_sqe_write_max(), 0U);
}

TEST_F(PosixFileTest, UringDirectWriteOfRecordShapeRoundTrips) {
    // The OffsetAllocator record shape: 24-byte header, key, zero padding to
    // 4 KiB, then a 128 KiB value, written at a 4 KiB offset. It is never
    // iovec-aligned, so it goes through the bounce buffer as one write call:
    // one SQE when the file's device allows 132 KiB, split otherwise.
    const std::string direct_filename = "direct_record_test.bin";
    remove(direct_filename.c_str());
    int direct_fd = open(direct_filename.c_str(),
                         O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC, 0644);
    ASSERT_GE(direct_fd, 0) << strerror(errno);
    if (fallocate(direct_fd, 0, 0, 1 << 20) != 0) {
        const int err = errno;
        close(direct_fd);
        remove(direct_filename.c_str());
        if (err == EOPNOTSUPP) {
            GTEST_SKIP() << "fallocate is not supported here";
        }
        FAIL() << strerror(err);
    }
    UringFile uring_file(direct_filename, direct_fd, 32, true);

    std::array<char, 24> header{};
    std::memcpy(header.data(), "RECORDHDR", 9);
    const std::string key = "record-shape-key";
    const std::string padding(4096 - header.size() - key.size(), '\0');
    std::string value(128 * 1024, '\0');
    for (size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<char>('a' + i % 26);
    }
    iovec write_iov[4] = {
        {header.data(), header.size()},
        {const_cast<char*>(key.data()), key.size()},
        {const_cast<char*>(padding.data()), padding.size()},
        {value.data(), value.size()},
    };
    constexpr off_t kOffset = 4096;
    auto write_result = uring_file.vector_write(write_iov, 4, kOffset);
    ASSERT_TRUE(write_result.has_value()) << toString(write_result.error());
    EXPECT_EQ(*write_result, 4096 + value.size());

    std::array<char, 24> out_header{};
    std::string out_key(key.size(), '\0');
    std::string out_value(value.size(), '\0');
    iovec header_iov{out_header.data(), out_header.size()};
    iovec key_iov{out_key.data(), out_key.size()};
    iovec value_iov{out_value.data(), out_value.size()};
    ASSERT_TRUE(uring_file.vector_read(&header_iov, 1, kOffset).has_value());
    ASSERT_TRUE(uring_file
                    .vector_read(&key_iov, 1,
                                 kOffset + static_cast<off_t>(header.size()))
                    .has_value());
    ASSERT_TRUE(
        uring_file.vector_read(&value_iov, 1, kOffset + 4096).has_value());
    EXPECT_EQ(out_header, header);
    EXPECT_EQ(out_key, key);
    EXPECT_EQ(out_value, value);
    remove(direct_filename.c_str());
}

TEST_F(PosixFileTest, UringVectorWriteAbortsWhenPreservationReadFails) {
    const std::string direct_filename = "direct_vector_preserve_fail.bin";
    remove(direct_filename.c_str());

    {
        int setup_fd = open(direct_filename.c_str(),
                            O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC, 0644);
        ASSERT_GE(setup_fd, 0) << strerror(errno);
        ASSERT_EQ(ftruncate(setup_fd, 16384), 0);
        UringFile setup_file(direct_filename, setup_fd, 32, true);

        const std::string neighbor = "keep-neighbor-bytes";
        iovec neighbor_iov;
        neighbor_iov.iov_base = const_cast<char*>(neighbor.data());
        neighbor_iov.iov_len = neighbor.size();
        auto neighbor_write = setup_file.vector_write(&neighbor_iov, 1, 0);
        ASSERT_TRUE(neighbor_write.has_value())
            << toString(neighbor_write.error());
    }

    // Re-open write-only so the RMW preservation read must fail. The write
    // must abort without clobbering the already-written neighbor bytes.
    int write_only_fd =
        open(direct_filename.c_str(), O_WRONLY | O_DIRECT | O_CLOEXEC);
    ASSERT_GE(write_only_fd, 0) << strerror(errno);
    {
        UringFile write_only_file(direct_filename, write_only_fd, 32, true);
        char payload[64];
        std::memset(payload, 'X', sizeof(payload));
        iovec write_iov;
        write_iov.iov_base = payload;
        write_iov.iov_len = sizeof(payload);

        constexpr off_t kOffset = 513;
        auto write_result =
            write_only_file.vector_write(&write_iov, 1, kOffset);
        ASSERT_FALSE(write_result.has_value());
        EXPECT_EQ(write_result.error(), ErrorCode::FILE_READ_FAIL);
    }

    int verify_fd =
        open(direct_filename.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
    ASSERT_GE(verify_fd, 0) << strerror(errno);
    UringFile verify_file(direct_filename, verify_fd, 32, true);

    const std::string neighbor = "keep-neighbor-bytes";
    std::string out_neighbor(neighbor.size(), '\0');
    iovec neighbor_read_iov;
    neighbor_read_iov.iov_base = out_neighbor.data();
    neighbor_read_iov.iov_len = out_neighbor.size();
    auto neighbor_read = verify_file.vector_read(&neighbor_read_iov, 1, 0);
    ASSERT_TRUE(neighbor_read.has_value()) << toString(neighbor_read.error());
    EXPECT_EQ(out_neighbor, neighbor);

    remove(direct_filename.c_str());
}
#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
