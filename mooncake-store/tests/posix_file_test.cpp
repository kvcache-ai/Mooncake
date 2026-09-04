#include <glog/logging.h>
#include <gtest/gtest.h>
#include <array>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <fcntl.h>
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
