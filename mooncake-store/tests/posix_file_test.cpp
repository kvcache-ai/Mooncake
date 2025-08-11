#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include "file_interface.h"

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

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}