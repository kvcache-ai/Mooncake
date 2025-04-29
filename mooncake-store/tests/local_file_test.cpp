#include <gtest/gtest.h>
#include <fstream>
#include <cstdio>
#include <cerrno>
#include <vector>
#include <sys/uio.h>

#include "local_file.h"  // Include the header for LocalFile class

namespace mooncake {
namespace testing {

class LocalFileTest : public ::testing::Test {
protected:
    // Test fixture setup and teardown
    static void SetUpTestSuite() {
        // This function will be run before any test cases are executed.
        // We can initialize shared resources if needed.
    }

    static void TearDownTestSuite() {
        // This function will be run after all test cases are executed.
        // We can release shared resources if needed.
    }

    void SetUp() override {
        // This function will be run before each test case.
        // You can set up individual resources like creating a temporary file.
    }

    void TearDown() override {
        // This function will be run after each test case.
        // Clean up resources after each test.
    }
};

TEST_F(LocalFileTest, TestRead) {
    // Create a temporary file with some content
    const std::string test_file = "test_file.txt";
    FILE *file = fopen(test_file.c_str(), "w+");
    ASSERT_TRUE(file != nullptr);

    const char *test_data = "Hello, world!";
    fwrite(test_data, 1, strlen(test_data), file);
    fflush(file);
    fseek(file, 0, SEEK_SET);  // Go back to the start of the file

    // Create LocalFile object
    LocalFile local_file(test_file, file);

    // Test normal read
    char buffer[50];
    ssize_t bytes_read = local_file.read(buffer, sizeof(buffer) - 1);
    ASSERT_GE(bytes_read, 0);
    buffer[bytes_read] = '\0';
    ASSERT_STREQ(buffer, test_data);

    // Test read with bad file
    LocalFile bad_file("bad_file.txt", nullptr);
    bytes_read = bad_file.read(buffer, sizeof(buffer) - 1);
    ASSERT_EQ(bytes_read, -1);
    ASSERT_EQ(bad_file.error_code_, ErrorCode::BAD_FILE);

}

TEST_F(LocalFileTest, TestWrite) {
    // Create a temporary file
    const std::string test_file = "test_file.txt";
    FILE *file = fopen(test_file.c_str(), "w+");
    ASSERT_TRUE(file != nullptr);

    // Create LocalFile object
    LocalFile local_file(test_file, file);

    // Test normal write
    const char *write_data = "Hello, GTest!";
    ssize_t bytes_written = local_file.write(write_data, strlen(write_data));
    ASSERT_GE(bytes_written, 0);
    ASSERT_EQ(bytes_written, strlen(write_data));

    // Check if the content was written correctly
    fseek(file, 0, SEEK_SET);
    char buffer[50];
    fread(buffer, 1, bytes_written, file);
    buffer[bytes_written] = '\0';
    ASSERT_STREQ(buffer, write_data);

    // Test write with bad file
    LocalFile bad_file("bad_file.txt", nullptr);
    bytes_written = bad_file.write(write_data, strlen(write_data));
    ASSERT_EQ(bytes_written, -1);
    ASSERT_EQ(bad_file.error_code_, ErrorCode::BAD_FILE);

}

TEST_F(LocalFileTest, TestPreadv) {
    const std::string test_file = "test_file.txt";
    FILE *file = fopen(test_file.c_str(), "w+");
    ASSERT_TRUE(file != nullptr);

    const char *test_data = "Hello, preadv! This is a test for the preadv function. "
                            "It should handle multiple buffers correctly, even with larger data.";
    fwrite(test_data, 1, strlen(test_data), file);
    fflush(file);

    fseek(file, 0, SEEK_SET);

    LocalFile local_file(test_file, file);

    struct iovec iov[3];
    char buffer1[20], buffer2[20], buffer3[50];
    iov[0].iov_base = buffer1;
    iov[0].iov_len = sizeof(buffer1) - 1;
    std::cout << "iov[0].iov_len=" << iov[0].iov_len << std::endl;
    iov[1].iov_base = buffer2;
    iov[1].iov_len = sizeof(buffer2) - 1;
    iov[2].iov_base = buffer3;
    iov[2].iov_len = sizeof(buffer3) - 1;

    ssize_t bytes_read = local_file.preadv(iov, 3, 0);
    ASSERT_GE(bytes_read, 0);
    std::cout << "bytes_read=" << bytes_read << std::endl;

    buffer1[iov[0].iov_len] = '\0';
    buffer2[iov[1].iov_len] = '\0';
    buffer3[iov[2].iov_len] = '\0';

    ASSERT_TRUE(bytes_read > 0);
    ASSERT_EQ(memcmp(buffer1, iov[0].iov_base, 19), 0);
    ASSERT_EQ(memcmp(buffer2, iov[1].iov_base, 19), 0);
    ASSERT_EQ(memcmp(buffer3, iov[2].iov_base, 49), 0);

    bytes_read = local_file.preadv(iov, 3, 500);
    ASSERT_EQ(bytes_read, 0);

    LocalFile bad_file("bad_file.txt", nullptr);
    bytes_read = bad_file.preadv(iov, 3, 0);
    ASSERT_EQ(bytes_read, -1);
    ASSERT_EQ(bad_file.error_code_, ErrorCode::BAD_FILE);
}

TEST_F(LocalFileTest, TestPwritev) {
    const std::string test_file = "test_file.txt";
    FILE *file = fopen(test_file.c_str(), "w+");
    ASSERT_TRUE(file != nullptr);

    // Create LocalFile object
    LocalFile local_file(test_file, file);

    // Prepare the iovec structure for pwritev
    const char *write_data = "Hello, pwritev!";
    struct iovec iov[1];
    iov[0].iov_base = const_cast<char*>(write_data);
    iov[0].iov_len = strlen(write_data);

    // Test normal pwritev
    ssize_t bytes_written = local_file.pwritev(iov, 1, 0);
    ASSERT_GE(bytes_written, 0);
    ASSERT_EQ(bytes_written, strlen(write_data));

    // Check if the content was written correctly
    fseek(file, 0, SEEK_SET);
    char buffer[50];
    fread(buffer, 1, bytes_written, file);
    buffer[bytes_written] = '\0';
    ASSERT_STREQ(buffer, write_data);

    // Test pwritev with bad file
    LocalFile bad_file("bad_file.txt", nullptr);
    bytes_written = bad_file.pwritev(iov, 1, 0);
    ASSERT_EQ(bytes_written, -1);
    ASSERT_EQ(bad_file.error_code_, ErrorCode::BAD_FILE);

}

// Test error codes
TEST_F(LocalFileTest, TestErrorCodes) {
    const std::string test_file = "test_file.txt";
    FILE *file = fopen(test_file.c_str(), "w+");
    ASSERT_TRUE(file != nullptr);

    // Create LocalFile object
    LocalFile local_file(test_file, file);

    // Test error code when file is null
    LocalFile bad_file("bad_file.txt", nullptr);
    bad_file.read(nullptr, 0);
    ASSERT_EQ(bad_file.error_code_, ErrorCode::BAD_FILE);

}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}