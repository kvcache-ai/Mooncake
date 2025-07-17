#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <vector>
#include <sys/uio.h>

#include "local_file.h"

namespace mooncake {

// Test fixture for LocalFile tests
class LocalFileTest : public ::testing::Test {
protected:
	void SetUp() override {
		google::InitGoogleLogging("LocalFileTest");
		FLAGS_logtostderr = 1;

		// Create a test file
		test_filename = "test_file.txt";
		std::ofstream outfile(test_filename);
		outfile << "Initial test content";
		outfile.close();
	}

	void TearDown() override {
		google::ShutdownGoogleLogging();

		// Remove the test file
		remove(test_filename.c_str());
	}

	std::string test_filename;
};

// Test basic file creation and destruction
TEST_F(LocalFileTest, FileLifecycle) {
	FILE *file = fopen(test_filename.c_str(), "r+");
	ASSERT_NE(file, nullptr);

	LocalFile local_file(test_filename, file);
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::OK);

	// Destructor will close the file
}

// Test basic write operation
TEST_F(LocalFileTest, BasicWrite) {
	FILE *file = fopen(test_filename.c_str(), "r+");
	ASSERT_NE(file, nullptr);

	LocalFile local_file(test_filename, file);

	std::string test_data = "Test write data";
	ssize_t result = local_file.write(test_data, test_data.size());

	EXPECT_EQ(result, test_data.size());
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::OK);
}

// Test basic read operation
TEST_F(LocalFileTest, BasicRead) {
	// First write some data
	{
		FILE *file = fopen(test_filename.c_str(), "w");
		ASSERT_NE(file, nullptr);
		LocalFile local_file(test_filename, file);
		std::string test_data = "Test read data";
		local_file.write(test_data, test_data.size());
	}

	// Now read it back
	FILE *file = fopen(test_filename.c_str(), "r");
	ASSERT_NE(file, nullptr);

	LocalFile local_file(test_filename, file);

	std::string buffer;
	ssize_t result = local_file.read(buffer, 100); // Read up to 100 bytes

	EXPECT_EQ(result, 14); // "Test read data" is 14 bytes
	EXPECT_EQ(buffer, "Test read data");
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::OK);
}

// Test vectorized write operation
TEST_F(LocalFileTest, VectorizedWrite) {
	FILE *file = fopen(test_filename.c_str(), "r+");
	ASSERT_NE(file, nullptr);

	LocalFile local_file(test_filename, file);

	std::string data1 = "First part ";
	std::string data2 = "Second part";

	iovec iov[2];
	iov[0].iov_base = const_cast<char *>(data1.data());
	iov[0].iov_len = data1.size();
	iov[1].iov_base = const_cast<char *>(data2.data());
	iov[1].iov_len = data2.size();

	ssize_t result = local_file.pwritev(iov, 2, 0);

	EXPECT_EQ(result, data1.size() + data2.size());
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::OK);
}

// Test vectorized read operation
TEST_F(LocalFileTest, VectorizedRead) {
	// First write some data
	{
		FILE *file = fopen(test_filename.c_str(), "w");
		ASSERT_NE(file, nullptr);
		LocalFile local_file(test_filename, file);
		std::string test_data = "Vectorized read test data";
		local_file.write(test_data, test_data.size());
	}

	// Now read it back with vectorized read
	FILE *file = fopen(test_filename.c_str(), "r");
	ASSERT_NE(file, nullptr);

	LocalFile local_file(test_filename, file);

	char buf1[10];
	char buf2[15];

	iovec iov[2];
	iov[0].iov_base = buf1;
	iov[0].iov_len = sizeof(buf1);
	iov[1].iov_base = buf2;
	iov[1].iov_len = sizeof(buf2);

	ssize_t result = local_file.preadv(iov, 2, 0);

	EXPECT_EQ(result, 25); // "Vectorized read test data" is 25 bytes
	EXPECT_EQ(std::string(buf1, 10), "Vectorized");
	EXPECT_EQ(std::string(buf2, 15), " read test data");
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::OK);
}

// Test error cases
TEST_F(LocalFileTest, ErrorCases) {
	// Test invalid file handle
	LocalFile local_file("nonexistent.txt", nullptr, ErrorCode::OK);
	EXPECT_EQ(local_file.get_error_code(), ErrorCode::FILE_INVALID_HANDLE);

	// Test write to invalid file
	std::string test_data = "test";
	ssize_t result = local_file.write(test_data, test_data.size());
	EXPECT_EQ(result, -1);

	// Test read from invalid file
	std::string buffer;
	result = local_file.read(buffer, 10);
	EXPECT_EQ(result, -1);
}

} // namespace mooncake

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
