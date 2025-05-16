#include <gtest/gtest.h>
#include "file_storage_backend.h"
#include <filesystem>
#include <cstring>
#include <memory>

namespace mooncake {
namespace testing {

class FileStorageBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "fsb_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directory(test_dir_);
    backend_ = std::make_unique<FileStorageBackend>(test_dir_.string());
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::unique_ptr<FileStorageBackend> backend_;
    std::filesystem::path test_dir_;
};

TEST_F(FileStorageBackendTest, WriteAndReadSingleKey) {
    std::string key = "test_key";
    std::string content = "Mooncake Store SSD Offload!";
    std::vector<char> buffer(content.begin(), content.end());
    Slice write_slice{buffer.data(), buffer.size()};
    std::vector<Slice> slices{write_slice};

    {
        EXPECT_EQ(backend_->Write(key, slices), ErrorCode::OK);
    }

    std::vector<char> read_buf(buffer.size(), 0);
    Slice read_slice{read_buf.data(), read_buf.size()};
    std::vector<Slice> read_slices{read_slice};

    EXPECT_EQ(backend_->Read(key, read_slices), ErrorCode::OK);
    EXPECT_EQ(std::memcmp(buffer.data(), read_buf.data(), buffer.size()), 0);
}

TEST_F(FileStorageBackendTest, WriteAndReadMultipleSlices) {
    std::string key = "multi_slice_key";
    std::string content1 = "Tensor1Data";
    std::string content2 = "Tensor2DataLonger";
    std::string content3 = "Tensor3DataLongest";

    std::vector<char> buffer1(content1.begin(), content1.end());
    std::vector<char> buffer2(content2.begin(), content2.end());
    std::vector<char> buffer3(content3.begin(), content3.end());

    Slice s1{buffer1.data(), buffer1.size()};
    Slice s2{buffer2.data(), buffer2.size()};
    Slice s3{buffer3.data(), buffer3.size()};
    std::vector<Slice> write_slices{s1, s2, s3};

    EXPECT_EQ(backend_->Write(key, write_slices), ErrorCode::OK);

    std::vector<char> read_buf1(buffer1.size(), 0);
    std::vector<char> read_buf2(buffer2.size(), 0);
    std::vector<char> read_buf3(buffer3.size(), 0);

    Slice rs1{read_buf1.data(), read_buf1.size()};
    Slice rs2{read_buf2.data(), read_buf2.size()};
    Slice rs3{read_buf3.data(), read_buf3.size()};
    std::vector<Slice> read_slices{rs1, rs2, rs3};

    EXPECT_EQ(backend_->Read(key, read_slices), ErrorCode::OK);
    EXPECT_EQ(std::memcmp(buffer1.data(), read_buf1.data(), buffer1.size()), 0);
    EXPECT_EQ(std::memcmp(buffer2.data(), read_buf2.data(), buffer2.size()), 0);
    EXPECT_EQ(std::memcmp(buffer3.data(), read_buf3.data(), buffer3.size()), 0);
}

TEST_F(FileStorageBackendTest, ReadNonExistentKey) {
    std::string key = "nonexistent";
    std::vector<char> read_buf(10, 0);
    Slice read_slice{read_buf.data(), read_buf.size()};
    std::vector<Slice> slices{read_slice};

    EXPECT_EQ(backend_->Read(key, slices), ErrorCode::NOT_FOUND);
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