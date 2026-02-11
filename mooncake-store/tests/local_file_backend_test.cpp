#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "serialize/serializer_backend.h"

namespace mooncake::test {

namespace fs = std::filesystem;

class LocalFileBackendTest : public ::testing::Test {
   protected:
    const std::string& tmp_dir() const { return tmp_dir_; }

    std::unique_ptr<LocalFileBackend> backend_;

    void SetUp() override {
        google::InitGoogleLogging("LocalFileBackendTest");
        FLAGS_logtostderr = true;

        // Create a unique temporary directory
        std::string tmpl = (fs::temp_directory_path() /
                            "local_file_backend_test_XXXXXX")
                               .string();
        char* dir = mkdtemp(tmpl.data());
        ASSERT_NE(dir, nullptr) << "Failed to create temp directory";
        tmp_dir_ = dir;

        backend_ = std::make_unique<LocalFileBackend>(tmp_dir());
    }

    void TearDown() override {
        backend_.reset();
        if (!tmp_dir().empty() && fs::exists(tmp_dir())) {
            fs::remove_all(tmp_dir());
        }
        google::ShutdownGoogleLogging();
    }

   private:
    std::string tmp_dir_;
};

// ========== Normal Functionality ==========

TEST_F(LocalFileBackendTest, UploadDownloadBuffer_Roundtrip) {
    std::vector<uint8_t> data = {0, 1, 2, 128, 254, 255};
    auto upload_result = backend_->UploadBuffer("test/buf", data);
    ASSERT_TRUE(upload_result.has_value()) << upload_result.error();

    std::vector<uint8_t> downloaded;
    auto download_result = backend_->DownloadBuffer("test/buf", downloaded);
    ASSERT_TRUE(download_result.has_value()) << download_result.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(LocalFileBackendTest, UploadDownloadString_Roundtrip) {
    std::string data = "hello mooncake snapshot";
    auto upload_result = backend_->UploadString("test/str", data);
    ASSERT_TRUE(upload_result.has_value()) << upload_result.error();

    std::string downloaded;
    auto download_result = backend_->DownloadString("test/str", downloaded);
    ASSERT_TRUE(download_result.has_value()) << download_result.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(LocalFileBackendTest, ListObjectsWithPrefix) {
    // Upload several files under the same prefix
    backend_->UploadString("snap/20240101/metadata", "m");
    backend_->UploadString("snap/20240101/segments", "s");
    backend_->UploadString("snap/20240102/metadata", "m2");

    std::vector<std::string> keys;
    auto result = backend_->ListObjectsWithPrefix("snap/20240101/", keys);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(keys.size(), 2u);

    // Broader prefix should list all
    keys.clear();
    result = backend_->ListObjectsWithPrefix("snap/", keys);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(keys.size(), 3u);
}

TEST_F(LocalFileBackendTest, DeleteObjectsWithPrefix) {
    backend_->UploadString("snap/20240101/metadata", "m");
    backend_->UploadString("snap/20240101/segments", "s");

    auto del_result = backend_->DeleteObjectsWithPrefix("snap/20240101/");
    ASSERT_TRUE(del_result.has_value()) << del_result.error();

    // Verify files are gone
    std::string data;
    auto dl = backend_->DownloadString("snap/20240101/metadata", data);
    EXPECT_FALSE(dl.has_value());
}

TEST_F(LocalFileBackendTest, GetConnectionInfo) {
    auto info = backend_->GetConnectionInfo();
    EXPECT_NE(info.find(tmp_dir()), std::string::npos);
}

TEST_F(LocalFileBackendTest, UploadBuffer_CreatesSubdirectories) {
    std::vector<uint8_t> data = {42};
    auto result = backend_->UploadBuffer("a/b/c/deep_file", data);
    ASSERT_TRUE(result.has_value()) << result.error();

    std::vector<uint8_t> downloaded;
    auto dl = backend_->DownloadBuffer("a/b/c/deep_file", downloaded);
    ASSERT_TRUE(dl.has_value()) << dl.error();
    EXPECT_EQ(downloaded, data);
}

// ========== Error Handling ==========

TEST_F(LocalFileBackendTest, Constructor_EmptyPath_Throws) {
    EXPECT_THROW(LocalFileBackend(""), std::runtime_error);
}

TEST_F(LocalFileBackendTest, DownloadBuffer_NonExistentKey) {
    std::vector<uint8_t> buf;
    auto result = backend_->DownloadBuffer("no/such/key", buf);
    EXPECT_FALSE(result.has_value());
}

TEST_F(LocalFileBackendTest, DownloadString_NonExistentKey) {
    std::string data;
    auto result = backend_->DownloadString("no/such/key", data);
    EXPECT_FALSE(result.has_value());
}

TEST_F(LocalFileBackendTest, UploadBuffer_EmptyBuffer) {
    std::vector<uint8_t> empty;
    auto result = backend_->UploadBuffer("test/empty", empty);
    EXPECT_FALSE(result.has_value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
