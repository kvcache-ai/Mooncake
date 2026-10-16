#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"

namespace mooncake::test {

namespace fs = std::filesystem;

class LocalFileSnapshotObjectStoreTest : public ::testing::Test {
   protected:
    const std::string& tmp_dir() const { return tmp_dir_; }

    std::unique_ptr<LocalFileSnapshotObjectStore> backend_;

    void SetUp() override {
        google::InitGoogleLogging("LocalFileSnapshotObjectStoreTest");
        FLAGS_logtostderr = true;

        if (const char* value = std::getenv("MOONCAKE_SNAPSHOT_LOCAL_PATH")) {
            original_path_ = value;
        }
        ASSERT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);

        // Create a unique temporary directory
        std::string tmpl = (fs::temp_directory_path() /
                            "local_file_snapshot_object_store_test_XXXXXX")
                               .string();
        char* dir = mkdtemp(tmpl.data());
        ASSERT_NE(dir, nullptr) << "Failed to create temp directory";
        tmp_dir_ = dir;

        backend_ = std::make_unique<LocalFileSnapshotObjectStore>(tmp_dir());
    }

    void TearDown() override {
        backend_.reset();
        if (original_path_) {
            EXPECT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH",
                             original_path_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);
        }
        if (!tmp_dir().empty() && fs::exists(tmp_dir())) {
            fs::remove_all(tmp_dir());
        }
        google::ShutdownGoogleLogging();
    }

   private:
    std::string tmp_dir_;
    std::optional<std::string> original_path_;
};

// ========== Normal Functionality ==========

TEST_F(LocalFileSnapshotObjectStoreTest, UploadDownloadBuffer_Roundtrip) {
    std::vector<uint8_t> data = {0, 1, 2, 128, 254, 255};
    auto upload_result = backend_->UploadBuffer("test/buf", data);
    ASSERT_TRUE(upload_result.has_value()) << upload_result.error();

    std::vector<uint8_t> downloaded;
    auto download_result = backend_->DownloadBuffer("test/buf", downloaded);
    ASSERT_TRUE(download_result.has_value()) << download_result.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(LocalFileSnapshotObjectStoreTest, UploadDownloadString_Roundtrip) {
    std::string data = "hello mooncake snapshot";
    auto upload_result = backend_->UploadString("test/str", data);
    ASSERT_TRUE(upload_result.has_value()) << upload_result.error();

    std::string downloaded;
    auto download_result = backend_->DownloadString("test/str", downloaded);
    ASSERT_TRUE(download_result.has_value()) << download_result.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(LocalFileSnapshotObjectStoreTest, ListObjectsWithPrefix) {
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

TEST_F(LocalFileSnapshotObjectStoreTest, DeleteObjectsWithPrefix) {
    backend_->UploadString("snap/20240101/metadata", "m");
    backend_->UploadString("snap/20240101/segments", "s");

    auto del_result = backend_->DeleteObjectsWithPrefix("snap/20240101/");
    ASSERT_TRUE(del_result.has_value()) << del_result.error();

    // Verify files are gone
    std::string data;
    auto dl = backend_->DownloadString("snap/20240101/metadata", data);
    EXPECT_FALSE(dl.has_value());
}

TEST_F(LocalFileSnapshotObjectStoreTest, GetConnectionInfo) {
    auto info = backend_->GetConnectionInfo();
    EXPECT_NE(info.find(tmp_dir()), std::string::npos);
}

TEST_F(LocalFileSnapshotObjectStoreTest, InspectObjectReturnsSize) {
    ASSERT_TRUE(backend_->UploadString("test/inspect", "data"));

    auto inspection = backend_->InspectObject("test/inspect");
    ASSERT_TRUE(inspection) << inspection.error();
    EXPECT_EQ(4u, inspection->stored_size);
    EXPECT_FALSE(inspection->crc32c.has_value());
}

TEST_F(LocalFileSnapshotObjectStoreTest, UploadBuffer_CreatesSubdirectories) {
    std::vector<uint8_t> data = {42};
    auto result = backend_->UploadBuffer("a/b/c/deep_file", data);
    ASSERT_TRUE(result.has_value()) << result.error();

    std::vector<uint8_t> downloaded;
    auto dl = backend_->DownloadBuffer("a/b/c/deep_file", downloaded);
    ASSERT_TRUE(dl.has_value()) << dl.error();
    EXPECT_EQ(downloaded, data);
}

// ========== Error Handling ==========

TEST_F(LocalFileSnapshotObjectStoreTest, Constructor_EmptyPath_Throws) {
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", tmp_dir().c_str(), 1), 0);
    try {
        LocalFileSnapshotObjectStore store("");
        FAIL()
            << "An empty explicit path must not fall back to the environment";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(
            error.what(),
            "LocalFileSnapshotObjectStore base_path is empty. "
            "Please provide a valid persistent directory path for snapshot "
            "storage.");
    }
}

TEST_F(LocalFileSnapshotObjectStoreTest, Constructor_MissingOrEmptyEnv_Throws) {
    for (const char* value : {static_cast<const char*>(nullptr), ""}) {
        if (value) {
            ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", value, 1), 0);
        } else {
            ASSERT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);
        }
        try {
            LocalFileSnapshotObjectStore store;
            FAIL() << "A missing or empty environment path must be rejected";
        } catch (const std::runtime_error& error) {
            EXPECT_STREQ(
                error.what(),
                "MOONCAKE_SNAPSHOT_LOCAL_PATH environment variable is not set. "
                "Please set it to a persistent directory path for snapshot "
                "storage. Example: export "
                "MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots");
        }
    }
}

TEST_F(LocalFileSnapshotObjectStoreTest, Constructor_ReadsEnvForEachStore) {
    const auto first_path = fs::path(tmp_dir()) / "first" / "snapshots";
    ASSERT_FALSE(fs::exists(first_path));
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", first_path.c_str(), 1), 0);
    LocalFileSnapshotObjectStore first;
    ASSERT_TRUE(fs::is_directory(first_path));

    const auto second_path = fs::path(tmp_dir()) / "second";
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", second_path.c_str(), 1),
              0);
    LocalFileSnapshotObjectStore second;

    ASSERT_TRUE(first.UploadString("key", "first snapshot"));
    ASSERT_TRUE(second.UploadString("key", "second snapshot"));
    EXPECT_TRUE(fs::is_regular_file(first_path / "key"));
    EXPECT_TRUE(fs::is_regular_file(second_path / "key"));

    std::string data;
    ASSERT_TRUE(first.DownloadString("key", data));
    EXPECT_EQ(data, "first snapshot");
    ASSERT_TRUE(second.DownloadString("key", data));
    EXPECT_EQ(data, "second snapshot");
}

TEST_F(LocalFileSnapshotObjectStoreTest, Constructor_EnvRegularFile_Throws) {
    ASSERT_TRUE(backend_->UploadString("regular-file", "data"));
    const auto path = fs::path(tmp_dir()) / "regular-file";
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", path.c_str(), 1), 0);
    EXPECT_THROW(LocalFileSnapshotObjectStore{}, std::runtime_error);
}

TEST_F(LocalFileSnapshotObjectStoreTest, Constructor_ExplicitPathIgnoresEnv) {
    EXPECT_NO_THROW(LocalFileSnapshotObjectStore{tmp_dir()});
    const std::string env_path = (fs::path(tmp_dir()) / "unused-env").string();
    const std::string explicit_path =
        (fs::path(tmp_dir()) / "explicit").string();
    for (const auto& value : {std::string{}, env_path}) {
        ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", value.c_str(), 1), 0);
        LocalFileSnapshotObjectStore store(explicit_path);
        ASSERT_TRUE(store.UploadString("key", "explicit path"));
        EXPECT_TRUE(fs::is_regular_file(fs::path(explicit_path) / "key"));
        EXPECT_FALSE(fs::exists(env_path));
    }
}

TEST_F(LocalFileSnapshotObjectStoreTest, DownloadBuffer_NonExistentKey) {
    std::vector<uint8_t> buf;
    auto result = backend_->DownloadBuffer("no/such/key", buf);
    EXPECT_FALSE(result.has_value());
}

TEST_F(LocalFileSnapshotObjectStoreTest, DownloadString_NonExistentKey) {
    std::string data;
    auto result = backend_->DownloadString("no/such/key", data);
    EXPECT_FALSE(result.has_value());
}

TEST_F(LocalFileSnapshotObjectStoreTest, UploadBuffer_EmptyBuffer) {
    std::vector<uint8_t> empty;
    auto result = backend_->UploadBuffer("test/empty", empty);
    EXPECT_FALSE(result.has_value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
