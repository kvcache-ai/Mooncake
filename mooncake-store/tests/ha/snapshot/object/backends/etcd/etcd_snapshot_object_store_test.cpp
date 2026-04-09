#ifdef STORE_USE_ETCD

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

#include "ha/snapshot/object/backends/etcd/etcd_snapshot_object_store.h"

DEFINE_string(etcd_endpoints, "127.0.0.1:2379",
              "etcd endpoints for snapshot object store test");

namespace mooncake::test {

class EtcdSnapshotObjectStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Build a unique key prefix per test to avoid collisions.
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        key_prefix_ = std::string("snapshot_obj_test/") +
                       std::to_string(getpid()) + "_" +
                       info->test_case_name() + "_" + info->name() + "/";

        backend_ = std::make_unique<EtcdSnapshotObjectStore>(
            FLAGS_etcd_endpoints);
    }

    void TearDown() override {
        if (backend_) {
            (void)backend_->DeleteObjectsWithPrefix(key_prefix_);
        }
        backend_.reset();
    }

    /// Return a full key by prepending the per-test prefix.
    std::string key(const std::string& suffix) const {
        return key_prefix_ + suffix;
    }

    std::unique_ptr<EtcdSnapshotObjectStore> backend_;
    std::string key_prefix_;
};

// ========== Normal Functionality ==========

TEST_F(EtcdSnapshotObjectStoreTest, UploadDownloadBuffer_Roundtrip) {
    std::vector<uint8_t> data = {0, 1, 2, 128, 254, 255};
    auto upload = backend_->UploadBuffer(key("buf"), data);
    ASSERT_TRUE(upload.has_value()) << upload.error();

    std::vector<uint8_t> downloaded;
    auto download = backend_->DownloadBuffer(key("buf"), downloaded);
    ASSERT_TRUE(download.has_value()) << download.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(EtcdSnapshotObjectStoreTest, UploadDownloadString_Roundtrip) {
    std::string data = "hello mooncake etcd snapshot";
    auto upload = backend_->UploadString(key("str"), data);
    ASSERT_TRUE(upload.has_value()) << upload.error();

    std::string downloaded;
    auto download = backend_->DownloadString(key("str"), downloaded);
    ASSERT_TRUE(download.has_value()) << download.error();
    EXPECT_EQ(downloaded, data);
}

TEST_F(EtcdSnapshotObjectStoreTest, ListObjectsWithPrefix) {
    backend_->UploadString(key("snap/20240101/metadata"), "m");
    backend_->UploadString(key("snap/20240101/segments"), "s");
    backend_->UploadString(key("snap/20240102/metadata"), "m2");

    std::vector<std::string> keys;
    auto result = backend_->ListObjectsWithPrefix(key("snap/20240101/"), keys);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(keys.size(), 2u);

    // Broader prefix should list all three.
    keys.clear();
    result = backend_->ListObjectsWithPrefix(key("snap/"), keys);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(keys.size(), 3u);
}

TEST_F(EtcdSnapshotObjectStoreTest, DeleteObjectsWithPrefix) {
    backend_->UploadString(key("snap/20240101/metadata"), "m");
    backend_->UploadString(key("snap/20240101/segments"), "s");

    auto del = backend_->DeleteObjectsWithPrefix(key("snap/20240101/"));
    ASSERT_TRUE(del.has_value()) << del.error();

    // Verify keys are gone.
    std::string data;
    auto dl = backend_->DownloadString(key("snap/20240101/metadata"), data);
    EXPECT_FALSE(dl.has_value());
}

TEST_F(EtcdSnapshotObjectStoreTest, GetConnectionInfo) {
    auto info = backend_->GetConnectionInfo();
    EXPECT_NE(info.find(FLAGS_etcd_endpoints), std::string::npos);
}

TEST_F(EtcdSnapshotObjectStoreTest, UploadBuffer_LargePayload) {
    // 1 MB payload — exercises chunked transfer and memory management.
    constexpr size_t kSize = 1024 * 1024;
    std::vector<uint8_t> data(kSize);
    std::iota(data.begin(), data.end(), uint8_t{0});

    auto upload = backend_->UploadBuffer(key("large"), data);
    ASSERT_TRUE(upload.has_value()) << upload.error();

    std::vector<uint8_t> downloaded;
    auto download = backend_->DownloadBuffer(key("large"), downloaded);
    ASSERT_TRUE(download.has_value()) << download.error();
    EXPECT_EQ(downloaded, data);
}

// ========== Error Handling ==========

TEST_F(EtcdSnapshotObjectStoreTest, DownloadBuffer_NonExistentKey) {
    std::vector<uint8_t> buf;
    auto result = backend_->DownloadBuffer(key("no/such/key"), buf);
    EXPECT_FALSE(result.has_value());
}

TEST_F(EtcdSnapshotObjectStoreTest, DownloadString_NonExistentKey) {
    std::string data;
    auto result = backend_->DownloadString(key("no/such/key"), data);
    EXPECT_FALSE(result.has_value());
}

TEST_F(EtcdSnapshotObjectStoreTest, IsNotFoundError) {
    // The implementation checks for "key not found" substring.
    EXPECT_TRUE(backend_->IsNotFoundError("etcd: key not found"));
    EXPECT_TRUE(backend_->IsNotFoundError("key not found"));
    EXPECT_FALSE(backend_->IsNotFoundError("connection refused"));
    EXPECT_FALSE(backend_->IsNotFoundError(""));
}

TEST_F(EtcdSnapshotObjectStoreTest, UploadBuffer_EmptyBuffer) {
    std::vector<uint8_t> empty;
    auto result = backend_->UploadBuffer(key("empty"), empty);
    // etcd allows storing empty values — verify roundtrip.
    ASSERT_TRUE(result.has_value()) << result.error();

    std::vector<uint8_t> downloaded;
    auto dl = backend_->DownloadBuffer(key("empty"), downloaded);
    ASSERT_TRUE(dl.has_value()) << dl.error();
    EXPECT_TRUE(downloaded.empty());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    return RUN_ALL_TESTS();
}

#else  // !STORE_USE_ETCD

#include <gtest/gtest.h>

TEST(EtcdSnapshotObjectStoreTest, Skipped) {
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif  // STORE_USE_ETCD
