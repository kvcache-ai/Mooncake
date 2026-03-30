#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"

namespace mooncake::test {

class FakeSnapshotObjectStore final : public SnapshotObjectStore {
   public:
    void SetDownloadStringError(std::string error) {
        download_string_error_ = std::move(error);
    }

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override {
        objects_[key] = std::string(buffer.begin(), buffer.end());
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override {
        auto iter = objects_.find(key);
        if (iter == objects_.end()) {
            return tl::make_unexpected("object not found");
        }
        buffer.assign(iter->second.begin(), iter->second.end());
        return {};
    }

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override {
        objects_[key] = data;
        return {};
    }

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override {
        if (!download_string_error_.empty()) {
            return tl::make_unexpected(download_string_error_);
        }
        auto iter = objects_.find(key);
        if (iter == objects_.end()) {
            return tl::make_unexpected("object not found");
        }
        data = iter->second;
        return {};
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        for (auto iter = objects_.begin(); iter != objects_.end();) {
            if (iter->first.starts_with(prefix)) {
                iter = objects_.erase(iter);
            } else {
                ++iter;
            }
        }
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override {
        object_keys.clear();
        for (const auto& [key, value] : objects_) {
            if (key.starts_with(prefix)) {
                object_keys.push_back(key);
            }
        }
        return {};
    }

    bool IsNotFoundError(const std::string& error) const override {
        return error == "object not found";
    }

    std::string GetConnectionInfo() const override { return "fake://snapshot"; }

   private:
    std::string download_string_error_;
    std::unordered_map<std::string, std::string> objects_;
};

class EmbeddedSnapshotCatalogStoreTest : public ::testing::Test {
   protected:
    static ha::SnapshotDescriptor MakeDescriptor(
        const std::string& snapshot_id) {
        ha::SnapshotDescriptor descriptor;
        descriptor.snapshot_id = snapshot_id;
        descriptor.manifest_key =
            "mooncake_master_snapshot/" + snapshot_id + "/manifest.txt";
        descriptor.object_prefix =
            "mooncake_master_snapshot/" + snapshot_id + "/";
        return descriptor;
    }

    void PutObject(const std::string& key, const std::string& value) {
        auto result = backend_.UploadString(key, value);
        ASSERT_TRUE(result.has_value()) << result.error();
    }

    FakeSnapshotObjectStore backend_;
    ha::backends::embedded::EmbeddedSnapshotCatalogStore store_{&backend_};
};

TEST_F(EmbeddedSnapshotCatalogStoreTest, PublishAndGetLatestRoundTrip) {
    auto publish_result = store_.Publish(MakeDescriptor("20240301_120000_001"));
    ASSERT_EQ(publish_result, ErrorCode::OK);

    auto latest = store_.GetLatest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->value().snapshot_id, "20240301_120000_001");
    EXPECT_EQ(latest->value().manifest_key,
              "mooncake_master_snapshot/20240301_120000_001/manifest.txt");
    EXPECT_EQ(latest->value().object_prefix,
              "mooncake_master_snapshot/20240301_120000_001/");
}

TEST_F(EmbeddedSnapshotCatalogStoreTest,
       GetLatestReturnsEmptyWhenMarkerMissing) {
    auto latest = store_.GetLatest();
    ASSERT_TRUE(latest.has_value());
    EXPECT_FALSE(latest->has_value());
}

TEST_F(EmbeddedSnapshotCatalogStoreTest,
       GetLatestReturnsErrorOnBackendReadFailure) {
    backend_.SetDownloadStringError("permission denied");

    auto latest = store_.GetLatest();
    ASSERT_FALSE(latest.has_value());
    EXPECT_EQ(latest.error(), ErrorCode::PERSISTENT_FAIL);
}

TEST_F(EmbeddedSnapshotCatalogStoreTest, GetLatestTrimsWhitespaceMarker) {
    PutObject("mooncake_master_snapshot/latest.txt",
              "  \n20240301_120000_002\t\r\n");

    auto latest = store_.GetLatest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->value().snapshot_id, "20240301_120000_002");
}

TEST_F(EmbeddedSnapshotCatalogStoreTest,
       ListReturnsSnapshotsInDescendingOrder) {
    PutObject("mooncake_master_snapshot/20240301_120000_001/manifest.txt",
              "m1");
    PutObject("mooncake_master_snapshot/20240303_120000_001/metadata", "d3");
    PutObject("mooncake_master_snapshot/20240302_120000_001/segments", "d2");
    PutObject("mooncake_master_snapshot/latest.txt", "20240303_120000_001");
    PutObject("mooncake_master_snapshot/not-a-snapshot/file.txt", "ignore");

    auto snapshots = store_.List(2);
    ASSERT_TRUE(snapshots.has_value());
    ASSERT_EQ(snapshots->size(), 2u);
    EXPECT_EQ(snapshots->at(0).snapshot_id, "20240303_120000_001");
    EXPECT_EQ(snapshots->at(1).snapshot_id, "20240302_120000_001");
}

TEST_F(EmbeddedSnapshotCatalogStoreTest, DeleteRemovesSnapshotObjectsByPrefix) {
    PutObject("mooncake_master_snapshot/20240301_120000_001/manifest.txt",
              "m1");
    PutObject("mooncake_master_snapshot/20240301_120000_001/metadata", "d1");
    PutObject("mooncake_master_snapshot/20240302_120000_001/manifest.txt",
              "m2");

    auto delete_result = store_.Delete("20240301_120000_001");
    ASSERT_EQ(delete_result, ErrorCode::OK);

    auto snapshots = store_.List(0);
    ASSERT_TRUE(snapshots.has_value());
    ASSERT_EQ(snapshots->size(), 1u);
    EXPECT_EQ(snapshots->at(0).snapshot_id, "20240302_120000_001");
}

TEST_F(EmbeddedSnapshotCatalogStoreTest,
       DeleteLatestFallsBackToPreviousSnapshot) {
    PutObject("mooncake_master_snapshot/20240301_120000_001/manifest.txt",
              "m1");
    PutObject("mooncake_master_snapshot/20240302_120000_001/manifest.txt",
              "m2");
    ASSERT_EQ(store_.Publish(MakeDescriptor("20240301_120000_001")),
              ErrorCode::OK);
    ASSERT_EQ(store_.Publish(MakeDescriptor("20240302_120000_001")),
              ErrorCode::OK);

    ASSERT_EQ(store_.Delete("20240302_120000_001"), ErrorCode::OK);

    auto latest = store_.GetLatest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->value().snapshot_id, "20240301_120000_001");
}

TEST_F(EmbeddedSnapshotCatalogStoreTest, DeleteLastSnapshotClearsLatestMarker) {
    PutObject("mooncake_master_snapshot/20240301_120000_001/manifest.txt",
              "m1");
    ASSERT_EQ(store_.Publish(MakeDescriptor("20240301_120000_001")),
              ErrorCode::OK);

    ASSERT_EQ(store_.Delete("20240301_120000_001"), ErrorCode::OK);

    auto latest = store_.GetLatest();
    ASSERT_TRUE(latest.has_value());
    EXPECT_FALSE(latest->has_value());
}

TEST_F(EmbeddedSnapshotCatalogStoreTest, RejectsInvalidSnapshotIds) {
    EXPECT_EQ(store_.Publish(MakeDescriptor("invalid-id")),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(store_.Delete("invalid-id"), ErrorCode::INVALID_PARAMS);

    PutObject("mooncake_master_snapshot/latest.txt", "invalid-id");
    auto latest = store_.GetLatest();
    EXPECT_FALSE(latest.has_value());
    EXPECT_EQ(latest.error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
