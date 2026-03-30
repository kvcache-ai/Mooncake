#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <msgpack.hpp>

#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "replica.h"
#include "segment.h"
#include "serialize/serializer.hpp"
#include "utils/zstd_util.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for catalog-backed snapshot provider tests");

namespace {

namespace fs = std::filesystem;

constexpr char kSnapshotLocalPathEnv[] = "MOONCAKE_SNAPSHOT_LOCAL_PATH";
constexpr char kSnapshotId[] = "20240330_120000_001";
constexpr char kObjectKey[] = "key-1";
constexpr char kDiskFilePath[] = "/tmp/mooncake_snapshot_disk.data";
constexpr uint64_t kObjectSize = 4096;
constexpr uint64_t kPutStartTimeMs = 1700000000000ULL;
constexpr uint64_t kLeaseTimeoutMs = 4102444800000ULL;

struct CatalogBackendParam {
    std::string name;
    std::string catalog_store_type;
    bool requires_redis{false};
};

class ScopedEnvVar {
   public:
    ScopedEnvVar(std::string name, std::string value) : name_(std::move(name)) {
        const char* previous = std::getenv(name_.c_str());
        if (previous != nullptr) {
            had_previous_value_ = true;
            previous_value_ = previous;
        }

        if (::setenv(name_.c_str(), value.c_str(), 1) != 0) {
            throw std::runtime_error("failed to set environment variable");
        }
    }

    ~ScopedEnvVar() {
        if (had_previous_value_) {
            ::setenv(name_.c_str(), previous_value_.c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    bool had_previous_value_{false};
    std::string previous_value_;
};

std::vector<CatalogBackendParam> BuildCatalogBackendParams() {
    std::vector<CatalogBackendParam> params;
    params.push_back(CatalogBackendParam{
        .name = "Embedded",
        .catalog_store_type = "embedded",
    });
#ifdef STORE_USE_REDIS
    params.push_back(CatalogBackendParam{
        .name = "Redis",
        .catalog_store_type = "redis",
        .requires_redis = true,
    });
#endif
    return params;
}

std::string MakeTempDir() {
    std::string templ =
        (fs::temp_directory_path() / "catalog_backed_snapshot_provider_XXXXXX")
            .string();
    char* dir = ::mkdtemp(templ.data());
    if (dir == nullptr) {
        throw std::runtime_error("failed to create temp directory");
    }
    return dir;
}

std::vector<uint8_t> ToByteVector(const msgpack::sbuffer& buffer) {
    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(buffer.data()),
        reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size());
}

void PackDiskReplica(MsgpackPacker& packer) {
    packer.pack_array(4);
    packer.pack(uint64_t{1});
    packer.pack(static_cast<int16_t>(ReplicaStatus::COMPLETE));
    packer.pack(static_cast<int8_t>(ReplicaType::DISK));
    packer.pack_array(2);
    packer.pack(std::string(kDiskFilePath));
    packer.pack(kObjectSize);
}

std::vector<uint8_t> BuildSegmentsPayload() {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    SegmentSerializer serializer(&segment_manager);
    auto serialized = serializer.Serialize();
    if (!serialized) {
        throw std::runtime_error(serialized.error().message);
    }
    return serialized.value();
}

std::vector<uint8_t> BuildMetadataPayload(const UUID& client_id) {
    msgpack::sbuffer shard_buffer;
    MsgpackPacker shard_packer(&shard_buffer);
    shard_packer.pack_map(1);
    shard_packer.pack(std::string("metadata"));
    shard_packer.pack_array(1);
    shard_packer.pack_array(2);
    shard_packer.pack(std::string(kObjectKey));

    shard_packer.pack_array(8);
    shard_packer.pack(UuidToString(client_id));
    shard_packer.pack(kPutStartTimeMs);
    shard_packer.pack(kObjectSize);
    shard_packer.pack(kLeaseTimeoutMs);
    shard_packer.pack(false);
    shard_packer.pack(uint64_t{0});
    shard_packer.pack(uint32_t{1});
    PackDiskReplica(shard_packer);

    auto compressed_shard =
        zstd_compress(reinterpret_cast<const uint8_t*>(shard_buffer.data()),
                      shard_buffer.size(), 3);

    msgpack::sbuffer root_buffer;
    MsgpackPacker root_packer(&root_buffer);
    root_packer.pack_map(1);
    root_packer.pack(std::string("shards"));
    root_packer.pack_map(1);
    root_packer.pack(std::string("0"));
    root_packer.pack_bin(compressed_shard.size());
    root_packer.pack_bin_body(
        reinterpret_cast<const char*>(compressed_shard.data()),
        compressed_shard.size());
    return ToByteVector(root_buffer);
}

MasterServiceSupervisorConfig MakeSnapshotProviderConfig(
    const CatalogBackendParam& param, const std::string& cluster_id) {
    MasterServiceSupervisorConfig config;
    config.cluster_id = cluster_id;
    config.snapshot_object_store_type = "local";
    config.snapshot_catalog_store_type = param.catalog_store_type;
    if (param.requires_redis) {
        config.snapshot_catalog_store_connstring = FLAGS_redis_endpoint;
        config.ha_backend_connstring = FLAGS_redis_endpoint;
    }
    return config;
}

std::unique_ptr<ha::SnapshotCatalogStore> CreateCatalogStoreForTest(
    const CatalogBackendParam& param, SnapshotObjectStore* object_store,
    const std::string& cluster_id) {
    if (param.catalog_store_type == "embedded") {
        return std::make_unique<
            ha::backends::embedded::EmbeddedSnapshotCatalogStore>(object_store);
    }

#ifdef STORE_USE_REDIS
    if (param.catalog_store_type == "redis") {
        return std::make_unique<ha::backends::redis::RedisSnapshotCatalogStore>(
            object_store, FLAGS_redis_endpoint, cluster_id);
    }
#endif

    return nullptr;
}

class CatalogBackedSnapshotProviderTest
    : public ::testing::TestWithParam<CatalogBackendParam> {
   protected:
    void SetUp() override {
        if (GetParam().requires_redis && FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }

        cluster_id_ = "snapshot-provider-test-" + UuidToString(generate_uuid());
        temp_dir_ = MakeTempDir();
        local_path_env_ =
            std::make_unique<ScopedEnvVar>(kSnapshotLocalPathEnv, temp_dir_);

        object_store_ =
            std::make_unique<LocalFileSnapshotObjectStore>(temp_dir_);
        catalog_store_ = CreateCatalogStoreForTest(
            GetParam(), object_store_.get(), cluster_id_);
        ASSERT_NE(catalog_store_, nullptr);

        descriptor_ = ha::snapshot_catalog_store_detail::MakeSnapshotDescriptor(
            kSnapshotId);
        descriptor_.last_included_seq = 42;
        descriptor_.producer_view_version = 7;
        descriptor_.created_at_ms = 1700000000000LL;
    }

    void TearDown() override {
        if (snapshot_published_ && catalog_store_ != nullptr) {
            (void)catalog_store_->Delete(descriptor_.snapshot_id);
        }
        catalog_store_.reset();
        object_store_.reset();
        local_path_env_.reset();
        if (!temp_dir_.empty() && fs::exists(temp_dir_)) {
            fs::remove_all(temp_dir_);
        }
    }

    void PublishSnapshotPayload() {
        const UUID client_id{1, 2};
        auto manifest = object_store_->UploadString(
            descriptor_.manifest_key, "messagepack|1.0.0|standby-test");
        ASSERT_TRUE(manifest.has_value()) << manifest.error();

        auto segments = object_store_->UploadBuffer(
            descriptor_.object_prefix + "segments", BuildSegmentsPayload());
        ASSERT_TRUE(segments.has_value()) << segments.error();

        auto metadata =
            object_store_->UploadBuffer(descriptor_.object_prefix + "metadata",
                                        BuildMetadataPayload(client_id));
        ASSERT_TRUE(metadata.has_value()) << metadata.error();

        ASSERT_EQ(catalog_store_->Publish(descriptor_), ErrorCode::OK);
        snapshot_published_ = true;
    }

    tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode> CreateProvider()
        const {
        return CreateCatalogBackedSnapshotProvider(
            MakeSnapshotProviderConfig(GetParam(), cluster_id_));
    }

    std::string cluster_id_;
    std::string temp_dir_;
    bool snapshot_published_{false};
    ha::SnapshotDescriptor descriptor_;
    std::unique_ptr<ScopedEnvVar> local_path_env_;
    std::unique_ptr<LocalFileSnapshotObjectStore> object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> catalog_store_;
};

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotReturnsEmptyWhenCatalogMissing) {
    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    EXPECT_FALSE(snapshot->has_value());
}

TEST_P(CatalogBackedSnapshotProviderTest, LoadLatestSnapshotRoundTrip) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    ASSERT_TRUE(snapshot->has_value());
    EXPECT_EQ(snapshot->value().snapshot_id, descriptor_.snapshot_id);
    ASSERT_EQ(snapshot->value().metadata.size(), 1u);

    const auto& [key, metadata] = snapshot->value().metadata.front();
    EXPECT_EQ(key, kObjectKey);
    EXPECT_EQ(metadata.client_id, (UUID{1, 2}));
    EXPECT_EQ(metadata.size, kObjectSize);
    EXPECT_EQ(metadata.last_sequence_id,
              snapshot->value().snapshot_sequence_id);
    ASSERT_EQ(metadata.replicas.size(), 1u);

    const auto& replica = metadata.replicas.front();
    EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
    ASSERT_TRUE(replica.is_disk_replica());
    EXPECT_EQ(replica.get_disk_descriptor().file_path, kDiskFilePath);
    EXPECT_EQ(replica.get_disk_descriptor().object_size, kObjectSize);
}

TEST_P(CatalogBackedSnapshotProviderTest, RejectsClusterMismatch) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot =
        provider.value()->LoadLatestSnapshot(cluster_id_ + "-other");
    ASSERT_FALSE(snapshot.has_value());
    EXPECT_EQ(snapshot.error(), ErrorCode::INVALID_PARAMS);
}

INSTANTIATE_TEST_SUITE_P(
    SnapshotCatalogBackends, CatalogBackedSnapshotProviderTest,
    ::testing::ValuesIn(BuildCatalogBackendParams()),
    [](const ::testing::TestParamInfo<CatalogBackendParam>& info) {
        return info.param.name;
    });

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return result;
}
