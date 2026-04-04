#pragma once

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <msgpack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "master_config.h"
#include "replica.h"
#include "segment.h"
#include "serialize/serializer.hpp"
#include "types.h"
#include "utils/zstd_util.h"

namespace mooncake::test {

namespace fs = std::filesystem;

constexpr char kSnapshotLocalPathEnv[] = "MOONCAKE_SNAPSHOT_LOCAL_PATH";
constexpr char kDefaultTestSnapshotId[] = "20240330_120000_001";
constexpr char kDefaultTestObjectKey[] = "key-1";
constexpr char kDefaultTestDiskFilePath[] = "/tmp/mooncake_snapshot_disk.data";
constexpr uint64_t kDefaultTestObjectSize = 4096;
constexpr uint64_t kDefaultTestPutStartTimeMs = 1700000000000ULL;
constexpr uint64_t kDefaultTestLeaseTimeoutMs = 4102444800000ULL;
constexpr uint64_t kDefaultTestSnapshotSeq = 42;
constexpr uint64_t kDefaultTestProducerViewVersion = 7;
constexpr int64_t kDefaultTestCreatedAtMs = 1700000000000LL;

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

inline std::vector<CatalogBackendParam> BuildCatalogBackendParams() {
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

inline std::string MakeSnapshotTestTempDir(
    std::string_view prefix = "mooncake_snapshot_test_") {
    std::string templ =
        (fs::temp_directory_path() / (std::string(prefix) + "XXXXXX")).string();
    char* dir = ::mkdtemp(templ.data());
    if (dir == nullptr) {
        throw std::runtime_error("failed to create temp directory");
    }
    return dir;
}

inline std::vector<uint8_t> ToByteVector(const msgpack::sbuffer& buffer) {
    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(buffer.data()),
        reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size());
}

inline void PackDiskReplica(
    MsgpackPacker& packer,
    std::string_view file_path = kDefaultTestDiskFilePath,
    uint64_t object_size = kDefaultTestObjectSize) {
    packer.pack_array(4);
    packer.pack(uint64_t{1});
    packer.pack(static_cast<int16_t>(ReplicaStatus::COMPLETE));
    packer.pack(static_cast<int8_t>(ReplicaType::DISK));
    packer.pack_array(2);
    packer.pack(std::string(file_path));
    packer.pack(object_size);
}

inline std::vector<uint8_t> BuildSegmentsPayload() {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    SegmentSerializer serializer(&segment_manager);
    auto serialized = serializer.Serialize();
    if (!serialized) {
        throw std::runtime_error(serialized.error().message);
    }
    return serialized.value();
}

inline std::vector<uint8_t> BuildMetadataPayload(
    const UUID& client_id, std::string_view object_key = kDefaultTestObjectKey,
    std::string_view disk_file_path = kDefaultTestDiskFilePath,
    uint64_t object_size = kDefaultTestObjectSize,
    uint64_t put_start_time_ms = kDefaultTestPutStartTimeMs,
    uint64_t lease_timeout_ms = kDefaultTestLeaseTimeoutMs) {
    msgpack::sbuffer shard_buffer;
    MsgpackPacker shard_packer(&shard_buffer);
    shard_packer.pack_map(1);
    shard_packer.pack(std::string("metadata"));
    shard_packer.pack_array(1);
    shard_packer.pack_array(2);
    shard_packer.pack(std::string(object_key));

    shard_packer.pack_array(8);
    shard_packer.pack(UuidToString(client_id));
    shard_packer.pack(put_start_time_ms);
    shard_packer.pack(object_size);
    shard_packer.pack(lease_timeout_ms);
    shard_packer.pack(false);
    shard_packer.pack(uint64_t{0});
    shard_packer.pack(uint32_t{1});
    PackDiskReplica(shard_packer, disk_file_path, object_size);

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

inline ha::SnapshotDescriptor MakeTestSnapshotDescriptor(
    std::string_view snapshot_id = kDefaultTestSnapshotId,
    uint64_t last_included_seq = kDefaultTestSnapshotSeq,
    uint64_t producer_view_version = kDefaultTestProducerViewVersion,
    int64_t created_at_ms = kDefaultTestCreatedAtMs) {
    auto descriptor = ha::snapshot_catalog_store_detail::MakeSnapshotDescriptor(
        std::string(snapshot_id));
    descriptor.last_included_seq = last_included_seq;
    descriptor.producer_view_version = producer_view_version;
    descriptor.created_at_ms = created_at_ms;
    return descriptor;
}

inline MasterServiceSupervisorConfig MakeSnapshotProviderConfig(
    const CatalogBackendParam& param, const std::string& cluster_id,
    const std::string& redis_endpoint) {
    MasterServiceSupervisorConfig config;
    config.cluster_id = cluster_id;
    config.snapshot_object_store_type = "local";
    config.snapshot_catalog_store_type = param.catalog_store_type;
    if (param.requires_redis) {
        config.snapshot_catalog_store_connstring = redis_endpoint;
        config.ha_backend_connstring = redis_endpoint;
    }
    return config;
}

inline std::unique_ptr<ha::SnapshotCatalogStore> CreateCatalogStoreForTest(
    const CatalogBackendParam& param, SnapshotObjectStore* object_store,
    const std::string& cluster_id, const std::string& redis_endpoint) {
    if (param.catalog_store_type == "embedded") {
        return std::make_unique<
            ha::backends::embedded::EmbeddedSnapshotCatalogStore>(object_store);
    }

#ifdef STORE_USE_REDIS
    if (param.catalog_store_type == "redis") {
        return std::make_unique<ha::backends::redis::RedisSnapshotCatalogStore>(
            object_store, redis_endpoint, cluster_id);
    }
#else
    (void)redis_endpoint;
#endif

    return nullptr;
}

inline tl::expected<void, std::string> PublishSnapshotPayload(
    SnapshotObjectStore& object_store, ha::SnapshotCatalogStore& catalog_store,
    const ha::SnapshotDescriptor& descriptor,
    const UUID& client_id = UUID{1, 2},
    std::string_view object_key = kDefaultTestObjectKey,
    std::string_view disk_file_path = kDefaultTestDiskFilePath,
    uint64_t object_size = kDefaultTestObjectSize) {
    auto manifest = object_store.UploadString(descriptor.manifest_key,
                                              "messagepack|1.0.0|standby-test");
    if (!manifest) {
        return tl::make_unexpected(manifest.error());
    }

    auto segments = object_store.UploadBuffer(
        descriptor.object_prefix + "segments", BuildSegmentsPayload());
    if (!segments) {
        return tl::make_unexpected(segments.error());
    }

    auto metadata = object_store.UploadBuffer(
        descriptor.object_prefix + "metadata",
        BuildMetadataPayload(client_id, object_key, disk_file_path,
                             object_size));
    if (!metadata) {
        return tl::make_unexpected(metadata.error());
    }

    auto publish_result = catalog_store.Publish(descriptor);
    if (publish_result != ErrorCode::OK) {
        return tl::make_unexpected(toString(publish_result));
    }

    return {};
}

}  // namespace mooncake::test
