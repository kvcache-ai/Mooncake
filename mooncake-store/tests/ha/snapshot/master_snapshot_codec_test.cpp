#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include <unistd.h>

#include <msgpack.hpp>

#include "ha/snapshot/master_snapshot_codec.h"
#include "master_config.h"
#include "master_service.h"
#include "segment.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "task_manager.h"
#include "tenant_id.h"
#include "common/zstd_util.h"

namespace mooncake::ha {

class MasterSnapshotCodecTest : public ::testing::Test {
   protected:
    void SetUp() override { master_service_ = MakeMasterService(); }

    void TearDown() override { master_service_.reset(); }

    static std::unique_ptr<MasterService> MakeMasterService() {
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 10000;
        config.eviction_ratio = 0.1;
        return std::make_unique<MasterService>(config);
    }

    // The fixture is befriended by MasterService, so private state access is
    // funneled through this helper (friendship is not inherited by the
    // TEST_F-generated subclasses).
    static MasterSnapshotStateView MakeStateView(MasterService& service) {
        return MasterSnapshotStateView(
            service, service.segment_manager_, service.local_ssd_manager_,
            service.nof_segment_manager_, service.task_manager_);
    }

    static void SeedDfsReplica(MasterService& service, const std::string& key,
                               const DistributedFSDescriptor& descriptor) {
        const TenantId tenant = TenantId::Default();
        const size_t shard_idx = service.getShardIndex(tenant, key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
        std::vector<Replica> replicas;
        replicas.emplace_back(descriptor, ReplicaStatus::COMPLETE);
        auto& tenant_state = shard->tenants[tenant];
        tenant_state.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(key),
            std::forward_as_tuple(
                generate_uuid(), std::chrono::system_clock::now(),
                descriptor.object_size, std::move(replicas), std::nullopt,
                false, ObjectDataType::UNKNOWN, std::string{}, tenant, key));
    }

    static DistributedFSDescriptor InstallRecoveringDfsAllocator(
        MasterService& service, const std::string& root,
        const std::string& key) {
        DistributedStorageConfig config;
        config.fsdir = root;
        config.fs_adapter_type = "posix";
        config.shard_count = 1;
        config.shard_capacity = 32 * 1024;
        config.alignment = 4096;
        config.eviction_enabled = false;
        config.deferred_free_duration = std::chrono::seconds(0);

        DistributedFSDescriptor descriptor;
        {
            DfsGlobalAllocator writer;
            EXPECT_TRUE(writer.Init(config).has_value());
            auto allocated = writer.Allocate(key, 1024);
            EXPECT_TRUE(allocated.has_value());
            if (allocated) descriptor = *allocated;
        }

        auto recovering = std::make_unique<DfsGlobalAllocator>();
        EXPECT_TRUE(recovering->Init(config, true).has_value());
        service.enable_dfs_ = true;
        service.dfs_allocator_ = std::move(recovering);
        return descriptor;
    }

    static tl::expected<void, ErrorCode> ReconcileDfsSnapshot(
        MasterService& service) {
        return service.ReconcileDfsMetadataAfterSnapshot();
    }

    static bool IsDfsRecoveryReady(const MasterService& service) {
        return service.dfs_allocator_ &&
               service.dfs_allocator_->IsRecoveryReady();
    }

    std::unique_ptr<MasterService> master_service_;
};

TEST_F(MasterSnapshotCodecTest, EncodeManifestPreservesSnapshotId) {
    std::vector<uint8_t> bytes = MasterSnapshotCodec::EncodeManifest(
        MasterSnapshotCodec::kSerializerType,
        MasterSnapshotCodec::kSerializerVersion, "snapshot-000042");
    std::string manifest(bytes.begin(), bytes.end());
    EXPECT_EQ(manifest, "messagepack|1.0.0|snapshot-000042");
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTrip) {
    MasterSnapshotCodec codec;

    MasterSnapshotStateView state_view = MakeStateView(*master_service_);

    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;

    const MasterSnapshotPayloads& payloads = encode_result.value();

    // All three payload buffers must be produced.
    EXPECT_FALSE(payloads.metadata.empty());
    EXPECT_FALSE(payloads.segments.empty());
    EXPECT_FALSE(payloads.task_manager.empty());

    // Decode into a fresh service.
    auto target_service = MakeMasterService();
    auto decode_result = codec.Decode(target_service.get(), payloads);
    ASSERT_TRUE(decode_result.has_value())
        << "Decode failed: " << decode_result.error().message;
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTripWithMemoryReplica) {
    // Mount a segment and store an object backed by a MEMORY replica. On
    // decode, the segment/allocator must be restored before the metadata,
    // otherwise deserializing the replica fails with SEGMENT_NOT_FOUND because
    // GetMountedSegment() cannot find its backing segment.
    constexpr size_t kSegmentBase = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    const std::string kKey = "memory_replica_key";
    const TenantId& kTenant = TenantId::Default();

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "codec_test_segment";
    segment.base = kSegmentBase;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;

    UUID client_id = generate_uuid();
    auto mount_result = master_service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    auto put_start = master_service_->PutStart(
        client_id, kKey, kTenant,
        /*slice_length=*/1024, ReplicateConfig{.replica_num = 1});
    ASSERT_TRUE(put_start.has_value())
        << "PutStart failed: " << static_cast<int>(put_start.error());
    auto put_end =
        master_service_->PutEnd(client_id, kKey, kTenant, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value())
        << "PutEnd failed: " << static_cast<int>(put_end.error());

    MasterSnapshotCodec codec;
    MasterSnapshotStateView state_view = MakeStateView(*master_service_);

    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;
    EXPECT_FALSE(encode_result.value().segments.empty());

    // Decode into a fresh service. This exercises the segments-before-metadata
    // restore order.
    auto target_service = MakeMasterService();
    auto decode_result =
        codec.Decode(target_service.get(), encode_result.value());
    ASSERT_TRUE(decode_result.has_value())
        << "Decode failed: " << decode_result.error().message;

    // The MEMORY replica must be fully restored and queryable.
    auto get_result = target_service->GetReplicaList(kKey, kTenant);
    ASSERT_TRUE(get_result.has_value())
        << "GetReplicaList failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(get_result.value().replicas.size(), 1u);
}

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTripWithDfsReplica) {
    const std::string key = "snapshot-dfs-key";
    const DistributedFSDescriptor descriptor{"/mnt/dfs/dfs_shard_02.data",
                                             12288, 777, 4096, 2};
    SeedDfsReplica(*master_service_, key, descriptor);

    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(state_view);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;
    auto target = MakeMasterService();
    auto decoded = codec.Decode(target.get(), *encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    auto replicas = target->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    ASSERT_EQ(replicas->replicas.size(), 1);
    ASSERT_TRUE(replicas->replicas.front().is_dfs_replica());
    const auto& restored = replicas->replicas.front().get_dfs_descriptor();
    EXPECT_EQ(restored.file_path, descriptor.file_path);
    EXPECT_EQ(restored.offset, descriptor.offset);
    EXPECT_EQ(restored.object_size, descriptor.object_size);
    EXPECT_EQ(restored.aligned_size, descriptor.aligned_size);
    EXPECT_EQ(restored.shard_idx, descriptor.shard_idx);
}

TEST_F(MasterSnapshotCodecTest, ReconciliationPrunesStaleDfsReplica) {
    const std::string root =
        (std::filesystem::temp_directory_path() /
         ("snapshot_dfs_prune_" + std::to_string(::getpid())))
            .string();
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    const std::string key = "snapshot-dfs-stale";
    auto descriptor =
        InstallRecoveringDfsAllocator(*master_service_, root, key);
    descriptor.offset += 4096;
    SeedDfsReplica(*master_service_, key, descriptor);

    ASSERT_TRUE(ReconcileDfsSnapshot(*master_service_).has_value());
    EXPECT_FALSE(
        master_service_->GetReplicaList(key, TenantId::Default()).has_value());
    EXPECT_TRUE(IsDfsRecoveryReady(*master_service_));

    master_service_.reset();
    std::filesystem::remove_all(root);
}

TEST_F(MasterSnapshotCodecTest, ReconciliationRetainsMatchingDfsReplica) {
    const std::string root =
        (std::filesystem::temp_directory_path() /
         ("snapshot_dfs_match_" + std::to_string(::getpid())))
            .string();
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    const std::string key = "snapshot-dfs-match";
    const auto descriptor =
        InstallRecoveringDfsAllocator(*master_service_, root, key);
    SeedDfsReplica(*master_service_, key, descriptor);

    ASSERT_TRUE(ReconcileDfsSnapshot(*master_service_).has_value());
    auto replicas = master_service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    ASSERT_EQ(replicas->replicas.size(), 1);
    EXPECT_EQ(replicas->replicas.front().get_dfs_descriptor().offset,
              descriptor.offset);
    EXPECT_TRUE(IsDfsRecoveryReady(*master_service_));

    master_service_.reset();
    std::filesystem::remove_all(root);
}

TEST_F(MasterSnapshotCodecTest, DecodeWithCorruptPayloadFails) {
    MasterSnapshotCodec codec;

    MasterSnapshotPayloads corrupt;
    corrupt.metadata = std::vector<uint8_t>{1, 2, 3};
    corrupt.segments = std::vector<uint8_t>{4, 5, 6};
    corrupt.task_manager = std::vector<uint8_t>{7, 8, 9};

    auto decode_result = codec.Decode(master_service_.get(), corrupt);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::DESERIALIZE_FAIL);
}

TEST_F(MasterSnapshotCodecTest, DecodeWithNullService) {
    MasterSnapshotCodec codec;

    MasterSnapshotPayloads payloads;
    auto decode_result = codec.Decode(nullptr, payloads);
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::INVALID_PARAMS);
}

// Regression test: a structurally valid MessagePack task-manager payload whose
// task id field has the wrong type used to throw msgpack::type_error out of
// TaskManagerSerializer::Deserialize() (the arr[0].as<std::string>() call sits
// outside the field-conversion try block). Since RestoreState() no longer
// wraps each candidate in a try/catch, an escaping exception here would abort
// restore and prevent fallback to an older healthy snapshot. Decode() must
// convert it into a SerializationError instead of throwing.
TEST_F(MasterSnapshotCodecTest, DecodeWithInvalidTaskFieldTypeReturnsError) {
    MasterSnapshotCodec codec;

    // Start from a valid encoded snapshot so the segments and metadata payloads
    // decode cleanly; we only want to corrupt the task-manager payload.
    MasterSnapshotStateView state_view = MakeStateView(*master_service_);
    auto encode_result = codec.Encode(state_view);
    ASSERT_TRUE(encode_result.has_value())
        << "Encode failed: " << encode_result.error().message;
    MasterSnapshotPayloads payloads = std::move(encode_result.value());

    // Build a structurally valid MessagePack task-manager payload: an outer
    // array of one task, the task itself a valid array with the expected field
    // count, but the id field (index 0, expected string) is an integer. This
    // unpacks cleanly and only fails at the arr[0].as<std::string>() step,
    // which used to throw msgpack::type_error out of Deserialize().
    constexpr size_t kTaskSerializedFields = 8;  // must match the serializer
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);
    packer.pack_array(1);  // one task
    packer.pack_array(kTaskSerializedFields);
    packer.pack(static_cast<int32_t>(12345));  // id: wrong type (int, not str)
    packer.pack(static_cast<int32_t>(0));      // type
    packer.pack(static_cast<int32_t>(0));      // status
    packer.pack(std::string("payload"));       // payload
    packer.pack(static_cast<int64_t>(0));      // created_at
    packer.pack(static_cast<int64_t>(0));      // last_updated_at
    packer.pack(std::string("message"));       // message
    packer.pack(std::string("assigned"));      // assigned_client

    payloads.task_manager = zstd_compress(
        reinterpret_cast<const uint8_t*>(sbuf.data()), sbuf.size(), 3);

    // Decode a fresh service. It must not throw; it must report a serialization
    // error so RestoreState() can fall back to another candidate snapshot.
    auto target_service = MakeMasterService();
    tl::expected<void, SerializationError> decode_result;
    ASSERT_NO_THROW(
        { decode_result = codec.Decode(target_service.get(), payloads); });
    EXPECT_FALSE(decode_result.has_value());
    EXPECT_EQ(decode_result.error().code, ErrorCode::DESERIALIZE_FAIL);
}

}  // namespace mooncake::ha
