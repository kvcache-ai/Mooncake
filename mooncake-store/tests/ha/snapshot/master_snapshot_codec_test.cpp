#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

#include <msgpack.hpp>

#include "ha/snapshot/master_snapshot_codec.h"
#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "segment.h"
#include "storage/distributed/shard_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "task_manager.h"
#include "tenant_id.h"
#include "common/zstd_util.h"

namespace mooncake::ha {

using mooncake::test::MasterServiceTestPeer;

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

    // Assemble the codec state view through the shared test peer.
    static MasterSnapshotStateView MakeStateView(MasterService& service) {
        return MasterSnapshotStateView(
            service, MasterServiceTestPeer::SegmentManager(service),
            MasterServiceTestPeer::LocalSsdManager(service),
            MasterServiceTestPeer::NofSegmentManager(service),
            MasterServiceTestPeer::TaskManager(service));
    }

    static WeightMetadataStore& WeightMetadata(MasterService& service) {
        return MasterServiceTestPeer::WeightMetadata(service);
    }

    static WeightRevisionMetadata PublishReady(MasterService& service,
                                               WeightRevisionIdentity identity,
                                               uint64_t now_ms) {
        auto& metadata_store = WeightMetadata(service);
        const auto group_id = MakeWeightPayloadGroupId(identity);
        auto begin = metadata_store.PrepareBeginImport(
            BeginWeightImportRequest{
                .identity = identity,
                .payload_group_id = group_id,
                .expected_payload_count = 1,
                .expected_logical_bytes = 1024,
            },
            now_ms);
        EXPECT_TRUE(begin.has_value());
        EXPECT_TRUE(metadata_store.Publish(*begin).has_value());
        auto commit = metadata_store.PrepareCommitImport(
            CommitWeightImportRequest{
                .identity = identity,
                .expected_metadata_generation = 1,
                .manifest =
                    WeightManifestReference{
                        .manifest_key =
                            "weights/" + identity.name_space + "/" +
                            identity.resource_id + "/" + identity.revision +
                            "/" + std::to_string(identity.weight_generation) +
                            "/manifest",
                        .manifest_sha256 = std::string(64, 'a'),
                        .payload_group_id = group_id,
                        .payload_keys_sha256 = std::string(64, 'b'),
                        .payload_count = 1,
                        .logical_bytes = 1024,
                    },
            },
            now_ms + 1);
        EXPECT_TRUE(commit.has_value());
        auto ready = metadata_store.Publish(*commit);
        EXPECT_TRUE(ready.has_value());
        return ready.value();
    }

    static std::vector<uint8_t> RewriteWeightMetadataStoreField(
        const std::vector<uint8_t>& metadata, bool keep_field,
        bool corrupt_field = false) {
        auto root = msgpack::unpack(
            reinterpret_cast<const char*>(metadata.data()), metadata.size());
        const auto& object = root.get();
        EXPECT_EQ(msgpack::type::MAP, object.type);
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(&buffer);
        packer.pack_map(object.via.map.size - (keep_field ? 0 : 1));
        for (uint32_t i = 0; i < object.via.map.size; ++i) {
            const auto& item = object.via.map.ptr[i];
            const auto key = item.key.as<std::string>();
            if (key == "weight_metadata" && !keep_field) {
                continue;
            }
            packer.pack(item.key);
            if (key == "weight_metadata" && corrupt_field) {
                packer.pack_bin(3);
                packer.pack_bin_body("bad", 3);
            } else {
                packer.pack(item.val);
            }
        }
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(buffer.data()),
            reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size());
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
            ShardAllocator writer;
            EXPECT_TRUE(writer.Init(config).has_value());
            auto allocated = writer.Allocate(key, 1024);
            EXPECT_TRUE(allocated.has_value());
            if (allocated) descriptor = *allocated;
        }

        auto recovering = std::make_unique<ShardAllocator>();
        EXPECT_TRUE(recovering->Init(config, true).has_value());
        service.enable_dfs_ = true;
        service.shard_allocator_ = recovering.get();
        service.dfs_allocator_ = std::move(recovering);
        return descriptor;
    }

    static tl::expected<void, ErrorCode> ReconcileDfsSnapshot(
        MasterService& service) {
        return service.ReconcileDfsMetadataAfterSnapshot();
    }

    static bool IsDfsRecoveryReady(const MasterService& service) {
        return service.dfs_allocator_ &&
               service.shard_allocator_->IsRecoveryReady();
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

TEST_F(MasterSnapshotCodecTest,
       RejectsMissingShardsWithoutClearingWeightMetadata) {
    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    const auto imported =
        master_service_->BeginWeightImport(BeginWeightImportRequest{
            .identity = identity,
            .payload_group_id = {},
            .expected_payload_count = 1,
            .expected_logical_bytes = 1024,
        });
    ASSERT_TRUE(imported.has_value());

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(0);
    const std::vector<uint8_t> metadata(
        reinterpret_cast<const uint8_t*>(buffer.data()),
        reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size());
    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto payloads = codec.Encode(state_view);
    ASSERT_TRUE(payloads.has_value());
    payloads->metadata = metadata;
    const auto decoded = codec.Decode(master_service_.get(), *payloads);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(ErrorCode::DESERIALIZE_FAIL, decoded.error().code);
    const auto retained = master_service_->GetWeightRevision(
        GetWeightRevisionRequest{.identity = identity});
    ASSERT_TRUE(retained.has_value());
    EXPECT_EQ(*imported, retained->metadata);
}

TEST_F(MasterSnapshotCodecTest,
       WeightMetadataStoreRoundTripPreservesLeasesOperationsAndDerivedIndex) {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto leased_identity = WeightRevisionIdentity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    auto operating_identity = leased_identity;
    operating_identity.revision = "step-200";
    operating_identity.weight_generation = 8;
    auto leased = PublishReady(*master_service_, leased_identity, now_ms);
    auto operating =
        PublishReady(*master_service_, operating_identity, now_ms + 10);

    auto lease =
        WeightMetadata(*master_service_)
            .PrepareAcquireLease(
                AcquireWeightRevisionLeaseRequest{
                    .identity = leased_identity,
                    .expected_metadata_generation = leased.metadata_generation,
                    .holder = "worker-0",
                    .ttl_ms = 60'000,
                },
                now_ms + 20);
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(WeightMetadata(*master_service_).Publish(*lease).has_value());

    auto operation = WeightMetadata(*master_service_)
                         .PrepareStartOperation(
                             StartWeightResidencyOperationRequest{
                                 .identity = operating_identity,
                                 .expected_metadata_generation =
                                     operating.metadata_generation,
                                 .target_residency = WeightResidencyState::COLD,
                             },
                             now_ms + 30);
    ASSERT_TRUE(operation.has_value());
    ASSERT_TRUE(
        WeightMetadata(*master_service_).Publish(*operation).has_value());
    const auto expected_snapshot =
        WeightMetadata(*master_service_).ExportSnapshot();

    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(state_view);
    ASSERT_TRUE(encoded.has_value());
    auto target = MakeMasterService();
    ASSERT_TRUE(codec.Decode(target.get(), *encoded).has_value());

    EXPECT_EQ(expected_snapshot, WeightMetadata(*target).ExportSnapshot());
    EXPECT_TRUE(WeightMetadata(*target).IsManagedGroup(
        MakeWeightPayloadGroupId(leased_identity)));
    auto leased_view = target->GetWeightRevision(
        GetWeightRevisionRequest{.identity = leased_identity});
    ASSERT_TRUE(leased_view.has_value());
    EXPECT_EQ(1, leased_view->active_lease_count);
    auto operating_view = target->GetWeightRevision(
        GetWeightRevisionRequest{.identity = operating_identity});
    ASSERT_TRUE(operating_view.has_value());
    EXPECT_EQ(WeightOperationState::EVICTING,
              operating_view->metadata.operation);
}

TEST_F(MasterSnapshotCodecTest,
       EncodeUsesFrozenWeightMetadataAndDefaultsToLiveState) {
    WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    PublishReady(*master_service_, identity, 100);
    const auto frozen = WeightMetadata(*master_service_).ExportSnapshot();
    identity.revision = "step-200";
    PublishReady(*master_service_, identity, 200);
    const auto live = WeightMetadata(*master_service_).ExportSnapshot();
    ASSERT_NE(frozen, live);

    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded_frozen = codec.Encode(state_view, &frozen);
    ASSERT_TRUE(encoded_frozen.has_value());
    auto target = MakeMasterService();
    ASSERT_TRUE(codec.Decode(target.get(), *encoded_frozen).has_value());
    EXPECT_EQ(frozen, WeightMetadata(*target).ExportSnapshot());

    auto encoded_live = codec.Encode(state_view);
    ASSERT_TRUE(encoded_live.has_value());
    ASSERT_TRUE(codec.Decode(target.get(), *encoded_live).has_value());
    EXPECT_EQ(live, WeightMetadata(*target).ExportSnapshot());

    const WeightMetadataSnapshot empty;
    auto encoded_empty = codec.Encode(state_view, &empty);
    ASSERT_TRUE(encoded_empty.has_value());
    ASSERT_TRUE(codec.Decode(target.get(), *encoded_empty).has_value());
    EXPECT_EQ(empty, WeightMetadata(*target).ExportSnapshot());
}

TEST_F(MasterSnapshotCodecTest,
       OldSnapshotWithoutWeightMetadataStoreRestoresEmpty) {
    PublishReady(*master_service_,
                 WeightRevisionIdentity{
                     .tenant_id = "default",
                     .name_space = "production",
                     .resource_id = "llama-70b",
                     .revision = "step-100",
                     .weight_generation = 7,
                 },
                 100);
    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(state_view);
    ASSERT_TRUE(encoded.has_value());
    encoded->metadata = RewriteWeightMetadataStoreField(encoded->metadata,
                                                        /*keep_field=*/false);

    auto target = MakeMasterService();
    ASSERT_TRUE(codec.Decode(target.get(), *encoded).has_value());
    EXPECT_TRUE(WeightMetadata(*target).ExportSnapshot().metadata.empty());

    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-200",
        .weight_generation = 8,
    };
    ASSERT_TRUE(target->BeginWeightImport(BeginWeightImportRequest{
        .identity = identity,
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    }));
    ASSERT_TRUE(codec.Decode(target.get(), *encoded).has_value());
    EXPECT_FALSE(target->GetWeightRevision(
        GetWeightRevisionRequest{.identity = identity}));
    EXPECT_TRUE(WeightMetadata(*target).ExportSnapshot().metadata.empty());
}

TEST_F(MasterSnapshotCodecTest, MalformedWeightMetadataStoreFailsClosed) {
    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(state_view);
    ASSERT_TRUE(encoded.has_value());
    encoded->metadata = RewriteWeightMetadataStoreField(
        encoded->metadata, /*keep_field=*/true, /*corrupt_field=*/true);

    auto target = MakeMasterService();
    auto decoded = codec.Decode(target.get(), *encoded);
    EXPECT_FALSE(decoded.has_value());
    EXPECT_TRUE(WeightMetadata(*target).ExportSnapshot().metadata.empty());

    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-200",
        .weight_generation = 8,
    };
    const auto imported = target->BeginWeightImport(BeginWeightImportRequest{
        .identity = identity,
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    });
    ASSERT_TRUE(imported.has_value());
    EXPECT_FALSE(codec.Decode(target.get(), *encoded).has_value());
    const auto retained = target->GetWeightRevision(
        GetWeightRevisionRequest{.identity = identity});
    ASSERT_TRUE(retained.has_value());
    EXPECT_EQ(*imported, retained->metadata);
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
