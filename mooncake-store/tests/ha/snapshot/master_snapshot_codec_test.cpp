#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <msgpack.hpp>

#include "ha/snapshot/master_snapshot_codec.h"
#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "segment.h"
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

    void CheckCopyRecovery(bool copy_before_drain, bool lose_target) {
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        master_service_ = std::make_unique<MasterService>(config);
        MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
        const auto owner = generate_uuid();
        const auto executor = generate_uuid();
        Segment source;
        source.id = generate_uuid();
        source.name = "copy_source";
        source.te_endpoint = source.name;
        source.base = 0x300000000;
        source.size = 16 * 1024 * 1024;
        ASSERT_TRUE(master_service_->MountSegment(source, owner));
        ASSERT_TRUE(
            master_service_->PutStart(owner, "copy_key", TenantId::Default(),
                                      1024, ReplicateConfig{.replica_num = 1}));
        ASSERT_TRUE(master_service_->PutEnd(
            owner, "copy_key", TenantId::Default(), ReplicaType::MEMORY));
        Segment target = source;
        target.id = generate_uuid();
        target.name = "copy_target";
        target.te_endpoint = target.name;
        target.base = 0x400000000;
        ASSERT_TRUE(master_service_->MountSegment(target, executor));
        Segment lost = target;
        lost.id = generate_uuid();
        lost.name = "lost_copy_target";
        lost.te_endpoint = lost.name;
        lost.base = 0x500000000;
        std::vector<std::string> targets{target.name};
        if (lose_target) {
            ASSERT_TRUE(master_service_->MountSegment(lost, executor));
            targets.push_back(lost.name);
        }
        std::string drain_target = target.name;
        if (copy_before_drain) {
            Segment fresh = target;
            fresh.id = generate_uuid();
            fresh.name = "after_copy_target";
            fresh.te_endpoint = fresh.name;
            fresh.base = 0x600000000;
            ASSERT_TRUE(master_service_->MountSegment(fresh, executor));
            drain_target = fresh.name;
        }
        if (copy_before_drain) {
            ASSERT_TRUE(master_service_->CopyStart(executor, "copy_key",
                                                   TenantId::Default(),
                                                   source.name, targets));
        }
        auto id =
            master_service_->CreateDrainJob({{source.name}, {drain_target}, 1});
        ASSERT_TRUE(id);
        MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
        EXPECT_EQ(copy_before_drain ? 0u : 1u,
                  master_service_->QueryDrainJob(*id)->active_units);
        if (!copy_before_drain) {
            ASSERT_TRUE(master_service_->CopyStart(executor, "copy_key",
                                                   TenantId::Default(),
                                                   source.name, targets));
        }
        if (lose_target) {
            ASSERT_TRUE(master_service_->UnmountSegment(lost.id, executor));
            // This accessor may prune the invalid target while leaving the
            // existing per-object task responsible for its pending charge.
            EXPECT_FALSE(master_service_->MoveStart(owner, "copy_key",
                                                    TenantId::Default(),
                                                    source.name, target.name));
        }
        MasterSnapshotCodec codec;
        auto view = MakeStateView(*master_service_);
        auto saved = codec.Encode(view);
        ASSERT_TRUE(saved);
        config.initial_snapshot_payloads =
            std::make_shared<MasterSnapshotPayloads>(*saved);
        auto recovered = std::make_unique<MasterService>(config);
        MasterServiceTestPeer::StopDrainDispatcher(*recovered);
        EXPECT_FALSE(recovered->MoveStart(
            owner, "copy_key", TenantId::Default(), source.name, target.name));
        auto before =
            recovered->GetReplicaList("copy_key", TenantId::Default());
        ASSERT_TRUE(before);
        ASSERT_EQ(1u, before->replicas.size());
        EXPECT_EQ(source.name, before->replicas.front()
                                   .get_memory_descriptor()
                                   .buffer_descriptor.transport_endpoint_);
        if (!copy_before_drain) {
            auto legacy = std::make_shared<MasterSnapshotPayloads>(*saved);
            legacy->drain_jobs.reset();
            config.initial_snapshot_payloads = legacy;
            auto orphan = std::make_unique<MasterService>(config);
            MasterServiceTestPeer::StopDrainDispatcher(*orphan);
            // Legacy orphan buffers remain quarantined and cannot be mistaken
            // for an already-completed move destination.
            auto blocked =
                orphan->MoveStart(owner, "copy_key", TenantId::Default(),
                                  source.name, target.name);
            ASSERT_FALSE(blocked);
            EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                      blocked.error());
            auto revoked =
                orphan->MoveRevoke(owner, "copy_key", TenantId::Default());
            ASSERT_FALSE(revoked);
            EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK, revoked.error());
            EXPECT_TRUE(
                orphan->GetReplicaList("copy_key", TenantId::Default()));
        }
        auto copied =
            recovered->CopyEnd(executor, "copy_key", TenantId::Default());
        if (lose_target) {
            ASSERT_FALSE(copied);
            EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, copied.error());
        } else {
            ASSERT_TRUE(copied);
        }
        MasterServiceTestPeer::ProcessDrainJobs(*recovered);
        auto tasks = recovered->FetchTasks(owner, 10);
        ASSERT_TRUE(tasks);
        ASSERT_EQ(1u, tasks->size());
        auto move = recovered->MoveStart(owner, "copy_key", TenantId::Default(),
                                         source.name, drain_target);
        ASSERT_TRUE(move);
        EXPECT_EQ(copy_before_drain, move->target.has_value());
        // A queued task can reuse the copy. A newly scheduled drain uses a
        // fresh destination, retaining the existing replica-count policy.
        ASSERT_TRUE(recovered->MoveEnd(owner, "copy_key", TenantId::Default()));
        TaskCompleteRequest complete;
        complete.id = tasks->front().id;
        complete.status = TaskStatus::SUCCESS;
        ASSERT_TRUE(recovered->MarkTaskToComplete(owner, complete));
        MasterServiceTestPeer::ProcessDrainJobs(*recovered);
        EXPECT_EQ(JobStatus::SUCCEEDED, recovered->QueryDrainJob(*id)->status);
        auto after = recovered->GetReplicaList("copy_key", TenantId::Default());
        ASSERT_TRUE(after);
        ASSERT_EQ(copy_before_drain ? 2u : 1u, after->replicas.size());
        EXPECT_TRUE(
            std::any_of(after->replicas.begin(), after->replicas.end(),
                        [&](const auto& replica) {
                            return replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_ ==
                                   drain_target;
                        }));
    }

    std::unique_ptr<MasterService> master_service_;
};

TEST_F(MasterSnapshotCodecTest, EncodeManifestPreservesSnapshotId) {
    std::vector<uint8_t> bytes = MasterSnapshotCodec::EncodeManifest(
        MasterSnapshotCodec::kSerializerType,
        MasterSnapshotCodec::kSerializerVersion, "snapshot-000042");
    std::string manifest(bytes.begin(), bytes.end());
    EXPECT_EQ(manifest, "messagepack|1.1.0|snapshot-000042");
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

TEST_F(MasterSnapshotCodecTest, EncodeDecodeRoundTripPreservesDrainJob) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "drain_snapshot_source";
    segment.base = 0x300000000;
    segment.size = 1024 * 1024 * 16;
    segment.te_endpoint = segment.name;
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(master_service_->MountSegment(segment, client_id).has_value());

    // An empty segment isolates job persistence from replica recovery.
    // The dispatcher may finish the job; its identity must still survive.
    CreateDrainJobRequest request;
    request.segments = {segment.name};
    auto job_id = master_service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());
    auto original_job = master_service_->QueryDrainJob(*job_id);
    ASSERT_TRUE(original_job.has_value());

    MasterSnapshotCodec codec;
    auto state_view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(state_view);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;
    auto target = MakeMasterService();
    auto decoded = codec.Decode(target.get(), *encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    auto restored_job = target->QueryDrainJob(*job_id);
    ASSERT_TRUE(restored_job.has_value())
        << "QueryDrainJob failed: " << static_cast<int>(restored_job.error());
    // Status and progress can change concurrently; identity must survive.
    EXPECT_EQ(restored_job->id, original_job->id);
    EXPECT_EQ(restored_job->type, original_job->type);
    EXPECT_EQ(restored_job->segments, original_job->segments);
    EXPECT_EQ(restored_job->created_at_ms_epoch,
              original_job->created_at_ms_epoch);
}

TEST_F(MasterSnapshotCodecTest, RestoresCompletedDrainWithEmptySourceName) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    Segment source;
    source.id = generate_uuid();
    source.name = "";
    source.te_endpoint = "empty_source_transport";
    source.base = 0x300000000;
    source.size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_service_->MountSegment(source, generate_uuid()));
    auto job = master_service_->CreateDrainJob({{source.name}, {}, 1});
    ASSERT_TRUE(job);
    MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
    ASSERT_EQ(JobStatus::SUCCEEDED,
              master_service_->QueryDrainJob(*job)->status);

    MasterSnapshotCodec codec;
    auto view = MakeStateView(*master_service_);
    auto saved = codec.Encode(view);
    ASSERT_TRUE(saved);
    auto recovered = MakeMasterService();
    MasterServiceTestPeer::StopDrainDispatcher(*recovered);
    auto restored = codec.Decode(recovered.get(), *saved);
    ASSERT_TRUE(restored) << restored.error().message;
    auto result = recovered->QueryDrainJob(*job);
    ASSERT_TRUE(result);
    EXPECT_EQ(JobStatus::SUCCEEDED, result->status);
    EXPECT_EQ(std::vector<std::string>{""}, result->segments);
}

TEST_F(MasterSnapshotCodecTest, RestoresPendingDrainWithEmptyTargetName) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    const auto client = generate_uuid();
    Segment source;
    source.id = generate_uuid();
    source.name = "named_source";
    source.te_endpoint = source.name;
    source.base = 0x300000000;
    source.size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_service_->MountSegment(source, client));
    ASSERT_TRUE(master_service_->PutStart(client, "empty_target_key",
                                          TenantId::Default(), 1024,
                                          ReplicateConfig{.replica_num = 1}));
    ASSERT_TRUE(master_service_->PutEnd(
        client, "empty_target_key", TenantId::Default(), ReplicaType::MEMORY));
    {
        MasterServiceTestPeer::MetadataAccessorRW metadata(
            master_service_.get(), {TenantId::Default(), "empty_target_key"});
        metadata.Get().lease_->SetDeadline(
            std::chrono::system_clock::time_point{});
    }
    Segment target = source;
    target.id = generate_uuid();
    target.name = "";
    target.te_endpoint = "empty_target_transport";
    target.base = 0x400000000;
    ASSERT_TRUE(master_service_->MountSegment(target, client));
    auto job =
        master_service_->CreateDrainJob({{source.name}, {target.name}, 1});
    ASSERT_TRUE(job);
    MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
    ASSERT_EQ(1u, master_service_->QueryDrainJob(*job)->active_units);

    MasterSnapshotCodec codec;
    auto view = MakeStateView(*master_service_);
    auto saved = codec.Encode(view);
    ASSERT_TRUE(saved);
    auto recovered = MakeMasterService();
    MasterServiceTestPeer::StopDrainDispatcher(*recovered);
    auto restored = codec.Decode(recovered.get(), *saved);
    ASSERT_TRUE(restored) << restored.error().message;
    ASSERT_TRUE(MasterServiceTestPeer::RebuildSnapshotLiveness(*recovered));
    ASSERT_EQ(1u, recovered->QueryDrainJob(*job)->active_units);
    auto tasks = recovered->FetchTasks(client, 1);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(1u, tasks->size());
    ReplicaMovePayload payload;
    struct_json::from_json(payload, tasks->front().payload);
    EXPECT_EQ(source.name, payload.source);
    EXPECT_TRUE(payload.target.empty());
}

TEST_F(MasterSnapshotCodecTest, PopulatedDrainingSegmentSurvivesSnapshot) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "populated_drain_source";
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    segment.te_endpoint = segment.name;
    const auto client = generate_uuid();
    ASSERT_TRUE(master_service_->MountSegment(segment, client));
    ASSERT_TRUE(master_service_->PutStart(client, "drain_key",
                                          TenantId::Default(), 1024,
                                          ReplicateConfig{.replica_num = 1}));
    ASSERT_TRUE(master_service_->PutEnd(
        client, "drain_key", TenantId::Default(), ReplicaType::MEMORY));
    auto job = master_service_->CreateDrainJob({{segment.name}, {}, 4});
    ASSERT_TRUE(job);
    MasterSnapshotCodec codec;
    auto view = MakeStateView(*master_service_);
    auto encoded = codec.Encode(view);
    ASSERT_TRUE(encoded);
    auto target = MakeMasterService();
    MasterServiceTestPeer::StopDrainDispatcher(*target);
    auto decoded = codec.Decode(target.get(), *encoded);
    ASSERT_TRUE(decoded) << decoded.error().message;
    auto replicas = target->GetReplicaList("drain_key", TenantId::Default());
    ASSERT_TRUE(replicas);
    EXPECT_EQ(1u, replicas->replicas.size());
    EXPECT_EQ(SegmentStatus::DRAINING,
              *target->QuerySegmentStatus(segment.name));
    EXPECT_FALSE(MasterServiceTestPeer::SegmentManager(*target)
                     .getSegmentAccess()
                     .IsSegmentAllocatable(segment.name));
    EXPECT_TRUE(target->QueryDrainJob(*job));
}

TEST_F(MasterSnapshotCodecTest,
       DrainRetriesAndMissingTaskHistorySurviveRestore) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    const auto client = generate_uuid();
    Segment source;
    source.id = generate_uuid();
    source.name = "retry_source";
    source.te_endpoint = source.name;
    source.base = 0x300000000;
    source.size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_service_->MountSegment(source, client));
    ASSERT_TRUE(master_service_->PutStart(client, "retry_key",
                                          TenantId::Default(), 1024,
                                          ReplicateConfig{.replica_num = 1}));
    ASSERT_TRUE(master_service_->PutEnd(
        client, "retry_key", TenantId::Default(), ReplicaType::MEMORY));
    {
        MasterServiceTestPeer::MetadataAccessorRW metadata(
            master_service_.get(), {TenantId::Default(), "retry_key"});
        metadata.Get().lease_->SetDeadline(
            std::chrono::system_clock::time_point{});
    }
    Segment target = source;
    target.id = generate_uuid();
    target.name = "retry_target";
    target.te_endpoint = target.name;
    target.base = 0x400000000;
    ASSERT_TRUE(master_service_->MountSegment(target, client));
    auto job =
        master_service_->CreateDrainJob({{source.name}, {target.name}, 1});
    ASSERT_TRUE(job);
    MasterSnapshotCodec codec;
    for (int attempt = 1; attempt <= 3; ++attempt) {
        MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
        auto tasks = master_service_->FetchTasks(client, 10);
        ASSERT_TRUE(tasks);
        ASSERT_EQ(1u, tasks->size());
        EXPECT_EQ(DEFAULT_MAX_RETRY_ATTEMPTS,
                  tasks->front().max_retry_attempts);
        TaskCompleteRequest failure;
        failure.id = tasks->front().id;
        failure.status = TaskStatus::FAILED;
        if (attempt == 1) {
            auto task = MasterServiceTestPeer::TaskManager(*master_service_)
                            .get_read_access()
                            .find_task_by_id(failure.id);
            ASSERT_TRUE(task);
            task->last_updated_at -= std::chrono::hours(24);
            {
                auto access =
                    MasterServiceTestPeer::TaskManager(*master_service_)
                        .get_write_access();
                access.clear_all();
                access.restore_task(std::move(*task));
            }
            auto timeout_view = MakeStateView(*master_service_);
            auto timeout_snapshot = codec.Encode(timeout_view);
            ASSERT_TRUE(timeout_snapshot);
            auto recovered = MakeMasterService();
            MasterServiceTestPeer::StopDrainDispatcher(*recovered);
            ASSERT_TRUE(codec.Decode(recovered.get(), *timeout_snapshot));
            ASSERT_TRUE(
                MasterServiceTestPeer::RebuildSnapshotLiveness(*recovered));
            EXPECT_EQ(TaskStatus::PROCESSING,
                      recovered->QueryTask(failure.id)->status);
            MasterServiceTestPeer::ProcessDrainJobs(*recovered);
            EXPECT_EQ(1u, recovered->QueryDrainJob(*job)->active_units);
            EXPECT_EQ(0u, recovered->QueryDrainJob(*job)->failed_units);
            MasterServiceTestPeer::TaskManager(*recovered)
                .get_write_access()
                .prune_expired_tasks();
            master_service_ = std::move(recovered);
        } else {
            ASSERT_TRUE(master_service_->MarkTaskToComplete(client, failure));
        }
        MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
        EXPECT_EQ(static_cast<uint64_t>(attempt),
                  master_service_->QueryDrainJob(*job)->failed_units);
        auto view = MakeStateView(*master_service_);
        auto saved = codec.Encode(view);
        ASSERT_TRUE(saved);
        auto restored = MakeMasterService();
        MasterServiceTestPeer::StopDrainDispatcher(*restored);
        ASSERT_TRUE(codec.Decode(restored.get(), *saved));
        ASSERT_TRUE(MasterServiceTestPeer::RebuildSnapshotLiveness(*restored));
        master_service_ = std::move(restored);
    }
    EXPECT_EQ(JobStatus::FAILED, master_service_->QueryDrainJob(*job)->status);
    MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
    EXPECT_EQ(3u, master_service_->QueryDrainJob(*job)->failed_units);
    // A second job whose task history has been pruned must not reissue a move
    // or claim success for an outcome that is no longer known.
    auto second =
        master_service_->CreateDrainJob({{source.name}, {target.name}, 1});
    ASSERT_TRUE(second);
    MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
    ASSERT_EQ(1u, master_service_->QueryDrainJob(*second)->active_units);
    MasterServiceTestPeer::TaskManager(*master_service_)
        .get_write_access()
        .clear_all();
    auto view = MakeStateView(*master_service_);
    auto saved = codec.Encode(view);
    ASSERT_TRUE(saved);
    auto restored = MakeMasterService();
    MasterServiceTestPeer::StopDrainDispatcher(*restored);
    ASSERT_TRUE(codec.Decode(restored.get(), *saved));
    ASSERT_TRUE(MasterServiceTestPeer::RebuildSnapshotLiveness(*restored));
    MasterServiceTestPeer::ProcessDrainJobs(*restored);
    EXPECT_EQ(JobStatus::FAILED, restored->QueryDrainJob(*second)->status);
    EXPECT_EQ(1u, restored->QueryDrainJob(*second)->failed_units);
    EXPECT_EQ(
        0u,
        MasterServiceTestPeer::TaskManager(*restored).get_read_access().size());
    MasterServiceTestPeer::ProcessDrainJobs(*restored);
    EXPECT_EQ(1u, restored->QueryDrainJob(*second)->failed_units);
}

TEST_F(MasterSnapshotCodecTest,
       RejectsMalformedDrainJobsAndClearsFailedRestoreState) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "validation_source";
    segment.te_endpoint = segment.name;
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_service_->MountSegment(segment, generate_uuid()));
    auto job = master_service_->CreateDrainJob({{segment.name}, {}, 1});
    ASSERT_TRUE(job);
    MasterSnapshotCodec codec;
    auto view = MakeStateView(*master_service_);
    auto saved = codec.Encode(view);
    ASSERT_TRUE(saved);
    auto root = msgpack::unpack(
        reinterpret_cast<const char*>(saved->drain_jobs->data()),
        saved->drain_jobs->size());
    const auto object = root.get().via.array.ptr[0].via.array.ptr[0];
    auto target = MakeMasterService();
    MasterServiceTestPeer::StopDrainDispatcher(*target);
    for (const auto [field, value] : std::vector<std::pair<int, int>>{
             {0, 77}, {1, 77}, {2, 99}, {5, 0}, {9, 1}, {13, 1}}) {
        auto corrupt = *saved;
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(&buffer);
        packer.pack_array(2);
        packer.pack_array(1);
        packer.pack_array(17);
        for (int i = 0; i < 17; ++i) {
            if (i == field)
                packer.pack(value);
            else
                packer.pack(object.via.array.ptr[i]);
        }
        packer.pack_array(0);  // no per-object replication records
        corrupt.drain_jobs =
            std::vector<uint8_t>(buffer.data(), buffer.data() + buffer.size());
        auto decoded = codec.Decode(target.get(), corrupt);
        EXPECT_FALSE(decoded) << field;
        EXPECT_FALSE(target->QueryDrainJob(*job));
        MasterServiceTestPeer::ResetSnapshotState(*target);
    }
    auto malformed = std::make_shared<MasterSnapshotPayloads>(*saved);
    malformed->drain_jobs = std::vector<uint8_t>{0x91, 0x90};
    MasterServiceConfig invalid_config;
    invalid_config.initial_snapshot_payloads = malformed;
    EXPECT_THROW(std::make_unique<MasterService>(invalid_config),
                 MasterSnapshotRestoreError);

    auto corrupt = *saved;
    corrupt.drain_jobs = std::vector<uint8_t>{};
    EXPECT_FALSE(codec.Decode(target.get(), corrupt));
    ASSERT_TRUE(codec.Decode(target.get(), *saved));
    EXPECT_TRUE(target->QueryDrainJob(*job));
    MasterServiceTestPeer::ResetSnapshotState(*target);
    EXPECT_FALSE(target->QueryDrainJob(*job));
    // Only explicit absence represents legacy format; empty bytes are corrupt.
    auto legacy = *saved;
    legacy.drain_jobs.reset();
    ASSERT_TRUE(codec.Decode(target.get(), legacy));
    EXPECT_FALSE(target->QueryDrainJob(*job));
    EXPECT_EQ(SegmentStatus::DRAINING,
              *target->QuerySegmentStatus(segment.name));
}

TEST_F(MasterSnapshotCodecTest,
       PreservesIndependentMoveAndBackwardClockUpdate) {
    MasterServiceTestPeer::StopDrainDispatcher(*master_service_);
    const auto owner = generate_uuid();
    const auto executor = generate_uuid();
    Segment source;
    source.id = generate_uuid();
    source.name = "independent_source";
    source.te_endpoint = source.name;
    source.base = 0x300000000;
    source.size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_service_->MountSegment(source, owner));
    ASSERT_TRUE(master_service_->PutStart(owner, "independent_key",
                                          TenantId::Default(), 1024,
                                          ReplicateConfig{.replica_num = 1}));
    ASSERT_TRUE(master_service_->PutEnd(
        owner, "independent_key", TenantId::Default(), ReplicaType::MEMORY));
    {
        MasterServiceTestPeer::MetadataAccessorRW metadata(
            master_service_.get(), {TenantId::Default(), "independent_key"});
        metadata.Get().lease_->SetDeadline(
            std::chrono::system_clock::time_point{});
    }
    Segment target = source;
    target.id = generate_uuid();
    target.name = "requested_target";
    target.te_endpoint = target.name;
    target.base = 0x400000000;
    ASSERT_TRUE(master_service_->MountSegment(target, owner));
    Segment independent = target;
    independent.id = generate_uuid();
    independent.name = "independent_target";
    independent.te_endpoint = independent.name;
    independent.base = 0x500000000;
    ASSERT_TRUE(master_service_->MountSegment(independent, executor));
    auto job =
        master_service_->CreateDrainJob({{source.name}, {target.name}, 1});
    ASSERT_TRUE(job);
    MasterServiceTestPeer::ProcessDrainJobs(*master_service_);
    ASSERT_EQ(1u, master_service_->QueryDrainJob(*job)->active_units);
    // This public operation is independent of the owner's queued drain task.
    ASSERT_TRUE(master_service_->MoveStart(executor, "independent_key",
                                           TenantId::Default(), source.name,
                                           independent.name));
    auto& original =
        *MasterServiceTestPeer::DrainJobs(*master_service_).at(*job);
    original.last_updated_at = original.created_at - std::chrono::seconds(1);
    const auto updated =
        master_service_->QueryDrainJob(*job)->last_updated_at_ms_epoch;
    MasterSnapshotCodec codec;
    auto view = MakeStateView(*master_service_);
    auto saved = codec.Encode(view);
    ASSERT_TRUE(saved);
    MasterServiceConfig config;
    config.initial_snapshot_payloads =
        std::make_shared<MasterSnapshotPayloads>(*saved);
    auto recovered = std::make_unique<MasterService>(config);
    MasterServiceTestPeer::StopDrainDispatcher(*recovered);
    EXPECT_EQ(updated,
              recovered->QueryDrainJob(*job)->last_updated_at_ms_epoch);
    ASSERT_TRUE(
        recovered->MoveEnd(executor, "independent_key", TenantId::Default()));
    auto replicas =
        recovered->GetReplicaList("independent_key", TenantId::Default());
    ASSERT_TRUE(replicas);
    ASSERT_EQ(1u, replicas->replicas.size());
    EXPECT_EQ(independent.name, replicas->replicas.front()
                                    .get_memory_descriptor()
                                    .buffer_descriptor.transport_endpoint_);
    auto pending = recovered->FetchTasks(owner, 10);
    ASSERT_TRUE(pending);
    ASSERT_EQ(1u, pending->size());
}

TEST_F(MasterSnapshotCodecTest,
       PendingDrainPreservesIndependentCopyAndOrphanSafety) {
    CheckCopyRecovery(false, false);
}

TEST_F(MasterSnapshotCodecTest,
       BlockedDrainResumesCopyStartedBeforeScheduling) {
    CheckCopyRecovery(true, false);
}

TEST_F(MasterSnapshotCodecTest, DrainRestoresCopyWithMissingTarget) {
    CheckCopyRecovery(true, true);
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
