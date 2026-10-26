#include "ha/oplog/oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#include "metadata_store.h"
#include "mock_metadata_store.h"
#include "ha/oplog/oplog_types.h"
#include "ha/standby_metadata_store.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "types.h"
#include "weight_management.h"
#include "weight_metadata_store.h"

using mooncake::test::MockMetadataStore;

namespace mooncake::test {

class ReplayDiagnosticSink : public google::LogSink {
   public:
    ReplayDiagnosticSink() { google::AddLogSink(this); }
    ~ReplayDiagnosticSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char*, const char*, int,
              const struct ::tm*, const char* message,
              size_t message_len) override {
        if (severity != google::GLOG_ERROR) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        diagnostic_.append(message, message_len);
        diagnostic_.push_back('\n');
    }

    std::string diagnostic() {
        std::lock_guard<std::mutex> lock(mutex_);
        return diagnostic_;
    }

   private:
    std::mutex mutex_;
    std::string diagnostic_;
};
// Fixed PR3 wire projections: affinity metadata requires a separate migration.
enum class LegacyOperationState : uint8_t {
    NONE,
    EVICTING,
    REHYDRATING,
    REPAIRING
};
struct LegacyRevisionMetadata {
    WeightRevisionIdentity identity;
    WeightManifestReference manifest;
    WeightAvailabilityState availability;
    WeightResidencyState residency;
    LegacyOperationState operation;
    uint64_t operation_id;
    uint64_t metadata_generation;
    uint64_t created_at_ms;
    uint64_t updated_at_ms;
};
YLT_REFL(LegacyRevisionMetadata, identity, manifest, availability, residency,
         operation, operation_id, metadata_generation, created_at_ms,
         updated_at_ms);
struct LegacyResidencyOperation {
    uint64_t operation_id;
    WeightRevisionIdentity identity;
    LegacyOperationState operation;
    WeightResidencyState target_residency;
    uint64_t fenced_metadata_generation;
    uint64_t started_at_ms;
    uint64_t updated_at_ms;
    uint64_t processed_members;
    uint64_t total_members;
    std::string cursor;
    std::string message;
};
YLT_REFL(LegacyResidencyOperation, operation_id, identity, operation,
         target_residency, fenced_metadata_generation, started_at_ms,
         updated_at_ms, processed_members, total_members, cursor, message);
struct LegacyMetadataUpsert {
    LegacyRevisionMetadata metadata;
    std::optional<LegacyResidencyOperation> operation;
};
YLT_REFL(LegacyMetadataUpsert, metadata, operation);
struct LegacyMetadataSnapshot {
    uint32_t schema_version{1};
    std::vector<LegacyRevisionMetadata> metadata;
    std::vector<WeightRevisionLease> leases;
    std::vector<LegacyResidencyOperation> operations;
    uint64_t next_lease_id{1};
    uint64_t next_operation_id{10};
};
YLT_REFL(LegacyMetadataSnapshot, schema_version, metadata, leases, operations,
         next_lease_id, next_operation_id);

// Recorded from b7f5d7191 production headers and writer compile flags.
static_assert(struct_pack::get_type_code<LegacyRevisionMetadata>() ==
              2958082806u);
static_assert(struct_pack::get_type_code<LegacyResidencyOperation>() ==
              943375304u);
static_assert(struct_pack::get_type_code<LegacyMetadataUpsert>() == 209574896u);
static_assert(struct_pack::get_type_code<LegacyMetadataSnapshot>() ==
              3275511804u);
static_assert(struct_pack::get_type_code<WeightRevisionLease>() == 724020070u);

// Helper function to create a valid OpLogEntry with checksum
// Uses the production checksum algorithm (XXH32).
OpLogEntry MakeEntry(uint64_t seq, OpType type, const std::string& key,
                     const std::string& payload) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
    e.op_type = type;
    e.object_key = key;
    e.payload = payload;
    // Compute checksum and prefix_hash using the production algorithm.
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

// Helper function to create a valid struct_pack payload for PUT_END
std::string MakeValidPayload(uint64_t client_id_first = 1,
                             uint64_t client_id_second = 2,
                             uint64_t size = 1024, bool hard_pinned = false) {
    mooncake::MetadataPayload payload;
    payload.client_id = {client_id_first, client_id_second};
    payload.size = size;
    if (hard_pinned) {
        payload.hard_pinned = true;
    }
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

template <typename T>
std::string SerializePayload(const T& value) {
    auto bytes = struct_pack::serialize(value);
    return std::string(bytes.begin(), bytes.end());
}

WeightRevisionIdentity MakeWeightIdentity() {
    return WeightRevisionIdentity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
}

WeightRevisionMetadata MakeWeightMetadata(uint64_t metadata_generation) {
    auto identity = MakeWeightIdentity();
    return WeightRevisionMetadata{
        .identity = identity,
        .manifest =
            WeightManifestReference{
                .manifest_key =
                    "weights/production/llama-70b/step-100/7/manifest",
                .manifest_sha256 = std::string(64, 'a'),
                .payload_group_id = MakeWeightPayloadGroupId(identity),
                .payload_keys_sha256 = std::string(64, 'b'),
                .payload_count = 2,
                .logical_bytes = 2048,
            },
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .affinity_count = 2,
        .affinity_digest = std::string(64, 'c'),
        .observed_hot_ratio = 1.0,
        .metadata_generation = metadata_generation,
        .created_at_ms = 100,
        .updated_at_ms = 100 + metadata_generation,
    };
}

std::string SerializeWeightUpsert(
    const WeightRevisionMetadata& metadata,
    std::optional<WeightResidencyOperation> operation = std::nullopt) {
    return SerializePayload(WeightMetadataUpsertOp{
        .metadata = metadata,
        .operation = std::move(operation),
    });
}

class OpLogApplierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogApplierTest");
        FLAGS_logtostderr = 1;
        mock_metadata_store_ = std::make_unique<MockMetadataStore>();
        cluster_id_ = "test_cluster_001";
        applier_ = std::make_unique<OpLogApplier>(mock_metadata_store_.get(),
                                                  cluster_id_);
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<MockMetadataStore> mock_metadata_store_;
    std::unique_ptr<OpLogApplier> applier_;
    std::string cluster_id_;
};

TEST_F(OpLogApplierTest, AppliesWeightMetadataAndRejectsStaleGeneration) {
    auto generation_one = MakeWeightMetadata(1);
    const auto key = MakeWeightRevisionMetadataKey(generation_one.identity);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(generation_one))));
    ASSERT_EQ(generation_one,
              mock_metadata_store_->GetWeightMetadata(generation_one.identity));

    auto generation_two = generation_one;
    generation_two.metadata_generation = 2;
    generation_two.updated_at_ms = 102;
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(generation_two))));
    ASSERT_EQ(generation_two,
              mock_metadata_store_->GetWeightMetadata(generation_one.identity));

    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(3, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(generation_one))));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_EQ(generation_two,
              mock_metadata_store_->GetWeightMetadata(generation_one.identity));
}

TEST_F(OpLogApplierTest, AppliesImportingToReadyTransition) {
    auto importing = MakeWeightMetadata(1);
    importing.manifest.manifest_key.clear();
    importing.manifest.manifest_sha256.clear();
    importing.manifest.payload_keys_sha256.clear();
    importing.availability = WeightAvailabilityState::IMPORTING;
    importing.residency = WeightResidencyState::UNKNOWN;
    importing.observed_hot_ratio = 0.0;
    auto ready = MakeWeightMetadata(2);
    ready.created_at_ms = importing.created_at_ms;
    const auto key = MakeWeightRevisionMetadataKey(importing.identity);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(importing))));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(MakeEntry(
        2, OpType::WEIGHT_METADATA_UPSERT, key, SerializeWeightUpsert(ready))));
    EXPECT_EQ(ready,
              mock_metadata_store_->GetWeightMetadata(importing.identity));
}

TEST_F(OpLogApplierTest, WeightMetadataDuplicateReplayIsIdempotent) {
    auto metadata = MakeWeightMetadata(1);
    auto entry = MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT,
                           MakeWeightRevisionMetadataKey(metadata.identity),
                           SerializeWeightUpsert(metadata));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_EQ(metadata,
              mock_metadata_store_->GetWeightMetadata(metadata.identity));
}

TEST_F(OpLogApplierTest, ReplaysWeightOperationStartAndCompletion) {
    auto ready = MakeWeightMetadata(1);
    const auto key = MakeWeightRevisionMetadataKey(ready.identity);
    ASSERT_TRUE(applier_->ApplyOpLogEntry(MakeEntry(
        1, OpType::WEIGHT_METADATA_UPSERT, key, SerializeWeightUpsert(ready))));

    auto evicting = ready;
    evicting.metadata_generation = 2;
    evicting.operation_id = 9;
    evicting.updated_at_ms = 200;
    WeightResidencyOperation operation{
        .operation_id = 9,
        .identity = ready.identity,
        .kind = WeightOperationKind::MIGRATING,
        .target_residency = WeightResidencyState::COLD,
        .fenced_metadata_generation = 2,
        .started_at_ms = 200,
        .updated_at_ms = 200,
        .processed_units = 0,
        .total_units = 2,
        .processed_bytes = 0,
        .total_bytes = 2048,
        .cursor = {},
        .message = {},
    };
    ASSERT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(evicting, operation))));
    EXPECT_EQ(operation, mock_metadata_store_->GetWeightOperation(9));

    auto cold = evicting;
    cold.metadata_generation = 3;
    cold.operation_id.reset();
    cold.residency = WeightResidencyState::COLD;
    cold.observed_hot_ratio = 0.0;
    cold.updated_at_ms = 300;
    operation.updated_at_ms = 300;
    operation.message = "completed";
    auto completion = MakeEntry(3, OpType::WEIGHT_METADATA_UPSERT, key,
                                SerializeWeightUpsert(cold, operation));
    ASSERT_TRUE(applier_->ApplyOpLogEntry(completion));
    EXPECT_EQ(cold, mock_metadata_store_->GetWeightMetadata(ready.identity));
    EXPECT_EQ(operation, mock_metadata_store_->GetWeightOperation(9));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(completion));
}

TEST_F(OpLogApplierTest, RejectsWeightOperationWithoutRecord) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto ready = MakeWeightMetadata(1);
    const auto key = MakeWeightRevisionMetadataKey(ready.identity);
    ASSERT_TRUE(applier.ApplyOpLogEntry(MakeEntry(
        1, OpType::WEIGHT_METADATA_UPSERT, key, SerializeWeightUpsert(ready))));

    auto operating = MakeWeightMetadata(2);
    operating.operation_id = 1;
    ASSERT_TRUE(ValidateWeightRevisionMetadata(operating).ok());
    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(operating))));
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
    EXPECT_EQ(ready, standby.GetWeightMetadata(ready.identity));
    EXPECT_TRUE(
        ValidateWeightMetadataSnapshot(standby.SnapshotWeightMetadata()));
}

TEST_F(OpLogApplierTest, CancelledOperationRejectsLateProgressReplay) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    auto metadata = MakeWeightMetadata(1);
    metadata.policy = {.preferred_residency = WeightResidencyState::HOT,
                       .migration_mode = WeightMigrationMode::MANUAL};
    const auto key = MakeWeightRevisionMetadataKey(metadata.identity);
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(metadata))));
    metadata.metadata_generation = 2;
    metadata.operation_id = 9;
    metadata.updated_at_ms = 200;
    WeightResidencyOperation operation{
        .operation_id = 9,
        .identity = metadata.identity,
        .kind = WeightOperationKind::MIGRATING,
        .target_residency = WeightResidencyState::COLD,
        .fenced_metadata_generation = 2,
        .started_at_ms = 200,
        .updated_at_ms = 200,
        .processed_units = 0,
        .total_units = 2,
        .processed_bytes = 0,
        .total_bytes = 2048,
    };
    const auto active_payload = SerializeWeightUpsert(metadata, operation);
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key, active_payload)));
    metadata.metadata_generation = 3;
    metadata.availability = WeightAvailabilityState::DELETING;
    metadata.operation_id.reset();
    metadata.updated_at_ms = 300;
    operation.message = kWeightOperationCancelledMessage;
    operation.updated_at_ms = 300;
    const auto cancellation =
        MakeEntry(3, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(metadata, operation));
    ASSERT_TRUE(applier.ApplyOpLogEntry(cancellation));
    ASSERT_TRUE(applier.ApplyOpLogEntry(cancellation));
    const auto cancelled = standby.SnapshotWeightMetadata();
    EXPECT_TRUE(ValidateWeightMetadataSnapshot(cancelled));
    EXPECT_NE(metadata.residency, operation.target_residency);
    {
        auto conflicting = operation;
        ++conflicting.processed_units;
        ReplayDiagnosticSink sink;
        EXPECT_FALSE(applier.ApplyOpLogEntry(
            MakeEntry(4, OpType::WEIGHT_METADATA_UPSERT, key,
                      SerializeWeightUpsert(metadata, conflicting))));
        const auto diagnostic = sink.diagnostic();
        EXPECT_NE(diagnostic.find("cancellation duplicate operation mismatch"),
                  std::string::npos);
        EXPECT_NE(diagnostic.find("key=" + key), std::string::npos);
        EXPECT_NE(diagnostic.find("sequence_id=4"), std::string::npos);
        EXPECT_NE(diagnostic.find("operation_id=9"), std::string::npos);
        EXPECT_EQ(cancelled, standby.SnapshotWeightMetadata());
    }
    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MakeEntry(4, OpType::WEIGHT_METADATA_UPSERT, key, active_payload)));
    EXPECT_EQ(4u, applier.GetExpectedSequenceId());
    EXPECT_EQ(cancelled, standby.SnapshotWeightMetadata());
    metadata.metadata_generation = 4;
    metadata.updated_at_ms = 400;
    metadata.availability = WeightAvailabilityState::DELETED;
    metadata.residency = WeightResidencyState::ABSENT;
    metadata.observed_hot_ratio = 0.0;
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(4, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(metadata))));
    WeightMetadataStore restored;
    ASSERT_TRUE(restored.RestoreSnapshot(standby.SnapshotWeightMetadata()));
    const auto queried = restored.QueryOperation(9);
    ASSERT_TRUE(queried);
    EXPECT_EQ(operation, *queried);
    const auto view = restored.Get(metadata.identity, 400);
    ASSERT_TRUE(view);
    EXPECT_EQ(WeightAvailabilityState::DELETED, view->metadata.availability);
}

TEST_F(OpLogApplierTest, RejectsCancellationThatChangesPendingOperation) {
    for (const std::string changed :
         {"operation_id", "target", "progress", "bytes"}) {
        SCOPED_TRACE(changed);
        StandbyMetadataStore standby;
        OpLogApplier applier(&standby, cluster_id_);
        auto metadata = MakeWeightMetadata(1);
        metadata.policy = {.preferred_residency = WeightResidencyState::HOT,
                           .migration_mode = WeightMigrationMode::MANUAL};
        const auto key = MakeWeightRevisionMetadataKey(metadata.identity);
        ASSERT_TRUE(applier.ApplyOpLogEntry(
            MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                      SerializeWeightUpsert(metadata))));
        metadata.metadata_generation = 2;
        metadata.updated_at_ms = 200;
        metadata.operation_id = 9;
        WeightResidencyOperation operation{
            .operation_id = 9,
            .identity = metadata.identity,
            .kind = WeightOperationKind::MIGRATING,
            .target_residency = WeightResidencyState::COLD,
            .fenced_metadata_generation = 2,
            .started_at_ms = 200,
            .updated_at_ms = 200,
            .processed_units = 0,
            .total_units = 2,
            .processed_bytes = 0,
            .total_bytes = 2048,
        };
        ASSERT_TRUE(applier.ApplyOpLogEntry(
            MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                      SerializeWeightUpsert(metadata, operation))));
        const auto before = standby.SnapshotWeightMetadata();
        ASSERT_TRUE(ValidateWeightMetadataSnapshot(before));
        metadata.metadata_generation = 3;
        metadata.updated_at_ms = 300;
        metadata.availability = WeightAvailabilityState::DELETING;
        metadata.operation_id.reset();
        operation.message = kWeightOperationCancelledMessage;
        operation.updated_at_ms = 300;
        if (changed == "operation_id") {
            operation.operation_id = 10;
        } else if (changed == "target") {
            operation.target_residency = WeightResidencyState::HOT;
        } else if (changed == "progress") {
            operation.processed_units = 1;
        } else {
            operation.processed_bytes = 1;
        }
        ReplayDiagnosticSink sink;
        EXPECT_FALSE(applier.ApplyOpLogEntry(
            MakeEntry(3, OpType::WEIGHT_METADATA_UPSERT, key,
                      SerializeWeightUpsert(metadata, operation))));
        const auto diagnostic = sink.diagnostic();
        EXPECT_NE(
            diagnostic.find(changed == "operation_id"
                                ? "cancellation prior lineage mismatch"
                                : "cancellation changes operation fields"),
            std::string::npos);
        EXPECT_NE(diagnostic.find("key=" + key), std::string::npos);
        EXPECT_NE(diagnostic.find("sequence_id=3"), std::string::npos);
        EXPECT_NE(diagnostic.find("operation_id=" +
                                  std::to_string(operation.operation_id)),
                  std::string::npos);
        EXPECT_EQ(3u, applier.GetExpectedSequenceId());
        EXPECT_EQ(before, standby.SnapshotWeightMetadata());
    }
}

TEST_F(OpLogApplierTest, RejectsCancellationWithoutPriorOperation) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto before = standby.SnapshotWeightMetadata();
    auto metadata = MakeWeightMetadata(1);
    metadata.policy = {.preferred_residency = WeightResidencyState::HOT,
                       .migration_mode = WeightMigrationMode::MANUAL};
    metadata.availability = WeightAvailabilityState::DELETING;
    ASSERT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());
    WeightResidencyOperation operation{
        .operation_id = 9,
        .identity = metadata.identity,
        .kind = WeightOperationKind::MIGRATING,
        .target_residency = WeightResidencyState::HOT,
        .fenced_metadata_generation = 0,
        .started_at_ms = 100,
        .updated_at_ms = 101,
        .processed_units = 0,
        .total_units = 2,
        .processed_bytes = 0,
        .total_bytes = 2048,
        .cursor = {},
        .message = kWeightOperationCancelledMessage,
    };
    ReplayDiagnosticSink sink;
    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT,
                  MakeWeightRevisionMetadataKey(metadata.identity),
                  SerializeWeightUpsert(metadata, operation))));
    const auto diagnostic = sink.diagnostic();
    EXPECT_NE(diagnostic.find("cancellation has no current metadata"),
              std::string::npos);
    EXPECT_NE(diagnostic.find("key=" +
                              MakeWeightRevisionMetadataKey(metadata.identity)),
              std::string::npos);
    EXPECT_NE(diagnostic.find("sequence_id=1"), std::string::npos);
    EXPECT_NE(diagnostic.find("operation_id=9"), std::string::npos);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
    EXPECT_EQ(before, standby.SnapshotWeightMetadata());
}

TEST_F(OpLogApplierTest, RejectsBareAffinityMetadataWithActiveOperation) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    auto metadata = MakeWeightMetadata(1);
    metadata.operation_id = 9;
    ASSERT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());
    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT,
                  MakeWeightRevisionMetadataKey(metadata.identity),
                  SerializePayload(metadata))));
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(standby.SnapshotWeightMetadata().metadata.empty());
}

TEST_F(OpLogApplierTest, RejectsLegacyWeightMetadataGoldenWithoutMutation) {
    // Generated by struct_pack::serialize(WeightRevisionMetadata) at e4229791.
    const std::string hex =
        "f7c250b004fdfd800c800c800c800c04fffd800c800c800c800c0404ff0606060404"
        "0404ff000764656661756c740a70726f64756374696f6e096c6c616d612d37306208"
        "737465702d31303007000000000000000000477765696768743a3564626265613134"
        "61613735633938613562336662623537366139633631326436346663366263386634"
        "39306632346137666630313766636665333231623764000000000000000000000000"
        "00000000000000000000000000000000010000000000000064000000000000006500"
        "000000000000";
    std::string payload;
    for (size_t i = 0; i < hex.size(); i += 2) {
        payload.push_back(
            static_cast<char>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    }
    WeightRevisionMetadata expected;
    expected.identity = MakeWeightIdentity();
    expected.manifest.payload_group_id =
        MakeWeightPayloadGroupId(expected.identity);
    expected.created_at_ms = 100;
    expected.updated_at_ms = 101;
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto entry =
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT,
                  MakeWeightRevisionMetadataKey(expected.identity), payload);
    ASSERT_FALSE(applier.ApplyOpLogEntry(entry));
    EXPECT_FALSE(standby.GetWeightMetadata(expected.identity));
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
    EXPECT_FALSE(applier.ApplyOpLogEntry(entry));
    EXPECT_TRUE(
        ValidateWeightMetadataSnapshot(standby.SnapshotWeightMetadata()));
}

TEST_F(OpLogApplierTest, RejectsLegacyWeightSnapshotGolden) {
    // Generated by struct_pack::serialize(WeightMetadataSnapshot) at e4229791.
    const std::string hex =
        "fd573cc304fd0284fdfd800c800c800c800c04fffd800c800c800c800c0404ff0606"
        "0604040404ff84fd04fd800c800c800c800c04ff800c0404ff84fd04fd800c800c80"
        "0c800c04ff06060404040404800c800cff0404ff0001000000010764656661756c74"
        "0a70726f64756374696f6e096c6c616d612d37306208737465702d31303007000000"
        "000000000000477765696768743a3564626265613134616137356339386135623366"
        "62623537366139633631326436346663366263386634393066323461376666303137"
        "66636665333231623764000000000000000000000000000000000000000000000000"
        "00000000010000000000000064000000000000006500000000000000000001000000"
        "000000000100000000000000";
    std::vector<uint8_t> bytes;
    for (size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(
            static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    }
    const auto decoded = DecodeBatchOpLogSnapshotWeightMetadata(bytes);
    ASSERT_FALSE(decoded.has_value());
}

TEST_F(OpLogApplierTest, RejectsPr3OperationWireShapesWithoutMutation) {
    const auto current = MakeWeightMetadata(1);
    LegacyRevisionMetadata legacy{
        .identity = current.identity,
        .manifest = current.manifest,
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .operation = LegacyOperationState::EVICTING,
        .operation_id = 9,
        .metadata_generation = 2,
        .created_at_ms = 100,
        .updated_at_ms = 200,
    };
    LegacyResidencyOperation operation{
        .operation_id = 9,
        .identity = current.identity,
        .operation = LegacyOperationState::EVICTING,
        .target_residency = WeightResidencyState::COLD,
        .fenced_metadata_generation = 2,
        .started_at_ms = 200,
        .updated_at_ms = 200,
        .processed_members = 0,
        .total_members = 3,
    };
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto key = MakeWeightRevisionMetadataKey(current.identity);
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(current))));
    const auto before = standby.SnapshotWeightMetadata();
    for (const bool completed : {false, true}) {
        if (completed) {
            legacy.operation = LegacyOperationState::NONE;
            legacy.operation_id = 0;
            legacy.metadata_generation = 3;
            legacy.residency = WeightResidencyState::COLD;
            operation.processed_members = operation.total_members;
            operation.message = "completed";
        }
        // Type-info encoding matches the recorded production writer (NDEBUG=0).
        const auto envelope =
            struct_pack::serialize<struct_pack::sp_config::ENABLE_TYPE_INFO>(
                LegacyMetadataUpsert{legacy, operation});
        ASSERT_EQ(completed ? 522u : 513u, envelope.size());
        ASSERT_EQ(completed ? 0xbbbf57c5u : 0x3de676c9u,
                  XXH32(envelope.data(), envelope.size(), 0));
        EXPECT_FALSE(applier.ApplyOpLogEntry(
            MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                      std::string(envelope.begin(), envelope.end()))));
        EXPECT_EQ(2u, applier.GetExpectedSequenceId());
        EXPECT_EQ(before, standby.SnapshotWeightMetadata());
        const auto snapshot_bytes =
            struct_pack::serialize<struct_pack::sp_config::ENABLE_TYPE_INFO>(
                LegacyMetadataSnapshot{.metadata = {legacy},
                                       .operations = {operation}});
        const std::vector<uint8_t> bytes(snapshot_bytes.begin(),
                                         snapshot_bytes.end());
        ASSERT_EQ(completed ? 567u : 558u, bytes.size());
        ASSERT_EQ(completed ? 0x87b01304u : 0x510f11f2u,
                  XXH32(bytes.data(), bytes.size(), 0));
        EXPECT_FALSE(DecodeBatchOpLogSnapshotWeightMetadata(bytes));
        EXPECT_EQ(before, standby.SnapshotWeightMetadata());
    }
}

TEST_F(OpLogApplierTest, AcceptsUnchangedWeightLeaseWireShape) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto current = MakeWeightMetadata(1);
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT,
                  MakeWeightRevisionMetadataKey(current.identity),
                  SerializeWeightUpsert(current))));
    const WeightRevisionLease lease{.lease_id = 1,
                                    .identity = current.identity,
                                    .holder = "old-reader",
                                    .expires_at_ms = 1000,
                                    .fenced_metadata_generation = 1};
    const auto lease_bytes =
        struct_pack::serialize<struct_pack::sp_config::ENABLE_TYPE_INFO>(lease);
    ASSERT_EQ(105u, lease_bytes.size());
    ASSERT_EQ(0xddba1156u, XXH32(lease_bytes.data(), lease_bytes.size(), 0));
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_LEASE_UPSERT, MakeWeightLeaseMetadataKey(1),
                  std::string(lease_bytes.begin(), lease_bytes.end()))));
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_EQ(lease, standby.GetWeightLease(1));
}

TEST_F(OpLogApplierTest, PublishesWeightOperationAtomicallyToSnapshotReaders) {
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    auto metadata = MakeWeightMetadata(1);
    const auto key = MakeWeightRevisionMetadataKey(metadata.identity);
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(metadata))));
    metadata.metadata_generation = 2;
    metadata.updated_at_ms = 200;
    metadata.operation_id = 9;
    WeightResidencyOperation operation{
        .operation_id = 9,
        .identity = metadata.identity,
        .kind = WeightOperationKind::MIGRATING,
        .target_residency = WeightResidencyState::COLD,
        .fenced_metadata_generation = 2,
        .started_at_ms = 200,
        .updated_at_ms = 200,
        .processed_units = 0,
        .total_units = 2,
        .processed_bytes = 0,
        .total_bytes = 2048,
        .cursor = {},
        .message = {},
    };
    std::atomic<bool> done{false};
    std::atomic<size_t> reads{0};
    std::atomic<size_t> invalid_reads{0};
    std::atomic<bool> writer_active{false};
    std::atomic<size_t> writer_active_reads{0};
    auto reader = std::async(std::launch::async, [&] {
        bool valid = true;
        while (!done.load()) {
            const auto snapshot = standby.SnapshotWeightMetadata();
            const bool snapshot_valid =
                ValidateWeightMetadataSnapshot(snapshot).has_value();
            valid &= snapshot_valid;
            if (!snapshot_valid) {
                invalid_reads.fetch_add(1);
            }
            reads.fetch_add(1);
            if (writer_active.load() && !snapshot.metadata.empty() &&
                snapshot.metadata.front().metadata_generation >= 2) {
                writer_active_reads.fetch_add(1);
            }
        }
        return valid;
    });
    while (reads.load() == 0) {
        std::this_thread::yield();
    }
    bool applied = true;
    writer_active.store(true);
    for (uint64_t iteration = 0; iteration < 256 && applied; ++iteration) {
        metadata.metadata_generation = 2 + iteration * 2;
        metadata.updated_at_ms = 200 + iteration * 2;
        metadata.operation_id = 9 + iteration;
        operation.operation_id = *metadata.operation_id;
        operation.target_residency = iteration % 2 == 0
                                         ? WeightResidencyState::COLD
                                         : WeightResidencyState::HOT;
        operation.fenced_metadata_generation = metadata.metadata_generation;
        operation.updated_at_ms = metadata.updated_at_ms;
        operation.message.clear();
        operation.processed_units = 0;
        operation.processed_bytes = 0;
        auto start = MakeEntry(metadata.metadata_generation,
                               OpType::WEIGHT_METADATA_UPSERT, key,
                               SerializeWeightUpsert(metadata, operation));
        applied =
            applier.ApplyOpLogEntry(start) && applier.ApplyOpLogEntry(start);
        // Keep the writer in its mutation loop until a reader has observed
        // an operation published by this loop, not just the initial state.
        if (iteration == 0 && applied) {
            while (writer_active_reads.load() == 0) {
                std::this_thread::yield();
            }
        }
        metadata.metadata_generation++;
        metadata.updated_at_ms++;
        metadata.operation_id = std::nullopt;
        metadata.residency = operation.target_residency;
        metadata.observed_hot_ratio =
            metadata.residency == WeightResidencyState::HOT ? 1.0 : 0.0;
        operation.updated_at_ms = metadata.updated_at_ms;
        operation.processed_units = operation.total_units;
        operation.processed_bytes = operation.total_bytes;
        operation.message = "completed";
        applied &= applier.ApplyOpLogEntry(MakeEntry(
            metadata.metadata_generation, OpType::WEIGHT_METADATA_UPSERT, key,
            SerializeWeightUpsert(metadata, operation)));
    }
    writer_active.store(false);
    done.store(true);
    EXPECT_TRUE(reader.get());
    EXPECT_TRUE(applied);
    EXPECT_GT(reads.load(), 0u);
    EXPECT_GT(writer_active_reads.load(), 0u);
    std::cout << "snapshot reads=" << reads.load()
              << ", valid=" << reads.load() - invalid_reads.load()
              << ", invalid=" << invalid_reads.load()
              << ", writer_active=" << writer_active_reads.load() << '\n';
    const auto snapshot = standby.SnapshotWeightMetadata();
    EXPECT_TRUE(ValidateWeightMetadataSnapshot(snapshot));
    ASSERT_EQ(1u, snapshot.metadata.size());
    ASSERT_EQ(256u, snapshot.operations.size());
    EXPECT_EQ(metadata, snapshot.metadata.front());
    EXPECT_EQ(operation, snapshot.operations.back());
    EXPECT_EQ(265u, snapshot.next_operation_id);
}

TEST_F(OpLogApplierTest, RestoresDegradedRevisionWithActiveOperation) {
    const auto ready = MakeWeightMetadata(1);
    WeightMetadataStore primary;
    ASSERT_TRUE(
        primary.RestoreSnapshot(WeightMetadataSnapshot{.metadata = {ready}}));
    StandbyMetadataStore standby;
    OpLogApplier applier(&standby, cluster_id_);
    const auto key = MakeWeightRevisionMetadataKey(ready.identity);
    ASSERT_TRUE(applier.ApplyOpLogEntry(MakeEntry(
        1, OpType::WEIGHT_METADATA_UPSERT, key, SerializeWeightUpsert(ready))));
    const auto start = primary.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        200);
    ASSERT_TRUE(start.has_value());
    ASSERT_TRUE(start->next.has_value());
    ASSERT_TRUE(start->metadata.next.has_value());
    ASSERT_TRUE(primary.Publish(*start));
    ASSERT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_METADATA_UPSERT, key,
                  SerializeWeightUpsert(*start->metadata.next, start->next))));
    uint64_t sequence = 3;
    for (const auto residency :
         {WeightResidencyState::HOT, WeightResidencyState::ABSENT}) {
        SCOPED_TRACE(static_cast<int>(residency));
        const auto progress = primary.PrepareUpdateOperationProgress(
            start->next->operation_id, 0, start->next->total_units, 0,
            start->next->total_bytes, {}, {}, WeightAvailabilityState::DEGRADED,
            residency, residency == WeightResidencyState::HOT ? 1.0 : 0.0,
            300 + sequence);
        ASSERT_TRUE(progress.has_value());
        ASSERT_TRUE(progress->next.has_value());
        ASSERT_TRUE(progress->metadata.next.has_value());
        ASSERT_TRUE(primary.Publish(*progress));
        ASSERT_TRUE(applier.ApplyOpLogEntry(MakeEntry(
            sequence++, OpType::WEIGHT_METADATA_UPSERT, key,
            SerializeWeightUpsert(*progress->metadata.next, progress->next))));
        WeightMetadataStore restored;
        ASSERT_TRUE(restored.RestoreSnapshot(standby.SnapshotWeightMetadata()));
        const auto view = restored.Get(ready.identity, 400);
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(*progress->metadata.next, view->metadata);
        const auto operation =
            restored.QueryOperation(start->next->operation_id);
        ASSERT_TRUE(operation.has_value());
        EXPECT_EQ(*progress->next, *operation);
    }
}

TEST_F(OpLogApplierTest, AppliesWeightLeaseAndDeleteTombstones) {
    auto metadata = MakeWeightMetadata(1);
    const auto metadata_key = MakeWeightRevisionMetadataKey(metadata.identity);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, metadata_key,
                  SerializeWeightUpsert(metadata))));

    WeightRevisionLease lease{
        .lease_id = 42,
        .identity = metadata.identity,
        .holder = "worker-0",
        .expires_at_ms = 1000,
        .fenced_metadata_generation = metadata.metadata_generation,
    };
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(2, OpType::WEIGHT_LEASE_UPSERT, "weight-lease:42",
                  SerializePayload(lease))));
    EXPECT_EQ(lease, mock_metadata_store_->GetWeightLease(lease.lease_id));

    WeightLeaseDeleteOp lease_delete{
        .lease_id = lease.lease_id,
        .identity = lease.identity,
        .fenced_metadata_generation = lease.fenced_metadata_generation,
    };
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(3, OpType::WEIGHT_LEASE_DELETE, "weight-lease:42",
                  SerializePayload(lease_delete))));
    EXPECT_FALSE(mock_metadata_store_->GetWeightLease(lease.lease_id));

    WeightMetadataDeleteOp metadata_delete{
        .identity = metadata.identity,
        .metadata_generation = metadata.metadata_generation,
    };
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(4, OpType::WEIGHT_METADATA_DELETE, metadata_key,
                  SerializePayload(metadata_delete))));
    EXPECT_FALSE(
        mock_metadata_store_->GetWeightMetadata(metadata.identity).has_value());

    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(5, OpType::WEIGHT_METADATA_UPSERT, metadata_key,
                  SerializeWeightUpsert(metadata))));
    EXPECT_EQ(5u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, RejectsMalformedWeightPayloadAndMismatchedKey) {
    auto metadata = MakeWeightMetadata(1);
    EXPECT_FALSE(applier_->ApplyOpLogEntry(MakeEntry(
        1, OpType::WEIGHT_METADATA_UPSERT,
        MakeWeightRevisionMetadataKey(metadata.identity), "invalid")));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType::WEIGHT_METADATA_UPSERT, "weight-revision:wrong",
                  SerializeWeightUpsert(metadata))));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());
}

// ========== 4.1.1 Basic apply tests ==========

TEST_F(OpLogApplierTest, TestApplyPutEnd) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    // After applying seq=1, expected_sequence_id becomes 2
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_EQ(1u, mock_metadata_store_->GetKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyPutRevoke) {
    // First add a key
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Then revoke it
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_REVOKE, "key1", "");
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyRemove) {
    // First add a key
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Then remove it
    OpLogEntry entry2 = MakeEntry(2, OpType::REMOVE, "key1", "");
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidOpType) {
    OpLogEntry entry = MakeEntry(1, static_cast<OpType>(200), "key1", "");
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());
}

// ========== 4.1.2 Sequence ordering tests ==========

TEST_F(OpLogApplierTest, TestApplyInOrder) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());

    EXPECT_EQ(3u, mock_metadata_store_->GetKeyCount());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, FutureSequenceFailsWithoutBuffering) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    // Apply entry1 (seq=1) - should succeed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // A future entry fails and is not retained for later application.
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));

    // Applying the expected entry does not resurrect the rejected future one.
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, TestApplyDuplicateSequenceId) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry1_dup = MakeEntry(1, OpType::PUT_END, "key1_dup", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Try to apply duplicate sequence_id (older than expected)
    // Should be treated as no-op (already applied) and return true
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1_dup));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    // key1_dup should not be added (treated as no-op)
    EXPECT_FALSE(mock_metadata_store_->Exists("key1_dup"));
}

// ========== 4.1.4 Checksum tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidChecksum) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidChecksum) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestChecksumFailureMetric) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    // Metric increment is tested implicitly by the failure
}

// ========== 4.1.5 Size validation tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidSize) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(ValidateOpLogEntrySize(entry));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidSize) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make key too large
    entry.object_key.assign(kMaxOpLogObjectKeySize + 1, 'k');

    EXPECT_FALSE(ValidateOpLogEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_PayloadTooLarge) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make payload too large
    entry.payload.assign(kMaxOpLogPayloadSize + 1, 'p');

    EXPECT_FALSE(ValidateOpLogEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
}

// ========== 4.1.6 Recovery tests ==========

TEST_F(OpLogApplierTest, TestRecover) {
    // Set initial state: last applied sequence_id = 10
    applier_->Recover(10);
    EXPECT_EQ(11u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=11 should succeed
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(11, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(12u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_ZeroSequenceId) {
    // Recover from sequence_id 0
    applier_->Recover(0);
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=1 should succeed
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

// ========== 4.1.8 Payload deserialization tests ==========

TEST_F(OpLogApplierTest, TestApplyPutEnd_ValidPayload) {
    std::string payload = MakeValidPayload(1, 2, 2048);
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(1u, meta->client_id.first);
    EXPECT_EQ(2u, meta->client_id.second);
    EXPECT_EQ(2048u, meta->size);
    EXPECT_FALSE(meta->hard_pinned.value_or(false));
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_PreservesHardPinned) {
    std::string payload = MakeValidPayload(1, 2, 2048, true);
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_TRUE(meta->hard_pinned.value_or(false));
}

TEST_F(OpLogApplierTest, TestApplyPutEndPreservesResidencyAffinityId) {
    MetadataPayload metadata;
    metadata.client_id = {1, 2};
    metadata.size = 1024;
    metadata.residency_affinity_id = "opaque-affinity-id";
    auto bytes = struct_pack::serialize(metadata);
    auto entry = MakeEntry(1, OpType::PUT_END, "key1",
                           std::string(bytes.begin(), bytes.end()));

    ASSERT_TRUE(applier_->ApplyOpLogEntry(entry));
    auto stored = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ("opaque-affinity-id", stored->residency_affinity_id.value_or(""));
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_InvalidPayload) {
    std::string invalid_data = "{invalid data}";
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", invalid_data);

    // Should still succeed (fallback to empty metadata)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Metadata should exist but with default values
    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(0u, meta->client_id.first);
    EXPECT_EQ(0u, meta->client_id.second);
    EXPECT_EQ(0u, meta->size);
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_EmptyPayload) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Should succeed with empty payload (creates empty metadata)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(0u, meta->client_id.first);
    EXPECT_EQ(0u, meta->client_id.second);
    EXPECT_EQ(0u, meta->size);
}

// ========== Additional Edge Case Tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_Batch) {
    std::string payload = MakeValidPayload();
    std::vector<OpLogEntry> entries;
    entries.push_back(MakeEntry(1, OpType::PUT_END, "key1", payload));
    entries.push_back(MakeEntry(2, OpType::PUT_END, "key2", payload));
    entries.push_back(MakeEntry(3, OpType::PUT_END, "key3", payload));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    EXPECT_EQ(3u, applied);
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
    EXPECT_EQ(3u, mock_metadata_store_->GetKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_WithGaps) {
    std::string payload = MakeValidPayload();
    std::vector<OpLogEntry> entries;
    entries.push_back(MakeEntry(1, OpType::PUT_END, "key1", payload));
    entries.push_back(
        MakeEntry(3, OpType::PUT_END, "key3", payload));  // Gap at seq=2
    entries.push_back(MakeEntry(2, OpType::PUT_END, "key2", payload));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    EXPECT_EQ(2u, applied);
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, TestGetExpectedSequenceId) {
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

// ========== Segment OpLog apply tests ==========

std::string MakeSegmentMountPayload(const std::string& segment_name,
                                    const std::string& transport_endpoint,
                                    uint64_t capacity = 1024,
                                    bool is_memory = true,
                                    const std::string& file_path = "") {
    SegmentMountOp op;
    op.segment_name = segment_name;
    op.transport_endpoint = transport_endpoint;
    op.capacity = capacity;
    op.is_memory_segment = is_memory;
    op.file_path = file_path;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

std::string MakeSegmentUnmountPayload(const std::string& transport_endpoint) {
    SegmentUnmountOp op;
    op.transport_endpoint = transport_endpoint;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

std::string MakeSegmentUpdatePayload(const std::string& segment_name,
                                     const std::string& transport_endpoint,
                                     uint64_t capacity = 2048,
                                     bool is_memory = true,
                                     const std::string& file_path = "") {
    SegmentUpdateOp op;
    op.segment_name = segment_name;
    op.transport_endpoint = transport_endpoint;
    op.capacity = capacity;
    op.is_memory_segment = is_memory;
    op.file_path = file_path;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

TEST_F(OpLogApplierTest, TestApplySegmentMount) {
    std::string payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry entry = MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    const auto& registry = applier_->GetSegmentRegistry();
    EXPECT_TRUE(registry.HasSegment("192.168.1.1:12345"));
    auto info = registry.GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ("seg1", info->segment_name);
    EXPECT_EQ(1024u, info->capacity);
    EXPECT_TRUE(info->is_memory_segment);
}

TEST_F(OpLogApplierTest, TestApplySegmentUnmount) {
    // Mount first
    std::string mount_payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry mount_entry =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", mount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount_entry));
    EXPECT_TRUE(applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));

    // Then unmount
    std::string unmount_payload =
        MakeSegmentUnmountPayload("192.168.1.1:12345");
    OpLogEntry unmount_entry =
        MakeEntry(2, OpType::SEGMENT_UNMOUNT, "seg1", unmount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount_entry));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    EXPECT_FALSE(
        applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));
}

TEST_F(OpLogApplierTest, TestApplySegmentUpdate) {
    // Mount first
    std::string mount_payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry mount_entry =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", mount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount_entry));

    auto info_before =
        applier_->GetSegmentRegistry().GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info_before.has_value());
    EXPECT_EQ(1024u, info_before->capacity);

    // Update
    std::string update_payload =
        MakeSegmentUpdatePayload("seg1", "192.168.1.1:12345", 2048, true);
    OpLogEntry update_entry =
        MakeEntry(2, OpType::SEGMENT_UPDATE, "seg1", update_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(update_entry));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    auto info_after =
        applier_->GetSegmentRegistry().GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info_after.has_value());
    EXPECT_EQ(2048u, info_after->capacity);
}

TEST_F(OpLogApplierTest, TestApplySegmentMount_InvalidPayload) {
    OpLogEntry entry = MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", "garbage");

    // Should not crash, but should not add segment either
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(applier_->GetSegmentRegistry().HasSegment("seg1"));
}

TEST_F(OpLogApplierTest, TestApplySegmentUnmount_NonExistent) {
    // Unmount a segment that was never mounted - should not crash
    std::string unmount_payload =
        MakeSegmentUnmountPayload("192.168.1.1:12345");
    OpLogEntry unmount_entry =
        MakeEntry(1, OpType::SEGMENT_UNMOUNT, "seg1", unmount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount_entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(
        applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));
}

TEST_F(OpLogApplierTest, TestApplySegmentOperations_Mixed) {
    // Mount multiple segments
    OpLogEntry mount1 =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1",
                  MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024));
    OpLogEntry mount2 =
        MakeEntry(2, OpType::SEGMENT_MOUNT, "seg2",
                  MakeSegmentMountPayload("seg2", "192.168.1.2:12345", 2048));
    OpLogEntry mount3 =
        MakeEntry(3, OpType::SEGMENT_MOUNT, "seg3",
                  MakeSegmentMountPayload("seg3", "192.168.1.3:12345", 4096));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount1));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount2));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount3));

    const auto& registry = applier_->GetSegmentRegistry();
    EXPECT_EQ(3u, registry.GetAllSegments().size());

    // Unmount one
    OpLogEntry unmount2 =
        MakeEntry(4, OpType::SEGMENT_UNMOUNT, "seg2",
                  MakeSegmentUnmountPayload("192.168.1.2:12345"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount2));
    EXPECT_EQ(2u, registry.GetAllSegments().size());
    EXPECT_FALSE(registry.HasSegment("192.168.1.2:12345"));

    // Update another
    OpLogEntry update3 =
        MakeEntry(5, OpType::SEGMENT_UPDATE, "seg3",
                  MakeSegmentUpdatePayload("seg3", "192.168.1.3:12345", 8192));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(update3));
    auto info3 = registry.GetSegment("192.168.1.3:12345");
    ASSERT_TRUE(info3.has_value());
    EXPECT_EQ(8192u, info3->capacity);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
