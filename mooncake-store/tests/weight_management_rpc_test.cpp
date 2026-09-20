#include <gtest/gtest.h>

#include <string>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "master_client.h"
#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

WeightRevisionIdentity Identity() {
    return WeightRevisionIdentity{
        .tenant_id = "forged-tenant",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
}

template <typename T>
const tl::expected<T, WeightManagementError>& Domain(
    const WeightRpcResult<T>& result) {
    EXPECT_TRUE(result.has_value());
    return *result;
}

TEST(WeightManagementRpcTest, RegistersEveryApiAndBindsTenant) {
    InProcMaster server;
    ASSERT_TRUE(server.Start(InProcMasterConfigBuilder().build()));
    MasterClient client(generate_uuid(), nullptr, "default");
    ASSERT_EQ(ErrorCode::OK, client.Connect(server.master_address()));

    const auto begin_request = BeginWeightImportRequest{
        .identity = Identity(),
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    };
    auto begin = client.BeginWeightImport(begin_request);
    ASSERT_TRUE(Domain(begin).has_value());
    EXPECT_EQ("default", Domain(begin)->identity.tenant_id);

    auto retry = client.BeginWeightImport(begin_request);
    ASSERT_TRUE(Domain(retry).has_value());
    EXPECT_EQ(*Domain(begin), *Domain(retry));

    auto get = client.GetWeightRevision(
        GetWeightRevisionRequest{.identity = Identity()});
    ASSERT_TRUE(Domain(get).has_value());
    EXPECT_EQ(Domain(begin)->metadata_generation,
              Domain(get)->metadata.metadata_generation);

    auto list = client.ListWeightRevisions(ListWeightRevisionsRequest{
        .tenant_id = "forged-tenant",
        .name_space = "production",
        .resource_id = "llama-70b",
        .limit = 1,
    });
    ASSERT_TRUE(Domain(list).has_value());
    ASSERT_EQ(1, Domain(list)->revisions.size());

    auto commit = client.CommitWeightImport(CommitWeightImportRequest{
        .identity = Identity(),
        .expected_metadata_generation = Domain(begin)->metadata_generation,
        .manifest =
            WeightManifestReference{
                .manifest_key =
                    "weights/production/llama-70b/step-100/7/manifest",
                .manifest_sha256 = std::string(64, 'a'),
                .payload_group_id = Domain(begin)->manifest.payload_group_id,
                .payload_keys_sha256 = std::string(64, 'b'),
                .payload_count = 1,
                .logical_bytes = 1024,
            },
    });
    ASSERT_FALSE(Domain(commit).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(commit).error());

    auto acquire = client.AcquireWeightRevisionLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = Identity(),
            .expected_metadata_generation =
                Domain(begin)->metadata_generation,
            .holder = "worker-0",
            .ttl_ms = 1000,
        });
    ASSERT_FALSE(Domain(acquire).has_value());
    EXPECT_EQ(WeightManagementError::NOT_READY, Domain(acquire).error());

    auto renew = client.RenewWeightRevisionLease(
        RenewWeightRevisionLeaseRequest{.lease_id = 999, .ttl_ms = 1000});
    ASSERT_FALSE(Domain(renew).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(renew).error());

    auto release = client.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = 999});
    EXPECT_TRUE(Domain(release).has_value());

    auto start = client.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = Identity(),
            .expected_metadata_generation =
                Domain(begin)->metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_FALSE(Domain(start).has_value());
    EXPECT_EQ(WeightManagementError::NOT_READY, Domain(start).error());

    auto query = client.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = 999});
    ASSERT_FALSE(Domain(query).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(query).error());

    auto abort = client.AbortWeightImport(AbortWeightImportRequest{
        .identity = Identity(),
        .expected_metadata_generation = Domain(begin)->metadata_generation,
    });
    ASSERT_TRUE(Domain(abort).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETING,
              Domain(abort)->availability);

    auto reconcile = client.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = Identity()});
    ASSERT_TRUE(Domain(reconcile).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED,
              Domain(reconcile)->availability);

    auto deleted = client.DeleteWeightRevision(DeleteWeightRevisionRequest{
        .identity = Identity(),
        .expected_metadata_generation =
            Domain(reconcile)->metadata_generation,
    });
    ASSERT_TRUE(Domain(deleted).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED,
              Domain(deleted)->availability);
}

TEST(WeightManagementRpcTest, RejectsUnboundedPaginationExactly) {
    InProcMaster server;
    ASSERT_TRUE(server.Start(InProcMasterConfigBuilder().build()));
    MasterClient client(generate_uuid());
    ASSERT_EQ(ErrorCode::OK, client.Connect(server.master_address()));

    auto list = client.ListWeightRevisions(ListWeightRevisionsRequest{
        .name_space = "production",
        .resource_id = "llama-70b",
        .limit = 1001,
    });
    ASSERT_FALSE(Domain(list).has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, Domain(list).error());
}

class WeightManagementTcpTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    static constexpr size_t kPayloadSize = 1024 * 1024;

    void SetUp() override {
        ASSERT_TRUE(server_.Start(InProcMasterConfigBuilder()
                                      .set_default_kv_lease_ttl(1000)
                                      .build()));
        const auto ports = getFreeTcpPorts(2);
        ASSERT_EQ(2u, ports.size());
        auto storage = Client::Create("127.0.0.1:" + std::to_string(ports[0]),
                                      server_.metadata_url(), "tcp",
                                      std::nullopt, server_.master_address());
        ASSERT_TRUE(storage.has_value());
        storage_ = *storage;
        // The backing allocator requires 16 MiB, but only the mounted 4 MiB
        // contributes to the master's allocation-pressure capacity.
        segment_ = allocate_buffer_allocator_memory(16 * 1024 * 1024);
        ASSERT_NE(nullptr, segment_);
        ASSERT_TRUE(storage_->MountSegment(segment_, kSegmentSize, "tcp"));
        mounted_ = true;

        // Only the storage client owns a segment, so this client's transfers
        // cross distinct TCP endpoints instead of using its local segment.
        auto reader = Client::Create("127.0.0.1:" + std::to_string(ports[1]),
                                     server_.metadata_url(), "tcp",
                                     std::nullopt, server_.master_address());
        ASSERT_TRUE(reader.has_value());
        reader_ = *reader;
        io_.resize(kPayloadSize);
        ASSERT_TRUE(reader_->RegisterLocalMemory(io_.data(), io_.size(),
                                                 "cpu:0", false, false));
        registered_ = true;
    }

    void TearDown() override {
        if (registered_) {
            EXPECT_TRUE(reader_->unregisterLocalMemory(io_.data(), false));
        }
        reader_.reset();
        if (mounted_) {
            EXPECT_TRUE(storage_->UnmountSegment(segment_, kSegmentSize));
        }
        storage_.reset();
        if (segment_) free_memory("", segment_);
        server_.Stop();
    }

    void PutBytes(const std::string& key, const std::string& bytes,
                  const ReplicateConfig& config) {
        ASSERT_LE(bytes.size(), io_.size());
        std::memcpy(io_.data(), bytes.data(), bytes.size());
        std::vector<Slice> slices{{io_.data(), bytes.size()}};
        ASSERT_TRUE(reader_->Put(key, slices, config));
    }

    void ExpectBytes(const std::string& key, const std::string& bytes) {
        std::fill(io_.begin(), io_.end(), '\0');
        std::vector<Slice> slices{{io_.data(), bytes.size()}};
        ASSERT_TRUE(reader_->Get(key, slices));
        EXPECT_EQ(bytes, std::string(io_.data(), bytes.size()));
    }

    InProcMaster server_;
    std::shared_ptr<Client> storage_;
    std::shared_ptr<Client> reader_;
    void* segment_ = nullptr;
    std::vector<char> io_;
    bool mounted_ = false;
    bool registered_ = false;
};

TEST_F(WeightManagementTcpTest, LeaseProtectsPublishedBytesUntilManagedDelete) {
    const std::string payload_key = "managed-rpc-payload";
    const std::string manifest_key =
        "weights/production/llama-70b/step-100/7/manifest";
    const std::string payload(kPayloadSize, 'w');
    const std::string manifest = "{}";
    auto begin = reader_->BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity(),
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = kPayloadSize,
    });
    ASSERT_TRUE(begin.has_value());
    ASSERT_TRUE(begin->has_value());
    EXPECT_EQ("default", (*begin)->identity.tenant_id);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.group_ids =
        std::vector<std::string>{(*begin)->manifest.payload_group_id};
    // No hard pin: protection must come from managed group membership.
    ASSERT_FALSE(config.with_hard_pin);
    ASSERT_NO_FATAL_FAILURE(PutBytes(payload_key, payload, config));
    config.data_type = ObjectDataType::METADATA;
    ASSERT_NO_FATAL_FAILURE(PutBytes(manifest_key, manifest, config));
    auto ready = reader_->CommitWeightImport(CommitWeightImportRequest{
        .identity = Identity(),
        .expected_metadata_generation = (*begin)->metadata_generation,
        .manifest =
            WeightManifestReference{
                .manifest_key = manifest_key,
                .manifest_sha256 = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe"
                                   "77e8310c060f61caaff8a",
                .payload_group_id = (*begin)->manifest.payload_group_id,
                .payload_keys_sha256 =
                    ComputeWeightPayloadKeysSha256({payload_key}),
                .payload_count = 1,
                .logical_bytes = kPayloadSize,
            },
    });
    ASSERT_TRUE(ready.has_value());
    ASSERT_TRUE(ready->has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, (*ready)->availability);
    EXPECT_EQ(WeightResidencyState::HOT, (*ready)->residency);
    auto view = reader_->GetWeightRevision({.identity = Identity()});
    ASSERT_TRUE(view.has_value());
    ASSERT_TRUE(view->has_value());
    EXPECT_EQ(**ready, (*view)->metadata);

    auto lease = reader_->AcquireWeightRevisionLease({
        .identity = Identity(),
        .expected_metadata_generation = (*ready)->metadata_generation,
        .holder = "tcp-reader",
        .ttl_ms = 60'000,
    });
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(lease->has_value());
    auto remove = reader_->Remove(payload_key, true);
    ASSERT_FALSE(remove.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, remove.error());
    const auto batch = reader_->BatchRemove({payload_key, manifest_key}, true);
    ASSERT_EQ(2u, batch.size());
    for (const auto& result : batch) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, result.error());
    }

    // Trigger the production allocation-pressure path through RPC. The
    // requested allocation would fit only if the managed payload were evicted.
    // Let any ordinary KV lease from publication expire before applying
    // pressure.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    MasterClient pressure(generate_uuid());
    ASSERT_EQ(ErrorCode::OK, pressure.Connect(server_.master_address()));
    ReplicateConfig pressure_config;
    pressure_config.replica_num = 1;
    auto allocation = pressure.PutStart(
        "pressure", {kSegmentSize - kPayloadSize}, pressure_config);
    ASSERT_FALSE(allocation.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, allocation.error());
    // Allocation failure wakes the asynchronous eviction worker.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    auto blocked = reader_->DeleteWeightRevision({
        .identity = Identity(),
        .expected_metadata_generation = (*ready)->metadata_generation,
    });
    ASSERT_TRUE(blocked.has_value());
    ASSERT_FALSE(blocked->has_value());
    EXPECT_EQ(WeightManagementError::BUSY, blocked->error());
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(payload_key, payload));
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(manifest_key, manifest));

    auto release =
        reader_->ReleaseWeightRevisionLease({.lease_id = (*lease)->lease_id});
    ASSERT_TRUE(release.has_value());
    ASSERT_TRUE(release->has_value());
    auto deleted = reader_->DeleteWeightRevision({
        .identity = Identity(),
        .expected_metadata_generation = (*ready)->metadata_generation,
    });
    ASSERT_TRUE(deleted.has_value());
    ASSERT_TRUE(deleted->has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED, (*deleted)->availability);
    EXPECT_EQ(WeightResidencyState::ABSENT, (*deleted)->residency);
    for (const auto& key : {payload_key, manifest_key}) {
        std::vector<Slice> slices{{io_.data(), io_.size()}};
        auto get = reader_->Get(key, slices);
        ASSERT_FALSE(get.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get.error());
    }
}

}  // namespace
}  // namespace mooncake::testing
