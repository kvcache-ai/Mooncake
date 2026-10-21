// The batch reads answer each key of one tenant as the point reads do, in
// request order, and the listings walk the tenant cursors.

#include "master_service_test_fixture.h"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "tenant/quota_policy_store.h"

namespace mooncake::test {
namespace {

const TenantId kTenantA("tenant-a");
const TenantId kTenantB("tenant-b");
const TenantId kTenantC("tenant-c");

constexpr int kKeysPerTenant = 300;

ReplicateConfig OneMemoryReplica() {
    ReplicateConfig config;
    config.replica_num = 1;
    return config;
}

class BatchReadTest : public MasterServiceTest {
   protected:
    void TearDown() override {
        std::filesystem::remove(policy_path_);
        MasterServiceTest::TearDown();
    }

    // A multi-tenant service on which tenants A, B and C may write.
    MasterServiceConfig MultiTenantConfig() {
        TenantQuotaPolicySnapshot snapshot;
        for (const TenantId& tenant_id : {kTenantA, kTenantB, kTenantC}) {
            snapshot.tenant_quotas.emplace(tenant_id.value(), 1ULL << 30);
        }
        policy_path_ = std::filesystem::temp_directory_path() /
                       ("mooncake_batch_read_test_" +
                        std::to_string(::getpid()) + ".yaml");
        std::ofstream(policy_path_) << FormatTenantQuotaPolicyYaml(snapshot);
        return MasterServiceConfig::builder()
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(policy_path_.string())
            .build();
    }

    // Tenant A holds a0..a299 and a key still being written; tenant B holds
    // b0..b299.
    void Populate(MasterService& service, const UUID& client_id) {
        for (int i = 0; i < kKeysPerTenant; ++i) {
            PutCompletedObject(service, client_id, "a" + std::to_string(i),
                               kTenantA, OneMemoryReplica());
            PutCompletedObject(service, client_id, "b" + std::to_string(i),
                               kTenantB, OneMemoryReplica());
        }
        ASSERT_TRUE(service
                        .PutStart(client_id, "a-pending", kTenantA, 1024,
                                  OneMemoryReplica())
                        .has_value());
    }

    // Tenant A's keys, with a repeated key, a key being written, a missing
    // key and one of tenant B's keys mixed in.
    static std::vector<std::string> MixedBatch() {
        std::vector<std::string> keys;
        for (int i = kKeysPerTenant - 1; i >= 0; i -= 2) {
            keys.push_back("a" + std::to_string(i));
        }
        keys.push_back("a7");
        keys.push_back("a-pending");
        keys.push_back("absent");
        keys.push_back("b3");
        keys.push_back("a7");
        return keys;
    }

    // Tenant A holds an object under the key, complete or not.
    static bool HeldByTenantA(const std::string& key) {
        return key != "absent" && key != "b3";
    }
    static bool ReadableInTenantA(const std::string& key) {
        return HeldByTenantA(key) && key != "a-pending";
    }

    std::filesystem::path policy_path_;
};

}  // namespace

TEST_F(BatchReadTest, BatchReadsAnswerAsPointReads) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    Populate(service, context.client_id);
    const std::vector<std::string> keys = MixedBatch();

    const auto exists = service.BatchExistKey(keys, kTenantA);
    const auto probes =
        service.BatchProbeKey(GrantLeasePolicy{}, keys, kTenantA);
    const auto gets = service.BatchGetReplicaList(keys, kTenantA);
    const auto admin_gets = service.BatchGetReplicaListForAdmin(keys, kTenantA);
    ASSERT_EQ(exists.size(), keys.size());
    ASSERT_EQ(probes.size(), keys.size());
    ASSERT_EQ(gets.size(), keys.size());
    ASSERT_EQ(admin_gets.size(), keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        EXPECT_EQ(exists[i], service.ExistKey(keys[i], kTenantA));
        EXPECT_EQ(probes[i], service.ProbeKey(keys[i], kTenantA));
        ASSERT_TRUE(exists[i].has_value());
        EXPECT_EQ(*exists[i], ReadableInTenantA(keys[i]));

        const auto get = service.GetReplicaList(keys[i], kTenantA);
        ASSERT_EQ(gets[i].has_value(), get.has_value());
        if (get.has_value()) {
            EXPECT_EQ(gets[i]->replicas.size(), get->replicas.size());
            EXPECT_EQ(gets[i]->object_checksum, get->object_checksum);
        } else {
            EXPECT_EQ(gets[i].error(), get.error());
        }
        const auto admin_get =
            service.GetReplicaListForAdmin(keys[i], kTenantA);
        ASSERT_EQ(admin_gets[i].has_value(), admin_get.has_value());
        if (!admin_get.has_value()) {
            EXPECT_EQ(admin_gets[i].error(), admin_get.error());
        }
    }
    EXPECT_EQ(gets[keys.size() - 4].error(), ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(gets[keys.size() - 3].error(), ErrorCode::OBJECT_NOT_FOUND);
    // Tenant B's key is not tenant A's.
    EXPECT_EQ(gets[keys.size() - 2].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(BatchReadTest, BatchReadsFindNothingOfATenantWithoutObjects) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    Populate(service, context.client_id);
    const std::vector<std::string> keys = MixedBatch();

    const auto exists = service.BatchExistKey(keys, kTenantC);
    const auto gets = service.BatchGetReplicaList(keys, kTenantC);
    ASSERT_EQ(exists.size(), keys.size());
    ASSERT_EQ(gets.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        EXPECT_EQ(exists[i], false);
        EXPECT_EQ(gets[i].error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

TEST_F(BatchReadTest, OnlyClientBatchReadsGrantLeases) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    Populate(service, context.client_id);

    // Neither a probe nor an admin read leases, so a remove takes the object.
    (void)service.BatchProbeKey(GrantLeasePolicy{}, {"a1"}, kTenantA);
    (void)service.BatchGetReplicaListForAdmin({"a2"}, kTenantA);
    EXPECT_TRUE(service.Remove("a1", kTenantA).has_value());
    EXPECT_TRUE(service.Remove("a2", kTenantA).has_value());

    (void)service.BatchExistKey({"a3"}, kTenantA);
    (void)service.BatchGetReplicaList({"a4"}, kTenantA);
    EXPECT_EQ(service.Remove("a3", kTenantA).error(),
              ErrorCode::OBJECT_HAS_LEASE);
    EXPECT_EQ(service.Remove("a4", kTenantA).error(),
              ErrorCode::OBJECT_HAS_LEASE);
}

TEST_F(BatchReadTest, ListsAndCountsThroughTheTenantCursors) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    Populate(service, context.client_id);

    // The key being written is counted but has nothing to list.
    EXPECT_EQ(service.GetKeyCount(), 2u * kKeysPerTenant + 1);
    const auto keys = service.GetAllKeys(kTenantB);
    ASSERT_TRUE(keys.has_value());
    EXPECT_EQ(keys->size(), static_cast<size_t>(kKeysPerTenant));

    const auto matched = service.GetReplicaListByRegex("^a1[0-9]$", kTenantA);
    ASSERT_TRUE(matched.has_value());
    EXPECT_EQ(matched->size(), 10u);
    EXPECT_TRUE(service.GetReplicaListByRegex("^a1", kTenantC)->empty());

    ASSERT_TRUE(service.Remove("b0", kTenantB).has_value());
    EXPECT_EQ(service.GetKeyCount(), 2u * kKeysPerTenant);
}

}  // namespace mooncake::test
