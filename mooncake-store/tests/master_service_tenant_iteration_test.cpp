// The tenant iteration primitives: VisitTenants names the tenants that hold
// objects, and a tenant cursor walks one tenant's objects.

#include "master_service_test_fixture.h"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "tenant/quota_policy_store.h"

namespace mooncake::test {
namespace {

const TenantId kTenantA("tenant-a");
const TenantId kTenantB("tenant-b");
const TenantId kTenantC("tenant-c");

ReplicateConfig OneMemoryReplica() {
    ReplicateConfig config;
    config.replica_num = 1;
    return config;
}

class TenantIterationTest : public MasterServiceTest {
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
                       ("mooncake_tenant_iteration_test_" +
                        std::to_string(::getpid()) + ".yaml");
        std::ofstream(policy_path_) << FormatTenantQuotaPolicyYaml(snapshot);
        return MasterServiceConfig::builder()
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(policy_path_.string())
            .build();
    }

    void PutKeys(MasterService& service, const UUID& client_id,
                 const TenantId& tenant_id, const std::string& prefix,
                 int count) {
        for (int i = 0; i < count; ++i) {
            PutCompletedObject(service, client_id, prefix + std::to_string(i),
                               tenant_id, OneMemoryReplica());
        }
    }

    std::set<std::string> KeysOf(MasterService& service,
                                 const TenantId& tenant_id) {
        std::set<std::string> keys;
        for (auto object :
             MasterServiceTestPeer(service).ReadCursor(tenant_id)) {
            EXPECT_TRUE(keys.insert(object.key()).second)
                << "visited twice: " << object.key();
        }
        return keys;
    }

    std::filesystem::path policy_path_;
};

}  // namespace

TEST_F(TenantIterationTest, VisitTenantsNamesTenantsHoldingObjects) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 3);
    PutKeys(service, context.client_id, kTenantB, "b", 3);
    // Tenant C held an object once and holds none now.
    PutKeys(service, context.client_id, kTenantC, "c", 1);
    ASSERT_TRUE(service.Remove("c0", kTenantC).has_value());

    std::vector<TenantId> visited;
    MasterServiceTestPeer(service).VisitTenants([&](const TenantId& tenant_id) {
        visited.push_back(tenant_id);
        // No lock is held: a write to the tenant's own keys goes through.
        EXPECT_EQ(service.RemoveAll(tenant_id, /*force=*/true), 3);
    });

    EXPECT_EQ(std::set<TenantId>(visited.begin(), visited.end()),
              (std::set<TenantId>{kTenantA, kTenantB}));
    EXPECT_EQ(visited.size(), 2u);
}

TEST_F(TenantIterationTest, ReadCursorVisitsEachObjectOfItsTenantOnce) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 64);
    PutKeys(service, context.client_id, kTenantB, "b", 8);

    std::set<std::string> expected;
    for (int i = 0; i < 64; ++i) {
        expected.insert("a" + std::to_string(i));
    }
    EXPECT_EQ(KeysOf(service, kTenantA), expected);
    EXPECT_EQ(KeysOf(service, kTenantB).size(), 8u);
    EXPECT_TRUE(KeysOf(service, kTenantC).empty());

    // A ref taken by the walk names the publication it saw.
    for (auto object : MasterServiceTestPeer(service).ReadCursor(kTenantA)) {
        EXPECT_EQ(object.ref().key, object.key());
        EXPECT_EQ(object.ref().generation, object.generation());
        EXPECT_EQ(object.metadata().user_key, object.key());
    }
}

TEST_F(TenantIterationTest, LeavingACursorEarlyReleasesItsShard) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 16);

    int seen = 0;
    for (auto object : MasterServiceTestPeer(service).ReadCursor(kTenantA)) {
        (void)object;
        if (++seen == 3) {
            break;
        }
    }
    EXPECT_EQ(seen, 3);
    // Every shard is free again, so writes to every key go through.
    for (int i = 0; i < 16; ++i) {
        EXPECT_TRUE(service.Remove("a" + std::to_string(i), kTenantA, true)
                        .has_value());
    }
}

TEST_F(TenantIterationTest, WriteCursorChangesStateInPlace) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 8);

    for (auto object : MasterServiceTestPeer(service).WriteCursor(kTenantA)) {
        object.state().is_processing = true;
    }
    int processing = 0;
    for (auto object : MasterServiceTestPeer(service).ReadCursor(kTenantA)) {
        processing += object.state().is_processing ? 1 : 0;
    }
    EXPECT_EQ(processing, 8);

    for (auto object : MasterServiceTestPeer(service).WriteCursor(kTenantA)) {
        object.state().is_processing = false;
    }
    EXPECT_EQ(service.RemoveAll(kTenantA, /*force=*/true), 8);
}

}  // namespace mooncake::test
