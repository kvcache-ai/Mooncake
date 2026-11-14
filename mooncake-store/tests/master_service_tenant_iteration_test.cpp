// The tenant iteration primitives: VisitNamespaces names the namespaces that
// hold objects, a cursor of a namespace's route walks one tenant's objects,
// and the removes are built on both.

#include "master_service_test_fixture.h"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <thread>
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

    // The tenant's namespace; null when it was never written to.
    static const metadata::Namespace* NamespaceOf(MasterService& service,
                                                  const TenantId& tenant_id) {
        return MasterServiceTestPeer::Namespaces(service).Lookup(tenant_id);
    }

    std::set<std::string> KeysOf(MasterService& service,
                                 const TenantId& tenant_id) {
        std::set<std::string> keys;
        const metadata::Namespace* ns = NamespaceOf(service, tenant_id);
        if (ns == nullptr) {
            return keys;
        }
        for (auto object : ns->objects.ReadCursor()) {
            EXPECT_TRUE(keys.insert(object.key()).second)
                << "visited twice: " << object.key();
        }
        return keys;
    }

    std::vector<std::string> InFlightKeysOf(MasterService& service,
                                            const TenantId& tenant_id) {
        const metadata::Namespace* ns = NamespaceOf(service, tenant_id);
        return ns == nullptr ? std::vector<std::string>{} : ns->InFlightKeys();
    }

    std::filesystem::path policy_path_;
};

}  // namespace

TEST_F(TenantIterationTest, VisitNamespacesNamesTenantsHoldingObjects) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 3);
    PutKeys(service, context.client_id, kTenantB, "b", 3);
    // Tenant C held an object once and holds none now.
    PutKeys(service, context.client_id, kTenantC, "c", 1);
    ASSERT_TRUE(service.Remove("c0", kTenantC).has_value());

    std::vector<TenantId> visited;
    MasterServiceTestPeer(service).VisitNamespaces(
        [&](const TenantId& tenant_id, metadata::Namespace&) {
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
    for (auto object : NamespaceOf(service, kTenantA)->objects.ReadCursor()) {
        EXPECT_EQ(object.ref().key, object.key());
        EXPECT_EQ(object.ref().generation, object.generation());
        EXPECT_EQ(object.metadata().user_key, object.key());
    }
}

TEST_F(TenantIterationTest, LeavingACursorEarlyReleasesItsStripe) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 16);

    int seen = 0;
    for (auto object : NamespaceOf(service, kTenantA)->objects.ReadCursor()) {
        (void)object;
        if (++seen == 3) {
            break;
        }
    }
    EXPECT_EQ(seen, 3);
    // Every stripe is free again, so writes to every key go through.
    for (int i = 0; i < 16; ++i) {
        EXPECT_TRUE(service.Remove("a" + std::to_string(i), kTenantA, true)
                        .has_value());
    }
}

TEST_F(TenantIterationTest, RemoveByRegexStaysInItsTenant) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "match_", 4);
    PutKeys(service, context.client_id, kTenantA, "keep_", 2);
    PutKeys(service, context.client_id, kTenantB, "match_", 3);

    auto removed = service.RemoveByRegex("^match_", kTenantA, /*force=*/true);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed.value(), 4);
    EXPECT_EQ(KeysOf(service, kTenantA),
              (std::set<std::string>{"keep_0", "keep_1"}));
    EXPECT_EQ(KeysOf(service, kTenantB).size(), 3u);
}

TEST_F(TenantIterationTest, RemoveAllWalksEveryTenant) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 5);
    PutKeys(service, context.client_id, kTenantB, "b", 7);

    EXPECT_EQ(service.RemoveAll(/*force=*/true), 12);
    std::vector<TenantId> left;
    MasterServiceTestPeer(service).VisitNamespaces(
        [&](const TenantId& tenant_id, metadata::Namespace&) {
            left.push_back(tenant_id);
        });
    EXPECT_TRUE(left.empty());
}

TEST_F(TenantIterationTest, RemoveAllLeavesLeasedObjectsUnlessForced) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 4);
    // A read grants every object a lease.
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(service.GetReplicaList("a" + std::to_string(i), kTenantA)
                        .has_value());
    }

    EXPECT_EQ(service.RemoveAll(kTenantA), 0);
    EXPECT_EQ(KeysOf(service, kTenantA).size(), 4u);
    EXPECT_EQ(service.RemoveAll(kTenantA, /*force=*/true), 4);
}

TEST_F(TenantIterationTest, InFlightKeysNameOnlyKeysWithWorkInFlight) {
    MasterService service(MultiTenantConfig());
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "done_", 3);
    for (const auto& [tenant_id, key] :
         {std::pair{kTenantA, "pending_a"}, std::pair{kTenantB, "pending_b"}}) {
        ASSERT_TRUE(service
                        .PutStart(context.client_id, key, tenant_id, 1024,
                                  OneMemoryReplica())
                        .has_value());
    }

    MasterServiceTestPeer peer(service);
    EXPECT_EQ(InFlightKeysOf(service, kTenantA),
              std::vector<std::string>{"pending_a"});
    EXPECT_EQ(InFlightKeysOf(service, kTenantB),
              std::vector<std::string>{"pending_b"});
    EXPECT_TRUE(InFlightKeysOf(service, kTenantC).empty());

    // The expiry sweep of one tenant leaves the others' writes alone.
    peer.DiscardExpiredProcessingReplicas(
        kTenantA, std::chrono::system_clock::time_point::max());
    EXPECT_TRUE(InFlightKeysOf(service, kTenantA).empty());
    EXPECT_EQ(KeysOf(service, kTenantA),
              (std::set<std::string>{"done_0", "done_1", "done_2"}));
    EXPECT_EQ(InFlightKeysOf(service, kTenantB),
              std::vector<std::string>{"pending_b"});
}

TEST_F(TenantIterationTest, MemoryEvictionCoversEveryTenant) {
    auto config = MultiTenantConfig();
    config.default_kv_lease_ttl = 1;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    PutKeys(service, context.client_id, kTenantA, "a", 4);
    PutKeys(service, context.client_id, kTenantB, "b", 6);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    EXPECT_TRUE(KeysOf(service, kTenantA).empty());
    EXPECT_TRUE(KeysOf(service, kTenantB).empty());
}

}  // namespace mooncake::test
