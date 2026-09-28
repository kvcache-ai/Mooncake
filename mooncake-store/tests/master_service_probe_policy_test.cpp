#include "master_service_test_fixture.h"
#include "rpc_service.h"

#include <barrier>
#include <limits>
#include <thread>

namespace mooncake::test {

class BatchProbePolicyTest : public MasterServiceTest {
   protected:
    static constexpr GrantLeasePolicy kLastPair{ProbeLeaseMode::LastHitOnly, 2};

    std::chrono::system_clock::time_point Deadline(MasterService& service,
                                                   const std::string& key) {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                key, TenantId::Default()));
        EXPECT_TRUE(accessor.Exists());
        return accessor.Exists() ? accessor.Get().EvictionDeadline()
                                 : std::chrono::system_clock::time_point{};
    }

    void ExpectMask(const std::vector<tl::expected<bool, ErrorCode>>& result,
                    const std::vector<bool>& expected) {
        ASSERT_EQ(result.size(), expected.size());
        for (size_t i = 0; i < result.size(); ++i) {
            ASSERT_TRUE(result[i].has_value()) << i;
            EXPECT_EQ(result[i].value(), expected[i]) << i;
        }
    }
};

TEST_F(BatchProbePolicyTest, NonePreservesReadAndSoftPinDeadlines) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(60000).build());
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(service, context.client_id, "key", config);
    const auto before = Deadline(service, "key");
    const auto soft_pin = GetSoftPinDeadline(service, "key");
    ASSERT_TRUE(soft_pin.has_value());

    // candidate_size is deliberately ignored in None mode.
    ExpectMask(
        service.BatchProbeKey({ProbeLeaseMode::None, 0},
                              {"key", "missing", "key"}, TenantId::Default()),
        {true, false, true});
    EXPECT_EQ(Deadline(service, "key"), before);
    EXPECT_EQ(GetSoftPinDeadline(service, "key"), soft_pin);

    ExpectMask(service.BatchExistKey({"key"}, TenantId::Default()), {true});
    EXPECT_GT(Deadline(service, "key"), before);
    const auto leased = Deadline(service, "key");
    ExpectMask(service.BatchProbeKey({}, {"key"}, TenantId::Default()), {true});
    EXPECT_EQ(Deadline(service, "key"), leased);
}

TEST_F(BatchProbePolicyTest, SelectsLastCompleteSparseCandidate) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(60000).build());
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    const std::vector<std::string> keys = {"p0-state", "p0-conv",  "p1-state",
                                           "p1-conv",  "p2-state", "p2-conv",
                                           "p3-state", "p3-conv"};
    for (size_t i : {0u, 1u, 4u, 5u, 6u}) {
        PutCompletedObject(service, context.client_id, keys[i], config);
    }
    const auto old = Deadline(service, keys[0]);
    const auto partial = Deadline(service, keys[6]);
    const auto chosen = Deadline(service, keys[4]);
    const auto soft_pin = GetSoftPinDeadline(service, keys[4]);
    ExpectMask(service.BatchProbeKey(kLastPair, keys, TenantId::Default()),
               {false, false, false, false, true, true, false, false});
    EXPECT_EQ(Deadline(service, keys[0]), old);
    EXPECT_EQ(Deadline(service, keys[6]), partial);
    EXPECT_GT(Deadline(service, keys[4]), chosen);
    EXPECT_EQ(GetSoftPinDeadline(service, keys[4]), soft_pin);
    for (size_t i : {4u, 5u}) {
        auto removed = service.Remove(keys[i], TenantId::Default());
        ASSERT_FALSE(removed.has_value());
        EXPECT_EQ(removed.error(), ErrorCode::OBJECT_HAS_LEASE);
    }
}

TEST_F(BatchProbePolicyTest, OnlySelectedCandidateSurvivesNormalEviction) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(60000).build());
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;
    const std::vector<std::string> keys = {"z", "y", "b", "a"};
    for (const auto& key : keys) {
        PutCompletedObject(service, context.client_id, key, config);
    }
    ExpectMask(service.BatchProbeKey(kLastPair, keys, TenantId::Default()),
               {false, false, true, true});
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    ExpectMask(service.BatchProbeKey({}, keys, TenantId::Default()),
               {false, false, true, true});
}

TEST_F(BatchProbePolicyTest, IncompleteAndUnreadableCandidatesAreNotLeased) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;
    PutCompletedObject(service, context.client_id, "complete", config);
    ASSERT_TRUE(service
                    .PutStart(context.client_id, "writing", TenantId::Default(),
                              1024, config)
                    .has_value());
    const auto before = Deadline(service, "complete");
    const std::vector<std::string> keys = {"complete", "writing"};
    ExpectMask(service.BatchProbeKey(kLastPair, keys, TenantId::Default()),
               {false, false});
    EXPECT_EQ(Deadline(service, "complete"), before);
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "writing", TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(service.UnmountSegment(context.segment_id, context.client_id)
                    .has_value());
    ExpectMask(service.BatchProbeKey(kLastPair, keys, TenantId::Default()),
               {false, false});
}

TEST_F(BatchProbePolicyTest, ValidationDoesNotLeaseAndEmptyInputIsEmpty) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    PutCompletedObject(service, context.client_id, "key", {.replica_num = 1});
    const auto before = Deadline(service, "key");
    for (const auto& policy :
         std::vector<GrantLeasePolicy>{{ProbeLeaseMode::LastHitOnly, 0},
                                       {ProbeLeaseMode::LastHitOnly, 3},
                                       {ProbeLeaseMode::LastHitOnly,
                                        std::numeric_limits<uint64_t>::max()},
                                       {static_cast<ProbeLeaseMode>(99), 1}}) {
        auto result =
            service.BatchProbeKey(policy, {"key", "key"}, TenantId::Default());
        ASSERT_EQ(result.size(), 2u);
        for (const auto& item : result) {
            ASSERT_FALSE(item.has_value());
            EXPECT_EQ(item.error(), ErrorCode::INVALID_PARAMS);
        }
        EXPECT_EQ(Deadline(service, "key"), before);
    }
    EXPECT_TRUE(
        service.BatchProbeKey(kLastPair, {}, TenantId::Default()).empty());
    EXPECT_TRUE(service.BatchProbeKey({}, {}, TenantId::Default()).empty());
}

TEST_F(BatchProbePolicyTest, DuplicateKeysAndSameShardAreLockedOnce) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const std::string first = "same-shard-first";
    std::string second;
    for (int i = 0; i < 100000; ++i) {
        const std::string candidate = "same-shard-" + std::to_string(i);
        if (MetadataShardIndex(service, candidate) ==
            MetadataShardIndex(service, first)) {
            second = candidate;
            break;
        }
    }
    ASSERT_FALSE(second.empty());
    for (const auto& key : {first, second}) {
        PutCompletedObject(service, context.client_id, key, {.replica_num = 1});
    }
    ExpectMask(service.BatchProbeKey(kLastPair, {first, first, first, second},
                                     TenantId::Default()),
               {false, false, true, true});
    ExpectMask(
        service.BatchProbeKey(kLastPair, {"missing", first, first, first},
                              TenantId::Default()),
        {false, false, true, true});
    ExpectMask(
        service.BatchProbeKey({ProbeLeaseMode::LastHitOnly, 1},
                              {first, "missing", second}, TenantId::Default()),
        {false, false, true});
}

TEST_F(BatchProbePolicyTest, PreservesExistingSharedGroupLease) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{"shared-group"};
    for (const auto& key : {"old", "selected"}) {
        PutCompletedObject(service, context.client_id, key, config);
    }
    const auto before = Deadline(service, "old");
    ExpectMask(
        service.BatchProbeKey({}, {"old", "selected"}, TenantId::Default()),
        {true, true});
    EXPECT_EQ(Deadline(service, "old"), before);
    ExpectMask(service.BatchProbeKey({ProbeLeaseMode::LastHitOnly, 1},
                                     {"old", "selected"}, TenantId::Default()),
               {false, true});
    EXPECT_GT(Deadline(service, "old"), before);
    EXPECT_EQ(Deadline(service, "old"), Deadline(service, "selected"));
}

TEST_F(BatchProbePolicyTest, TenantIsolationAndErrorsThroughWrapper) {
    WrappedMasterService service(MakeStrictWrappedConfig({"left", "right"}));
    const auto segment = MakeSegment("tenant-probe-segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());
    const std::vector<std::string> keys = {"a", "b", "c", "d"};
    for (const auto& tenant : {std::string("left"), std::string("right")}) {
        const size_t offset = tenant == "left" ? 0 : 2;
        const std::vector<std::string> present = {keys[offset],
                                                  keys[offset + 1]};
        auto started = service.BatchPutStart(client_id, present, {1024, 1024},
                                             {.replica_num = 1}, tenant);
        for (const auto& item : started) ASSERT_TRUE(item.has_value());
        auto ended = service.BatchPutEnd(client_id, MakeObjectMetas(present),
                                         ReplicaType::MEMORY, tenant);
        for (const auto& item : ended) ASSERT_TRUE(item.has_value());
    }
    ExpectMask(service.BatchProbeKey(kLastPair, keys, "left"),
               {true, true, false, false});
    ExpectMask(service.BatchProbeKey(kLastPair, keys, "right"),
               {false, false, true, true});
    ExpectMask(service.BatchProbeKey(kLastPair, keys, "unregistered"),
               {false, false, false, false});
    auto invalid = service.BatchProbeKey(kLastPair, keys, "_invalid-tenant");
    ASSERT_EQ(invalid.size(), keys.size());
    for (const auto& item : invalid) {
        ASSERT_FALSE(item.has_value());
        EXPECT_EQ(item.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(BatchProbePolicyTest, ConcurrentRemovalNeverReturnsPartialSelection) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(60000).build());
    const auto context = PrepareSimpleSegment(service);
    for (int iteration = 0; iteration < 64; ++iteration) {
        std::vector<std::string> keys;
        std::vector<std::chrono::system_clock::time_point> before;
        for (int part = 0; part < 4; ++part) {
            keys.push_back("race-" + std::to_string(iteration) + "-" +
                           std::to_string(part));
            PutCompletedObject(service, context.client_id, keys.back(),
                               {.replica_num = 1});
            before.push_back(Deadline(service, keys.back()));
        }
        std::barrier start(2);
        std::thread remover([&] {
            start.arrive_and_wait();
            for (const auto& key : keys)
                service.Remove(key, TenantId::Default());
        });
        start.arrive_and_wait();
        auto result = service.BatchProbeKey({ProbeLeaseMode::LastHitOnly, 4},
                                            keys, TenantId::Default());
        remover.join();
        ASSERT_EQ(result.size(), keys.size());
        ASSERT_TRUE(result.front().has_value());
        ExpectMask(result,
                   std::vector<bool>(keys.size(), result.front().value()));
        for (size_t i = 0; i < keys.size(); ++i) {
            if (result.front().value()) {
                auto removed = service.Remove(keys[i], TenantId::Default());
                ASSERT_FALSE(removed.has_value());
                EXPECT_EQ(removed.error(), ErrorCode::OBJECT_HAS_LEASE);
            } else if (service.ProbeKey(keys[i], TenantId::Default()).value()) {
                EXPECT_EQ(Deadline(service, keys[i]), before[i]);
            }
        }
    }
}

}  // namespace mooncake::test
