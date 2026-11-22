#include "types.h"
#include "replica.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

#include <chrono>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace mooncake::test {

class ObjectDataTypeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ObjectDataTypeTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }
};

// Verify enum values match the RFC spec
TEST_F(ObjectDataTypeTest, EnumValues) {
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::UNKNOWN), 0);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::KVCACHE), 1);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::TENSOR), 2);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::WEIGHT), 3);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::SAMPLE), 4);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::ACTIVATION), 5);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::GRADIENT), 6);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::OPTIMIZER_STATE), 7);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::METADATA), 8);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::GENERAL), 9);
}

// Verify stream operator produces readable output
TEST_F(ObjectDataTypeTest, StreamOperator) {
    std::ostringstream oss;
    oss << ObjectDataType::KVCACHE;
    EXPECT_EQ(oss.str(), "KVCACHE");

    oss.str("");
    oss << ObjectDataType::UNKNOWN;
    EXPECT_EQ(oss.str(), "UNKNOWN");

    oss.str("");
    oss << ObjectDataType::OPTIMIZER_STATE;
    EXPECT_EQ(oss.str(), "OPTIMIZER_STATE");

    oss.str("");
    oss << ObjectDataType::GENERAL;
    EXPECT_EQ(oss.str(), "GENERAL");

    // Out-of-range value should print "UNKNOWN"
    oss.str("");
    oss << static_cast<ObjectDataType>(200);
    EXPECT_EQ(oss.str(), "UNKNOWN");
}

// ReplicateConfig defaults to UNKNOWN
TEST_F(ObjectDataTypeTest, ReplicateConfigDefaultDataType) {
    ReplicateConfig config;
    EXPECT_EQ(config.data_type, ObjectDataType::UNKNOWN);
}

// ReplicateConfig stream output includes data_type
TEST_F(ObjectDataTypeTest, ReplicateConfigStreamIncludesDataType) {
    ReplicateConfig config;
    config.data_type = ObjectDataType::TENSOR;
    std::ostringstream oss;
    oss << config;
    EXPECT_NE(oss.str().find("data_type: TENSOR"), std::string::npos);
}

// PutStart accepts the default UNKNOWN data type.
TEST_F(ObjectDataTypeTest, PutStartDefaultDataType) {
    std::unique_ptr<MasterService> service(new MasterService());
    Segment segment = MakeSegment();
    UUID client_id = generate_uuid();
    auto mount_result = service->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    UUID put_client = generate_uuid();
    ReplicateConfig config;
    config.replica_num = 1;
    // data_type left as default (UNKNOWN)

    auto result = service->PutStart(put_client, "key_default",
                                    TenantId::Default(), 1024, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().empty());
}

TEST_F(ObjectDataTypeTest, KVCacheKindValuesAndNamesAreStable) {
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::UNSPECIFIED), 0);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::FULL), 1);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::SWA), 2);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::DSA), 3);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::INDEXER), 4);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::MLA), 5);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::MAMBA), 6);
    EXPECT_EQ(static_cast<uint8_t>(KVCacheKind::LINEAR), 7);
    EXPECT_EQ(KVCacheKindName(KVCacheKind::DSA), "DSA");
    EXPECT_EQ(KVCacheKindName(KVCacheKind::MAMBA), "MAMBA");
    EXPECT_FALSE(IsKnownKVCacheKind(static_cast<KVCacheKind>(200)));
}

TEST_F(ObjectDataTypeTest, KVCacheKindPolicyParsesAndRejects) {
    std::string error;
    auto policy = KVCacheKindPolicy::Parse(
        " MAMBA = PROTECT , INDEXER=DEFER, SWA=DEFER ", &error);
    ASSERT_TRUE(policy) << error;
    EXPECT_EQ(policy->ControlFor(KVCacheKind::MAMBA),
              KVCacheEvictControl::PROTECT);
    EXPECT_EQ(policy->ControlFor(KVCacheKind::INDEXER),
              KVCacheEvictControl::DEFER);
    EXPECT_EQ(policy->ControlFor(KVCacheKind::SWA), KVCacheEvictControl::DEFER);
    EXPECT_EQ(policy->ControlFor(KVCacheKind::DSA),
              KVCacheEvictControl::NORMAL);
    EXPECT_EQ(policy->ControlFor(KVCacheKind::UNSPECIFIED),
              KVCacheEvictControl::NORMAL);

    EXPECT_FALSE(KVCacheKindPolicy::Parse("SWA=PIN", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("NOPE=NORMAL", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("SWA=NORMAL,SWA=DEFER", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("MAMBA=PROTECT,", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("MAMBA=PROTECT, \t\n", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse(" , ", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("SWA=DEFER, ,MAMBA=PROTECT", &error));
}

TEST_F(ObjectDataTypeTest, EmptyKVCacheKindPoliciesKeepNormalEviction) {
    for (const auto* spec : {"", "   ", " \t\r\n\f\v "}) {
        std::string error;
        auto policy = KVCacheKindPolicy::Parse(spec, &error);
        ASSERT_TRUE(policy) << error;
        for (uint8_t value = 0;
             value <= static_cast<uint8_t>(KVCacheKind::LINEAR); ++value) {
            EXPECT_EQ(policy->ControlFor(static_cast<KVCacheKind>(value)),
                      KVCacheEvictControl::NORMAL);
        }
    }
}

TEST_F(ObjectDataTypeTest, PerKeyKindsSplitWithTheBatch) {
    ReplicateConfig config;
    config.data_type = ObjectDataType::KVCACHE;
    config.kv_cache_kinds = std::vector<KVCacheKind>{
        KVCacheKind::DSA, KVCacheKind::INDEXER, KVCacheKind::SWA};
    EXPECT_TRUE(config.ValidKVCacheKinds(3));
    EXPECT_FALSE(config.HasValidKeyHints(2));
    config.kv_cache_kinds->push_back(static_cast<KVCacheKind>(200));
    EXPECT_FALSE(config.ValidKVCacheKinds(4));

    config.kv_cache_kinds = std::vector<KVCacheKind>{
        KVCacheKind::DSA, KVCacheKind::INDEXER, KVCacheKind::MAMBA};
    auto subset = config.ForKeySubset({2, 0});
    ASSERT_TRUE(subset.kv_cache_kinds);
    EXPECT_EQ(subset.kv_cache_kinds->at(0), KVCacheKind::MAMBA);
    EXPECT_EQ(subset.kv_cache_kinds->at(1), KVCacheKind::DSA);
    EXPECT_EQ(config.ForSingleKey(1).KVCacheKindForKey(0),
              KVCacheKind::INDEXER);

    std::ostringstream oss;
    oss << config;
    EXPECT_NE(oss.str().find("kv_cache_kinds: [DSA, INDEXER, MAMBA]"),
              std::string::npos);
}

struct LegacyReplicateConfigWithoutKind {
    size_t replica_num{1};
    size_t nof_replica_num{0};
    size_t dfs_replica_num{0};
    SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
    std::optional<uint64_t> soft_pin_ttl_ms;
    bool with_hard_pin{false};
    std::vector<std::string> preferred_segments;
    std::string preferred_segment;
    std::vector<std::string> preferred_nof_segments;
    bool prefer_alloc_in_same_node{false};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string host_id;
    std::optional<std::vector<std::string>> group_ids;
};

TEST_F(ObjectDataTypeTest, KindFieldIsWireCompatibleAndRoundTrips) {
    LegacyReplicateConfigWithoutKind legacy;
    legacy.data_type = ObjectDataType::KVCACHE;
    legacy.preferred_segments = {"seg"};
    auto decoded = struct_pack::deserialize<ReplicateConfig>(
        struct_pack::serialize(legacy));
    ASSERT_TRUE(decoded);
    EXPECT_FALSE(decoded->kv_cache_kinds);
    EXPECT_EQ(decoded->data_type, ObjectDataType::KVCACHE);
    EXPECT_EQ(decoded->KVCacheKindForKey(0), KVCacheKind::UNSPECIFIED);

    decoded->kv_cache_kinds =
        std::vector<KVCacheKind>{KVCacheKind::SWA, KVCacheKind::MAMBA};
    auto current = struct_pack::deserialize<ReplicateConfig>(
        struct_pack::serialize(*decoded));
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->kv_cache_kinds);
    EXPECT_EQ(*current->kv_cache_kinds, *decoded->kv_cache_kinds);

    auto old_reader =
        struct_pack::deserialize<LegacyReplicateConfigWithoutKind>(
            struct_pack::serialize(*current));
    ASSERT_TRUE(old_reader);
    EXPECT_EQ(old_reader->data_type, ObjectDataType::KVCACHE);
    EXPECT_EQ(old_reader->preferred_segments, legacy.preferred_segments);

    MetadataPayload payload;
    payload.data_type = ObjectDataType::KVCACHE;
    payload.kv_cache_kind = KVCacheKind::DSA;
    auto payload_decoded = struct_pack::deserialize<MetadataPayload>(
        struct_pack::serialize(payload));
    ASSERT_TRUE(payload_decoded);
    EXPECT_EQ(payload_decoded->kv_cache_kind.value_or(KVCacheKind::FULL),
              KVCacheKind::DSA);
    EXPECT_EQ(payload_decoded->ToStandbyMetadata().kv_cache_kind.value_or(
                  KVCacheKind::FULL),
              KVCacheKind::DSA);
}

TEST_F(ObjectDataTypeTest, MasterAppliesKindEvictionPolicy) {
    auto config =
        MasterServiceConfig::builder()
            .set_kv_cache_kind_evict("MAMBA=PROTECT,INDEXER=DEFER,SWA=DEFER")
            .build();
    MasterService service(config);
    EXPECT_EQ(service.EvictControlForKind(KVCacheKind::MAMBA),
              KVCacheEvictControl::PROTECT);
    EXPECT_EQ(service.EvictControlForKind(KVCacheKind::INDEXER),
              KVCacheEvictControl::DEFER);
    EXPECT_EQ(service.EvictControlForKind(KVCacheKind::SWA),
              KVCacheEvictControl::DEFER);
    EXPECT_EQ(service.EvictControlForKind(KVCacheKind::FULL),
              KVCacheEvictControl::NORMAL);
    EXPECT_EQ(service.EvictControlForKind(KVCacheKind::UNSPECIFIED),
              KVCacheEvictControl::NORMAL);
}

class KVCacheKindEvictionTest
    : public ObjectDataTypeTest,
      public ::testing::WithParamInterface<KVCacheEvictControl> {};

TEST_P(KVCacheKindEvictionTest, ControlsOnlyApplyToKVCacheObjects) {
    const auto control = GetParam();
    const std::string control_name(KVCacheEvictControlName(control));
    MasterServiceConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.kv_cache_kind_evict =
        "UNSPECIFIED=" + control_name + ",MAMBA=" + control_name;
    MasterService service(config);
    MasterServiceTestPeer::EvictionRunning(service) = false;
    MasterServiceTestPeer::EvictionThread(service).join();
    ASSERT_TRUE(
        service.MountSegment(MakeSegment(), generate_uuid()).has_value());

    const auto expired =
        std::chrono::system_clock::now() - std::chrono::hours(1);
    std::vector<std::pair<std::string, ObjectDataType>> objects;
    for (const auto kind : {KVCacheKind::UNSPECIFIED, KVCacheKind::MAMBA}) {
        for (const auto data_type :
             {ObjectDataType::UNKNOWN, ObjectDataType::KVCACHE,
              ObjectDataType::TENSOR, ObjectDataType::WEIGHT,
              ObjectDataType::SAMPLE, ObjectDataType::ACTIVATION,
              ObjectDataType::GRADIENT, ObjectDataType::OPTIMIZER_STATE,
              ObjectDataType::METADATA, ObjectDataType::GENERAL}) {
            const auto key = "kind-eviction-" + std::to_string(objects.size());
            ReplicateConfig replicate_config;
            replicate_config.data_type = data_type;
            if (kind != KVCacheKind::UNSPECIFIED) {
                replicate_config.kv_cache_kinds =
                    std::vector<KVCacheKind>{kind};
            }
            const auto client_id = generate_uuid();
            ASSERT_TRUE(service
                            .PutStart(client_id, key, TenantId::Default(), 1024,
                                      replicate_config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(client_id, key, TenantId::Default(),
                                    ReplicaType::MEMORY)
                            .has_value());
            auto hold = MasterServiceTestPeer::WriteObject(
                service, {TenantId::Default(), key});
            ASSERT_TRUE(hold);
            auto& metadata = hold->metadata();
            SpinLocker locker(&metadata.lock);
            metadata.lease_->SetDeadline(expired);
            objects.emplace_back(key, data_type);
        }
    }

    // With no fallback quota, only KV objects may be protected or deferred.
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 0.0);
    for (const auto& [key, data_type] : objects) {
        auto exists = service.ProbeKey(key, TenantId::Default());
        ASSERT_TRUE(exists.has_value()) << key;
        EXPECT_EQ(*exists, data_type == ObjectDataType::KVCACHE) << key;
    }

    // The fallback pass can reclaim DEFER objects, but never PROTECT ones.
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    for (const auto& [key, data_type] : objects) {
        auto exists = service.ProbeKey(key, TenantId::Default());
        ASSERT_TRUE(exists.has_value()) << key;
        EXPECT_EQ(*exists, data_type == ObjectDataType::KVCACHE &&
                               control == KVCacheEvictControl::PROTECT)
            << key;
    }
}

INSTANTIATE_TEST_SUITE_P(KVCacheKindControls, KVCacheKindEvictionTest,
                         ::testing::Values(KVCacheEvictControl::PROTECT,
                                           KVCacheEvictControl::DEFER));

TEST_F(ObjectDataTypeTest, PutStartRecordsTheKVCacheKind) {
    MasterService service;
    Segment segment = MakeSegment("kind_segment", 0x310000000);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    UUID put_client = generate_uuid();
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::KVCACHE;
    config.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::MAMBA};
    auto started = service.PutStart(put_client, "mamba-state",
                                    TenantId::Default(), 1024, config);
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(service
                    .PutEnd(put_client, "mamba-state", TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());

    MasterServiceTestPeer::MetadataAccessorRO accessor(
        &service, MasterServiceTestPeer::ObjectIdentity{TenantId::Default(),
                                                        "mamba-state"});
    ASSERT_TRUE(accessor.Exists());
    EXPECT_EQ(accessor.Get().data_type, ObjectDataType::KVCACHE);
    EXPECT_EQ(accessor.Get().kv_cache_kind, KVCacheKind::MAMBA);

    config.kv_cache_kinds =
        std::vector<KVCacheKind>{KVCacheKind::SWA, KVCacheKind::DSA};
    auto rejected = service.PutStart(generate_uuid(), "bad-kind-count",
                                     TenantId::Default(), 1024, config);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ObjectDataTypeTest,
       UpsertUpdatesOrPreservesKindAcrossBufferReplacement) {
    auto settings =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    settings.memory_allocator = BufferAllocatorType::OFFSET;
    MasterService service(settings);
    const auto writer = generate_uuid();
    const auto tenant = TenantId::Default();
    const std::string key = "upsert-kind";
    ASSERT_TRUE(service.MountSegment(MakeSegment(), writer));
    ReplicateConfig tagged;
    tagged.data_type = ObjectDataType::KVCACHE;
    tagged.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::FULL};
    ASSERT_TRUE(service.PutStart(writer, key, tenant, 1024, tagged));
    ASSERT_TRUE(service.PutEnd(writer, key, tenant, ReplicaType::MEMORY));

    auto check = [&](size_t size, const ReplicateConfig& config,
                     ObjectDataType type, KVCacheKind expected) {
        ASSERT_TRUE(service.UpsertStart(writer, key, tenant, size, config));
        ASSERT_TRUE(
            service.UpsertEnd(writer, key, tenant, ReplicaType::MEMORY));
        MasterServiceTestPeer::MetadataAccessorRO object(&service,
                                                         {tenant, key});
        ASSERT_TRUE(object.Exists());
        EXPECT_EQ(object.Get().data_type, type);
        EXPECT_EQ(object.Get().kv_cache_kind, expected);
        EXPECT_EQ(object.Get().TotalLength(), size);
    };
    tagged.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::MAMBA};
    check(1024, tagged, ObjectDataType::KVCACHE, KVCacheKind::MAMBA);
    check(1024, {}, ObjectDataType::KVCACHE, KVCacheKind::MAMBA);
    check(2048, {}, ObjectDataType::KVCACHE, KVCacheKind::MAMBA);
    tagged.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::SWA};
    check(4096, tagged, ObjectDataType::KVCACHE, KVCacheKind::SWA);
    tagged.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::UNSPECIFIED};
    check(4096, tagged, ObjectDataType::KVCACHE, KVCacheKind::UNSPECIFIED);
    ReplicateConfig tensor;
    tensor.data_type = ObjectDataType::TENSOR;
    check(4096, tensor, ObjectDataType::TENSOR, KVCacheKind::UNSPECIFIED);
    tagged.kv_cache_kinds = std::vector<KVCacheKind>{KVCacheKind::DSA};
    check(4096, tagged, ObjectDataType::KVCACHE, KVCacheKind::DSA);
}

TEST_F(ObjectDataTypeTest,
       KindOnlyTensorBatchesKeepTheOriginalIndexForEveryShard) {
    ReplicateConfig config;
    config.kv_cache_kinds =
        std::vector<KVCacheKind>{KVCacheKind::FULL, KVCacheKind::MAMBA};
    const auto all = config.ForRepeatedKeySubset({0, 1}, 2);
    ASSERT_TRUE(all.ValidKVCacheKinds(4));
    EXPECT_EQ(
        *all.kv_cache_kinds,
        (std::vector<KVCacheKind>{KVCacheKind::FULL, KVCacheKind::FULL,
                                  KVCacheKind::MAMBA, KVCacheKind::MAMBA}));
    const auto partial = config.ForRepeatedKeySubset({1}, 2);
    ASSERT_TRUE(partial.ValidKVCacheKinds(2));
    EXPECT_EQ(*partial.kv_cache_kinds,
              std::vector<KVCacheKind>(2, KVCacheKind::MAMBA));
    EXPECT_TRUE(config.ValidKVCacheKinds(2));
}

TEST_F(ObjectDataTypeTest, NoFLastReplicaHonorsKindAndFallbackQuota) {
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "nof", kDefaultSegmentBase, kDefaultSegmentSize, "endpoint", "nvmeof",
        ReplicaType::NOF_SSD);
    MasterService service(
        MasterServiceConfig::builder()
            .set_kv_cache_kind_evict("MAMBA=PROTECT,SWA=DEFER")
            .build());
    MasterServiceTestPeer peer(service);
    const auto tenant = TenantId::Default();
    auto& objects = peer.GetOrCreateNamespace(tenant).objects;
    // The evictor operates on master descriptors. Offset allocations give each
    // object a live NoF-only replica without requiring an NVMe target.
    for (auto kind :
         {KVCacheKind::MAMBA, KVCacheKind::SWA, KVCacheKind::FULL}) {
        const std::string key(KVCacheKindName(kind));
        auto buffer = allocator->allocate(4096);
        ASSERT_TRUE(buffer);
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(buffer), ReplicaStatus::COMPLETE,
                              ReplicaType::NOF_SSD);
        auto guard = objects.WriteKey(key);
        guard.Publish(generate_uuid(), std::chrono::system_clock::now(), 4096,
                      std::move(replicas), std::nullopt, false,
                      ObjectDataType::KVCACHE, "", tenant, key, "", kind);
    }
    peer.RunNoFBatchEvictForTesting(1.0, 0.0);
    EXPECT_FALSE(objects.Contains("FULL"));
    EXPECT_TRUE(objects.Contains("SWA"));
    EXPECT_TRUE(objects.Contains("MAMBA"));
    peer.RunNoFBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(objects.Contains("SWA"));
    EXPECT_TRUE(objects.Contains("MAMBA"));
}

}  // namespace mooncake::test
