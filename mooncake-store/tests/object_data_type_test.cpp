#include "types.h"
#include "replica.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

#include <sstream>

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

    EXPECT_TRUE(KVCacheKindPolicy::Parse("", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("SWA=PIN", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("NOPE=NORMAL", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("SWA=NORMAL,SWA=DEFER", &error));
    EXPECT_FALSE(KVCacheKindPolicy::Parse("MAMBA=PROTECT,", &error));
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
    MasterServiceConfig config;
    config.kv_cache_kind_evict = "MAMBA=PROTECT,INDEXER=DEFER,SWA=DEFER";
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

}  // namespace mooncake::test
