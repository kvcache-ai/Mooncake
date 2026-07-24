#include "master_service_test_harness.h"

namespace mooncake::test {
namespace {

bool PutIfPossible(MasterService& service, const UUID& client_id,
                   const std::string& key, uint64_t size,
                   const ReplicateConfig& config) {
    auto start =
        service.PutStart(client_id, key, TenantId::Default(), size, config);
    if (!start) {
        return false;
    }
    auto end = service.PutEnd(client_id, key, TenantId::Default(),
                              ReplicaType::MEMORY);
    EXPECT_TRUE(end.has_value());
    return end.has_value();
}

}  // namespace

TEST_F(MasterServiceTest, EvictObject) {
    constexpr uint64_t kLeaseTtl = 2000;
    constexpr size_t kObjectCount = 1024 * 16;
    constexpr size_t kObjectSize = 1024 * 15;
    auto service =
        std::make_unique<MasterService>(MasterServiceConfig::builder()
                                            .set_default_kv_lease_ttl(kLeaseTtl)
                                            .build());
    const UUID client_id = generate_uuid();
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service, "test_segment", kDefaultSegmentBase,
                             kObjectCount * kObjectSize);

    int success_count = 0;
    ReplicateConfig config;
    config.replica_num = 1;
    for (size_t index = 0; index < kObjectCount + 50; ++index) {
        if (PutIfPossible(*service, client_id,
                          "test_key" + std::to_string(index), kObjectSize,
                          config)) {
            ++success_count;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    EXPECT_GT(success_count, kObjectCount);
    std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtl));
    service->RemoveAll();
}

TEST_F(MasterServiceTest, SoftPinObjectsNotEvictedBeforeOtherObjects) {
    constexpr uint64_t kLeaseTtl = 200;
    constexpr uint64_t kSoftPinTtl = 10000;
    constexpr size_t kObjectSize = 1024 * 1024;
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kLeaseTtl)
            .set_default_kv_soft_pin_ttl(kSoftPinTtl)
            .set_allow_evict_soft_pinned_objects(true)
            .set_eviction_ratio(0.5)
            .build());
    const UUID client_id = generate_uuid();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, 16 * 1024 * 1024);

    ReplicateConfig soft_pin_config;
    soft_pin_config.replica_num = 1;
    soft_pin_config.with_soft_pin = true;
    ReplicateConfig normal_config;
    normal_config.replica_num = 1;
    for (int iteration = 0; iteration < 5; ++iteration) {
        ASSERT_TRUE(PutIfPossible(*service, client_id, "pin_key0", kObjectSize,
                                  soft_pin_config));
        ASSERT_TRUE(PutIfPossible(*service, client_id, "pin_key1", kObjectSize,
                                  soft_pin_config));
        int failed_count = 0;
        for (int index = 0; index < 20; ++index) {
            if (!PutIfPossible(*service, client_id,
                               "key" + std::to_string(index), kObjectSize,
                               normal_config)) {
                ++failed_count;
            }
        }
        EXPECT_GT(failed_count, 0);
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kLeaseTtl + 1000));
        EXPECT_TRUE(service->GetReplicaList("pin_key0", TenantId::Default())
                        .has_value());
        EXPECT_TRUE(service->GetReplicaList("pin_key1", TenantId::Default())
                        .has_value());
        std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtl));
        service->RemoveAll();
    }
}

TEST_F(MasterServiceTest, SoftPinObjectsCanBeEvicted) {
    constexpr uint64_t kLeaseTtl = 200;
    constexpr size_t kObjectSize = 1024 * 1024;
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kLeaseTtl)
            .set_default_kv_soft_pin_ttl(10000)
            .set_allow_evict_soft_pinned_objects(true)
            .build());
    const UUID client_id = generate_uuid();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, 16 * 1024 * 1024);

    ReplicateConfig config;
    config.replica_num = 1;
    config.with_soft_pin = true;
    int success_count = 0;
    for (int index = 0; index < 66; ++index) {
        if (PutIfPossible(*service, client_id,
                          "test_key" + std::to_string(index), kObjectSize,
                          config)) {
            ++success_count;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    EXPECT_GT(success_count, 16);
    std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtl));
    service->RemoveAll();
}

TEST_F(MasterServiceTest, SoftPinExtendedOnGet) {
    constexpr uint64_t kLeaseTtl = 200;
    constexpr uint64_t kSoftPinTtl = 1000;
    constexpr size_t kObjectSize = 1024 * 1024;
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kLeaseTtl)
            .set_default_kv_soft_pin_ttl(kSoftPinTtl)
            .set_allow_evict_soft_pinned_objects(true)
            .set_eviction_ratio(0.5)
            .build());
    const UUID client_id = generate_uuid();
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, 16 * 1024 * 1024);

    ReplicateConfig soft_pin_config;
    soft_pin_config.replica_num = 1;
    soft_pin_config.with_soft_pin = true;
    ReplicateConfig normal_config;
    normal_config.replica_num = 1;
    for (int iteration = 0; iteration < 3; ++iteration) {
        ASSERT_TRUE(PutIfPossible(*service, client_id, "pin_key0", kObjectSize,
                                  soft_pin_config));
        ASSERT_TRUE(PutIfPossible(*service, client_id, "pin_key1", kObjectSize,
                                  soft_pin_config));
        std::this_thread::sleep_for(std::chrono::milliseconds(kSoftPinTtl));
        ASSERT_TRUE(service->GetReplicaList("pin_key0", TenantId::Default())
                        .has_value());
        ASSERT_TRUE(service->GetReplicaList("pin_key1", TenantId::Default())
                        .has_value());

        int failed_count = 0;
        for (int index = 0; index < 16; ++index) {
            if (!PutIfPossible(*service, client_id,
                               "key" + std::to_string(index), kObjectSize,
                               normal_config)) {
                ++failed_count;
            }
        }
        EXPECT_GT(failed_count, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtl));
        EXPECT_TRUE(service->GetReplicaList("pin_key0", TenantId::Default())
                        .has_value());
        EXPECT_TRUE(service->GetReplicaList("pin_key1", TenantId::Default())
                        .has_value());
        std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtl));
        service->RemoveAll();
    }
}

}  // namespace mooncake::test
