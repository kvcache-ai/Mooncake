#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake::test {

namespace {

std::unique_ptr<MasterService> CreateMasterService() {
    return std::make_unique<MasterService>(MasterServiceConfig{});
}

std::vector<NicLoadStat> MakeStats(
    const std::vector<std::pair<std::string, uint64_t>>& devices) {
    std::vector<NicLoadStat> stats;
    stats.reserve(devices.size());
    for (const auto& [name, inflight] : devices) {
        stats.emplace_back(name, inflight, 1e9);
    }
    return stats;
}

UUID MountSegment(MasterService& service, const std::string& segment_name,
                  size_t base_addr) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = base_addr;
    segment.size = 64 * 1024 * 1024;
    segment.te_endpoint = segment_name;
    UUID client_id = generate_uuid();
    auto result = service.MountSegment(segment, client_id);
    EXPECT_TRUE(result.has_value());
    return client_id;
}

}  // namespace

class MasterServiceNicLoadTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceNicLoadTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // TEST_F bodies don't inherit friendship; mirror private constants here.
    static constexpr size_t kMaxDevicesPerReport =
        MasterService::kMaxNicDevicesPerReport;
    static constexpr size_t kMaxDeviceNameLength =
        MasterService::kMaxNicDeviceNameLength;

    // Rewind a snapshot's timestamp beyond the TTL so lazy eviction removes it.
    static void AgeNicLoadEntryBeyondTtl(MasterService& service,
                                         const UUID& client_id) {
        std::lock_guard<std::mutex> lock(service.nic_load_stats_mutex_);
        auto it = service.nic_load_stats_.find(client_id);
        ASSERT_TRUE(it != service.nic_load_stats_.end());
        it->second.updated_at_ms -= MasterService::kNicLoadStatsTtlMs + 1;
    }

    static size_t NicLoadEntryCount(MasterService& service) {
        std::lock_guard<std::mutex> lock(service.nic_load_stats_mutex_);
        return service.nic_load_stats_.size();
    }

    // Force the endpoint index to rebuild on the next query.
    static void ExpireEndpointIndex(MasterService& service) {
        std::lock_guard<std::mutex> lock(service.endpoint_index_mutex_);
        service.endpoint_index_built_at_ms_ = 0;
    }

    // Pin the endpoint index so it cannot expire during the test.
    static void PinEndpointIndex(MasterService& service) {
        std::lock_guard<std::mutex> lock(service.endpoint_index_mutex_);
        service.endpoint_index_built_at_ms_ =
            std::numeric_limits<uint64_t>::max() / 2;
    }
};

// --- ReportNicLoadStats ---

TEST_F(MasterServiceNicLoadTest, ReportValidStats) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    auto stats = MakeStats({{"eth0", 1024}, {"eth1", 2048}});

    auto result = service->ReportNicLoadStats(client_id, stats);
    ASSERT_TRUE(result.has_value());
}

TEST_F(MasterServiceNicLoadTest, ReportEmptyDeviceNameRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    auto stats = MakeStats({{"", 1024}});

    auto result = service->ReportNicLoadStats(client_id, stats);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportNegativeBandwidthRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    std::vector<NicLoadStat> stats;
    stats.emplace_back("eth0", 1024, -1.0);

    auto result = service->ReportNicLoadStats(client_id, stats);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportNanBandwidthRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    std::vector<NicLoadStat> stats;
    stats.emplace_back("eth0", 1024, std::numeric_limits<double>::quiet_NaN());

    auto result = service->ReportNicLoadStats(client_id, stats);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportInfBandwidthRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    std::vector<NicLoadStat> stats;
    stats.emplace_back("eth0", 1024, std::numeric_limits<double>::infinity());

    auto result = service->ReportNicLoadStats(client_id, stats);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportTooManyDevicesRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    std::vector<std::pair<std::string, uint64_t>> devices;
    for (size_t i = 0; i <= kMaxDevicesPerReport; ++i) {
        devices.emplace_back("eth" + std::to_string(i), 100);
    }

    auto result = service->ReportNicLoadStats(client_id, MakeStats(devices));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportOverlongDeviceNameRejected) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    std::string long_name(kMaxDeviceNameLength + 1, 'x');

    auto result =
        service->ReportNicLoadStats(client_id, MakeStats({{long_name, 100}}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
}

TEST_F(MasterServiceNicLoadTest, ReportOverwritesPrevious) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();

    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 100}}))
            .has_value());
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 200}}))
            .has_value());

    auto result = service->BatchGetNicLoadStats({client_id});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ(200u, (*result)[0].devices[0].inflight_bytes);
}

// --- BatchGetNicLoadStats ---

TEST_F(MasterServiceNicLoadTest, BatchGetKnownClient) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    auto stats = MakeStats({{"eth0", 1024}, {"eth1", 2048}});

    ASSERT_TRUE(service->ReportNicLoadStats(client_id, stats).has_value());

    auto result = service->BatchGetNicLoadStats({client_id});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ(client_id, (*result)[0].client_id);
    ASSERT_EQ(2u, (*result)[0].devices.size());
    EXPECT_EQ("eth0", (*result)[0].devices[0].device_name);
    EXPECT_EQ(1024u, (*result)[0].devices[0].inflight_bytes);
    EXPECT_EQ("eth1", (*result)[0].devices[1].device_name);
    EXPECT_EQ(2048u, (*result)[0].devices[1].inflight_bytes);
}

TEST_F(MasterServiceNicLoadTest, BatchGetUnknownClientReturnsEmpty) {
    auto service = CreateMasterService();
    UUID unknown_id = generate_uuid();

    auto result = service->BatchGetNicLoadStats({unknown_id});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST_F(MasterServiceNicLoadTest, BatchGetMixedClients) {
    auto service = CreateMasterService();
    UUID client_a = generate_uuid();
    UUID client_b = generate_uuid();
    UUID client_c = generate_uuid();  // never reported

    ASSERT_TRUE(
        service->ReportNicLoadStats(client_a, MakeStats({{"eth0", 100}}))
            .has_value());
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_b, MakeStats({{"eth0", 200}}))
            .has_value());

    auto result = service->BatchGetNicLoadStats({client_a, client_b, client_c});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(2u, result->size());
}

TEST_F(MasterServiceNicLoadTest, BatchGetEmptyInput) {
    auto service = CreateMasterService();

    auto result = service->BatchGetNicLoadStats({});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

// --- BatchGetNicLoadStatsByEndpoints ---

TEST_F(MasterServiceNicLoadTest, BatchGetBySegmentName) {
    auto service = CreateMasterService();
    UUID client_id = MountSegment(*service, "endpoint_a", 0x300000000);
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 512}}))
            .has_value());

    auto result = service->BatchGetNicLoadStatsByEndpoints({"endpoint_a"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ("endpoint_a", (*result)[0].endpoint);
    EXPECT_EQ(client_id, (*result)[0].client_id);
    ASSERT_EQ(1u, (*result)[0].devices.size());
    EXPECT_EQ(512u, (*result)[0].devices[0].inflight_bytes);
}

TEST_F(MasterServiceNicLoadTest, BatchGetByTeEndpoint) {
    auto service = CreateMasterService();

    // Mount segment where name != te_endpoint
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "seg_name";
    segment.base = 0x310000000;
    segment.size = 64 * 1024 * 1024;
    segment.te_endpoint = "te_addr:1234";
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 256}}))
            .has_value());

    // Query by te_endpoint
    auto result = service->BatchGetNicLoadStatsByEndpoints({"te_addr:1234"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ("te_addr:1234", (*result)[0].endpoint);
    EXPECT_EQ(client_id, (*result)[0].client_id);
}

TEST_F(MasterServiceNicLoadTest, BatchGetByUnknownEndpointReturnsEmpty) {
    auto service = CreateMasterService();

    auto result = service->BatchGetNicLoadStatsByEndpoints({"nonexistent"});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST_F(MasterServiceNicLoadTest, BatchGetByEndpointNoStatsReported) {
    auto service = CreateMasterService();
    // Mount segment but never report NIC load stats
    MountSegment(*service, "no_stats_endpoint", 0x320000000);

    auto result =
        service->BatchGetNicLoadStatsByEndpoints({"no_stats_endpoint"});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST_F(MasterServiceNicLoadTest, BatchGetByMultipleEndpoints) {
    auto service = CreateMasterService();
    UUID client_a = MountSegment(*service, "endpoint_a", 0x300000000);
    UUID client_b = MountSegment(*service, "endpoint_b", 0x310000000);
    MountSegment(*service, "endpoint_c_no_stats", 0x320000000);

    ASSERT_TRUE(
        service->ReportNicLoadStats(client_a, MakeStats({{"eth0", 100}}))
            .has_value());
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_b, MakeStats({{"eth0", 200}}))
            .has_value());

    auto result = service->BatchGetNicLoadStatsByEndpoints(
        {"endpoint_a", "endpoint_b", "endpoint_c_no_stats", "unknown"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(2u, result->size());
}

TEST_F(MasterServiceNicLoadTest, EndpointIndexCachedWithinTtl) {
    auto service = CreateMasterService();
    UUID client_a = MountSegment(*service, "endpoint_a", 0x300000000);
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_a, MakeStats({{"eth0", 100}}))
            .has_value());

    // First query builds the endpoint index; pin it so it cannot expire
    // mid-test on a slow machine.
    auto result = service->BatchGetNicLoadStatsByEndpoints({"endpoint_a"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    PinEndpointIndex(*service);

    // A segment mounted within the TTL is invisible until the index expires.
    UUID client_b = MountSegment(*service, "endpoint_b", 0x310000000);
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_b, MakeStats({{"eth0", 200}}))
            .has_value());
    result = service->BatchGetNicLoadStatsByEndpoints({"endpoint_b"});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());

    ExpireEndpointIndex(*service);
    result = service->BatchGetNicLoadStatsByEndpoints({"endpoint_b"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(1u, result->size());
}

// --- Eviction ---

TEST_F(MasterServiceNicLoadTest, FreshEntriesNotEvicted) {
    auto service = CreateMasterService();
    UUID client_id = generate_uuid();
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 100}}))
            .has_value());

    // Read paths evict lazily per queried key; fresh entry must survive.
    auto result = service->BatchGetNicLoadStats({client_id});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ(client_id, (*result)[0].client_id);
}

TEST_F(MasterServiceNicLoadTest, StaleEntriesEvicted) {
    auto service = CreateMasterService();
    UUID stale_id = generate_uuid();
    UUID fresh_id = generate_uuid();
    ASSERT_TRUE(
        service->ReportNicLoadStats(stale_id, MakeStats({{"eth0", 100}}))
            .has_value());
    ASSERT_TRUE(
        service->ReportNicLoadStats(fresh_id, MakeStats({{"eth0", 200}}))
            .has_value());

    AgeNicLoadEntryBeyondTtl(*service, stale_id);

    auto result = service->BatchGetNicLoadStats({stale_id, fresh_id});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1u, result->size());
    EXPECT_EQ(fresh_id, (*result)[0].client_id);
}

TEST_F(MasterServiceNicLoadTest, StaleEntriesEvictedOnReport) {
    auto service = CreateMasterService();
    UUID stale_id = generate_uuid();
    UUID fresh_id = generate_uuid();
    ASSERT_TRUE(
        service->ReportNicLoadStats(stale_id, MakeStats({{"eth0", 100}}))
            .has_value());

    AgeNicLoadEntryBeyondTtl(*service, stale_id);

    // A report from any client evicts stale entries without any query.
    ASSERT_TRUE(
        service->ReportNicLoadStats(fresh_id, MakeStats({{"eth0", 200}}))
            .has_value());
    EXPECT_EQ(1u, NicLoadEntryCount(*service));
}

TEST_F(MasterServiceNicLoadTest, StaleEntriesEvictedOnEndpointQuery) {
    auto service = CreateMasterService();
    UUID client_id = MountSegment(*service, "stale_endpoint", 0x300000000);
    ASSERT_TRUE(
        service->ReportNicLoadStats(client_id, MakeStats({{"eth0", 100}}))
            .has_value());

    AgeNicLoadEntryBeyondTtl(*service, client_id);

    auto result = service->BatchGetNicLoadStatsByEndpoints({"stale_endpoint"});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

}  // namespace mooncake::test
