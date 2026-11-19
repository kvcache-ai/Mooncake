#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common/network.h"
#include "master_client.h"
#include "master_config.h"
#include "master_metric_manager.h"
#include "rpc_service.h"
#include "tenant/quota_policy_store.h"
#include "types.h"

namespace mooncake {
namespace {

class SoftPinRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        policy_path_ =
            std::filesystem::temp_directory_path() /
            ("soft_pin_rpc_" + UuidToString(generate_uuid()) + ".yaml");
        TenantQuotaPolicySnapshot policy;
        policy.tenant_quotas = {{"default", 64 * 1024 * 1024},
                                {"tenant-a", 64 * 1024 * 1024}};
        {
            std::ofstream output(policy_path_);
            ASSERT_TRUE(output.is_open());
            output << FormatTenantQuotaPolicyYaml(policy);
            ASSERT_TRUE(output.good());
        }
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        config.enable_metric_reporting = false;
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_type = "file";
        config.tenant_quota_connector_uri = policy_path_.string();
        config.default_kv_soft_pin_ttl = 1000;
        config.max_kv_soft_pin_ttl = 60000;
        service_ = std::make_unique<WrappedMasterService>(config);

        const auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, ports.front(), "127.0.0.1", std::chrono::seconds(0), true);
        RegisterRpcService(*server_, *service_);
        auto started = server_->async_start();
        ASSERT_FALSE(started.hasResult());
        address_ = "127.0.0.1:" + std::to_string(ports.front());

        Segment segment;
        segment.id = generate_uuid();
        segment.name = "soft_pin_rpc_segment";
        segment.base = 0x300000000;
        segment.size = 16 * 1024 * 1024;
        ASSERT_TRUE(service_->MountSegment(segment, client_id_));
        for (const std::string tenant : {"default", "tenant-a"}) {
            ASSERT_TRUE(service_->PutStart(client_id_, "key", 1024,
                                           ReplicateConfig{}, tenant));
            ASSERT_TRUE(service_->PutEnd(client_id_, {"key", std::nullopt},
                                         ReplicaType::MEMORY, tenant));
        }
        ASSERT_TRUE(service_->PutStart(client_id_, "writing", 1024,
                                       ReplicateConfig{}, "tenant-a"));
    }

    void TearDown() override {
        if (server_) server_->stop();
        service_.reset();
        std::error_code ignored;
        std::filesystem::remove(policy_path_, ignored);
    }

    int64_t PinCount() {
        return MasterMetricManager::instance().get_soft_pin_key_count();
    }

    UUID client_id_ = generate_uuid();
    std::filesystem::path policy_path_;
    std::unique_ptr<WrappedMasterService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::string address_;
};

TEST_F(SoftPinRpcTest, PinRenewUnpinAndTenantIsolation) {
    MasterClient tenant(client_id_, nullptr, "tenant-a");
    MasterClient defaults(client_id_);
    ASSERT_EQ(tenant.Connect(address_), ErrorCode::OK);
    ASSERT_EQ(defaults.Connect(address_), ErrorCode::OK);
    const auto baseline = PinCount();

    auto results =
        tenant.BatchUpdateSoftPin({"key", "missing", "", "writing"}, 30000);
    ASSERT_EQ(results.size(), 4u);
    ASSERT_TRUE(results[0]);
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_FALSE(results[2]);
    EXPECT_EQ(results[2].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_FALSE(results[3]);
    EXPECT_EQ(results[3].error(), ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(PinCount(), baseline + 1);

    // Updating the TTL must not double-count the same pinned object.
    results = tenant.BatchUpdateSoftPin({"key"}, 60000);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(PinCount(), baseline + 1);

    results = defaults.BatchUpdateSoftPin({"key"}, 0);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(PinCount(), baseline + 1);
    results = defaults.BatchUpdateSoftPin({"key"}, 30000);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(PinCount(), baseline + 2);

    for (int i = 0; i < 2; ++i) {
        results = tenant.BatchUpdateSoftPin({"key"}, 0);
        ASSERT_EQ(results.size(), 1u);
        EXPECT_TRUE(results[0]);
        EXPECT_EQ(PinCount(), baseline + 1);
    }
    results = defaults.BatchUpdateSoftPin({"key"}, 0);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(PinCount(), baseline);
    auto exists = service_->ExistKey("key", "tenant-a");
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
}

TEST_F(SoftPinRpcTest,
       RejectsInvalidTtlWithoutChangingPinAndAcceptsEmptyBatch) {
    MasterClient tenant(client_id_, nullptr, "tenant-a");
    ASSERT_EQ(tenant.Connect(address_), ErrorCode::OK);
    const auto baseline = PinCount();
    auto results = tenant.BatchUpdateSoftPin({"key"}, 30000);
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0]);
    results = tenant.BatchUpdateSoftPin({"key", "missing"}, 60001);
    ASSERT_EQ(results.size(), 2u);
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(PinCount(), baseline + 1);
    EXPECT_TRUE(tenant.BatchUpdateSoftPin({}, 0).empty());
    results = tenant.BatchUpdateSoftPin({"key"}, 0);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(PinCount(), baseline);
}

}  // namespace
}  // namespace mooncake
