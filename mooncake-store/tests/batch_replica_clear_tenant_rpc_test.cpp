#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common/network.h"
#include "master_client.h"
#include "master_config.h"
#include "rpc_service.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake {
namespace {

class ScopedPolicyFile {
   public:
    ScopedPolicyFile() {
        const UUID suffix = generate_uuid();
        path_ = std::filesystem::temp_directory_path() /
                ("batch_replica_clear_tenants_" +
                 std::to_string(suffix.first) + "_" +
                 std::to_string(suffix.second) + ".yaml");
    }

    ~ScopedPolicyFile() {
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    std::filesystem::path path_;
};

TEST(BatchReplicaClearTenantRpcTest, UsesClientTenantAndKeepsTenantsIsolated) {
    constexpr char kTenant[] = "tenant-a";
    constexpr char kSharedKey[] = "shared-batch-clear-key";
    constexpr uint64_t kObjectSize = 1024;

    ScopedPolicyFile policy_file;
    TenantQuotaPolicySnapshot policy;
    policy.tenant_quotas = {
        {std::string(TenantId::kDefaultValue), 64 * 1024 * 1024},
        {kTenant, 64 * 1024 * 1024},
    };
    {
        std::ofstream output(policy_file.path());
        ASSERT_TRUE(output.is_open());
        output << FormatTenantQuotaPolicyYaml(policy);
        ASSERT_TRUE(output.good());
    }

    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 0;
    service_config.enable_metric_reporting = false;
    service_config.enable_multi_tenants = true;
    service_config.tenant_quota_connector_type = "file";
    service_config.tenant_quota_connector_uri = policy_file.path().string();
    auto service = std::make_shared<WrappedMasterService>(service_config);

    const auto ports = getFreeTcpPorts(1);
    ASSERT_EQ(ports.size(), 1u);
    coro_rpc::coro_rpc_server server(
        /*thread_num=*/2, /*port=*/ports.front(), /*address=*/"127.0.0.1",
        std::chrono::seconds(0), /*tcp_no_delay=*/true);
    RegisterRpcService(server, *service);
    auto start_result = server.async_start();
    ASSERT_FALSE(start_result.hasResult());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const UUID client_id = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "batch_replica_clear_tenant_segment";
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    for (const std::string& tenant :
         {std::string(kTenant), std::string(TenantId::kDefaultValue)}) {
        auto put_start = service->PutStart(client_id, kSharedKey, kObjectSize,
                                           replicate_config, tenant);
        ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
        auto put_end = service->PutEnd(
            client_id, ObjectMeta{kSharedKey, std::nullopt},
            ReplicaType::MEMORY, tenant);
        ASSERT_TRUE(put_end.has_value()) << toString(put_end.error());
    }

    {
        MasterClient tenant_client(client_id, nullptr, kTenant);
        ASSERT_EQ(tenant_client.Connect("127.0.0.1:" +
                                        std::to_string(ports.front())),
                  ErrorCode::OK);
        auto cleared = tenant_client.BatchReplicaClear({kSharedKey}, client_id,
                                                        "");
        ASSERT_TRUE(cleared.has_value()) << toString(cleared.error());
        EXPECT_EQ(cleared.value(), std::vector<std::string>{kSharedKey});
    }

    auto tenant_exists = service->ExistKey(kSharedKey, kTenant);
    ASSERT_TRUE(tenant_exists.has_value());
    EXPECT_FALSE(tenant_exists.value());
    auto default_exists =
        service->ExistKey(kSharedKey,
                          std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(default_exists.has_value());
    EXPECT_TRUE(default_exists.value());

    {
        MasterClient default_client(client_id);
        ASSERT_EQ(default_client.Connect("127.0.0.1:" +
                                         std::to_string(ports.front())),
                  ErrorCode::OK);
        auto cleared = default_client.BatchReplicaClear({kSharedKey}, client_id,
                                                         "");
        ASSERT_TRUE(cleared.has_value()) << toString(cleared.error());
        EXPECT_EQ(cleared.value(), std::vector<std::string>{kSharedKey});
    }

    default_exists = service->ExistKey(
        kSharedKey, std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(default_exists.has_value());
    EXPECT_FALSE(default_exists.value());

    server.stop();
}

}  // namespace
}  // namespace mooncake
