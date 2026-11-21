#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "dummy_client.h"
#include "master_client.h"
#include "rpc_service.h"

namespace mooncake {
namespace {

constexpr size_t kMiB = 1024 * 1024;
constexpr size_t kCapacity = 16 * kMiB;
constexpr char kHomeEndpoint[] = "127.0.0.1:21001";
constexpr char kOtherEndpoint[] = "127.0.0.1:21002";

class StrictPlacementRpc : public ::testing::Test {
   protected:
    void Start(bool legacy = false) {
        WrappedMasterServiceConfig config;
        config.enable_metric_reporting = false;
        config.default_kv_lease_ttl = 5000;
        config.memory_allocator = BufferAllocatorType::OFFSET;
        service = std::make_unique<WrappedMasterService>(config);
        server = std::make_unique<coro_rpc::coro_rpc_server>(
            2, 0, "127.0.0.1", std::chrono::seconds(0), true);
        if (legacy) {
            server->register_handler<&WrappedMasterService::ServiceReady,
                                     &WrappedMasterService::PutStart,
                                     &WrappedMasterService::BatchPutStart>(
                service.get());
        } else {
            RegisterRpcService(*server, *service);
        }
        auto started = server->async_start();
        ASSERT_FALSE(started.hasResult());
        ASSERT_GT(server->port(), 0);
        ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server->port())),
                  ErrorCode::OK);
        ASSERT_NO_FATAL_FAILURE(Mount("home", 0x300000000, kHomeEndpoint));
        ASSERT_NO_FATAL_FAILURE(Mount("other", 0x400000000, kOtherEndpoint));
    }

    void Mount(const std::string& name, uintptr_t base,
               const std::string& endpoint) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.host_id = name + "-host";
        segment.base = base;
        segment.size = kCapacity;
        segment.te_endpoint = endpoint;
        segment.protocol = "tcp";
        ASSERT_TRUE(service->MountSegment(segment, writer));
    }

    void ExpectUsage(const std::string& name, size_t bytes) {
        const auto usage = service->QuerySegmentForAdmin(name);
        ASSERT_TRUE(usage);
        EXPECT_EQ(usage->first, bytes);
    }

    void ExpectEndpoint(const std::vector<Replica::Descriptor>& replicas,
                        const std::string& endpoint) {
        ASSERT_EQ(replicas.size(), 1u);
        ASSERT_TRUE(replicas.front().is_memory_replica());
        EXPECT_EQ(replicas.front()
                      .get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  endpoint);
    }

    std::unique_ptr<WrappedMasterService> service;
    std::unique_ptr<coro_rpc::coro_rpc_server> server;
    UUID writer = generate_uuid();
    MasterClient client{writer};
};

class TwoKeyBatchPlacement : public StrictPlacementRpc,
                             public ::testing::WithParamInterface<bool> {};

TEST_P(TwoKeyBatchPlacement, OnlyBestEffortSpillsTheSecondKey) {
    ASSERT_NO_FATAL_FAILURE(Start());
    const bool strict = GetParam();
    ReplicateConfig config;
    config.preferred_segments = {"home"};
    if (strict) config.allowed_memory_segments = config.preferred_segments;
    const auto results = client.BatchPutStart(
        {"first", "second"}, {{12 * kMiB}, {8 * kMiB}}, config);
    ASSERT_EQ(results.size(), 2u);
    ASSERT_TRUE(results[0]);
    ExpectEndpoint(*results[0], kHomeEndpoint);
    if (strict) {
        ASSERT_FALSE(results[1]);
        EXPECT_EQ(results[1].error(), ErrorCode::NO_AVAILABLE_HANDLE);
    } else {
        ASSERT_TRUE(results[1]);
        ExpectEndpoint(*results[1], kOtherEndpoint);
        ASSERT_TRUE(
            client.PutEnd({"second", std::nullopt}, ReplicaType::MEMORY));
    }
    ASSERT_TRUE(client.PutEnd({"first", std::nullopt}, ReplicaType::MEMORY));
    const auto first = client.GetReplicaList("first");
    ASSERT_TRUE(first);
    ExpectEndpoint(first->replicas, kHomeEndpoint);
    const auto second = client.GetReplicaList("second");
    if (strict) {
        ASSERT_FALSE(second);
        EXPECT_EQ(second.error(), ErrorCode::OBJECT_NOT_FOUND);
    } else {
        ASSERT_TRUE(second);
        ExpectEndpoint(second->replicas, kOtherEndpoint);
    }
    ExpectUsage("home", 12 * kMiB);
    ExpectUsage("other", strict ? 0 : 8 * kMiB);
}

INSTANTIATE_TEST_SUITE_P(PlacementMode, TwoKeyBatchPlacement, ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& mode) {
                             return mode.param ? "Strict" : "BestEffort";
                         });

TEST_F(StrictPlacementRpc, ExhaustionDoesNotSpillOrStopTheBatch) {
    ASSERT_NO_FATAL_FAILURE(Start());
    ReplicateConfig config;
    config.allowed_memory_segments = std::vector<std::string>{"home"};
    config.preferred_segment = "other";
    config.host_id = "other-host";
    config.prefer_alloc_in_same_node = true;
    config.with_hard_pin = true;
    auto results =
        client.BatchPutStart({"first", "no-room", "later"},
                             {{12 * kMiB}, {8 * kMiB}, {kMiB}}, config);
    ASSERT_EQ(results.size(), 3u);
    ASSERT_TRUE(results[0]);
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::NO_AVAILABLE_HANDLE);
    ASSERT_TRUE(results[2]);
    ExpectEndpoint(*results[0], kHomeEndpoint);
    ExpectEndpoint(*results[2], kHomeEndpoint);
    ExpectUsage("home", 13 * kMiB);
    ExpectUsage("other", 0);
    const auto completed =
        client.BatchPutEnd({{"first", std::nullopt}, {"later", std::nullopt}},
                           ReplicaType::MEMORY);
    ASSERT_EQ(completed.size(), 2u);
    EXPECT_TRUE(completed[0]);
    EXPECT_TRUE(completed[1]);
    auto missing = client.GetReplicaList("no-room");
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);

    config.allowed_memory_segments.reset();
    config.preferred_segment = "home";
    auto soft = client.PutStart("no-room", {8 * kMiB}, config);
    ASSERT_TRUE(soft);
    ExpectEndpoint(*soft, kOtherEndpoint);
}

TEST_F(StrictPlacementRpc, SinglePutUsesLogicalNamesAndValidatesTheList) {
    ASSERT_NO_FATAL_FAILURE(Start());
    ReplicateConfig config;
    for (const auto& names : std::vector<std::vector<std::string>>{
             {}, {"home", ""}, {kHomeEndpoint}, {"unmounted"}}) {
        config.allowed_memory_segments = names;
        auto result = client.PutStart("key", {kMiB}, config);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), names.empty() || names.back().empty()
                                      ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::NO_AVAILABLE_HANDLE);
    }
    ExpectUsage("home", 0);
    ExpectUsage("other", 0);
    config.allowed_memory_segments =
        std::vector<std::string>{"unmounted", "other"};
    config.dfs_replica_num = 1;
    auto mixed = client.PutStart("key", {kMiB}, config);
    ASSERT_FALSE(mixed);
    EXPECT_EQ(mixed.error(), ErrorCode::INVALID_PARAMS);
    config.dfs_replica_num = 0;
    auto upsert = client.UpsertStart("key", {kMiB}, config);
    ASSERT_FALSE(upsert);
    EXPECT_EQ(upsert.error(), ErrorCode::INVALID_PARAMS);
    auto placed = client.PutStart("key", {kMiB}, config);
    ASSERT_TRUE(placed);
    ExpectEndpoint(*placed, kOtherEndpoint);
}

TEST_F(StrictPlacementRpc, OldMethodSetRejectsStrictButServesBestEffort) {
    ASSERT_NO_FATAL_FAILURE(Start(true));
    ReplicateConfig config;
    config.allowed_memory_segments = std::vector<std::string>{"home"};
    auto single = client.PutStart("one", {kMiB}, config);
    ASSERT_FALSE(single);
    EXPECT_EQ(single.error(), ErrorCode::RPC_FAIL);
    auto batch =
        client.BatchPutStart({"two", "three"}, {{kMiB}, {kMiB}}, config);
    ASSERT_EQ(batch.size(), 2u);
    for (const auto& result : batch) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
    }
    ExpectUsage("home", 0);
    ExpectUsage("other", 0);
    config.allowed_memory_segments.reset();
    config.preferred_segment = "home";
    auto soft = client.PutStart("one", {kMiB}, config);
    ASSERT_TRUE(soft);
    ExpectEndpoint(*soft, kHomeEndpoint);
    auto soft_batch = client.BatchPutStart({"two"}, {{kMiB}}, config);
    ASSERT_EQ(soft_batch.size(), 1u);
    ASSERT_TRUE(soft_batch[0]);
    ExpectEndpoint(*soft_batch[0], kHomeEndpoint);
}

TEST(StrictPlacementProxy, RejectsBeforeContactingAnUnversionedProxy) {
    DummyClient proxy;
    ReplicateConfig config;
    config.allowed_memory_segments = std::vector<std::string>{"home"};
    EXPECT_EQ(proxy.put("key", "value", config),
              toInt(ErrorCode::INVALID_PARAMS));
    std::vector<CudaIpcWriteRequest> requests(2);
    EXPECT_EQ(
        proxy.batch_put_from_cuda_ipc(requests, config),
        std::vector<int>(requests.size(), toInt(ErrorCode::INVALID_PARAMS)));
}

}  // namespace
}  // namespace mooncake
