#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/network.h"
#include "p2p/master/p2p_master_client.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "ha/oplog/mock_oplog_store.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/p2p_status_http_server.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"

// Standard-library and YLT dependencies are included before exposing only the
// master's private runner, following the existing master test convention.
#define private public
#include "p2p/master/p2p_master.h"
#undef private

namespace mooncake::testing {
namespace {

// Exercise the concrete client/server boundary without the public Client
// facade or client data path in the link graph.
struct TestHttpResponse {
    int status;
    std::string body;
};

TestHttpResponse HttpPostForTest(const std::string& url) {
    coro_http::coro_http_client client;
    auto response =
        client.post(url, "", coro_http::req_content_type::octet_stream);
    return {response.status, std::string(response.resp_body)};
}

class P2PMasterRpcTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("p2p_master_rpc_test");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        P2PMasterConfig config;
        config.metrics.enable_reporting = false;
        config.metrics.http_port = 0;
        config.routes.max_clients_per_key = 0;
        config.client_lifecycle.live_ttl_seconds = 60;
        config.client_lifecycle.crashed_ttl_seconds = 180;
        service_ = std::make_unique<P2PMasterRpcService>(config, kViewVersion);

        const int port = getFreeTcpPort();
        ASSERT_GT(port, 0);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, port, "127.0.0.1", std::chrono::seconds(0), true);
        RegisterP2PRpcService(*server_, *service_);
        auto started = server_->async_start();
        ASSERT_FALSE(started.hasResult()) << "P2P RPC server failed to start";

        client_id_ = generate_uuid();
        client_ = std::make_unique<P2PMasterClient>(client_id_);
        ASSERT_EQ(client_->Connect("127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        first_ = MakeSegment("first");
        second_ = MakeSegment("second");
        auto registered =
            client_->RegisterClient({.client_id = client_id_,
                                     .segments = {first_, second_},
                                     .ip_address = "127.0.0.1",
                                     .rpc_port = kClientPort});
        ASSERT_TRUE(registered.has_value()) << registered.error();
        EXPECT_EQ(*registered, kViewVersion);
        ASSERT_TRUE(
            client_
                ->Heartbeat({.client_id = client_id_,
                             .service_state = P2PClientServiceState::ONLINE})
                .has_value());
    }

    void TearDown() override {
        client_.reset();
        if (server_) {
            server_->stop();
            server_.reset();
        }
        service_.reset();
    }

    static P2PSegment MakeSegment(std::string name) {
        P2PSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.size = kSegmentSize;
        segment.memory_type = MemoryType::DRAM;
        return segment;
    }

    auto Publish(std::string_view key, const UUID& segment_id,
                 uint64_t size = 1024) {
        return client_->PublishRoute({.key = key,
                                      .object_size = size,
                                      .client_id = client_id_,
                                      .segment_id = segment_id});
    }

    static constexpr ViewVersionId kViewVersion = 42;
    static constexpr uint16_t kClientPort = 50052;
    static constexpr size_t kSegmentSize = 8192;
    std::unique_ptr<P2PMasterRpcService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<P2PMasterClient> client_;
    UUID client_id_;
    P2PSegment first_;
    P2PSegment second_;
};

TEST_F(P2PMasterRpcTest, ClientQueriesRoundTripWithoutReadFiltering) {
    ASSERT_TRUE(Publish("owners", first_.id).has_value());
    ASSERT_TRUE(Publish("owners", second_.id).has_value());
    ASSERT_TRUE(
        client_
            ->Heartbeat({.client_id = client_id_,
                         .service_state = P2PClientServiceState::LOCAL_ONLY})
            .has_value());
    auto owners = client_->GetKeyClients("owners");
    ASSERT_TRUE(owners.has_value());
    ASSERT_EQ(owners->size(), 1u);
    EXPECT_EQ(owners->front().client_id, client_id_);
    EXPECT_EQ(owners->front().ip_address, "127.0.0.1");
    EXPECT_EQ(owners->front().rpc_port, kClientPort);
    auto all = client_->ListClients();
    ASSERT_TRUE(all.has_value());
    ASSERT_EQ(all->size(), 1u);
    EXPECT_EQ(all->front().client_id, client_id_);
    EXPECT_EQ(all->front().rpc_port, kClientPort);
    auto missing = client_->GetKeyClients("absent");
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_TRUE(client_->UnregisterClient(client_id_).has_value());
    all = client_->ListClients();
    ASSERT_TRUE(all.has_value());
    EXPECT_TRUE(all->empty());
}

TEST_F(P2PMasterRpcTest, BatchQueryIpPreservesPartialResultsAndItemMetrics) {
    auto& metrics = P2PMasterMetricManager::instance();
    const auto requests = metrics.get_batch_query_ip_requests();
    const auto failures = metrics.get_batch_query_ip_failures();
    const auto partial = metrics.get_batch_query_ip_partial_successes();
    const auto items = metrics.get_batch_query_ip_items();
    const auto failed_items = metrics.get_batch_query_ip_failed_items();
    const UUID missing = generate_uuid();

    auto result = client_->BatchQueryIp({client_id_, missing, client_id_});
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 1u);
    ASSERT_EQ(result->count(client_id_), 1u);
    EXPECT_EQ(result->at(client_id_), (std::vector<std::string>{"127.0.0.1"}));
    EXPECT_EQ(result->count(missing), 0u);
    EXPECT_EQ(metrics.get_batch_query_ip_requests(), requests + 1);
    EXPECT_EQ(metrics.get_batch_query_ip_items(), items + 3);
    EXPECT_EQ(metrics.get_batch_query_ip_partial_successes(), partial + 1);
    EXPECT_EQ(metrics.get_batch_query_ip_failed_items(), failed_items + 1);
    EXPECT_EQ(metrics.get_batch_query_ip_failures(), failures);

    auto empty = client_->BatchQueryIp({});
    ASSERT_TRUE(empty.has_value()) << empty.error();
    EXPECT_TRUE(empty->empty());
    EXPECT_EQ(metrics.get_batch_query_ip_requests(), requests + 2);
    EXPECT_EQ(metrics.get_batch_query_ip_items(), items + 3);
    EXPECT_EQ(metrics.get_batch_query_ip_failures(), failures);

    auto absent = client_->BatchQueryIp({missing, missing});
    ASSERT_TRUE(absent.has_value()) << absent.error();
    EXPECT_TRUE(absent->empty());
    EXPECT_EQ(metrics.get_batch_query_ip_requests(), requests + 3);
    EXPECT_EQ(metrics.get_batch_query_ip_items(), items + 5);
    EXPECT_EQ(metrics.get_batch_query_ip_failures(), failures + 1);
    EXPECT_EQ(metrics.get_batch_query_ip_partial_successes(), partial + 1);
    EXPECT_EQ(metrics.get_batch_query_ip_failed_items(), failed_items + 3);
}

TEST_F(P2PMasterRpcTest, MixedSyncPreservesItemErrorsAndReadOrder) {
    ASSERT_TRUE(Publish("existing", first_.id).has_value());
    auto& metrics = P2PMasterMetricManager::instance();
    const auto published = metrics.get_publish_route_requests();
    const auto publish_failures = metrics.get_publish_route_failures();
    const auto withdrawn = metrics.get_withdraw_route_requests();
    const auto withdraw_failures = metrics.get_withdraw_route_failures();
    {
        // Request string_views must not become stored route-key references.
        const std::string new_key = "new-key";
        P2PBatchSyncRoutesRequest request;
        request.client_id = client_id_;
        request.publish_operations = {
            {.key = new_key, .object_size = 2048, .segment_id = first_.id},
            {.key = "bad-segment",
             .object_size = 1024,
             .segment_id = generate_uuid()},
            {.key = "cycle", .object_size = 1024, .segment_id = second_.id}};
        request.withdraw_operations = {
            {.key = "existing", .segment_id = first_.id},
            {.key = "missing", .segment_id = first_.id},
            {.key = "cycle", .segment_id = second_.id}};
        auto synced = client_->BatchSyncRoutes(request);
        ASSERT_TRUE(synced.has_value()) << synced.error();
        EXPECT_EQ(
            synced->publish_results,
            (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::SEGMENT_NOT_FOUND,
                                    ErrorCode::OK}));
        EXPECT_EQ(synced->withdraw_results,
                  (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::OK,
                                          ErrorCode::OK}));
    }

    EXPECT_EQ(metrics.get_publish_route_requests(), published + 3);
    EXPECT_EQ(metrics.get_publish_route_failures(), publish_failures + 1);
    EXPECT_EQ(metrics.get_withdraw_route_requests(), withdrawn + 3);
    EXPECT_EQ(metrics.get_withdraw_route_failures(), withdraw_failures);

    const auto routes = client_->BatchGetReadRoute(
        {"new-key", "existing", "bad-segment", "cycle", "new-key"}, {});
    ASSERT_EQ(routes.size(), 5u);
    for (size_t index : {0u, 4u}) {
        ASSERT_TRUE(routes[index].has_value()) << routes[index].error();
        ASSERT_EQ(routes[index]->size(), 1u);
        const auto& route = routes[index]->front();
        EXPECT_EQ(route.client_id, client_id_);
        EXPECT_EQ(route.segment_id, first_.id);
        EXPECT_EQ(route.ip_address, "127.0.0.1");
        EXPECT_EQ(route.rpc_port, kClientPort);
        EXPECT_EQ(route.object_size, 2048u);
    }
    for (size_t index : {1u, 2u, 3u}) {
        ASSERT_FALSE(routes[index].has_value());
        EXPECT_EQ(routes[index].error(), ErrorCode::OBJECT_NOT_FOUND);
    }

    auto regex = client_->GetReadRouteByRegex("^new-key$");
    ASSERT_TRUE(regex.has_value()) << regex.error();
    ASSERT_EQ(regex->size(), 1u);
    ASSERT_EQ(regex->count("new-key"), 1u);
    ASSERT_EQ(regex->at("new-key").size(), 1u);
    EXPECT_EQ(regex->at("new-key").front().object_size, 2048u);
    auto invalid_regex = client_->GetReadRouteByRegex("[");
    ASSERT_FALSE(invalid_regex.has_value());
    EXPECT_EQ(invalid_regex.error(), ErrorCode::INVALID_PARAMS);

    auto async_routes = async_simple::coro::syncAwait(
        client_->AsyncGetReadRoute("new-key", {}));
    ASSERT_TRUE(async_routes.has_value()) << async_routes.error();
    ASSERT_EQ(async_routes->size(), 1u);
    EXPECT_EQ(async_routes->front().object_size, 2048u);
}

TEST_F(P2PMasterRpcTest, BatchWithdrawMissingAndRepeatedLocationsIsIdempotent) {
    ASSERT_TRUE(Publish("shared", first_.id).has_value());
    ASSERT_TRUE(Publish("shared", second_.id).has_value());
    auto& metrics = P2PMasterMetricManager::instance();
    const auto requests = metrics.get_batch_withdraw_route_requests();
    const auto items = metrics.get_batch_withdraw_route_items();
    const auto failures = metrics.get_batch_withdraw_route_failures();
    const auto failed_items = metrics.get_batch_withdraw_route_failed_items();
    const auto partial = metrics.get_batch_withdraw_route_partial_successes();
    P2PBatchWithdrawRouteRequest request{
        .key = "shared",
        .client_id = client_id_,
        .segment_ids = {first_.id, generate_uuid(), first_.id, second_.id}};
    for (std::string_view key : {"shared", "shared", "missing-key"}) {
        request.key = key;
        auto withdrawn = client_->BatchWithdrawRoute(request);
        ASSERT_EQ(withdrawn.size(), request.segment_ids.size());
        for (const auto& result : withdrawn) {
            EXPECT_TRUE(result.has_value());
        }
    }
    auto exists = client_->ExistKey("shared");
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
    EXPECT_EQ(metrics.get_batch_withdraw_route_requests(), requests + 3);
    EXPECT_EQ(metrics.get_batch_withdraw_route_items(), items + 12);
    EXPECT_EQ(metrics.get_batch_withdraw_route_failures(), failures);
    EXPECT_EQ(metrics.get_batch_withdraw_route_failed_items(), failed_items);
    EXPECT_EQ(metrics.get_batch_withdraw_route_partial_successes(), partial);
}

TEST_F(P2PMasterRpcTest, BatchReadRoutesWaitForReadyAndPreserveOrder) {
    const auto cold = generate_uuid();
    const auto segment = MakeSegment("not-ready");
    P2PRegisterClientRequest registration;
    registration.client_id = cold;
    registration.segments = {segment};
    registration.ip_address = "127.0.0.1";
    registration.rpc_port = 50099;
    ASSERT_TRUE(client_->RegisterClient(registration).has_value());
    P2PPublishRouteRequest publish;
    publish.key = "ready-barrier";
    publish.client_id = cold;
    publish.segment_id = segment.id;
    publish.object_size = 64;
    ASSERT_TRUE(client_->PublishRoute(publish).has_value());
    auto heartbeat = client_->Heartbeat({.client_id = cold});
    ASSERT_TRUE(heartbeat.has_value());
    auto routes = client_->BatchGetReadRoute({"ready-barrier", "missing"}, {});
    ASSERT_EQ(routes.size(), 2);
    ASSERT_FALSE(routes[0].has_value());
    ASSERT_FALSE(routes[1].has_value());
    EXPECT_EQ(routes[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(routes[1].error(), ErrorCode::OBJECT_NOT_FOUND);

    ASSERT_TRUE(
        client_
            ->Heartbeat({.client_id = cold,
                         .service_state = P2PClientServiceState::ONLINE})
            .has_value());
    routes = client_->BatchGetReadRoute({"ready-barrier", "missing"}, {});
    ASSERT_EQ(routes.size(), 2);
    ASSERT_TRUE(routes[0].has_value());
    ASSERT_FALSE(routes[1].has_value());
    EXPECT_EQ(routes[1].error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_EQ(routes[0]->size(), 1);
    EXPECT_EQ(routes[0]->front().client_id, cold);
    heartbeat = client_->Heartbeat(
        {.client_id = cold, .service_state = P2PClientServiceState::ONLINE});
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_TRUE(client_->UnregisterClient(cold).has_value());
}

TEST_F(P2PMasterRpcTest, LifecycleRemovesRoutesAndUpdatesClientStatus) {
    const auto extra = MakeSegment("extra");
    ASSERT_TRUE(client_->MountSegment(extra).has_value());
    ASSERT_TRUE(Publish("unmounted", extra.id).has_value());
    ASSERT_TRUE(Publish("unregistered", first_.id).has_value());
    ASSERT_TRUE(client_->UnmountSegment(extra.id).has_value());
    auto exists = client_->BatchExistKey({"unmounted", "unregistered"});
    ASSERT_EQ(exists.size(), 2u);
    for (const auto& result : exists) {
        ASSERT_TRUE(result.has_value()) << result.error();
    }
    EXPECT_FALSE(*exists[0]);
    EXPECT_TRUE(*exists[1]);

    auto heartbeat =
        client_->Heartbeat({.client_id = client_id_,
                            .service_state = P2PClientServiceState::ONLINE});
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error();
    EXPECT_EQ(heartbeat->view_version, kViewVersion);
    EXPECT_EQ(heartbeat->status, P2PClientStatus::HEALTH);
    auto status = client_->QueryClientStatus(client_id_);
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_EQ(*status, P2PClientStatus::HEALTH);
    auto unregistered = client_->UnregisterClient(client_id_);
    ASSERT_TRUE(unregistered.has_value()) << unregistered.error();
    EXPECT_EQ(*unregistered, kViewVersion);
    auto missing = client_->GetReadRoute("unregistered", {});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    status = client_->QueryClientStatus(client_id_);
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_EQ(*status, P2PClientStatus::UNDEFINED);
}

TEST_F(P2PMasterRpcTest, BatchWriteReturnsAlignedCandidateAndValidationErrors) {
    auto& metrics = P2PMasterMetricManager::instance();
    const auto requests = metrics.get_batch_get_write_route_requests();
    const auto items = metrics.get_batch_get_write_route_items();
    const auto failures = metrics.get_batch_get_write_route_failures();
    const auto partial = metrics.get_batch_get_write_route_partial_successes();
    const auto failed_items = metrics.get_batch_get_write_route_failed_items();
    P2PBatchGetWriteRouteRequest request;
    request.client_id = client_id_;
    request.keys = {"fits", "too-large"};
    request.object_sizes = {1024, 4 * kSegmentSize};
    request.config.remote_weight = 0.0;
    auto candidates = client_->BatchGetWriteRoute(request);
    ASSERT_TRUE(candidates.has_value()) << candidates.error();
    ASSERT_EQ(candidates->responses.size(), 2u);
    EXPECT_EQ(candidates->error_codes,
              (std::vector<ErrorCode>{ErrorCode::OK,
                                      ErrorCode::NO_AVAILABLE_CANDIDATE}));
    ASSERT_EQ(candidates->responses[0].size(), 1u);
    EXPECT_EQ(candidates->responses[0][0].client_id, client_id_);
    EXPECT_TRUE(candidates->responses[1].empty());

    request.object_sizes.pop_back();
    auto invalid = client_->BatchGetWriteRoute(request);
    ASSERT_TRUE(invalid.has_value()) << invalid.error();
    ASSERT_EQ(invalid->responses.size(), request.keys.size());
    EXPECT_EQ(invalid->error_codes,
              (std::vector<ErrorCode>{ErrorCode::INVALID_PARAMS,
                                      ErrorCode::INVALID_PARAMS}));
    for (const auto& response : invalid->responses) {
        EXPECT_TRUE(response.empty());
    }
    EXPECT_EQ(metrics.get_batch_get_write_route_requests(), requests + 2);
    EXPECT_EQ(metrics.get_batch_get_write_route_items(), items + 4);
    EXPECT_EQ(metrics.get_batch_get_write_route_failures(), failures + 1);
    EXPECT_EQ(metrics.get_batch_get_write_route_partial_successes(),
              partial + 1);
    EXPECT_EQ(metrics.get_batch_get_write_route_failed_items(),
              failed_items + 3);
}

class GatedOpLogStore final : public mooncake::test::MockOpLogStore {
   public:
    GatedOpLogStore()
        : release_(release_promise_.get_future().share()),
          write_entered_(write_entered_promise_.get_future()),
          second_write_(second_write_promise_.get_future()) {}

    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync) override {
        bool block = false;
        {
            std::lock_guard<std::mutex> lock(event_mutex_);
            ++write_events_;
            if (entry.op_type == OpType_PUBLISH_ROUTE) {
                ++publish_write_events_;
            }
            if (draining_) {
                ++writes_after_drain_;
            }
            if (entry.op_type == OpType_PUBLISH_ROUTE &&
                !first_write_entered_) {
                first_write_entered_ = true;
                block = true;
                write_entered_promise_.set_value();
            } else if (entry.op_type == OpType_PUBLISH_ROUTE &&
                       publish_write_events_ == 2) {
                second_write_promise_.set_value();
            }
        }
        if (block) {
            release_.wait();
        }
        return mooncake::test::MockOpLogStore::WriteOpLog(entry, sync);
    }

    OpLogDrainResult DrainWriting(uint32_t timeout_ms) override {
        {
            std::lock_guard<std::mutex> lock(event_mutex_);
            ++drain_events_;
            drain_timeout_ms_ = timeout_ms;
            draining_ = true;
        }
        return mooncake::test::MockOpLogStore::DrainWriting(timeout_ms);
    }

    void Release() {
        std::call_once(release_once_, [this] { release_promise_.set_value(); });
    }

    std::future_status WaitForFirstWrite(std::chrono::seconds timeout) {
        return write_entered_.wait_for(timeout);
    }

    std::future_status WaitForSecondWrite(std::chrono::seconds timeout) {
        return second_write_.wait_for(timeout);
    }

    size_t Drains() const {
        std::lock_guard<std::mutex> lock(event_mutex_);
        return drain_events_;
    }

    uint32_t DrainTimeoutMs() const {
        std::lock_guard<std::mutex> lock(event_mutex_);
        return drain_timeout_ms_;
    }

    size_t Writes() const {
        std::lock_guard<std::mutex> lock(event_mutex_);
        return write_events_;
    }

    size_t WritesAfterDrain() const {
        std::lock_guard<std::mutex> lock(event_mutex_);
        return writes_after_drain_;
    }

   private:
    std::promise<void> release_promise_;
    uint32_t drain_timeout_ms_{0};
    std::shared_future<void> release_;
    std::promise<void> write_entered_promise_;
    std::promise<void> second_write_promise_;
    std::shared_future<void> write_entered_;
    std::shared_future<void> second_write_;
    std::once_flag release_once_;
    mutable std::mutex event_mutex_;
    size_t write_events_{0};
    size_t publish_write_events_{0};
    size_t drain_events_{0};
    size_t writes_after_drain_{0};
    bool first_write_entered_{false};
    bool draining_{false};
};

class GatedRpcHandler {
   public:
    GatedRpcHandler()
        : release_(release_promise_.get_future().share()),
          entered_(entered_promise_.get_future()) {}

    bool WaitForRelease() {
        entered_promise_.set_value();
        release_.wait();
        exited_ = true;
        return true;
    }

    void Release() {
        std::call_once(release_once_, [this] { release_promise_.set_value(); });
    }

    std::future_status WaitForEntry(std::chrono::seconds timeout) {
        return entered_.wait_for(timeout);
    }

    bool HasExited() const { return exited_.load(); }

   private:
    std::promise<void> entered_promise_;
    std::promise<void> release_promise_;
    std::shared_future<void> release_;
    std::future<void> entered_;
    std::once_flag release_once_;
    std::atomic<bool> exited_{false};
};

class P2PMasterRpcShutdownTest : public ::testing::TestWithParam<bool> {
   public:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("p2p_master_rpc_shutdown_test");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

   protected:
    void SetUp() override {
        P2PMasterConfig config;
        config.rpc.address = "127.0.0.1";
        config.rpc.port = 0;
        config.rpc.thread_num = 2;
        config.metrics.enable_reporting = false;
        config.metrics.http_port = 0;
        if (GetParam()) {
            const int port = getFreeTcpPort();
            ASSERT_GT(port, 0);
            config.rpc.heartbeat_port = port;
        }
        heartbeat_port_ = config.rpc.heartbeat_port;
        master_ = std::make_unique<P2PMaster>(config);
        service_ = std::make_unique<P2PMasterRpcService>(config);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.rpc.thread_num, config.rpc.port, config.rpc.address);
        server_->register_handler<&GatedRpcHandler::WaitForRelease>(&handler_);

        std::packaged_task<int()> run([this] {
            return master_->RunActiveRpcServers(
                *server_, *service_, {},
                [this] {
                    handler_exited_before_drain_ = handler_.HasExited();
                    after_stop_called_ = true;
                },
                [this] { return shutdown_requested_.load(); });
        });
        run_result_ = run.get_future();
        run_thread_ = std::thread(std::move(run));

        // An ephemeral port is published only after the listener is bound.
        const auto deadline = std::chrono::steady_clock::now() + kTimeout;
        while (server_->port() == 0 &&
               std::chrono::steady_clock::now() < deadline &&
               run_result_.wait_for(std::chrono::milliseconds(1)) ==
                   std::future_status::timeout) {
        }
        ASSERT_NE(server_->port(), 0);
        client_ = std::make_unique<coro_rpc::coro_rpc_client>();
        auto connected = async_simple::coro::syncAwait(client_->connect(
            "127.0.0.1", std::to_string(server_->port()), kTimeout));
        ASSERT_FALSE(connected) << connected.message();
    }

    void TearDown() override {
        // Always unblock the handler before joining stop(), including after
        // ASSERT failures. Keep the service alive until every RPC is drained.
        handler_.Release();
        if (server_) {
            server_->stop();
        }
        for (auto* thread : {&request_thread_, &stop_thread_, &run_thread_}) {
            if (thread->joinable()) {
                thread->join();
            }
        }
        client_.reset();
        server_.reset();
        service_.reset();
        master_.reset();
    }

    bool WaitForListenerClosed(uint16_t port) {
        asio::io_context context;
        const asio::ip::tcp::endpoint endpoint(
            asio::ip::make_address("127.0.0.1"), port);
        const auto deadline = std::chrono::steady_clock::now() + kTimeout;
        while (std::chrono::steady_clock::now() < deadline) {
            asio::ip::tcp::socket socket(context);
            asio::error_code error;
            socket.connect(endpoint, error);
            if (error == asio::error::connection_refused) {
                return true;
            }
            if (error == asio::error::connection_reset) {
                // A probe can race with stop() closing an accepted socket.
                // Keep waiting for connection_refused to prove closure.
                LOG(WARNING)
                    << "Listener probe reset during shutdown: " << error;
            } else if (error) {
                LOG(ERROR) << "Unexpected listener probe failure: " << error;
                return false;
            }
            // Poll listener state; request/stop ordering is controlled by
            // gates.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        LOG(ERROR) << "Timed out waiting for listener to close on port "
                   << port;
        return false;
    }

    static constexpr auto kTimeout = std::chrono::seconds(5);
    uint16_t heartbeat_port_{0};
    GatedRpcHandler handler_;
    std::unique_ptr<P2PMaster> master_;
    std::unique_ptr<P2PMasterRpcService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<coro_rpc::coro_rpc_client> client_;
    std::thread run_thread_;
    std::thread request_thread_;
    std::thread stop_thread_;
    std::future<int> run_result_;
    std::future<void> request_result_;
    std::future<void> stop_result_;
    std::atomic<bool> shutdown_requested_{false};
    std::atomic<bool> after_stop_called_{false};
    bool handler_exited_before_drain_{false};
};

TEST_P(P2PMasterRpcShutdownTest, RunnerWaitsForInFlightRpcAfterListenerCloses) {
    std::packaged_task<void()> request([this] {
        // Closing the connection may fail the client call; the server handler
        // must still finish before the production runner returns.
        (void)async_simple::coro::syncAwait(
            client_->call_for<&GatedRpcHandler::WaitForRelease>(kTimeout));
    });
    request_result_ = request.get_future();
    request_thread_ = std::thread(std::move(request));
    ASSERT_EQ(handler_.WaitForEntry(kTimeout), std::future_status::ready);

    std::packaged_task<void()> stop([this] { server_->stop(); });
    stop_result_ = stop.get_future();
    stop_thread_ = std::thread(std::move(stop));
    ASSERT_TRUE(WaitForListenerClosed(server_->port()));

    EXPECT_EQ(run_result_.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout);
    EXPECT_EQ(stop_result_.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_FALSE(handler_.HasExited());
    EXPECT_FALSE(after_stop_called_.load());

    handler_.Release();
    ASSERT_EQ(run_result_.wait_for(kTimeout), std::future_status::ready);
    // An immediate stop can complete the start future before the runner's
    // startup check. Both existing exit classifications must drain the RPCs.
    const int run_result = run_result_.get();
    EXPECT_TRUE(run_result == 0 || run_result == -1);
    EXPECT_TRUE(handler_.HasExited());
    EXPECT_TRUE(after_stop_called_.load());
    EXPECT_TRUE(handler_exited_before_drain_);
    ASSERT_EQ(stop_result_.wait_for(kTimeout), std::future_status::ready);
    stop_result_.get();
    ASSERT_EQ(request_result_.wait_for(kTimeout), std::future_status::ready);
    request_result_.get();
    if (heartbeat_port_ > 0) {
        EXPECT_TRUE(WaitForListenerClosed(heartbeat_port_));
    }
}

TEST_P(P2PMasterRpcShutdownTest, ShutdownDrainsOnlyAfterInFlightRpcReturns) {
    std::packaged_task<void()> request([this] {
        (void)async_simple::coro::syncAwait(
            client_->call_for<&GatedRpcHandler::WaitForRelease>(kTimeout));
    });
    request_result_ = request.get_future();
    request_thread_ = std::thread(std::move(request));
    ASSERT_EQ(handler_.WaitForEntry(kTimeout), std::future_status::ready);
    shutdown_requested_ = true;
    ASSERT_TRUE(WaitForListenerClosed(server_->port()));
    EXPECT_FALSE(after_stop_called_.load());
    EXPECT_EQ(run_result_.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    handler_.Release();
    ASSERT_EQ(run_result_.wait_for(kTimeout), std::future_status::ready);
    EXPECT_EQ(run_result_.get(), 0);
    EXPECT_TRUE(after_stop_called_.load());
    EXPECT_TRUE(handler_exited_before_drain_);
    if (heartbeat_port_ > 0) {
        EXPECT_TRUE(WaitForListenerClosed(heartbeat_port_));
    }
}

INSTANTIATE_TEST_SUITE_P(HeartbeatModes, P2PMasterRpcShutdownTest,
                         ::testing::Bool());

TEST_F(P2PMasterRpcTest, StepdownDrainsOpLogAfterActiveWritesStop) {
    constexpr auto kTimeout = std::chrono::seconds(5);
    struct StatusResetGuard {
        ~StatusResetGuard() { HAStatusManager::instance().Reset(); }
    } reset_guard;

    P2PMasterConfig config;
    config.ha.enabled = true;
    config.rpc.address = "127.0.0.1";
    config.rpc.port = 0;
    config.rpc.thread_num = 2;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    config.routes.max_clients_per_key = 0;
    config.oplog.enabled = true;
    config.oplog.store_type = "localfs";
    config.oplog.data_dir =
        "/tmp/mooncake_p2p_rpc_stepdown_" + std::to_string(::getpid()) + "_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    config.cluster_id = "p2p_rpc_stepdown";

    P2PMaster master(config);
    auto service = std::make_unique<P2PMasterRpcService>(config, kViewVersion);
    auto oplog_store = std::make_shared<GatedOpLogStore>();
    auto* oplog_manager = service->GetMasterService().GetOpLogManager();
    ASSERT_NE(oplog_manager, nullptr);
    oplog_manager->SetOpLogStore(oplog_store);
    service->init();

    const int port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    auto server = std::make_unique<coro_rpc::coro_rpc_server>(
        2, port, "127.0.0.1", std::chrono::seconds(0), false);
    std::atomic<bool> stepdown_requested{false};
    std::atomic<uint32_t> drain_timeout_ms{0};
    service->SetStepdownCallback(
        [&server, &stepdown_requested, &drain_timeout_ms](
            const P2PMasterRpcService::StepdownRequest& request) {
            drain_timeout_ms.store(request.drain_timeout_ms);
            stepdown_requested.store(true, std::memory_order_seq_cst);
            server->stop();
        });

    std::packaged_task<int()> run([&master, &server, &service, oplog_manager,
                                   &drain_timeout_ms, &stepdown_requested] {
        return master.RunActiveRpcServers(
            *server, *service,
            [] {
                HAStatusManager::instance().SetRoleState("primary", "RUNNING");
            },
            [oplog_manager, &drain_timeout_ms] {
                P2PMaster::DrainOpLogBeforeStop(*oplog_manager,
                                                drain_timeout_ms.load());
            },
            /*stopping=*/{},
            [&stepdown_requested] { return stepdown_requested.load(); });
    });
    auto run_result = run.get_future();
    std::thread run_thread(std::move(run));
    while (server->port() == 0 && run_result.wait_for(std::chrono::milliseconds(
                                      1)) == std::future_status::timeout) {
    }
    ASSERT_NE(server->port(), 0);

    std::array<UUID, 2> client_ids{};
    std::array<std::unique_ptr<P2PMasterClient>, 2> clients;
    std::array<P2PSegment, 2> segments;
    for (size_t i = 0; i < clients.size(); ++i) {
        client_ids[i] = generate_uuid();
        clients[i] = std::make_unique<P2PMasterClient>(client_ids[i]);
        ASSERT_EQ(clients[i]->Connect("127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        segments[i] = MakeSegment("stepdown-oplog-" + std::to_string(i));
        ASSERT_TRUE(clients[i]
                        ->RegisterClient({.client_id = client_ids[i],
                                          .segments = {segments[i]},
                                          .ip_address = "127.0.0.1",
                                          .rpc_port = kClientPort})
                        .has_value());
    }
    // Establish both business connections before opening the separate
    // heartbeat connections. The server assigns connections round-robin to
    // two workers; interleaving heartbeats would put both writers on the same
    // worker, so the first blocked write would prevent the second from running.
    for (size_t i = 0; i < clients.size(); ++i) {
        ASSERT_TRUE(
            clients[i]
                ->Heartbeat({.client_id = client_ids[i],
                             .service_state = P2PClientServiceState::ONLINE})
                .has_value());
    }

    // The first oplog write blocks inside the accepted RPC. A second writer is
    // dispatched before stepdown so it is also an accepted, in-flight handler.
    std::vector<std::thread> writers;
    writers.emplace_back([&clients, &client_ids, &segments] {
        for (int i = 0;; ++i) {
            if (!clients[0]
                     ->PublishRoute({.key = "stepdown-0-" + std::to_string(i),
                                     .object_size = 1024,
                                     .client_id = client_ids[0],
                                     .segment_id = segments[0].id})
                     .has_value()) {
                break;
            }
        }
    });
    EXPECT_EQ(oplog_store->WaitForFirstWrite(kTimeout),
              std::future_status::ready);
    writers.emplace_back([&clients, &client_ids, &segments] {
        for (int i = 0;; ++i) {
            if (!clients[1]
                     ->PublishRoute({.key = "stepdown-1-" + std::to_string(i),
                                     .object_size = 1024,
                                     .client_id = client_ids[1],
                                     .segment_id = segments[1].id})
                     .has_value()) {
                break;
            }
        }
    });

    EXPECT_EQ(oplog_store->WaitForSecondWrite(kTimeout),
              std::future_status::ready);

    std::thread stepdown([&service] {
        const auto response = HttpPostForTest(
            "http://127.0.0.1:" + std::to_string(service->GetHttpPort()) +
            "/ha/stepdown?cooldown_sec=0&drain_timeout_ms=1234");
        EXPECT_EQ(response.status, 200);
    });
    while (!stepdown_requested.load(std::memory_order_seq_cst) &&
           run_result.wait_for(std::chrono::milliseconds(1)) ==
               std::future_status::timeout) {
    }
    EXPECT_TRUE(stepdown_requested.load(std::memory_order_seq_cst));

    // The blocked RPC handlers must keep the runner active; drain must not
    // start until every accepted write handler has returned.
    EXPECT_EQ(run_result.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout);
    EXPECT_EQ(oplog_store->Drains(), 0u);
    oplog_store->Release();

    for (auto& writer : writers) {
        writer.join();
    }
    stepdown.join();
    EXPECT_EQ(run_result.get(), 0);
    EXPECT_GT(oplog_store->Drains(), 0u);
    EXPECT_EQ(oplog_store->DrainTimeoutMs(), 1234u);
    EXPECT_GT(oplog_store->Writes(), 0u);
    EXPECT_EQ(oplog_store->WritesAfterDrain(), 0u);

    run_thread.join();
}

TEST_F(P2PMasterRpcTest, StepdownDrainDiagnosticsSurviveDemotionAndRecover) {
    struct StatusResetGuard {
        ~StatusResetGuard() { HAStatusManager::instance().Reset(); }
    } reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "stepdown_diagnostics", "local");
    status.SetMasterView("local", 1);
    status.SetRoleState("primary", "RUNNING");

    struct ControlledDrainStore final : mooncake::test::MockOpLogStore {
        OpLogDrainResult result{false, 2, 1, 10, 13};
        uint32_t last_timeout_ms{0};
        OpLogDrainResult DrainWriting(uint32_t timeout_ms) override {
            last_timeout_ms = timeout_ms;
            return result;
        }
    };
    auto store = std::make_shared<ControlledDrainStore>();
    OpLogManager manager;
    manager.SetOpLogStore(store);
    P2PStatusHttpServer diagnostics(/*port=*/0);
    diagnostics.Start();
    ASSERT_NE(diagnostics.port(), 0);
    auto read_json = [&](const char* path) {
        coro_http::coro_http_client client;
        auto response = client.get(
            "http://127.0.0.1:" + std::to_string(diagnostics.port()) + path);
        EXPECT_EQ(response.status, 200) << response.resp_body;
        return std::string(response.resp_body);
    };
    auto read_doctor = [&] {
        HADoctorReport report;
        struct_json::from_json(report, read_json("/ha/doctor"));
        return report;
    };
    auto read_last_error = [&] {
        HAStatusSnapshot snapshot;
        struct_json::from_json(snapshot, read_json("/ha/status"));
        return snapshot.last_error;
    };
    auto find_drain = [](const HADoctorReport& report) {
        return std::find_if(report.issues.begin(), report.issues.end(),
                            [](const auto& issue) {
                                return issue.code == "OPLOG_DRAIN_INCOMPLETE";
                            });
    };

    // Exercise the same drain/diagnostic path as the real HA supervisor.
    P2PMaster::DrainOpLogBeforeStop(manager, 1234);
    EXPECT_EQ(store->last_timeout_ms, 1234u);
    auto report = read_doctor();
    auto issue = find_drain(report);
    ASSERT_NE(issue, report.issues.end());
    EXPECT_EQ(report.health, "degraded");
    EXPECT_EQ(issue->category, "oplog");
    EXPECT_EQ(issue->component, "oplog_writer");
    EXPECT_EQ(issue->severity, "warning");
    for (const auto* detail :
         {"drain_timeout_ms=1234", "queued=2", "inflight=1",
          "committed_sequence_id=10", "max_known_sequence_id=13"}) {
        EXPECT_NE(issue->message.find(detail), std::string::npos);
    }
    const auto drain_error = issue->message;
    EXPECT_EQ(read_last_error(), drain_error);

    // Reproduce demotion and the next supervisor round, including replacing
    // the HTTP server. Per-round issues clear; the failed drain stays visible.
    status.SetRoleState("standby", "STEPPED_DOWN");
    status.SetDoctorIssue("ELECTION_FAILED", "old round failure");
    diagnostics.Stop();
    status.ClearDoctorIssues(/*preserve_stepdown_drain=*/true);
    status.Configure(true, "stepdown_diagnostics", "local");
    status.SetRoleState("standby", "WATCHING");
    diagnostics.Start();
    ASSERT_NE(diagnostics.port(), 0);
    report = read_doctor();
    issue = find_drain(report);
    ASSERT_NE(issue, report.issues.end());
    EXPECT_EQ(report.issues.size(), 1u);
    EXPECT_EQ(report.health, "degraded");
    EXPECT_EQ(issue->message, drain_error);
    EXPECT_EQ(read_last_error(), drain_error);

    // A later successful drain clears this failure and its last_error.
    status.SetRoleState("primary", "RUNNING");
    store->result = {true, 0, 0, 13, 13};
    P2PMaster::DrainOpLogBeforeStop(manager, 3000);
    report = read_doctor();
    EXPECT_EQ(find_drain(report), report.issues.end());
    EXPECT_EQ(report.health, "ok");
    EXPECT_TRUE(read_last_error().empty());

    // Recovery must not erase an unrelated, more recent error.
    store->result = {false, 2, 1, 10, 13};
    P2PMaster::DrainOpLogBeforeStop(manager, 0);
    status.SetLastError("unrelated registry failure");
    store->result = {true, 0, 0, 13, 13};
    P2PMaster::DrainOpLogBeforeStop(manager, 3000);
    report = read_doctor();
    EXPECT_EQ(find_drain(report), report.issues.end());
    EXPECT_EQ(read_last_error(), "unrelated registry failure");

    store->result = {false, 2, 1, 10, 13};
    EXPECT_FALSE(P2PMaster::DrainOpLogBeforeStop(manager, 50, "shutdown"));
    EXPECT_NE(read_last_error().find("shutdown OpLog drain incomplete"),
              std::string::npos);
    // A successful drain clears an older drain failure from either reason.
    store->result = {true, 0, 0, 13, 13};
    EXPECT_TRUE(P2PMaster::DrainOpLogBeforeStop(manager, 3000));
    EXPECT_TRUE(read_last_error().empty());
    store->result = {false, 2, 1, 10, 13};
    EXPECT_FALSE(P2PMaster::DrainOpLogBeforeStop(manager, 0));
    store->result = {true, 0, 0, 13, 13};
    EXPECT_TRUE(P2PMaster::DrainOpLogBeforeStop(manager, 3000, "shutdown"));
    EXPECT_TRUE(read_last_error().empty());

    store->result = {false, 2, 1, 10, 13};
    P2PMaster::DrainOpLogBeforeStop(manager, 0);
    status
        .ClearDoctorIssues();  // Explicit clearing keeps its original meaning.
    report = read_doctor();
    EXPECT_EQ(find_drain(report), report.issues.end());
    status.Reset();
    EXPECT_TRUE(read_last_error().empty());
}

}  // namespace
}  // namespace mooncake::testing
