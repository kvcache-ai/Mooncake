#include <glog/logging.h>
#include <gtest/gtest.h>

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

#include "p2p/master/p2p_master_client.h"
#include "p2p/master/p2p_master_metric_manager.h"
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
        ASSERT_TRUE(client_->CompleteRouteSync(client_id_).has_value());
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

    auto heartbeat = client_->Heartbeat({.client_id = client_id_});
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
            return master_->RunActiveRpcServers(*server_, *service_);
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
            if (error) {
                LOG(ERROR) << "Unexpected listener probe failure: " << error;
                return false;
            }
            // Poll listener state; request/stop ordering is controlled by
            // gates.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
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

    handler_.Release();
    ASSERT_EQ(run_result_.wait_for(kTimeout), std::future_status::ready);
    // An immediate stop can complete the start future before the runner's
    // startup check. Both existing exit classifications must drain the RPCs.
    const int run_result = run_result_.get();
    EXPECT_TRUE(run_result == 0 || run_result == -1);
    EXPECT_TRUE(handler_.HasExited());
    ASSERT_EQ(stop_result_.wait_for(kTimeout), std::future_status::ready);
    stop_result_.get();
    ASSERT_EQ(request_result_.wait_for(kTimeout), std::future_status::ready);
    request_result_.get();
    if (heartbeat_port_ > 0) {
        EXPECT_TRUE(WaitForListenerClosed(heartbeat_port_));
    }
}

INSTANTIATE_TEST_SUITE_P(HeartbeatModes, P2PMasterRpcShutdownTest,
                         ::testing::Bool());

}  // namespace
}  // namespace mooncake::testing
