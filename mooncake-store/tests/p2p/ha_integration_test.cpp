/**
 * @file ha_integration_test.cpp
 * @brief Integration tests for HA recovery with two P2PClientService instances.
 *
 * Group A: Client-side HA logic (long TTL master, stopped heartbeat,
 *          manual Service observations for deterministic state control).
 * Group B: End-to-end failure scenarios (master restart, client disconnect).
 *
 * Two clients (client1_, client2_) connect to the same InProcP2PMaster.
 * Heartbeat threads are stopped after Init(); tests manually control HA
 * state via Service handlers and manual heartbeat RPCs.
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <stdexcept>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

#ifdef __linux__
#include <cerrno>
#include <cstdio>
#include <dlfcn.h>
#include <pthread.h>

namespace {
struct ThreadStartFailure {
    std::atomic<int> starts_before_failure{-1};
    std::atomic<int> failures{0};
};
thread_local ThreadStartFailure* thread_start_failure = nullptr;

class ScopedThreadStartFailure {
   public:
    explicit ScopedThreadStartFailure(ThreadStartFailure& failure) {
        thread_start_failure = &failure;
    }
    ~ScopedThreadStartFailure() { thread_start_failure = nullptr; }
};
}  // namespace

// Fail only thread creation on the test caller, without changing production
// APIs.
extern "C" int pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                              void* (*entry)(void*), void* arg) noexcept {
    if (thread_start_failure) {
        auto& remaining = thread_start_failure->starts_before_failure;
        if (remaining.load() >= 0 && remaining.fetch_sub(1) == 0) {
            ++thread_start_failure->failures;
            return EAGAIN;
        }
    }
    static const auto real_create = reinterpret_cast<decltype(&pthread_create)>(
        dlsym(RTLD_NEXT, "pthread_create"));
    if (!real_create) {
        std::fputs("Failed to resolve pthread_create for fault injection\n",
                   stderr);
        return EAGAIN;
    }
    return real_create(thread, attr, entry, arg);
}
#endif

#define private public
#define protected public
#include "p2p/client/p2p_client_config_builder.h"
#include "p2p/client/p2p_client_service.h"
#include "p2p/master/p2p_master_service.h"
#include "master_service.h"
#undef protected
#undef private

#include "p2p/master/p2p_client_meta.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

// Test-only RPC endpoint: use the real master for state changes, while keeping
// automatic heartbeats from racing assertions about Init's return state.
class StartupMaster {
   public:
    explicit StartupMaster(P2PMasterRpcService& service)
        : service_(service), server_(4, 0) {
        server_.register_handler<&P2PMasterRpcService::HeartbeatServiceReady,
                                 &P2PMasterRpcService::BatchSyncRoutes>(
            &service_);
        server_.register_handler<&StartupMaster::ServiceReady>(
            this, coro_rpc::func_id<&P2PMasterRpcService::ServiceReady>());
        server_.register_handler<&StartupMaster::RegisterClient>(
            this, coro_rpc::func_id<&P2PMasterRpcService::RegisterClient>());
        server_.register_handler<&StartupMaster::Heartbeat>(
            this, coro_rpc::func_id<&P2PMasterRpcService::Heartbeat>());
        server_.register_handler<&StartupMaster::UnregisterClient>(
            this, coro_rpc::func_id<&P2PMasterRpcService::UnregisterClient>());
    }

    ~StartupMaster() { server_.stop(); }

    bool Start() { return !server_.async_start().hasResult(); }
    std::string Address() const {
        return "127.0.0.1:" + std::to_string(server_.port());
    }

    tl::expected<std::string, ErrorCode> ServiceReady() {
        {
            std::lock_guard<std::mutex> lk(history_mutex);
            connection_times.push_back(std::chrono::steady_clock::now());
        }
        return service_.ServiceReady();
    }

    tl::expected<ViewVersionId, ErrorCode> RegisterClient(
        const P2PRegisterClientRequest& req) {
        ++registration_calls;
        if (before_register) {
            before_register();
        }
        if (reject_registration) {
            return tl::make_unexpected(registration_error.load());
        }
        auto result = service_.RegisterClient(req);
        if (lose_registration_reply) {
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        return result;
    }

    tl::expected<ViewVersionId, ErrorCode> UnregisterClient(
        const UUID& client_id) {
        ++unregistration_calls;
        if (reject_unregistration) {
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        return service_.UnregisterClient(client_id);
    }

    tl::expected<P2PHeartbeatResponse, ErrorCode> Heartbeat(
        const P2PHeartbeatRequest& req) {
        {
            std::lock_guard<std::mutex> lk(history_mutex);
            heartbeat_times.push_back(std::chrono::steady_clock::now());
        }
        ++heartbeat_calls;
        if (before_heartbeat) {
            before_heartbeat();
        }
        last_service_state.store(req.service_state);
        if (forward_heartbeats) {
            auto result = service_.Heartbeat(req);
            if (lose_heartbeat_reply) {
                return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            return result;
        }
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    // Assigned before the tested operation; production exposes no test hooks.
    std::function<void()> before_register;
    std::function<void()> before_heartbeat;
    std::mutex history_mutex;
    std::vector<std::chrono::steady_clock::time_point> connection_times;
    std::vector<std::chrono::steady_clock::time_point> heartbeat_times;
    std::atomic<int> registration_calls{0};
    std::atomic<int> unregistration_calls{0};
    std::atomic<bool> reject_unregistration{false};
    std::atomic<bool> reject_registration{false};
    std::atomic<ErrorCode> registration_error{ErrorCode::RPC_FAIL};
    std::atomic<bool> lose_registration_reply{false};
    std::atomic<bool> lose_heartbeat_reply{false};
    std::atomic<bool> forward_heartbeats{false};
    std::atomic<int> heartbeat_calls{0};
    std::atomic<P2PClientServiceState> last_service_state{
        P2PClientServiceState::INITIALIZING};

   private:
    P2PMasterRpcService& service_;
    coro_rpc::coro_rpc_server server_;
};

// Exercise discovery-mode policy without requiring an external etcd/Redis.
// Actual discovery connectivity remains a Linux integration check.
class FixedMasterView final : public P2PMasterView {
   public:
    explicit FixedMasterView(std::string address)
        : address_(std::move(address)) {}
    void ElectLeader(const std::string&, ViewVersionId&,
                     EtcdLeaseId&) override {}
    void KeepLeader(EtcdLeaseId) override {}
    void CancelKeepAlive(EtcdLeaseId) override {}
    int GetLeaderLeaseTTLSeconds() const override { return 0; }
    ErrorCode GetMasterView(std::string& address,
                            ViewVersionId& version) override {
        ++lookups;
        if (throw_once.exchange(false)) {
            throw std::runtime_error("discovery injection");
        }
        address = address_;
        version = 0;
        return ErrorCode::OK;
    }
    std::atomic<int> lookups{0};
    std::atomic<bool> throw_once{false};

   private:
    std::string address_;
};

// ============================================================================
// Test fixture
// ============================================================================

class HAIntegrationTest : public ::testing::Test {
   protected:
    using ClientEvent = P2PClientService::ClientEvent;

    static ErrorCode HandleEvent(
        const std::shared_ptr<P2PClientService>& client, ClientEvent event) {
        return client->HandleEvent(event);
    }

    // Isolate event tests from the timer without adding production test hooks.
    static void StopHeartbeatForManualEvents(
        const std::shared_ptr<P2PClientService>& client) {
        std::thread heartbeat;
        P2PClientServiceState state;
        {
            MutexLocker lk(&client->lifecycle_mutex_);
            state = client->GetServiceState();
            client->service_state_.store(P2PClientServiceState::STOPPING);
            client->lifecycle_cv_.notify_all();
            heartbeat = std::move(client->heartbeat_thread_);
        }
        if (heartbeat.joinable()) {
            heartbeat.join();
        }
        {
            MutexLocker lk(&client->lifecycle_mutex_);
            client->service_state_.store(state);
        }
    }

    static P2PClientConfig LifecycleConfig(uint16_t rpc_port = 0) {
        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            "localhost:" + std::to_string(getFreeTcpPort()), "P2PHANDSHAKE",
            "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);
        config.local_transfer_mode = LocalTransferMode::MEMCPY;
        config.async_sender_thread_count = 0;
        config.enable_http_server = false;
        return config;
    }

    // Create a P2PClient and connect to the given master address.
    // async_sender_thread_count > 0 enables the async notifier for recovery
    // metadata sync.
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, const std::string& master_addr,
        uint32_t rpc_port = 0, size_t async_sender_thread_count = 1) {
        const uint16_t http_port = static_cast<uint16_t>(getFreeTcpPort());

        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_addr,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/60 * 1000,
            /*local_transfer_mode=*/"memcpy",
            /*local_memcpy_async_worker_num=*/32, http_port,
            /*enable_http_server=*/true,
            /*labels=*/{}, async_sender_thread_count);

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);

        EXPECT_TRUE(WaitForRoutableClient(client->GetMasterClient(),
                                          client->GetClientID()));
        return client;
    }

    // ---- Helpers ----

    // Default write config for these HA tests: force local placement, matching
    // the previous local-first default the tests were written against.
    static P2PWriteRouteConfig LocalWriteConfig() {
        P2PWriteRouteConfig c;
        c.remote_weight = 0.0;  // force local write
        return c;
    }

    static tl::expected<void, ErrorCode> PutData(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        const std::string& data,
        const P2PWriteRouteConfig& config = LocalWriteConfig()) {
        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        return client->Put(key, slices, config);
    }

    static tl::expected<std::string, ErrorCode> GetData(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        size_t buf_size) {
        std::vector<char> buf(buf_size, 0);
        auto result = client->Get(key, {(void*)buf.data()}, {buf.size()});
        if (!result.has_value()) {
            return tl::unexpected(result.error());
        }

        // ATTENTION:
        // TCP TE marks WRITE complete when data reaches the kernel send buffer,
        // not when the receiver's async readBody() finishes.
        // Sleep to avoid reading stale zeros or freeing buf before the receiver
        // writes into it.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        size_t actual_size = static_cast<size_t>(result.value());
        return std::string(buf.data(), actual_size);
    }

    // For cross-client reads immediately after a Put on another client, the
    // async BatchSyncRoutes notification may not have reached master yet.
    // Retry on OBJECT_NOT_FOUND until master learns about the replica.
    static tl::expected<std::string, ErrorCode> GetDataWithRetry(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        size_t buf_size,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        tl::expected<std::string, ErrorCode> result =
            tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
        do {
            result = GetData(client, key, buf_size);
            if (result.has_value() ||
                result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } while (std::chrono::steady_clock::now() < deadline);
        return result;
    }

    static void ForceDegraded(std::shared_ptr<P2PClientService>& client) {
        ASSERT_NE(client->recovery_worker_, nullptr);
        HandleEvent(client, ClientEvent::MASTER_UNREACHABLE);
        ASSERT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED)
            << "Expected DEGRADED state after MASTER_UNREACHABLE";
    }

    static bool RegisterClientForRecovery(
        std::shared_ptr<P2PClientService>& client, ErrorCode& error) {
        auto reg = client->RegisterClient();
        if (reg.has_value()) {
            error = ErrorCode::OK;
            return true;
        }
        error = reg.error();
        return false;
    }

    static void WaitForRecovery(std::shared_ptr<P2PClientService>& client) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (client->recovery_worker_->GetStatus() ==
                   MetadataRecoveryWorker::Status::RUNNING &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        const auto result = client->recovery_worker_->GetStatus();
        EXPECT_TRUE(result == MetadataRecoveryWorker::Status::COMPLETED ||
                    result == MetadataRecoveryWorker::Status::IDLE);
    }

    static void ForceRecover(std::shared_ptr<P2PClientService>& client) {
        ASSERT_NE(client->recovery_worker_, nullptr);
        if (client->GetServiceState() == P2PClientServiceState::LOCAL_ONLY) {
            ASSERT_TRUE(client->RegisterClient().has_value());
            StopHeartbeatForManualEvents(client);
        }
        HandleHealthyMaster(client);
        WaitForRecovery(client);
    }

    static void SendManualHeartbeat(std::shared_ptr<P2PClientService>& client) {
        P2PHeartbeatRequest req;
        req.client_id = client->GetClientID();
        req.service_state = client->GetServiceState();
        auto result = client->GetMasterClient().Heartbeat(req);
        ASSERT_TRUE(result.has_value()) << "Manual heartbeat failed";
    }

    static void HandleOneHeartbeat(std::shared_ptr<P2PClientService>& client) {
        auto response =
            master_.GetWrapped().Heartbeat(client->build_heartbeat_request());
        ASSERT_TRUE(response.has_value());
        auto event = client->HandleHeartbeatResponse(*response);
        if (event) {
            (void)HandleEvent(client, *event);
        }
    }

    static void HandleHealthyMaster(std::shared_ptr<P2PClientService>& client) {
        HandleOneHeartbeat(client);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
        WaitForRecovery(client);
        EXPECT_TRUE(master_.GetWrapped()
                        .Heartbeat(client->build_heartbeat_request())
                        .has_value());
    }

    // ---- Suite setup / teardown ----

    static void SetUpTestSuite() {
        google::InitGoogleLogging("HAIntegrationTest");
        FLAGS_logtostderr = 1;

        // Start master with long TTL so it won't mark clients as
        // DISCONNECTION when heartbeat is stopped.
        InProcP2PMasterConfigBuilder builder;
        builder.set_client_live_ttl_sec(3600);
        builder.set_client_crashed_ttl_sec(7200);
        auto master_config = builder.build();

        ASSERT_TRUE(master_.Start(master_config))
            << "Failed to start P2P master";
        master_address_ = master_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        client1_ = CreateP2PClient("localhost:18901", master_address_);
        ASSERT_NE(client1_, nullptr);
        client2_ = CreateP2PClient("localhost:18902", master_address_);
        ASSERT_NE(client2_, nullptr);

        // Stop heartbeat threads to prevent race conditions.
        StopHeartbeatForManualEvents(client1_);
        StopHeartbeatForManualEvents(client2_);

        // Brief wait to let any in-flight heartbeat RPC complete.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    static void TearDownTestSuite() {
        if (client1_) {
            client1_->Stop();
            client1_->Destroy();
            client1_.reset();
        }
        if (client2_) {
            client2_->Stop();
            client2_->Destroy();
            client2_.reset();
        }
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // Per-test setup: ensure both clients are READY.
    void SetUp() override {
        for (auto* client : {&client1_, &client2_}) {
            if ((*client)->recovery_worker_ &&
                (*client)->GetServiceState() != P2PClientServiceState::ONLINE) {
                ForceRecover(*client);
            }
        }
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client1_;
    static std::shared_ptr<P2PClientService> client2_;
};

InProcP2PMaster HAIntegrationTest::master_;
std::string HAIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> HAIntegrationTest::client1_ = nullptr;
std::shared_ptr<P2PClientService> HAIntegrationTest::client2_ = nullptr;

// ============================================================================
// Group A: Client-side HA logic tests
// (long TTL master, stopped heartbeat, manual HandleEvent)
// ============================================================================

// A1: Baseline — put from client1 routed to client2, then read back.
TEST_F(HAIntegrationTest, RemotePutAndGet) {
    // Put with remote_weight=1 (force remote): master routes to client2
    P2PWriteRouteConfig config;
    config.remote_weight = 1.0;
    config.local_write_waterline = 0.0;

    auto put = PutData(client1_, "a1_client1_remote_baseline", "hello", config);
    ASSERT_TRUE(put.has_value())
        << "Remote put failed: " << static_cast<int>(put.error());

    // client1 Get: local miss → queries master → reads from client2
    auto get = GetData(client1_, "a1_client1_remote_baseline", 5);
    ASSERT_TRUE(get.has_value())
        << "Remote get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "hello");

    // Verify data physically resides on client2
    auto exist = client2_->IsExist("a1_client1_remote_baseline");
    ASSERT_TRUE(exist.has_value());
    EXPECT_TRUE(exist.value());
}

TEST_F(HAIntegrationTest, InitialRegistrationStartsReadyWithoutRecovery) {
    auto config = LifecycleConfig();
    config.async_sender_thread_count = 1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    SendManualHeartbeat(client);
    EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());
    EXPECT_EQ(client->recovery_worker_->need_abort_, nullptr);
    EXPECT_TRUE(client->async_route_notifier_->running_.load());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_TRUE(client->client_rpc_service_->IsReady());
    auto meta =
        master_.GetWrapped().GetMasterService().GetClientManager().GetClient(
            client->GetClientID());
    ASSERT_NE(meta, nullptr);
    EXPECT_TRUE(meta->IsReady());
    EXPECT_NE(meta->get_rpc_port(), 0);
    EXPECT_EQ(meta->get_rpc_port(), client->GetRpcPort());
    auto segments = meta->GetSegments();
    ASSERT_TRUE(segments.has_value());
    EXPECT_EQ(segments->size(), client->CollectTierSegments().size());
    for (const auto& segment : client->CollectTierSegments()) {
        auto registered = meta->QuerySegment(segment.id);
        ASSERT_TRUE(registered.has_value());
        EXPECT_EQ(registered->name, segment.name);
        EXPECT_EQ(registered->size, segment.size);
    }
}

TEST_F(HAIntegrationTest, MetadataCallbacksStayLocalBeforeServicePublication) {
    auto config = LifecycleConfig();
    auto client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, false, config.labels);
    ASSERT_EQ(client->resources_.InitTransferEngine("127.0.0.1", "P2PHANDSHAKE",
                                                    "tcp", std::nullopt),
              ErrorCode::OK);
    client->initTeEndpoint();
    ASSERT_EQ(client->InitStorage(config), ErrorCode::OK);
    EXPECT_EQ(client->recovery_worker_, nullptr);
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    // Registration alone must not publish metadata before Service READY.
    client->client_rpc_service_.emplace(*client->data_manager_,
                                        client->metrics_);
    auto segments = client->CollectTierSegments();
    ASSERT_FALSE(segments.empty());
    EXPECT_TRUE(
        client->BuildAddReplicaCallback()("before_ha", segments[0].id, 64)
            .has_value());
    EXPECT_TRUE(
        client->BuildRemoveReplicaCallback()("before_ha", segments[0].id)
            .has_value());
    EXPECT_TRUE(
        client->BuildSegmentSyncCallback()(segments[0], true).has_value());
    EXPECT_TRUE(
        client->BuildSegmentSyncCallback()(segments[0], false).has_value());
    client->data_manager_->RectifyReadRoute("before_ha", segments[0].id);
    client->data_manager_->RectifyReadRoute("before_ha");
    EXPECT_EQ(client->metrics_->master_client_metric.summary_metrics(),
              "=== RPC Metrics Summary ===\nNo RPC calls recorded\n");
    P2PHeartbeatResponse response;
    response.status = P2PClientStatus::HEALTH;
    EXPECT_EQ(client->HandleHeartbeatResponse(response),
              ClientEvent::HEARTBEAT_HEALTHY);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::INITIALIZING);
    EXPECT_EQ(client->recovery_worker_, nullptr);
}

TEST_F(HAIntegrationTest, LocalOnlyStartupNeverContactsMaster) {
    auto config = LifecycleConfig();
    config.start_local_only = true;
    config.master_server_entry = "redis://127.0.0.1:0";
    config.redis_db_index = -1;  // Discovery would fail if it were attempted.
    config.async_sender_thread_count = 1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");
    EXPECT_EQ(client->master_view_, nullptr);
    EXPECT_EQ(client->master_client_.client_accessor_.GetClientPool(), nullptr);
    EXPECT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_FALSE(client->async_route_notifier_->running_.load());
    for (const auto& shard : client->async_route_notifier_->shards_) {
        EXPECT_FALSE(shard->sender_thread.joinable());
    }
    EXPECT_EQ(client->recovery_worker_->GetStatus(),
              MetadataRecoveryWorker::Status::IDLE);
    EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());
    P2PWriteRouteConfig remote;
    remote.remote_weight = 1.0;
    ASSERT_TRUE(PutData(client, "local-start", "value", remote).has_value());
    auto get = GetData(client, "local-start", 5);
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(*get, "value");
    EXPECT_FALSE(client->Query("missing-local-start").has_value());
    auto ips = client->BatchQueryIp({client->GetClientID()});
    ASSERT_FALSE(ips.has_value());
    EXPECT_EQ(ips.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto regex = client->QueryByRegex(".*");
    ASSERT_FALSE(regex.has_value());
    EXPECT_EQ(regex.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(client->metrics_->master_client_metric.summary_metrics(),
              "=== RPC Metrics Summary ===\nNo RPC calls recorded\n");
    // Explicit connection failure must not opt into automatic retries.
    EXPECT_FALSE(client->RegisterClient().has_value());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");
    EXPECT_TRUE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, LocalOnlyRegistrationFailureRequiresManualRetry) {
    for (bool lose_reply : {false, true}) {
        SCOPED_TRACE(lose_reply);
        StartupMaster endpoint(master_.GetWrapped());
        endpoint.reject_registration = !lose_reply;
        endpoint.lose_registration_reply = lose_reply;
        ASSERT_TRUE(endpoint.Start());
        auto config = LifecycleConfig();
        config.start_local_only = true;
        config.master_server_entry = endpoint.Address();
        config.async_sender_thread_count = 1;
        auto created = P2PClientService::Create(config);
        ASSERT_TRUE(created.has_value());
        auto client = *created;
        ASSERT_FALSE(client->RegisterClient().has_value());
        EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");
        EXPECT_TRUE(client->heartbeat_thread_.joinable());
        EXPECT_EQ(endpoint.heartbeat_calls.load(), 0);
        EXPECT_FALSE(client->async_route_notifier_->running_.load());
        auto meta = master_.GetWrapped()
                        .GetMasterService()
                        .GetClientManager()
                        .GetClient(client->GetClientID());
        if (meta) {
            EXPECT_FALSE(meta->IsReady());
        }
        endpoint.reject_registration = false;
        endpoint.lose_registration_reply = false;
        ASSERT_TRUE(client->RegisterClient().has_value());
        EXPECT_EQ(client->GetHealthStatus(), "ONLINE");
        const auto thread_id = client->heartbeat_thread_.get_id();
        ASSERT_TRUE(client->RegisterClient().has_value());
        EXPECT_EQ(client->heartbeat_thread_.get_id(), thread_id);
    }
}

TEST_F(HAIntegrationTest, LocalJoinRespectsDiscoveryRecoveryPolicy) {
    for (const std::string entry : {master_address_, std::string("etcd://test"),
                                    std::string("redis://test")}) {
        for (size_t senders : {0u, 1u}) {
            SCOPED_TRACE(entry + "/senders=" + std::to_string(senders));
            auto config = LifecycleConfig();
            config.start_local_only = true;
            config.master_server_entry = entry;
            config.async_sender_thread_count = senders;
            auto created = P2PClientService::Create(config);
            ASSERT_TRUE(created.has_value());
            auto client = *created;
            const auto key =
                "local-join-" + std::to_string(client->GetClientID().first);
            ASSERT_TRUE(PutData(client, key, "local-value").has_value());
            if (entry != master_address_) {
                client->master_view_ =
                    std::make_unique<FixedMasterView>(master_address_);
                client->master_view_entry_ = entry;
            }
            ASSERT_TRUE(client->RegisterClient().has_value());
            StopHeartbeatForManualEvents(client);
            WaitForRecovery(client);
            SendManualHeartbeat(client);
            auto& master = master_.GetWrapped().GetMasterService();
            auto meta =
                master.GetClientManager().GetClient(client->GetClientID());
            ASSERT_NE(meta, nullptr);
            EXPECT_EQ(meta->get_rpc_port(), client->GetRpcPort());
            auto segments = meta->GetSegments();
            ASSERT_TRUE(segments.has_value());
            EXPECT_EQ(segments->size(), client->CollectTierSegments().size());
            auto route = master.GetReadRoute(key);
            if (entry.rfind("redis://", 0) == 0) {
                EXPECT_FALSE(route.has_value());
                EXPECT_FALSE(
                    client->recovery_worker_->recovery_thread_.joinable());
                continue;
            }
            ASSERT_TRUE(route.has_value());
            EXPECT_EQ(route->front().client_id, client->GetClientID());
            auto remote_get = GetDataWithRetry(client2_, key, 11);
            ASSERT_TRUE(remote_get.has_value());
            EXPECT_EQ(*remote_get, "local-value");
        }
    }
}

TEST_F(HAIntegrationTest, RedisReconnectKeepsMasterRoutesWithoutLocalReplay) {
    auto config = LifecycleConfig();
    config.start_local_only = true;
    config.master_server_entry = "redis://test";
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    client->master_view_ = std::make_unique<FixedMasterView>(master_address_);
    client->master_view_entry_ = config.master_server_entry;
    ASSERT_TRUE(client->RegisterClient().has_value());
    StopHeartbeatForManualEvents(client);
    SendManualHeartbeat(client);
    ASSERT_TRUE(PutData(client, "redis-before-loss", "value").has_value());
    auto& master = master_.GetWrapped().GetMasterService();
    ASSERT_TRUE(master.GetReadRoute("redis-before-loss").has_value());
    auto original = master.GetClientManager().GetClient(client->GetClientID());

    ForceDegraded(client);
    ASSERT_TRUE(PutData(client, "redis-during-loss", "value").has_value());
    HandleHealthyMaster(client);
    EXPECT_EQ(master.GetClientManager().GetClient(client->GetClientID()),
              original);
    EXPECT_TRUE(master.GetReadRoute("redis-before-loss").has_value());
    EXPECT_FALSE(master.GetReadRoute("redis-during-loss").has_value());
    EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());
}

#ifdef __linux__
TEST_F(HAIntegrationTest, InitializationEventsReportHeartbeatStartupFailure) {
    for (auto event :
         {ClientEvent::INITIALIZE_LOCAL, ClientEvent::INITIALIZE_ONLINE}) {
        SCOPED_TRACE(static_cast<int>(event));
        auto client =
            std::make_shared<P2PClientService>("P2PHANDSHAKE", 0, false);
        ThreadStartFailure failure;
        failure.starts_before_failure = 0;
        {
            ScopedThreadStartFailure injection(failure);
            EXPECT_EQ(HandleEvent(client, event), ErrorCode::INTERNAL_ERROR);
        }
        EXPECT_EQ(failure.failures.load(), 1);
        EXPECT_FALSE(client->heartbeat_thread_.joinable());
        EXPECT_EQ(client->GetServiceState(),
                  P2PClientServiceState::INITIALIZING);
        EXPECT_EQ(client->master_client_.client_accessor_.GetClientPool(),
                  nullptr);
    }
}

TEST_F(HAIntegrationTest, InitialNotifierStartupFailureFailsInit) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    config.async_sender_thread_count = 2;
    auto client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, false, config.labels);
    ThreadStartFailure failure;
    endpoint.before_register = [&] { failure.starts_before_failure = 1; };
    {
        ScopedThreadStartFailure injection(failure);
        EXPECT_EQ(client->Init(config), ErrorCode::INTERNAL_ERROR);
    }
    EXPECT_EQ(failure.failures.load(), 1);
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_FALSE(client->async_route_notifier_->running_.load());
    for (const auto& shard : client->async_route_notifier_->shards_) {
        EXPECT_FALSE(shard->sender_thread.joinable());
    }
    EXPECT_EQ(
        master_.GetWrapped().GetMasterService().GetClientManager().GetClient(
            client->GetClientID()),
        nullptr);
    client->Stop();
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, JoinResourceFailureKeepsParkedThreadAndCanBeRetried) {
    for (bool fail_notifier : {false, true}) {
        SCOPED_TRACE(fail_notifier);
        auto config = LifecycleConfig();
        config.start_local_only = true;
        config.async_sender_thread_count = fail_notifier ? 2 : 0;
        auto created = P2PClientService::Create(config);
        ASSERT_TRUE(created.has_value());
        auto client = *created;
        ASSERT_EQ(client->GetMasterClient().Connect(master_address_),
                  ErrorCode::OK);
        const auto heartbeat_id = client->heartbeat_thread_.get_id();
        ThreadStartFailure failure;
        // Fail the second sender, or the recovery thread when senders are
        // disabled.
        failure.starts_before_failure = fail_notifier ? 1 : 0;
        {
            ScopedThreadStartFailure injection(failure);
            auto result = client->RegisterClient();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
        }
        EXPECT_EQ(failure.failures.load(), 1);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::LOCAL_ONLY);
        EXPECT_EQ(client->heartbeat_thread_.get_id(), heartbeat_id);
        EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());
        if (client->async_route_notifier_) {
            EXPECT_FALSE(client->async_route_notifier_->running_.load());
            for (const auto& shard : client->async_route_notifier_->shards_) {
                EXPECT_FALSE(shard->sender_thread.joinable());
            }
        }
        EXPECT_EQ(master_.GetWrapped()
                      .GetMasterService()
                      .GetClientManager()
                      .GetClient(client->GetClientID()),
                  nullptr);
        ASSERT_TRUE(client->RegisterClient().has_value());
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
        EXPECT_EQ(client->heartbeat_thread_.get_id(), heartbeat_id);
    }
}

#endif

TEST_F(HAIntegrationTest, ReadyDuringRecoveryAndLeaveCancelsWithoutReopening) {
    for (bool shutdown : {false, true}) {
        SCOPED_TRACE(shutdown);
        auto config = LifecycleConfig();
        config.start_local_only = true;
        auto created = P2PClientService::Create(config);
        ASSERT_TRUE(created.has_value());
        auto client = *created;
        ASSERT_TRUE(PutData(client, "blocked-recovery", "value").has_value());
        std::promise<void> entered;
        std::promise<void> release;
        auto released = release.get_future().share();
        client->recovery_worker_->publish_replica_ =
            [&](std::string_view, const UUID&,
                size_t) -> tl::expected<void, ErrorCode> {
            entered.set_value();
            released.wait();
            return {};
        };
        auto registered = client->RegisterClient();
        if (!registered) {
            release.set_value();
            FAIL() << "Registration failed: " << registered.error();
        }
        auto publication = entered.get_future();
        if (publication.wait_for(std::chrono::seconds(5)) !=
            std::future_status::ready) {
            release.set_value();
            FAIL() << "Recovery did not reach publication";
        }
        EXPECT_EQ(client->GetHealthStatus(), "ONLINE");
        EXPECT_EQ(client->recovery_worker_->GetStatus(),
                  MetadataRecoveryWorker::Status::RUNNING);
        SendManualHeartbeat(client);
        auto meta = master_.GetWrapped()
                        .GetMasterService()
                        .GetClientManager()
                        .GetClient(client->GetClientID());
        EXPECT_TRUE(meta && meta->IsReady());
        auto leaving = std::async(std::launch::async, [&] {
            if (shutdown) {
                client->Stop();
            } else {
                EXPECT_TRUE(client->UnregisterClient().has_value());
            }
        });
        EXPECT_EQ(leaving.wait_for(std::chrono::milliseconds(50)),
                  std::future_status::timeout);
        release.set_value();
        leaving.get();
        EXPECT_EQ(client->GetServiceState(),
                  shutdown ? P2PClientServiceState::STOPPED
                           : P2PClientServiceState::LOCAL_ONLY);
        EXPECT_EQ(client->GetHealthStatus(),
                  shutdown ? "STOPPED" : "LOCAL_ONLY");
        EXPECT_EQ(client->recovery_worker_->GetStatus(),
                  MetadataRecoveryWorker::Status::CANCELLED);
        EXPECT_EQ(client->heartbeat_thread_.joinable(), !shutdown);
    }
}

TEST_F(HAIntegrationTest, HeartbeatRequestReportsStoredLifecycleState) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    for (auto state :
         {P2PClientServiceState::INITIALIZING, P2PClientServiceState::ONLINE,
          P2PClientServiceState::DEGRADED, P2PClientServiceState::LOCAL_ONLY,
          P2PClientServiceState::STOPPING, P2PClientServiceState::STOPPED}) {
        // Protocol regression: component readiness must not rewrite the report.
        client->service_state_.store(state);
        EXPECT_EQ(client->build_heartbeat_request().service_state, state);
    }
    client->service_state_.store(P2PClientServiceState::ONLINE);
}

TEST_F(HAIntegrationTest, RegistrationKeepsItsOriginStateUntilConfirmed) {
    for (auto origin :
         {P2PClientServiceState::INITIALIZING,
          P2PClientServiceState::LOCAL_ONLY, P2PClientServiceState::DEGRADED}) {
        SCOPED_TRACE(toString(origin));
        StartupMaster endpoint(master_.GetWrapped());
        ASSERT_TRUE(endpoint.Start());
        auto config = LifecycleConfig();
        config.master_server_entry = endpoint.Address();
        config.start_local_only = origin == P2PClientServiceState::LOCAL_ONLY;
        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port, false, config.labels);
        if (origin != P2PClientServiceState::INITIALIZING) {
            endpoint.reject_registration =
                origin == P2PClientServiceState::DEGRADED;
            ASSERT_EQ(client->Init(config), ErrorCode::OK);
            StopHeartbeatForManualEvents(client);
            endpoint.heartbeat_calls = 0;
            endpoint.reject_registration = false;
        }
        auto entered = std::make_shared<std::promise<void>>();
        auto release = std::make_shared<std::promise<void>>();
        auto signalled = std::make_shared<std::atomic<bool>>(false);
        auto released = release->get_future().share();
        endpoint.before_register = [entered, released, signalled] {
            if (!signalled->exchange(true)) {
                entered->set_value();
            }
            released.wait();
        };
        auto operation = std::async(std::launch::async, [&] {
            if (origin == P2PClientServiceState::INITIALIZING) {
                return client->Init(config);
            }
            auto result = client->RegisterClient();
            return result ? ErrorCode::OK : result.error();
        });
        auto registration = entered->get_future();
        if (registration.wait_for(std::chrono::seconds(5)) !=
            std::future_status::ready) {
            release->set_value();
            FAIL() << "Registration did not reach Master";
        }
        EXPECT_EQ(client->GetServiceState(), origin);
        EXPECT_EQ(endpoint.heartbeat_calls.load(), 0);
        release->set_value();
        EXPECT_EQ(operation.get(), ErrorCode::OK);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
        StopHeartbeatForManualEvents(client);
    }
}

TEST_F(HAIntegrationTest, DegradationPreservesMasterRegistration) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    auto& manager = master_.GetWrapped().GetMasterService().GetClientManager();
    auto registered = manager.GetClient(client->GetClientID());
    ASSERT_NE(registered, nullptr);
    for (int attempt = 0; attempt < 2; ++attempt) {
        EXPECT_EQ(HandleEvent(client, ClientEvent::MASTER_UNREACHABLE),
                  ErrorCode::OK);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
        EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
        EXPECT_EQ(manager.GetClient(client->GetClientID()), registered);
    }
    client->Stop();
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
}

TEST_F(HAIntegrationTest, ManualLeaveAndOnlineStopEachOwnOneUnregister) {
    for (bool stop_directly : {false, true}) {
        StartupMaster endpoint(master_.GetWrapped());
        ASSERT_TRUE(endpoint.Start());
        auto config = LifecycleConfig();
        config.master_server_entry = endpoint.Address();
        auto created = P2PClientService::Create(config);
        ASSERT_TRUE(created.has_value());
        auto client = *created;
        StopHeartbeatForManualEvents(client);
        endpoint.reject_unregistration = true;
        if (!stop_directly) {
            EXPECT_FALSE(client->UnregisterClient().has_value());
            EXPECT_EQ(client->GetServiceState(),
                      P2PClientServiceState::LOCAL_ONLY);
            EXPECT_TRUE(client->UnregisterClient().has_value());
        }
        client->Stop();
        client->Stop();
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
        EXPECT_FALSE(client->heartbeat_thread_.joinable());
        EXPECT_FALSE(client->client_rpc_service_->IsReady());
        EXPECT_EQ(endpoint.unregistration_calls.load(), 1);
    }
}

TEST_F(HAIntegrationTest, MissingRegistrationSuspendsMetadataUntilConfirmed) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    config.async_sender_thread_count = 1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    auto& master = master_.GetWrapped().GetMasterService();
    ASSERT_TRUE(master.UnregisterClient(client->GetClientID()).has_value());
    auto response = master.Heartbeat(client->build_heartbeat_request());
    ASSERT_TRUE(response.has_value());
    ASSERT_EQ(response->status, P2PClientStatus::UNDEFINED);

    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    endpoint.before_register = [&] {
        entered.set_value();
        released.wait();
    };
    auto joining = std::async(std::launch::async, [&] {
        return HandleEvent(client, ClientEvent::REGISTRATION_REQUIRED);
    });
    if (entered.get_future().wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        release.set_value();
        FAIL() << "Re-registration did not reach Master";
    }
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_FALSE(client->async_route_notifier_->running_.load());
    EXPECT_TRUE(PutData(client, "registration-gap", "value").has_value());
    EXPECT_FALSE(master.GetReadRoute("registration-gap").has_value());
    release.set_value();
    EXPECT_EQ(joining.get(), ErrorCode::OK);
    HandleHealthyMaster(client);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_TRUE(client->async_route_notifier_->running_.load());
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!master.GetReadRoute("registration-gap").has_value() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(master.GetReadRoute("registration-gap").has_value());
}

TEST_F(HAIntegrationTest, RegistrationRetriesDoNotUnregister) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    endpoint.reject_registration = true;
    EXPECT_NE(HandleEvent(client, ClientEvent::REGISTRATION_REQUIRED),
              ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
    EXPECT_NE(HandleEvent(client, ClientEvent::HEARTBEAT_HEALTHY),
              ErrorCode::OK);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
    endpoint.reject_registration = false;
    ASSERT_EQ(HandleEvent(client, ClientEvent::HEARTBEAT_HEALTHY),
              ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
}

TEST_F(HAIntegrationTest, StoppingRejectsLateEventsBeforeResourcesAreReleased) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    std::optional<InflightTracker::Guard> call{client->AcquireInflightGuard()};
    auto stopped = std::async(std::launch::async, [&] { client->Stop(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (client->GetServiceState() != P2PClientServiceState::STOPPING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(client->GetHealthStatus(), "STOPPING");
    EXPECT_FALSE(client->RegisterClient().has_value());
    for (auto event :
         {ClientEvent::MASTER_UNREACHABLE, ClientEvent::REGISTRATION_REQUIRED,
          ClientEvent::HEARTBEAT_HEALTHY}) {
        (void)HandleEvent(client, event);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPING);
    }
    call.reset();
    stopped.get();
    EXPECT_EQ(client->GetHealthStatus(), "STOPPED");
    EXPECT_EQ(HandleEvent(client, ClientEvent::REGISTRATION_REQUIRED),
              ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, HeartbeatResponseOnlyMapsCommunicationEvents) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    P2PHeartbeatResponse response;
    response.view_version = client->view_version_.load();
    response.status = P2PClientStatus::HEALTH;
    EXPECT_EQ(client->HandleHeartbeatResponse(response),
              ClientEvent::HEARTBEAT_HEALTHY);
    client->service_state_.store(P2PClientServiceState::DEGRADED);
    EXPECT_EQ(client->HandleHeartbeatResponse(response),
              ClientEvent::HEARTBEAT_HEALTHY);
    response.status = P2PClientStatus::UNDEFINED;
    EXPECT_EQ(client->HandleHeartbeatResponse(response),
              ClientEvent::REGISTRATION_REQUIRED);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_EQ(client->recovery_worker_->GetStatus(),
              MetadataRecoveryWorker::Status::IDLE);
    client->service_state_.store(P2PClientServiceState::ONLINE);
    for (auto status :
         {P2PClientStatus::DISCONNECTION, P2PClientStatus::CRASHED,
          static_cast<P2PClientStatus>(-1)}) {
        response.status = status;
        EXPECT_FALSE(client->HandleHeartbeatResponse(response).has_value());
    }
}

TEST_F(HAIntegrationTest, ClientEventWaitsForLifecycleLockAndHonorsLocalOnly) {
    auto config = LifecycleConfig();
    config.start_local_only = true;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    std::future<ErrorCode> event;
    {
        MutexLocker lk(&client->lifecycle_mutex_);
        event = std::async(std::launch::async, [&] {
            return HandleEvent(client, ClientEvent::REGISTRATION_REQUIRED);
        });
        EXPECT_EQ(event.wait_for(std::chrono::milliseconds(100)),
                  std::future_status::timeout);
    }
    EXPECT_EQ(event.get(), ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::LOCAL_ONLY);
    EXPECT_EQ(client->master_client_.client_accessor_.GetClientPool(), nullptr);
}

TEST_F(HAIntegrationTest, DiscoveryAdapterConvertsProviderExceptions) {
    auto config = LifecycleConfig();
    config.start_local_only = true;
    config.master_server_entry = "redis://test";
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    auto discovery = std::make_unique<FixedMasterView>(master_address_);
    discovery->throw_once = true;
    MutexLocker lk(&client->lifecycle_mutex_);
    client->master_view_ = std::move(discovery);
    client->master_view_entry_ = config.master_server_entry;
    std::string address;
    EXPECT_EQ(client->ResolveMasterAddress(config.master_server_entry, address),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(client->ResolveMasterAddress(config.master_server_entry, address),
              ErrorCode::OK);
    EXPECT_EQ(address, master_address_);
}

TEST_F(HAIntegrationTest, HeartbeatRecoversAfterDiscoveryAdapterFailure) {
    auto config = LifecycleConfig();
    config.master_server_entry = "redis://127.0.0.1:0";
    config.redis_db_index = -1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    auto discovery = std::make_unique<FixedMasterView>(master_address_);
    auto* observed = discovery.get();
    observed->throw_once = true;
    client->master_view_ = std::move(discovery);
    client->master_view_entry_ = config.master_server_entry;
    {
        MutexLocker lk(&client->lifecycle_mutex_);
        ASSERT_EQ(client->StartHeartbeat(), ErrorCode::OK);
    }
    // Exercise the unchanged ten-failure threshold through the real loop.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(25);
    while (client->GetServiceState() != P2PClientServiceState::ONLINE &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    StopHeartbeatForManualEvents(client);
    EXPECT_GE(observed->lookups.load(), 2);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
}

TEST_F(HAIntegrationTest,
       SuccessfulReconnectImmediatelyProbesAndResetsFailures) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    endpoint.heartbeat_calls = 0;
    {
        std::lock_guard<std::mutex> lk(endpoint.history_mutex);
        endpoint.connection_times.clear();
        endpoint.heartbeat_times.clear();
    }
    {
        MutexLocker lk(&client->lifecycle_mutex_);
        ASSERT_EQ(client->StartHeartbeat(), ErrorCode::OK);
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (endpoint.heartbeat_calls.load() < 12 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    StopHeartbeatForManualEvents(client);
    std::lock_guard<std::mutex> lk(endpoint.history_mutex);
    ASSERT_GE(endpoint.heartbeat_times.size(), 12);
    ASSERT_EQ(endpoint.connection_times.size(), 1);
    EXPECT_LT(endpoint.heartbeat_times[10] - endpoint.connection_times[0],
              std::chrono::milliseconds(500));
    EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
}

TEST_F(HAIntegrationTest, LocalOnlyWaitResetsHeartbeatFailureStreak) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (endpoint.heartbeat_calls.load() < 8 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_GE(endpoint.heartbeat_calls.load(), 8);
    ASSERT_TRUE(client->UnregisterClient().has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    const auto failures_before_rejoin = endpoint.heartbeat_calls.load();
    ASSERT_TRUE(client->RegisterClient().has_value());
    size_t connections;
    {
        std::lock_guard<std::mutex> lk(endpoint.history_mutex);
        connections = endpoint.connection_times.size();
    }
    const auto rejoin_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (endpoint.heartbeat_calls.load() < failures_before_rejoin + 3 &&
           std::chrono::steady_clock::now() < rejoin_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    StopHeartbeatForManualEvents(client);
    EXPECT_GE(endpoint.heartbeat_calls.load(), failures_before_rejoin + 3);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    std::lock_guard<std::mutex> lk(endpoint.history_mutex);
    EXPECT_EQ(endpoint.connection_times.size(), connections);
}

TEST_F(HAIntegrationTest, HeartbeatResponseCompletesBeforeUnregister) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    config.start_local_only = true;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<bool> signalled{false};
    endpoint.forward_heartbeats = true;
    endpoint.before_heartbeat = [&] {
        if (!signalled.exchange(true)) {
            entered.set_value();
        }
        released.wait();
    };
    ASSERT_TRUE(client->RegisterClient().has_value());
    if (entered.get_future().wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        release.set_value();
        FAIL() << "Heartbeat did not reach Master";
    }
    auto leaving = std::async(std::launch::async,
                              [&] { return client->UnregisterClient(); });
    EXPECT_EQ(leaving.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    release.set_value();
    EXPECT_TRUE(leaving.get().has_value());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::LOCAL_ONLY);
    EXPECT_TRUE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, StopReleasesLifecycleLockBeforeJoiningHttp) {
    auto config = LifecycleConfig();
    config.enable_http_server = true;
    config.http_port = 0;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    const auto url =
        "http://127.0.0.1:" + std::to_string(client->GetHttpPort()) +
        "/register";
    std::optional<InflightTracker::Guard> request{
        client->AcquireInflightGuard()};
    auto stopped = std::async(std::launch::async, [&] { client->Stop(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (client->GetServiceState() != P2PClientServiceState::STOPPING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    auto registration = std::async(std::launch::async, [&] {
        coro_http::coro_http_client http;
        return http.post(url, "", coro_http::req_content_type::octet_stream)
            .status;
    });
    auto response = registration.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(response, std::future_status::ready);
    if (response == std::future_status::ready) {
        EXPECT_EQ(registration.get(), 503);
    }
    request.reset();
    stopped.get();
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
}

TEST_F(HAIntegrationTest, MissingMasterPoolReturnsRpcErrors) {
    P2PMasterClient client(generate_uuid());
    P2PHeartbeatRequest heartbeat;
    auto result = client.Heartbeat(heartbeat);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
    auto single = client.ExistKey("no_pool");
    ASSERT_FALSE(single.has_value());
    EXPECT_EQ(single.error(), ErrorCode::RPC_FAIL);
    auto batch = client.BatchExistKey({"first", "second"});
    ASSERT_EQ(batch.size(), 2);
    for (const auto& entry : batch) {
        ASSERT_FALSE(entry.has_value());
        EXPECT_EQ(entry.error(), ErrorCode::RPC_FAIL);
    }
    EXPECT_TRUE(client.BatchExistKey({}).empty());
}

TEST_F(HAIntegrationTest, ServiceStateSurvivesHeartbeatFailureAndReplyLoss) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    config.async_sender_thread_count = 1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    auto meta =
        master_.GetWrapped().GetMasterService().GetClientManager().GetClient(
            client->GetClientID());
    ASSERT_NE(meta, nullptr);
    EXPECT_FALSE(meta->IsReady());
    auto failed =
        client->GetMasterClient().Heartbeat(client->build_heartbeat_request());
    EXPECT_FALSE(failed.has_value());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_FALSE(meta->IsReady());

    endpoint.forward_heartbeats = true;
    endpoint.lose_heartbeat_reply = true;
    auto lost =
        client->GetMasterClient().Heartbeat(client->build_heartbeat_request());
    EXPECT_FALSE(lost.has_value());
    EXPECT_TRUE(meta->IsReady());
    EXPECT_TRUE(client->async_route_notifier_->running_.load());
    endpoint.lose_heartbeat_reply = false;
    EXPECT_TRUE(client->GetMasterClient()
                    .Heartbeat(client->build_heartbeat_request())
                    .has_value());
    EXPECT_TRUE(meta->IsReady());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
}

TEST_F(HAIntegrationTest, DiscoveryFailureStartsDegradedWithoutRpcPool) {
    auto config = LifecycleConfig();
    config.master_server_entry = "redis://127.0.0.1:0";
    // Fail discovery before any network connection, with or without Redis.
    config.redis_db_index = -1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_TRUE(client->client_rpc_service_->IsReady());
    EXPECT_EQ(client->master_client_.client_accessor_.GetClientPool(), nullptr);
    auto result =
        client->master_client_.Heartbeat(client->build_heartbeat_request());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(HAIntegrationTest, MasterInternalErrorStillAllowsDegradedStartup) {
    StartupMaster endpoint(master_.GetWrapped());
    endpoint.reject_registration = true;
    endpoint.registration_error = ErrorCode::INTERNAL_ERROR;
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_TRUE(client->heartbeat_thread_.joinable());
    ASSERT_TRUE(PutData(client, "initial-master-error", "value").has_value());
}

TEST_F(HAIntegrationTest, InitialRegistrationFailureRetriesFromHeartbeat) {
    // Cover both an absent registration (UNDEFINED heartbeat) and a lost reply
    // after the master accepted it (HEALTH while Service has not entered
    // ONLINE).
    for (bool lost_reply : {false, true}) {
        SCOPED_TRACE(lost_reply);
        StartupMaster endpoint(master_.GetWrapped());
        endpoint.reject_registration = !lost_reply;
        endpoint.lose_registration_reply = lost_reply;
        ASSERT_TRUE(endpoint.Start());
        auto config = LifecycleConfig();
        config.master_server_entry = endpoint.Address();
        config.async_sender_thread_count = 1;
        auto created = P2PClientService::Create(config);
        ASSERT_TRUE(created.has_value());
        auto client = *created;
        StopHeartbeatForManualEvents(client);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
        EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
        EXPECT_EQ(client->build_heartbeat_request().service_state,
                  P2PClientServiceState::DEGRADED);
        EXPECT_FALSE(client->async_route_notifier_->running_.load());
        EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());

        endpoint.reject_registration = false;
        endpoint.lose_registration_reply = false;
        HandleHealthyMaster(client);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
        // The heartbeat worker must not start a second heartbeat thread.
        EXPECT_FALSE(client->heartbeat_thread_.joinable());
        auto meta = master_.GetWrapped()
                        .GetMasterService()
                        .GetClientManager()
                        .GetClient(client->GetClientID());
        ASSERT_NE(meta, nullptr);
        EXPECT_TRUE(meta->IsReady());
    }
}

TEST_F(HAIntegrationTest, LostReregistrationReplyKeepsServiceDegraded) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    config.async_sender_thread_count = 1;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    auto& master = master_.GetWrapped().GetMasterService();
    ASSERT_TRUE(master.UnregisterClient(client->GetClientID()).has_value());
    endpoint.lose_registration_reply = true;
    HandleOneHeartbeat(client);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    auto meta = master.GetClientManager().GetClient(client->GetClientID());
    ASSERT_NE(meta, nullptr);
    EXPECT_FALSE(meta->IsReady());
    EXPECT_FALSE(client->async_route_notifier_->running_.load());
    endpoint.lose_registration_reply = false;
    HandleHealthyMaster(client);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_TRUE(meta->IsReady());
}

TEST_F(HAIntegrationTest, LocalOnlyParksHeartbeatAndRejoinUsesSameThread) {
    StartupMaster endpoint(master_.GetWrapped());
    endpoint.forward_heartbeats = true;
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    const auto heartbeat_id = client->heartbeat_thread_.get_id();
    ASSERT_NE(heartbeat_id, std::thread::id{});
    ASSERT_TRUE(client->UnregisterClient().has_value());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::LOCAL_ONLY);
    const auto heartbeats = endpoint.heartbeat_calls.load();
    const auto registrations = endpoint.registration_calls.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(endpoint.heartbeat_calls.load(), heartbeats);
    EXPECT_EQ(endpoint.registration_calls.load(), registrations);
    EXPECT_EQ(client->heartbeat_thread_.get_id(), heartbeat_id);
    EXPECT_EQ(
        master_.GetWrapped().GetMasterService().GetClientManager().GetClient(
            client->GetClientID()),
        nullptr);
    ASSERT_TRUE(client->RegisterClient().has_value());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_EQ(client->heartbeat_thread_.get_id(), heartbeat_id);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (endpoint.heartbeat_calls.load() == heartbeats &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(endpoint.heartbeat_calls.load(), heartbeats);
    ASSERT_TRUE(client->RegisterClient().has_value());
    EXPECT_EQ(client->heartbeat_thread_.get_id(), heartbeat_id);
    client->Stop();
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
    client->Destroy();
    client->Stop();
    client->Destroy();
}

TEST_F(HAIntegrationTest, RegistrationRpcHasNoLifecycleSideEffects) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    ASSERT_TRUE(client->GetMasterClient()
                    .UnregisterClient(client->GetClientID())
                    .has_value());
    {
        MutexLocker lk(&client->lifecycle_mutex_);
        ASSERT_TRUE(client->InnerRegisterClient().has_value());
    }
    EXPECT_FALSE(client->recovery_worker_->recovery_thread_.joinable());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, MemcpyLocalModePublishesForwardTransferEndpoints) {
    auto config = LifecycleConfig();
    config.start_local_only = true;
    config.transfer_direction_mode = TransferDirectionMode::FORWARD;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    ASSERT_TRUE(PutData(client, "forward-source", "value").has_value());
    auto& rpc = *client->client_rpc_service_;
    auto pin = rpc.PinKey({"forward-source", std::nullopt});
    ASSERT_TRUE(pin.has_value());
    EXPECT_FALSE(pin->remote_buffer.segment_endpoint.empty());
    EXPECT_EQ(pin->remote_buffer.segment_endpoint, client->get_te_endpoint());
    EXPECT_TRUE(
        rpc.UnPinKey({"forward-source", pin->read_operation_id}).has_value());
    auto write = rpc.PreWrite({"forward-destination", 5, std::nullopt});
    ASSERT_TRUE(write.has_value());
    EXPECT_FALSE(write->remote_buffer.segment_endpoint.empty());
    EXPECT_EQ(write->remote_buffer.segment_endpoint, client->get_te_endpoint());
    EXPECT_TRUE(
        rpc.WriteRevoke({"forward-destination", write->write_operation_id})
            .has_value());
}

TEST_F(HAIntegrationTest, PeerAdmissionDoesNotDependOnServiceMode) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    client->recovery_worker_->Stop();
    for (auto state :
         {P2PClientServiceState::ONLINE, P2PClientServiceState::DEGRADED,
          P2PClientServiceState::LOCAL_ONLY}) {
        // Background recovery is stopped so each state remains deterministic.
        client->service_state_.store(state);
        auto prewrite = client->client_rpc_service_->PreWrite(
            {"peer_admission_ha", 64, std::nullopt});
        EXPECT_TRUE(prewrite.has_value()) << toString(state);
        if (prewrite) {
            EXPECT_TRUE(client->client_rpc_service_
                            ->WriteRevoke({"peer_admission_ha",
                                           prewrite->write_operation_id})
                            .has_value());
        }
    }
    // Restore the actual lifecycle mode after the isolated peer-admission
    // checks.
    client->service_state_.store(P2PClientServiceState::ONLINE);
}

TEST_F(HAIntegrationTest, StopEventReportsFailureAfterCompletingCleanup) {
    StartupMaster endpoint(master_.GetWrapped());
    endpoint.reject_unregistration = true;
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    EXPECT_EQ(HandleEvent(client, ClientEvent::STOP_REQUESTED),
              ErrorCode::RPC_FAIL);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
    EXPECT_FALSE(client->client_rpc_service_->IsReady());
    EXPECT_FALSE(client->local_inflight_tracker_.is_running());
    EXPECT_EQ(HandleEvent(client, ClientEvent::STOP_REQUESTED), ErrorCode::OK);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 1);
}

TEST_F(HAIntegrationTest, ConcurrentStopWaitsForOneShutdown) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = endpoint.Address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    std::optional<InflightTracker::Guard> request{
        client->AcquireInflightGuard()};
    auto first = std::async(std::launch::async, [&] { client->Stop(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (client->GetServiceState() != P2PClientServiceState::STOPPING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPING);
    auto second = std::async(std::launch::async, [&] { client->Stop(); });
    EXPECT_EQ(first.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_EQ(second.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    request.reset();
    first.get();
    second.get();
    EXPECT_EQ(endpoint.unregistration_calls.load(), 1);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPED);
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
}

TEST_F(HAIntegrationTest, ClientEventsAreExplicitAndIdempotentAcrossStates) {
    StartupMaster endpoint(master_.GetWrapped());
    ASSERT_TRUE(endpoint.Start());
    auto config = LifecycleConfig();
    config.start_local_only = true;
    config.master_server_entry = endpoint.Address();
    auto client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, false, config.labels);
    EXPECT_EQ(HandleEvent(client, ClientEvent::REGISTER_REQUESTED),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(HandleEvent(client, ClientEvent::UNREGISTER_REQUESTED),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_EQ(client->Init(config), ErrorCode::OK);
    StopHeartbeatForManualEvents(client);
    for (auto event :
         {ClientEvent::HEARTBEAT_HEALTHY, ClientEvent::MASTER_UNREACHABLE,
          ClientEvent::REGISTRATION_REQUIRED,
          ClientEvent::UNREGISTER_REQUESTED}) {
        EXPECT_EQ(HandleEvent(client, event), ErrorCode::OK);
        EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::LOCAL_ONLY);
    }
    EXPECT_EQ(endpoint.registration_calls.load(), 0);
    ASSERT_EQ(HandleEvent(client, ClientEvent::REGISTER_REQUESTED),
              ErrorCode::OK);
    const auto registrations = endpoint.registration_calls.load();
    EXPECT_EQ(HandleEvent(client, ClientEvent::REGISTER_REQUESTED),
              ErrorCode::OK);
    EXPECT_EQ(HandleEvent(client, ClientEvent::HEARTBEAT_HEALTHY),
              ErrorCode::OK);
    EXPECT_EQ(endpoint.registration_calls.load(), registrations);
    EXPECT_EQ(HandleEvent(client, ClientEvent::MASTER_UNREACHABLE),
              ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::DEGRADED);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 0);
    EXPECT_EQ(HandleEvent(client, ClientEvent::HEARTBEAT_HEALTHY),
              ErrorCode::OK);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::ONLINE);
    client->Stop();
    EXPECT_EQ(HandleEvent(client, ClientEvent::REGISTER_REQUESTED),
              ErrorCode::SHUTTING_DOWN);
    EXPECT_EQ(HandleEvent(client, ClientEvent::UNREGISTER_REQUESTED),
              ErrorCode::SHUTTING_DOWN);
    EXPECT_EQ(HandleEvent(client, ClientEvent::HEARTBEAT_HEALTHY),
              ErrorCode::OK);
    EXPECT_EQ(HandleEvent(client, ClientEvent::STOP_REQUESTED), ErrorCode::OK);
    EXPECT_EQ(endpoint.unregistration_calls.load(), 1);
}

TEST_F(HAIntegrationTest, StopWaitsForCallsBeforeSequentialDestroy) {
    auto created = P2PClientService::Create(LifecycleConfig());
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    EXPECT_TRUE(client->client_rpc_service_->IsReady());
    auto& rpc = *client->client_rpc_service_;
    auto prewrite = rpc.PreWrite({"concurrent_close", 64, std::nullopt});
    ASSERT_TRUE(prewrite.has_value());
    std::optional<InflightTracker::Guard> local_call{
        client->AcquireInflightGuard()};
    std::optional<InflightTracker::Guard> completion_call{
        rpc.finish_op_tracker_.Enter()};
    ASSERT_TRUE(local_call->is_valid());
    ASSERT_TRUE(completion_call->is_valid());
    auto stopped = std::async(std::launch::async, [client] { client->Stop(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (client->local_inflight_tracker_.is_running() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(client->local_inflight_tracker_.is_running());
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::STOPPING);
    EXPECT_TRUE(rpc.IsReady());
    EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    // Lifecycle reports that cleanup is still in progress.
    EXPECT_EQ(client->GetHealthStatus(), "STOPPING");
    auto registration = client->RegisterClient();
    EXPECT_FALSE(registration.has_value());
    if (!registration) {
        EXPECT_EQ(registration.error(), ErrorCode::SHUTTING_DOWN);
    }
    auto unregistration = client->UnregisterClient();
    EXPECT_FALSE(unregistration.has_value());
    if (!unregistration) {
        EXPECT_EQ(unregistration.error(), ErrorCode::SHUTTING_DOWN);
    }
    local_call.reset();
    const auto peer_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (rpc.IsReady() && std::chrono::steady_clock::now() < peer_deadline) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(rpc.IsReady());
    EXPECT_EQ(
        master_.GetWrapped().GetMasterService().GetClientManager().GetClient(
            client->GetClientID()),
        nullptr);
    auto rejected = rpc.PreWrite({"after_close", 64, std::nullopt});
    EXPECT_FALSE(rejected.has_value());
    if (!rejected) {
        EXPECT_EQ(rejected.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    EXPECT_TRUE(
        rpc.WriteRevoke({"concurrent_close", prewrite->write_operation_id})
            .has_value());
    EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    // The outstanding completion handler still owns access to DataManager.
    completion_call.reset();
    stopped.get();
    EXPECT_EQ(client->GetHealthStatus(), "STOPPED");
    client->Destroy();
    EXPECT_EQ(client->data_manager_, nullptr);
    EXPECT_FALSE(client->client_rpc_service_.has_value());
    EXPECT_EQ(client->client_rpc_server_, nullptr);
    client->Stop();
    client->Destroy();
}

TEST_F(HAIntegrationTest, RejectsOccupiedPeerPortBeforeRegistration) {
    coro_rpc::coro_rpc_server occupied(1, 0);
    auto started = occupied.async_start();
    ASSERT_FALSE(started.hasResult());
    auto config = LifecycleConfig(occupied.port());
    auto client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, false, config.labels);
    EXPECT_EQ(client->Init(config), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(client->GetHealthStatus(), "INITIALIZING");
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    ASSERT_NE(client->recovery_worker_, nullptr);
    EXPECT_EQ(client->recovery_worker_->GetStatus(),
              MetadataRecoveryWorker::Status::IDLE);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::INITIALIZING);
    ASSERT_TRUE(client->client_rpc_service_.has_value());
    EXPECT_FALSE(client->client_rpc_service_->IsReady());
    client->Stop();
    client->Destroy();
    EXPECT_FALSE(client->local_inflight_tracker_.is_running());
    occupied.stop();
}

TEST_F(HAIntegrationTest, InvalidRuntimeFailsBeforeResourceCreation) {
    auto config = LifecycleConfig();
    config.runtime_config_json = Json::Value(Json::objectValue);
    config.runtime_config_json["write"] = "invalid-write-object";
    auto client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, false, config.labels);
    EXPECT_EQ(client->Init(config), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(client->GetServiceState(), P2PClientServiceState::INITIALIZING);
    EXPECT_EQ(client->resources_.GetTransferEngine(), nullptr);
    EXPECT_EQ(client->data_manager_, nullptr);
    EXPECT_EQ(client->client_rpc_server_, nullptr);
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    client->Stop();
    client->Destroy();
}

TEST_F(HAIntegrationTest, InjectedEngineRemainsUsableAfterDestroy) {
    ClientResources owner;
    ASSERT_EQ(owner.InitTransferEngine("127.0.0.1", "P2PHANDSHAKE", "tcp",
                                       std::nullopt),
              ErrorCode::OK);
    auto engine = owner.GetTransferEngine();
    const auto endpoint = engine->getLocalIpAndPort();
    auto config = LifecycleConfig();
    config.transfer_engine = engine;
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    EXPECT_EQ(client->resources_.GetTransferEngine(), engine);
    // Production releases resources through the destructor's Stop -> Destroy.
    created.reset();
    client.reset();
    EXPECT_EQ(engine->getLocalIpAndPort(), endpoint);
    std::array<char, 4096> buffer{};
    ASSERT_EQ(engine->registerLocalMemory(buffer.data(), buffer.size()), 0);
    EXPECT_EQ(engine->unregisterLocalMemory(buffer.data()), 0);
}

TEST_F(HAIntegrationTest, MastersBindDistinctEphemeralPorts) {
    InProcP2PMaster first;
    InProcP2PMaster second;
    ASSERT_TRUE(first.Start());
    ASSERT_TRUE(second.Start());
    EXPECT_GT(first.rpc_port(), 0);
    EXPECT_GT(second.rpc_port(), 0);
    EXPECT_NE(first.rpc_port(), second.rpc_port());
    P2PMasterClient first_client(generate_uuid());
    P2PMasterClient second_client(generate_uuid());
    EXPECT_EQ(first_client.Connect(first.master_address()), ErrorCode::OK);
    EXPECT_EQ(second_client.Connect(second.master_address()), ErrorCode::OK);
}

TEST_F(HAIntegrationTest, UnregisterFailureDoesNotReopenAdmission) {
    InProcP2PMaster isolated_master;
    ASSERT_TRUE(isolated_master.Start());
    auto config = LifecycleConfig();
    config.master_server_entry = isolated_master.master_address();
    auto created = P2PClientService::Create(config);
    ASSERT_TRUE(created.has_value());
    auto client = *created;
    StopHeartbeatForManualEvents(client);
    client->recovery_worker_->Stop();
    // An unreachable Master must not prevent local shutdown.
    client->service_state_.store(P2PClientServiceState::DEGRADED);
    isolated_master.Stop();
    client->Stop();
    EXPECT_EQ(client->GetHealthStatus(), "STOPPED");
    EXPECT_NE(client->GetServiceState(), P2PClientServiceState::ONLINE);
    EXPECT_FALSE(client->client_rpc_service_->IsReady());
    auto rejected = client->client_rpc_service_->PreWrite(
        {"after_failed_unregister", 64, std::nullopt});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    client->Destroy();
}

// A2: Degraded mode — local Put/Get/IsExist all work; remote-only keys
//     and nonexistent keys return false for IsExist.
TEST_F(HAIntegrationTest, DegradedModeLocalOps) {
    // Write locally on client1 (default helper config forces a local write).
    auto put_a = PutData(client1_, "a2_client1_local", "local_data");
    ASSERT_TRUE(put_a.has_value());

    // Write locally on client2 (client1's cache never populated)
    auto put_b = PutData(client2_, "a2_client2_remote_only", "remote_data");
    ASSERT_TRUE(put_b.has_value());

    ForceDegraded(client1_);

    // Degraded Put falls back to PutLocal
    auto put_c = PutData(client1_, "a2_client1_degraded_put", "degraded_data");
    ASSERT_TRUE(put_c.has_value())
        << "Degraded PutLocal failed: " << static_cast<int>(put_c.error());

    // Local Get works for pre-existing and degraded-mode keys
    auto get_a = GetData(client1_, "a2_client1_local", 10);
    ASSERT_TRUE(get_a.has_value());
    EXPECT_EQ(get_a.value(), "local_data");

    auto get_c = GetData(client1_, "a2_client1_degraded_put", 13);
    ASSERT_TRUE(get_c.has_value());
    EXPECT_EQ(get_c.value(), "degraded_data");

    // IsExist: local key → true
    auto exist_local = client1_->IsExist("a2_client1_local");
    ASSERT_TRUE(exist_local.has_value());
    EXPECT_TRUE(exist_local.value());

    // IsExist: key only on client2 → false (local miss, degraded skip)
    auto exist_remote = client1_->IsExist("a2_client2_remote_only");
    ASSERT_TRUE(exist_remote.has_value());
    EXPECT_FALSE(exist_remote.value());

    // IsExist: nonexistent key → false
    auto exist_none = client1_->IsExist("a2_nonexistent_key");
    ASSERT_TRUE(exist_none.has_value());
    EXPECT_FALSE(exist_none.value());

    ForceRecover(client1_);
    StopHeartbeatForManualEvents(client1_);
}

// A3: Degraded mode — remote Get returns OBJECT_NOT_FOUND;
TEST_F(HAIntegrationTest, DegradedModeRemoteOpsFail) {
    // Write on client2 — client1's route cache not populated
    auto put = PutData(client2_, "a3_client2_key", "remote_val");
    ASSERT_TRUE(put.has_value());

    // Verify data is accessible before degradation (client1 → master →
    // client2). Use retry: async BatchSyncRoutes may not have reached master
    // yet.
    auto get_before = GetDataWithRetry(client1_, "a3_client2_key", 10);
    ASSERT_TRUE(get_before.has_value()) << "Pre-degradation get failed: "
                                        << static_cast<int>(get_before.error());
    EXPECT_EQ(get_before.value(), "remote_val");

    ForceDegraded(client1_);

    // Get: local miss → degraded check → OBJECT_NOT_FOUND (master unreachable)
    std::vector<char> buf(100, 0);
    auto get =
        client1_->Get("a3_client2_key", {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get.has_value());
    EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Query: local-first + master fallback; degraded + local miss → 404.
    auto query = client1_->Query("a3_client1_query_key");
    EXPECT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);

    ForceRecover(client1_);
}

// A4: Recovery — after degraded, remote data on client2 becomes accessible.
TEST_F(HAIntegrationTest, RecoverFromDegradedRemoteGet) {
    auto put = PutData(client2_, "a4_client2_key", "recoverable");
    ASSERT_TRUE(put.has_value());

    // Verify data is accessible before degradation (client1 → master →
    // client2). Use retry: async BatchSyncRoutes may not have reached master
    // yet.
    auto get_before = GetDataWithRetry(client1_, "a4_client2_key", 11);
    ASSERT_TRUE(get_before.has_value()) << "Pre-degradation get failed: "
                                        << static_cast<int>(get_before.error());
    EXPECT_EQ(get_before.value(), "recoverable");

    ForceDegraded(client1_);

    // Verify Get fails during degradation (local miss → OBJECT_NOT_FOUND)
    std::vector<char> buf(11, 0);
    auto get_fail =
        client1_->Get("a4_client2_key", {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get_fail.has_value());
    EXPECT_EQ(get_fail.error(), ErrorCode::OBJECT_NOT_FOUND);

    ForceRecover(client1_);

    // After recovery, Get succeeds via master routing to client2
    auto get_ok = GetData(client1_, "a4_client2_key", 11);
    ASSERT_TRUE(get_ok.has_value())
        << "Post-recovery get failed: " << static_cast<int>(get_ok.error());
    EXPECT_EQ(get_ok.value(), "recoverable");
}

// A5: Degraded writes are synced to master after recovery,
//     allowing the other client to read them.
TEST_F(HAIntegrationTest, RecoverDegradedWritesSyncToMaster) {
    ForceDegraded(client1_);

    // Write during degraded mode → PutLocal with metadata skip.
    // The add_replica_callback detects degraded mode and skips notifier
    // enqueue, so the local write succeeds without master involvement.
    auto put = PutData(client1_, "a5_client1_degraded_write", "synced_data");
    ASSERT_TRUE(put.has_value())
        << "Degraded PutLocal failed: " << static_cast<int>(put.error());

    // Recovery: notifier syncs all local metadata to master
    ForceRecover(client1_);

    // client2 reads data written by client1 during degradation.
    // master now has the route (synced by recovery pipeline).
    auto get = GetData(client2_, "a5_client1_degraded_write", 11);
    ASSERT_TRUE(get.has_value())
        << "Cross-client get after recovery sync failed: "
        << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "synced_data");
}

// A6: Eviction (Delete) during degraded mode succeeds locally without
//     failing on master notification.
TEST_F(HAIntegrationTest, DegradedModeEvictionSkipsMasterSync) {
    // Write a key locally on client1 while READY
    auto put = PutData(client1_, "a6_client1_evict_key", "evict_data");
    ASSERT_TRUE(put.has_value());

    // Verify key exists before degradation
    auto exist_before = client1_->IsExist("a6_client1_evict_key");
    ASSERT_TRUE(exist_before.has_value());
    EXPECT_TRUE(exist_before.value());

    ForceDegraded(client1_);

    // Simulate eviction: delete the key via data_manager while degraded.
    // Without the degraded skip fix in remove_replica_callback, this would
    // fail with ASYNC_ENQUEUE_FAILED because the notifier is stopped.
    auto del = client1_->data_manager_->Delete("a6_client1_evict_key");
    ASSERT_TRUE(del.has_value())
        << "Degraded Delete failed: " << static_cast<int>(del.error());

    // Verify key is gone locally
    auto exist_after = client1_->IsExist("a6_client1_evict_key");
    ASSERT_TRUE(exist_after.has_value());
    EXPECT_FALSE(exist_after.value());

    // A retained route must not make the deleted replica readable.
    ForceRecover(client1_);

    // client2 tries to read the deleted key — should fail (key doesn't exist)
    std::vector<char> buf(100, 0);
    auto get = client2_->Get("a6_client1_evict_key", {(void*)buf.data()},
                             {buf.size()});
    EXPECT_FALSE(get.has_value());
}

// A7: Rapid degraded/recovery cycles are consistent.
TEST_F(HAIntegrationTest, RapidDegradedRecoveryCycle) {
    for (int i = 0; i < 5; ++i) {
        HandleEvent(client1_, ClientEvent::MASTER_UNREACHABLE);
        EXPECT_EQ(client1_->GetServiceState(), P2PClientServiceState::DEGRADED);

        HandleHealthyMaster(client1_);
        auto state = client1_->GetServiceState();
        EXPECT_EQ(state, P2PClientServiceState::ONLINE);

        WaitForRecovery(client1_);
    }
    EXPECT_EQ(client1_->GetServiceState(), P2PClientServiceState::ONLINE);
}

// A7: Master-side state verification.
TEST_F(HAIntegrationTest, MasterSideState) {
    SendManualHeartbeat(client1_);
    SendManualHeartbeat(client2_);

    // Both clients should be ready on the Master side
    auto& svc = master_.GetWrapped().GetMasterService();
    for (auto* client : {&client1_, &client2_}) {
        auto client_meta =
            svc.GetClientManager().GetClient((*client)->GetClientID());
        ASSERT_NE(client_meta, nullptr);

        auto p2p_meta = std::dynamic_pointer_cast<P2PClientMeta>(client_meta);
        ASSERT_NE(p2p_meta, nullptr);
        EXPECT_TRUE(p2p_meta->IsReady());
    }
}

// A8: Re-registration reports current local tier segments to the master.
TEST_F(HAIntegrationTest, ReRegisterReportsCurrentTierSegments) {
    auto expected_segments = client1_->CollectTierSegments();
    ASSERT_FALSE(expected_segments.empty());

    auto& svc = master_.GetWrapped().GetMasterService();
    auto unreg_result = svc.UnregisterClient(client1_->GetClientID());
    ASSERT_TRUE(unreg_result.has_value())
        << "UnregisterClient failed: " << unreg_result.error();

    HandleHealthyMaster(client1_);

    auto registered_segments =
        svc.GetClientManager().GetClientSegments(client1_->GetClientID());
    ASSERT_TRUE(registered_segments.has_value())
        << "GetClientSegments failed: " << registered_segments.error();
    EXPECT_EQ(registered_segments.value().size(), expected_segments.size());

    for (const auto& segment : expected_segments) {
        auto registered = svc.GetClientManager().QuerySegment(
            client1_->GetClientID(), segment.id);
        ASSERT_TRUE(registered.has_value())
            << "Missing registered segment: " << segment.name;
        EXPECT_EQ(registered->name, segment.name);
        EXPECT_EQ(registered->size, segment.size);
        EXPECT_EQ(registered->memory_type, segment.memory_type);
    }

    ForceRecover(client1_);
}

// ============================================================================
// Group B: End-to-end failure scenario tests
// ============================================================================

// B1: Master crashes and restarts — both clients recover.
TEST_F(HAIntegrationTest, MasterRestartRecovery) {
    // Write data locally on client1 before master crash
    auto put = PutData(client1_, "b1_client1_before_crash", "persistent");
    ASSERT_TRUE(put.has_value());

    int port = master_.rpc_port();

    // Stop master (simulates crash)
    master_.Stop();

    // Both clients detect master down → DEGRADED
    ForceDegraded(client1_);
    ForceDegraded(client2_);

    // Restart master on the same port
    InProcP2PMasterConfigBuilder builder;
    builder.set_rpc_port(port);
    builder.set_client_live_ttl_sec(3600);
    builder.set_client_crashed_ttl_sec(7200);
    ASSERT_TRUE(master_.Start(builder.build())) << "Failed to restart master";

    // Reconnect RPC clients to the restarted master (old connections are
    // stale). Retry with backoff since the restarted RPC server may not
    // be fully accepting connections immediately.
    // Each client has its own connection pool, so both need to clear stale
    // connections independently.
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto err = client1_->GetMasterClient().Connect(master_address_);
        if (err == ErrorCode::OK) break;
        if (attempt == 9) FAIL() << "Reconnect client1 failed after retries";
    }
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto err = client2_->GetMasterClient().Connect(master_address_);
        if (err == ErrorCode::OK) break;
        if (attempt == 9) FAIL() << "Reconnect client2 failed after retries";
    }

    // Re-register clients with the restarted master. Duplicate P2P
    // registration is handled as an idempotent HA re-register by the master.
    ErrorCode reg1_error = ErrorCode::OK;
    ASSERT_TRUE(RegisterClientForRecovery(client1_, reg1_error))
        << "Re-register client1 failed: " << static_cast<int>(reg1_error);
    ErrorCode reg2_error = ErrorCode::OK;
    ASSERT_TRUE(RegisterClientForRecovery(client2_, reg2_error))
        << "Re-register client2 failed: " << static_cast<int>(reg2_error);

    // Recovery: DEGRADED -> READY with background metadata replay
    ForceRecover(client1_);
    ForceRecover(client2_);

    // After recovery, verify the system is functional:
    // new Put + Get should work through the restarted master.
    auto put2 = PutData(client1_, "b1_client1_after_restart", "works");
    ASSERT_TRUE(put2.has_value())
        << "Post-restart put failed: " << static_cast<int>(put2.error());
    auto get = GetData(client1_, "b1_client1_after_restart", 5);
    ASSERT_TRUE(get.has_value())
        << "Post-restart get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "works");
}

// B2: Single client disconnects (network failure), master marks it
//     DISCONNECTION, then client recovers via heartbeat.
//     Uses independent short-TTL master to avoid affecting other tests.
TEST_F(HAIntegrationTest, ClientDisconnectAndRecover) {
    // Start an independent master with short TTL
    InProcP2PMaster short_ttl_master;
    InProcP2PMasterConfigBuilder builder;
    builder.set_client_live_ttl_sec(2);
    builder.set_client_crashed_ttl_sec(20);
    ASSERT_TRUE(short_ttl_master.Start(builder.build()));

    std::string short_master_addr = short_ttl_master.master_address();

    // Create two temporary clients on the short-TTL master
    auto tmp1 = CreateP2PClient("localhost:19001", short_master_addr);
    ASSERT_NE(tmp1, nullptr);
    auto tmp2 = CreateP2PClient("localhost:19002", short_master_addr);
    ASSERT_NE(tmp2, nullptr);

    // Verify both are HEALTH initially
    {
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                tmp1->GetClientID());
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), P2PClientStatus::HEALTH);
    }

    // Simulate client1 network failure: stop its heartbeat
    StopHeartbeatForManualEvents(tmp1);

    // Wait for master to mark tmp1 as DISCONNECTION (TTL=2s)
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Verify master side: tmp1 is DISCONNECTION
    {
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                tmp1->GetClientID());
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value(), P2PClientStatus::DISCONNECTION)
            << "Master should have marked disconnected client";
    }

    // Verify tmp2 is still HEALTH
    {
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                tmp2->GetClientID());
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value(), P2PClientStatus::HEALTH);
    }

    // Recover: manually send heartbeat from tmp1
    {
        P2PHeartbeatRequest req;
        req.client_id = tmp1->GetClientID();
        req.service_state = tmp1->GetServiceState();
        auto hb_res = tmp1->GetMasterClient().Heartbeat(req);
        ASSERT_TRUE(hb_res.has_value()) << "Recovery heartbeat failed";
        EXPECT_EQ(hb_res.value().status, P2PClientStatus::HEALTH)
            << "Client should recover to HEALTH after heartbeat";
    }

    // Verify master side: tmp1 is HEALTH again
    {
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                tmp1->GetClientID());
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value(), P2PClientStatus::HEALTH)
            << "Client should be HEALTH after recovery heartbeat";
    }

    // Cleanup
    tmp1->Stop();
    tmp1->Destroy();
    tmp2->Stop();
    tmp2->Destroy();
    short_ttl_master.Stop();
}

}  // namespace testing
}  // namespace mooncake
