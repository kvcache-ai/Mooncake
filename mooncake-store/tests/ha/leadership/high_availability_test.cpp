#include <sys/wait.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/high_availability_test_fixture.h"
#include "ha/leadership/master_service_supervisor.h"
#include "master_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

DEFINE_string(etcd_endpoints, "127.0.0.1:2379", "Etcd endpoints");
DEFINE_string(etcd_test_key_prefix, "mooncake-store/test/",
              "The prefix of the test keys in ETCD");
DEFINE_bool(ha_supervisor_child, false,
            "Run a real HA supervisor for the parent integration test");
DEFINE_int32(ha_supervisor_rpc_port, 0, "Child supervisor RPC port");
DEFINE_int32(ha_supervisor_admin_port, 0, "Child supervisor admin port");
DEFINE_string(ha_supervisor_cluster_id, "", "Child supervisor cluster ID");

void HighAvailabilityTest::SetUpTestSuite() {
    // Initialize glog
    google::InitGoogleLogging("HighAvailabilityTest");

    // Set VLOG level to 1 for detailed logs
    google::SetVLOGLevel("*", 1);
    FLAGS_logtostderr = 1;
}

void HighAvailabilityTest::TearDownTestSuite() {
    google::ShutdownGoogleLogging();
}

namespace {

#ifdef STORE_USE_ETCD
std::once_flag g_etcd_probe_once;
bool g_etcd_available = false;

void ProbeEtcdAvailability() {
    ErrorCode err = EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to initialize etcd client, skipping tests.";
        g_etcd_available = false;
        return;
    }

    std::string val;
    EtcdRevisionId rev;
    err = EtcdHelper::Get("probe_connection_key", 20, val, rev);
    if (err == ErrorCode::ETCD_OPERATION_ERROR) {
        LOG(WARNING) << "Failed to connect to Etcd at " << FLAGS_etcd_endpoints
                     << " (Error: " << static_cast<int>(err)
                     << "). Integration tests will be skipped.";
        g_etcd_available = false;
        return;
    }

    g_etcd_available = true;
}
#endif

std::optional<std::string> GetEtcdSkipReason() {
#ifdef STORE_USE_ETCD
    std::call_once(g_etcd_probe_once, ProbeEtcdAvailability);
    if (!g_etcd_available) {
        return "Etcd server not reachable at " + FLAGS_etcd_endpoints;
    }
    return std::nullopt;
#else
    return "Etcd HA backend is not enabled in this build";
#endif
}

ha::HABackendSpec MakeEtcdBackendSpec(const std::string& endpoints) {
    return ha::HABackendSpec{
        .type = ha::HABackendType::ETCD,
        .connstring = endpoints,
        .cluster_namespace = "",
    };
}

std::unique_ptr<ha::LeaderCoordinator> CreateEtcdCoordinatorOrNull(
    const std::string& endpoints) {
    auto coordinator =
        ha::CreateLeaderCoordinator(MakeEtcdBackendSpec(endpoints));
    if (!coordinator) {
        return nullptr;
    }
    return std::move(coordinator.value());
}

class FakeLeaderCoordinator : public ha::LeaderCoordinator {
   public:
    explicit FakeLeaderCoordinator(ViewVersionId view_version)
        : session_{.view = {.leader_address = "fake-leader",
                            .view_version = view_version},
                   .owner_token = "fake-owner",
                   .lease_ttl = std::chrono::milliseconds(0)} {}

    tl::expected<std::optional<ha::MasterView>, ErrorCode> ReadCurrentView()
        override {
        return std::optional<ha::MasterView>{};
    }

    tl::expected<ha::AcquireLeadershipResult, ErrorCode> TryAcquireLeadership(
        const std::string& /*leader_address*/) override {
        return ha::AcquireLeadershipResult{
            .status = ha::AcquireLeadershipStatus::ACQUIRED,
            .session = session_,
            .observed_view = std::nullopt,
        };
    }

    tl::expected<bool, ErrorCode> RenewLeadership(
        const ha::LeadershipSession& /*session*/) override {
        return true;
    }

    tl::expected<ha::ViewChangeResult, ErrorCode> WaitForViewChange(
        std::optional<ViewVersionId> /*known_version*/,
        std::chrono::milliseconds /*timeout*/) override {
        return ha::ViewChangeResult{};
    }

    tl::expected<std::unique_ptr<ha::LeadershipMonitorHandle>, ErrorCode>
    StartLeadershipMonitor(
        const ha::LeadershipSession& /*session*/,
        ha::LeadershipLostCallback /*on_leadership_lost*/) override {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    ErrorCode ReleaseLeadership(
        const ha::LeadershipSession& /*session*/) override {
        return ErrorCode::OK;
    }

   private:
    ha::LeadershipSession session_;
};

class SupervisorChildProcess {
   public:
    SupervisorChildProcess(int rpc_port, int admin_port, std::string cluster_id)
        : rpc_port_(rpc_port),
          admin_port_(admin_port),
          cluster_id_(std::move(cluster_id)) {}

    ~SupervisorChildProcess() { Stop(); }

    bool Start() {
        pid_ = fork();
        if (pid_ != 0) {
            return pid_ > 0;
        }

        const std::string endpoints_arg =
            "--etcd_endpoints=" + FLAGS_etcd_endpoints;
        const std::string rpc_port_arg =
            "--ha_supervisor_rpc_port=" + std::to_string(rpc_port_);
        const std::string admin_port_arg =
            "--ha_supervisor_admin_port=" + std::to_string(admin_port_);
        const std::string cluster_arg =
            "--ha_supervisor_cluster_id=" + cluster_id_;
        execl("/proc/self/exe", "/proc/self/exe", "--ha_supervisor_child=true",
              endpoints_arg.c_str(), rpc_port_arg.c_str(),
              admin_port_arg.c_str(), cluster_arg.c_str(), nullptr);
        _exit(127);
    }

    bool IsRunning() {
        if (pid_ <= 0) {
            return false;
        }
        int status = 0;
        if (waitpid(pid_, &status, WNOHANG) == 0) {
            return true;
        }
        pid_ = 0;
        return false;
    }

    void Stop() {
        if (pid_ <= 0) {
            return;
        }
        kill(pid_, SIGKILL);
        waitpid(pid_, nullptr, 0);
        pid_ = 0;
    }

   private:
    pid_t pid_{0};
    int rpc_port_;
    int admin_port_;
    std::string cluster_id_;
};

struct HttpResponse {
    int status;
    std::string body;
};

HttpResponse HttpGet(int port, const std::string& path) {
    coro_http::coro_http_client client;
    auto result = client.get("http://127.0.0.1:" + std::to_string(port) + path);
    return {.status = result.status, .body = std::string(result.resp_body)};
}

bool WaitForServing(SupervisorChildProcess& child, int admin_port,
                    std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && child.IsRunning()) {
        const auto health = HttpGet(admin_port, "/health");
        if (health.status == 200 &&
            health.body.find("\"ha_state\":\"serving\"") != std::string::npos &&
            health.body.find("\"service_ready\":true") != std::string::npos) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

}  // namespace

int RunSupervisorChild() {
    MasterServiceSupervisorConfig config;
    config.enable_metric_reporting = false;
    config.metrics_port = FLAGS_ha_supervisor_admin_port;
    config.metrics_host = "127.0.0.1";
    config.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    config.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    config.allow_evict_soft_pinned_objects = true;
    config.eviction_ratio = DEFAULT_EVICTION_RATIO;
    config.eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    config.nof_eviction_ratio = DEFAULT_NOF_EVICTION_RATIO;
    config.nof_eviction_high_watermark_ratio =
        DEFAULT_NOF_EVICTION_HIGH_WATERMARK_RATIO;
    config.client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    config.nof_heartbeat_interval_sec = DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC;
    config.nof_heartbeat_probe_timeout_ms =
        DEFAULT_NOF_HEARTBEAT_PROBE_TIMEOUT_MS;
    config.nof_heartbeat_failures_threshold =
        DEFAULT_NOF_HEARTBEAT_FAILURES_THRESHOLD;
    config.enable_offload = false;
    config.rpc_address = "127.0.0.1";
    config.rpc_port = FLAGS_ha_supervisor_rpc_port;
    config.rpc_thread_num = 1;
    config.local_hostname =
        config.rpc_address + ":" + std::to_string(FLAGS_ha_supervisor_rpc_port);
    config.ha_backend_type = "etcd";
    config.ha_backend_connstring = FLAGS_etcd_endpoints;
    config.etcd_endpoints = FLAGS_etcd_endpoints;
    config.cluster_id = FLAGS_ha_supervisor_cluster_id;
    config.enable_oplog = false;

    ha::MasterServiceSupervisor supervisor(config);
    return supervisor.Start();
}

TEST_F(HighAvailabilityTest, AcquiredViewFlowsIntoServingMasterService) {
    constexpr ViewVersionId kAcquiredView = 42;
    FakeLeaderCoordinator coordinator(kAcquiredView);
    auto acquired = coordinator.TryAcquireLeadership("primary");
    ASSERT_TRUE(acquired.has_value());
    ASSERT_TRUE(acquired->session.has_value());

    MasterServiceSupervisorConfig supervisor_config;
    supervisor_config.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    supervisor_config.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    supervisor_config.allow_evict_soft_pinned_objects = true;
    supervisor_config.enable_metric_reporting = false;
    supervisor_config.metrics_port = 0;
    supervisor_config.eviction_ratio = DEFAULT_EVICTION_RATIO;
    supervisor_config.eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    supervisor_config.nof_eviction_ratio = DEFAULT_NOF_EVICTION_RATIO;
    supervisor_config.nof_eviction_high_watermark_ratio =
        DEFAULT_NOF_EVICTION_HIGH_WATERMARK_RATIO;
    supervisor_config.client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    supervisor_config.nof_heartbeat_interval_sec =
        DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC;
    supervisor_config.nof_heartbeat_probe_timeout_ms =
        DEFAULT_NOF_HEARTBEAT_PROBE_TIMEOUT_MS;
    supervisor_config.nof_heartbeat_failures_threshold =
        DEFAULT_NOF_HEARTBEAT_FAILURES_THRESHOLD;
    supervisor_config.enable_offload = false;

    WrappedMasterServiceConfig serving_config(
        supervisor_config, acquired->session->view.view_version);
    MasterService service{MasterServiceConfig(serving_config)};

    auto ping = service.Ping(generate_uuid());
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(kAcquiredView, ping->view_version_id);
}

#ifdef STORE_USE_ETCD

TEST_F(HighAvailabilityTest, HaWithoutOplogRestoresEmptyContextAndServes) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto ports = getFreeTcpPorts(2);
    ASSERT_EQ(ports.size(), 2);
    const std::string cluster_id =
        "ha-without-oplog-supervisor-" + std::to_string(getpid());
    SupervisorChildProcess child(ports[0], ports[1], cluster_id);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(WaitForServing(child, ports[1], std::chrono::seconds(20)));

    EXPECT_EQ(HttpGet(ports[1], "/get_all_keys").status, 200);
}

TEST_F(HighAvailabilityTest, EtcdBasicOperations) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    // == Test grant lease, create kv and get kv ==
    int64_t lease_ttl = 10;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    // Ordinary key-value pair
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_key1"));
    values.push_back("test_value1");
    // Key-value pair with null bytes in the middle
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_\0\0key2"));
    values.push_back("test_\0\0value2");
    // Key-value pair with null bytes at the end
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_key3\0\0"));
    values.push_back("test_value3\0\0");
    // Key-value pair with null bytes at the beginning
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("\0\0test_key4"));
    values.push_back("\0\0test_value4");

    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        auto& value = values[i];
        EtcdLeaseId lease_id;
        EtcdRevisionId version = 0;

        ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::CreateWithLease(
                                     key.c_str(), key.size(), value.c_str(),
                                     value.size(), lease_id, version));
        std::string get_value;
        EtcdRevisionId get_version;
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::Get(key.c_str(), key.size(),
                                                 get_value, get_version));
        ASSERT_EQ(value, get_value);
        ASSERT_EQ(version, get_version);
    }

    // == Test keep alive and cancel keep alive ==
    lease_ttl = 2;
    EtcdLeaseId lease_id;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));

    std::promise<ErrorCode> promise;
    std::future<ErrorCode> future = promise.get_future();

    std::thread keep_alive_thread([&]() {
        ErrorCode result = EtcdHelper::KeepAlive(lease_id);
        promise.set_value(result);
    });
    // Check if keep alive can extend the lease's life time
    ASSERT_NE(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    std::string key =
        FLAGS_etcd_test_key_prefix + std::string("keep_alive_key");
    std::string value = "keep_alive_value";
    EtcdRevisionId version = 0;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CreateWithLease(
                                 key.c_str(), key.size(), value.c_str(),
                                 value.size(), lease_id, version));
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), value, version));

    // Test cancel keep alive
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CancelKeepAlive(lease_id));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::ETCD_CTX_CANCELLED);
    keep_alive_thread.join();

    // == Test explicit lease revoke ==
    lease_ttl = 10;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
    std::string revoke_key =
        FLAGS_etcd_test_key_prefix + std::string("revoke_key");
    std::string revoke_value = "revoke_value";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CreateWithLease(
                  revoke_key.c_str(), revoke_key.size(), revoke_value.c_str(),
                  revoke_value.size(), lease_id, version));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::RevokeLease(lease_id));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(revoke_key.c_str(), revoke_key.size(),
                              revoke_value, version));

    // == Test watch key and cancel watch ==
    lease_ttl = 2;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
    std::string watch_key =
        FLAGS_etcd_test_key_prefix + std::string("watch_key");
    std::string watch_value = "watch_value";
    EtcdRevisionId watch_version = 0;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CreateWithLease(
                  watch_key.c_str(), watch_key.size(), watch_value.c_str(),
                  watch_value.size(), lease_id, watch_version));

    promise = std::promise<ErrorCode>();
    future = promise.get_future();
    keep_alive_thread = std::thread([&]() { EtcdHelper::KeepAlive(lease_id); });
    std::thread watch_thread([&]() {
        ErrorCode result =
            EtcdHelper::WatchUntilDeleted(watch_key.c_str(), watch_key.size());
        promise.set_value(result);
    });
    // Check the watch thread is blocked if the key is not deleted
    ASSERT_NE(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    // Check the watch thread returns after the key is deleted
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CancelKeepAlive(lease_id));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::OK);
    watch_thread.join();
    keep_alive_thread.join();

    // Test cancel watch
    lease_ttl = 10;
    int64_t watch_wait_time = 2;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
    watch_key = FLAGS_etcd_test_key_prefix + std::string("watch_key2");
    watch_value = "watch_value2";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CreateWithLease(
                  watch_key.c_str(), watch_key.size(), watch_value.c_str(),
                  watch_value.size(), lease_id, watch_version));

    promise = std::promise<ErrorCode>();
    future = promise.get_future();
    watch_thread = std::thread([&]() {
        ErrorCode result =
            EtcdHelper::WatchUntilDeleted(watch_key.c_str(), watch_key.size());
        promise.set_value(result);
    });
    // Wait for the watch thread to call WatchUntilDeleted
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Cancel the watch
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CancelWatch(watch_key.c_str(), watch_key.size()));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(watch_wait_time)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::ETCD_CTX_CANCELLED);
    watch_thread.join();
}

#endif

TEST_F(HighAvailabilityTest, BasicMasterViewOperations) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    std::string master_address = "0.0.0.0:8888";

    // Initially, the master view is not set
    auto current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_FALSE(current_view.value().has_value());

    auto acquire = coordinator->TryAcquireLeadership(master_address);
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    // The ownership reservation must block both upgraded and legacy
    // candidates while the service endpoint is still hidden.
    auto contender = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(contender, nullptr);
    auto contended = contender->TryAcquireLeadership("other-master:8888");
    ASSERT_TRUE(contended.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::CONTENDED, contended->status);
    ASSERT_FALSE(contended->observed_view.has_value());

#ifdef STORE_USE_ETCD
    EtcdLeaseId legacy_lease = 0;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::GrantLease(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC,
                                     legacy_lease));
    EtcdRevisionId legacy_revision = 0;
    const std::string master_view_key =
        "mooncake-store/mooncake_cluster/master_view";
    const std::string legacy_address = "legacy-master:8888";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              EtcdHelper::CreateWithLease(
                  master_view_key.c_str(), master_view_key.size(),
                  legacy_address.c_str(), legacy_address.size(), legacy_lease,
                  legacy_revision));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::RevokeLease(legacy_lease));
#endif

    // The lease-bound placeholder is not exposed as a routable master view.
    current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_FALSE(current_view.value().has_value());

    auto stale_session = session;
    stale_session.owner_token = "stale-owner";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              coordinator->PublishServiceReady(stale_session));

    ASSERT_EQ(ErrorCode::OK, coordinator->PublishServiceReady(session));
    EXPECT_EQ(ErrorCode::OK, coordinator->PublishServiceReady(session));

    // Check the master view is published after readiness.
    current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_TRUE(current_view.value().has_value());
    ASSERT_EQ(current_view.value()->leader_address, master_address);
    ASSERT_EQ(current_view.value()->view_version, session.view.view_version);

    auto no_change = coordinator->WaitForViewChange(
        session.view.view_version, std::chrono::milliseconds(200));
    ASSERT_TRUE(no_change.has_value());
    ASSERT_FALSE(no_change->changed);
    ASSERT_TRUE(no_change->timed_out);

    // Check the master view does not change
    std::this_thread::sleep_for(
        std::chrono::seconds(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC + 2));
    current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_TRUE(current_view.value().has_value());
    ASSERT_EQ(current_view.value()->leader_address, master_address);
    ASSERT_EQ(current_view.value()->view_version, session.view.view_version);

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));

    auto released = coordinator->WaitForViewChange(session.view.view_version,
                                                   std::chrono::seconds(2));
    ASSERT_TRUE(released.has_value());
    ASSERT_TRUE(released->changed);
    ASSERT_FALSE(released->current_view.has_value());

    current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_FALSE(current_view.value().has_value());

    auto reacquire = coordinator->TryAcquireLeadership("0.0.0.0:9999");
    ASSERT_TRUE(reacquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, reacquire->status);
    ASSERT_TRUE(reacquire->session.has_value());
    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*reacquire->session));
}

#ifdef STORE_USE_ETCD

// WaitForViewChange must return promptly when the leader's master_view key is
// deleted, driven by the etcd watch rather than the timeout. We give it a long
// (5s) timeout but delete the key after ~300ms; a watch-based implementation
// returns shortly after the deletion, well before the timeout would fire.
TEST_F(HighAvailabilityTest, WaitForViewChangeReturnsPromptlyOnLeaderLoss) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:5555");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    ASSERT_EQ(ErrorCode::OK, coordinator->PublishServiceReady(session));

    // Release leadership from another thread after a short delay; this revokes
    // the lease and deletes the master_view key, which the watch observes.
    std::thread releaser([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        coordinator->ReleaseLeadership(session);
    });

    const auto start = std::chrono::steady_clock::now();
    auto changed = coordinator->WaitForViewChange(session.view.view_version,
                                                  std::chrono::seconds(5));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    releaser.join();

    ASSERT_TRUE(changed.has_value());
    EXPECT_TRUE(changed->changed);
    EXPECT_FALSE(changed->current_view.has_value());
    // Must be driven by the watch (key deleted at ~300ms), not the 5s timeout.
    EXPECT_LT(elapsed, std::chrono::seconds(3));
}

// WaitForViewChange must honor its timeout when the leader is stable: the watch
// blocks with no deletion event, and the timer cancels it so the call returns
// timed_out at roughly the requested deadline -- neither returning early nor
// hanging past it.
TEST_F(HighAvailabilityTest, WaitForViewChangeTimesOutWhenStable) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:4444");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    ASSERT_EQ(ErrorCode::OK, coordinator->PublishServiceReady(session));

    const auto timeout = std::chrono::milliseconds(500);
    const auto start = std::chrono::steady_clock::now();
    auto result =
        coordinator->WaitForViewChange(session.view.view_version, timeout);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->changed);
    EXPECT_TRUE(result->timed_out);
    // Did not return early (watch did not spuriously fire) ...
    EXPECT_GE(elapsed, std::chrono::milliseconds(400));
    // ... and did not hang past the deadline (timer cancelled the watch).
    EXPECT_LT(elapsed, std::chrono::seconds(3));

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
}

// When the observed view already differs from the caller's known version,
// WaitForViewChange returns promptly: it arms the watch (now established
// synchronously via WithCreatedNotify) and then the initial read short-circuits
// on the version mismatch before any event is awaited. Passing no known version
// while a leader exists is one such case.
TEST_F(HighAvailabilityTest, WaitForViewChangeReturnsCurrentViewImmediately) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:3333");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    ASSERT_EQ(ErrorCode::OK, coordinator->PublishServiceReady(session));

    const auto start = std::chrono::steady_clock::now();
    auto result =
        coordinator->WaitForViewChange(std::nullopt, std::chrono::seconds(5));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->changed);
    ASSERT_TRUE(result->current_view.has_value());
    EXPECT_EQ(result->current_view->view_version, session.view.view_version);
    EXPECT_LT(elapsed, std::chrono::seconds(1));

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
}

TEST_F(HighAvailabilityTest, LeadershipMonitorReportsKeepAliveLoss) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:7777");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    std::promise<ha::LeadershipLossReason> loss_promise;
    auto loss_future = loss_promise.get_future();
    auto monitor = coordinator->StartLeadershipMonitor(
        session, [&loss_promise](ha::LeadershipLossReason reason) {
            loss_promise.set_value(reason);
        });
    ASSERT_TRUE(monitor.has_value());

    const auto lease_id =
        static_cast<EtcdLeaseId>(std::stoll(session.owner_token));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CancelKeepAlive(lease_id));
    ASSERT_EQ(loss_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(ha::LeadershipLossReason::kLostLeadership, loss_future.get());

    monitor.value()->Stop();
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
}

TEST_F(HighAvailabilityTest, EtcdStoreClientResetKeepsBasicOperationsWorking) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const std::string key =
        FLAGS_etcd_test_key_prefix + "reset_basic_operation";
    const std::string value_before = "before_reset";
    const std::string value_after = "after_reset";

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Put(key.c_str(), key.size(), value_before.c_str(),
                              value_before.size()));

    std::string got;
    EtcdRevisionId rev = 0;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), got, rev));
    ASSERT_EQ(value_before, got);

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::ResetEtcdStoreClient(FLAGS_etcd_endpoints));

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Put(key.c_str(), key.size(), value_after.c_str(),
                              value_after.size()));
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), got, rev));
    ASSERT_EQ(value_after, got);

    EtcdLeaseId lease_id = 0;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(10, lease_id));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::RevokeLease(lease_id));
}

TEST_F(HighAvailabilityTest, EtcdStoreClientResetStopsOldKeepAlive) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    EtcdLeaseId lease_id = 0;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(10, lease_id));

    std::promise<ErrorCode> promise;
    auto future = promise.get_future();
    std::thread keep_alive_thread(
        [&]() { promise.set_value(EtcdHelper::KeepAlive(lease_id)); });

    auto cleanup_keep_alive = [&]() {
        if (keep_alive_thread.joinable()) {
            (void)EtcdHelper::CancelKeepAlive(lease_id);
            keep_alive_thread.join();
        }
    };

    auto ready_err = EtcdHelper::WaitKeepAliveReady(lease_id, 1000);
    if (ready_err != ErrorCode::OK) {
        cleanup_keep_alive();
    }
    ASSERT_EQ(ErrorCode::OK, ready_err);

    auto reset_err = EtcdHelper::ResetEtcdStoreClient(FLAGS_etcd_endpoints);
    if (reset_err != ErrorCode::OK) {
        cleanup_keep_alive();
    }
    ASSERT_EQ(ErrorCode::OK, reset_err);

    auto keep_alive_status = future.wait_for(std::chrono::seconds(5));
    if (keep_alive_status != std::future_status::ready) {
        cleanup_keep_alive();
    }
    ASSERT_EQ(keep_alive_status, std::future_status::ready);
    EXPECT_NE(ErrorCode::OK, future.get());
    keep_alive_thread.join();

    EtcdLeaseId new_lease_id = 0;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(10, new_lease_id));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::RevokeLease(new_lease_id));
}

// Static data for prefix watch callback (C-linkage required for Go callback)
static std::atomic<int> g_prefix_watch_event_count{0};
static std::atomic<int> g_prefix_watch_broken_count{0};

extern "C" void PrefixWatchTestCallback(void* /*ctx*/, const char* /*key*/,
                                        size_t /*key_size*/,
                                        const char* /*value*/,
                                        size_t /*value_size*/, int event_type,
                                        int64_t /*mod_revision*/) {
    if (event_type == 2) {
        g_prefix_watch_broken_count.fetch_add(1);
    } else {
        g_prefix_watch_event_count.fetch_add(1);
    }
}

bool WaitForAtomicAtLeast(const std::atomic<int>& value, int expected,
                          std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (value.load() >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return value.load() >= expected;
}

TEST_F(HighAvailabilityTest,
       EtcdStoreClientResetTriggersPrefixWatchBrokenAndReconnect) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const std::string prefix =
        FLAGS_etcd_test_key_prefix + "reset_prefix_watch/";
    const std::string key1 = prefix + "key1";
    const std::string value1 = "value1";
    const std::string value2 = "value2";

    // Clean up any leftover keys
    std::string prefix_end = prefix;
    if (!prefix_end.empty()) prefix_end.back()++;
    (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(),
                                  prefix_end.c_str(), prefix_end.size());

    // Reset counters and start prefix watch
    g_prefix_watch_event_count.store(0);
    g_prefix_watch_broken_count.store(0);

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::WatchWithPrefixFromRevision(
                  prefix.c_str(), prefix.size(), /*start_revision=*/0,
                  /*callback_context=*/nullptr, PrefixWatchTestCallback));

    // Allow watch to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Write first value -- watch should receive it
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Put(key1.c_str(), key1.size(),
                                             value1.c_str(), value1.size()));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_GE(g_prefix_watch_event_count.load(), 1);

    // Trigger reset
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::ResetEtcdStoreClient(FLAGS_etcd_endpoints));

    // Wait for WATCH_BROKEN callback
    ASSERT_TRUE(WaitForAtomicAtLeast(g_prefix_watch_broken_count, 1,
                                     std::chrono::seconds(5)))
        << "Expected WATCH_BROKEN after reset";
    EXPECT_EQ(1, g_prefix_watch_broken_count.load());

    // Wait for old goroutine to fully exit
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::WaitWatchWithPrefixStopped(
                                 prefix.c_str(), prefix.size(), 5000));

    // Start a new watch -- should succeed (old entry cleaned up by defer)
    g_prefix_watch_event_count.store(0);
    g_prefix_watch_broken_count.store(0);

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::WatchWithPrefixFromRevision(
                  prefix.c_str(), prefix.size(), /*start_revision=*/0,
                  /*callback_context=*/nullptr, PrefixWatchTestCallback));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Write second value -- new watch should receive it
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Put(key1.c_str(), key1.size(),
                                             value2.c_str(), value2.size()));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_GE(g_prefix_watch_event_count.load(), 1)
        << "New watch should receive events after reset";

    // Clean up
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CancelWatchWithPrefix(prefix.c_str(), prefix.size()));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::WaitWatchWithPrefixStopped(
                                 prefix.c_str(), prefix.size(), 5000));
}

TEST_F(HighAvailabilityTest, EtcdStorePrefixWatchCancelDoesNotReportBroken) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const std::string prefix =
        FLAGS_etcd_test_key_prefix + "cancel_prefix_watch/";
    const std::string key = prefix + "key";
    const std::string value = "value";

    std::string prefix_end = prefix;
    if (!prefix_end.empty()) prefix_end.back()++;
    (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(),
                                  prefix_end.c_str(), prefix_end.size());

    g_prefix_watch_event_count.store(0);
    g_prefix_watch_broken_count.store(0);

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::WatchWithPrefixFromRevision(
                  prefix.c_str(), prefix.size(), /*start_revision=*/0,
                  /*callback_context=*/nullptr, PrefixWatchTestCallback));

    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Put(key.c_str(), key.size(),
                                             value.c_str(), value.size()));
    ASSERT_TRUE(WaitForAtomicAtLeast(g_prefix_watch_event_count, 1,
                                     std::chrono::seconds(5)));

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CancelWatchWithPrefix(prefix.c_str(), prefix.size()));
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::WaitWatchWithPrefixStopped(
                                 prefix.c_str(), prefix.size(), 5000));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(0, g_prefix_watch_broken_count.load())
        << "Explicit cancel should not report WATCH_BROKEN";
}

#endif

TEST_F(HighAvailabilityTest, LeadershipMonitorIgnoresExplicitRelease) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:6666");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    auto callback_fired = std::make_shared<std::atomic<bool>>(false);
    auto monitor = coordinator->StartLeadershipMonitor(
        session, [callback_fired](ha::LeadershipLossReason) {
            callback_fired->store(true);
        });
    ASSERT_TRUE(monitor.has_value());

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_FALSE(callback_fired->load());
}

#ifdef STORE_USE_ETCD

TEST_F(HighAvailabilityTest, OpLogPersistenceInterfaces) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    // 1. Basic Put & Get
    std::string key = FLAGS_etcd_test_key_prefix + "oplog_test_1";
    std::string val = "v1";
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Put(key.c_str(), key.size(),
                                             val.c_str(), val.size()));

    std::string got_val;
    EtcdRevisionId rev;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), got_val, rev));
    ASSERT_EQ(got_val, val);

    // 2. CAS Create
    std::string cas_key = FLAGS_etcd_test_key_prefix + "oplog_cas_1";
    EtcdHelper::DeleteRange(cas_key.c_str(), cas_key.size(),
                            (cas_key + "\0").c_str(), cas_key.size() + 1);

    // First create success
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Create(cas_key.c_str(), cas_key.size(),
                                                "initial", 7));
    // Second create fails
    ASSERT_EQ(
        ErrorCode::ETCD_TRANSACTION_FAIL,
        EtcdHelper::Create(cas_key.c_str(), cas_key.size(), "conflict", 8));

    // 3. Range Operations
    std::string prefix = FLAGS_etcd_test_key_prefix + "range/";
    std::string k1 = prefix + "a";
    std::string k2 = prefix + "b";
    std::string k3 = prefix + "c";

    // Clean up
    std::string prefix_end = prefix;
    if (!prefix_end.empty()) prefix_end.back()++;
    EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(), prefix_end.c_str(),
                            prefix_end.size());

    EtcdHelper::Put(k1.c_str(), k1.size(), "val_a", 5);
    EtcdHelper::Put(k2.c_str(), k2.size(), "val_b", 5);
    EtcdHelper::Put(k3.c_str(), k3.size(), "val_c", 5);

    std::string first, last;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GetFirstKeyWithPrefix(
                                 prefix.c_str(), prefix.size(), first));
    EXPECT_EQ(first, k1);
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GetLastKeyWithPrefix(
                                 prefix.c_str(), prefix.size(), last));
    EXPECT_EQ(last, k3);

    // GetRangeAsJson
    std::string json;
    EtcdRevisionId json_rev;
    // Get all keys in range [k1, k3) (end is exclusive); limit=0 means no
    // limit, so we get k1 and k2 but not k3
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::GetRangeAsJson(k1.c_str(), k1.size(), k3.c_str(),
                                         k3.size(), 0, json, json_rev));
    // Should contain val_a and val_b but NOT val_c
    EXPECT_NE(json.find("val_a"), std::string::npos);
    EXPECT_NE(json.find("val_b"), std::string::npos);
    EXPECT_EQ(json.find("val_c"), std::string::npos);

    // CreateWithLease & DeleteRange
    int64_t lease_ttl = 10;
    EtcdLeaseId lease_id;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));

    // Use DeleteRange to clear k1-k3
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::DeleteRange(k1.c_str(), k1.size(),
                                      (k3 + "\0").c_str(), k3.size() + 1));

    std::string dummy_val;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k1.c_str(), k1.size(), dummy_val, rev));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k2.c_str(), k2.size(), dummy_val, rev));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k3.c_str(), k3.size(), dummy_val, rev));
}

#endif

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (mooncake::testing::FLAGS_ha_supervisor_child) {
        google::InitGoogleLogging("HighAvailabilitySupervisorChild");
        FLAGS_logtostderr = 1;
        return mooncake::testing::RunSupervisorChild();
    }

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}
