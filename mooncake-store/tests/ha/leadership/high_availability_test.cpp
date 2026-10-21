#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif
#include "common/network.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/high_availability_test_fixture.h"
#include "ha/leadership/master_service_supervisor.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_types.h"
#include "master_client.h"
#include "master_service.h"
#include "shutdown_request.h"
#include "types.h"

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
DEFINE_bool(ha_supervisor_enable_oplog, false,
            "Child supervisor enables the batch OpLog");
DEFINE_string(ha_supervisor_shutdown_file, "",
              "Child supervisor also treats this file's existence as a "
              "shutdown request");
DEFINE_int64(ha_supervisor_client_live_ttl_sec, DEFAULT_CLIENT_LIVE_TTL_SEC,
             "Child client active TTL");
DEFINE_int64(ha_supervisor_client_suspicion_ttl_sec,
             DEFAULT_CLIENT_SUSPICION_TTL_SEC, "Child client suspicion TTL");

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
        return current_view_;
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
        std::chrono::milliseconds timeout) override {
        ++wait_calls_;
        last_wait_timeout_ = timeout;
        return wait_result_;
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

    void SetCurrentView(std::optional<ha::MasterView> view) {
        current_view_ = std::move(view);
    }

    void SetWaitResult(ha::ViewChangeResult result) {
        wait_result_ = std::move(result);
    }

    size_t wait_calls() const { return wait_calls_; }
    std::chrono::milliseconds last_wait_timeout() const {
        return last_wait_timeout_;
    }

   private:
    ha::LeadershipSession session_;
    std::optional<ha::MasterView> current_view_;
    ha::ViewChangeResult wait_result_;
    size_t wait_calls_{0};
    std::chrono::milliseconds last_wait_timeout_{0};
};

class SupervisorChildProcess {
   public:
    SupervisorChildProcess(
        int rpc_port, int admin_port, std::string cluster_id,
        bool enable_oplog = false, std::string log_path = "",
        std::string shutdown_file = "",
        int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC,
        int64_t client_suspicion_ttl_sec = DEFAULT_CLIENT_SUSPICION_TTL_SEC)
        : rpc_port_(rpc_port),
          admin_port_(admin_port),
          cluster_id_(std::move(cluster_id)),
          enable_oplog_(enable_oplog),
          log_path_(std::move(log_path)),
          shutdown_file_(std::move(shutdown_file)),
          client_live_ttl_sec_(client_live_ttl_sec),
          client_suspicion_ttl_sec_(client_suspicion_ttl_sec) {}

    ~SupervisorChildProcess() { Stop(); }

    bool Start() {
        // The parent may run client threads, so build everything the child
        // needs before fork(); between fork and exec the child only calls
        // dup2, execv and _exit.
        std::vector<std::string> args = {
            "/proc/self/exe",
            "--ha_supervisor_child=true",
            "--etcd_endpoints=" + FLAGS_etcd_endpoints,
            "--ha_supervisor_rpc_port=" + std::to_string(rpc_port_),
            "--ha_supervisor_admin_port=" + std::to_string(admin_port_),
            "--ha_supervisor_cluster_id=" + cluster_id_,
            std::string("--ha_supervisor_enable_oplog=") +
                (enable_oplog_ ? "true" : "false"),
            "--ha_supervisor_client_live_ttl_sec=" +
                std::to_string(client_live_ttl_sec_),
            "--ha_supervisor_client_suspicion_ttl_sec=" +
                std::to_string(client_suspicion_ttl_sec_),
        };
        if (!shutdown_file_.empty()) {
            args.push_back("--ha_supervisor_shutdown_file=" + shutdown_file_);
        }
        std::vector<char*> argv;
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        int log_fd = -1;
        if (!log_path_.empty()) {
            log_fd = open(log_path_.c_str(),
                          O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
            if (log_fd < 0) {
                return false;
            }
        }

        pid_ = fork();
        if (pid_ == 0) {
            if (log_fd >= 0) {
                dup2(log_fd, STDERR_FILENO);
            }
            execv("/proc/self/exe", argv.data());
            _exit(127);
        }
        if (log_fd >= 0) {
            close(log_fd);
        }
        return pid_ > 0;
    }

    bool IsRunning() {
        if (pid_ <= 0) {
            return false;
        }
        int status = 0;
        if (waitpid(pid_, &status, WNOHANG) == 0) {
            return true;
        }
        exit_status_ = status;
        pid_ = 0;
        return false;
    }

    bool Signal(int signo) { return pid_ > 0 && kill(pid_, signo) == 0; }

    // Raw wait status, or nullopt if the child is still running at timeout.
    std::optional<int> WaitForExit(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (IsRunning()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return std::nullopt;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return exit_status_;
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
    std::optional<int> exit_status_;
    int rpc_port_;
    int admin_port_;
    std::string cluster_id_;
    bool enable_oplog_;
    std::string log_path_;
    std::string shutdown_file_;
    int64_t client_live_ttl_sec_;
    int64_t client_suspicion_ttl_sec_;
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
    config.metrics = MetricsBootstrapConfig{
        .enabled = false,
        .port = static_cast<uint32_t>(FLAGS_ha_supervisor_admin_port),
        .host = "127.0.0.1",
    };
    config.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    config.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    config.allow_evict_soft_pinned_objects = true;
    config.eviction_ratio = DEFAULT_EVICTION_RATIO;
    config.eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    config.nof_eviction_ratio = DEFAULT_NOF_EVICTION_RATIO;
    config.nof_eviction_high_watermark_ratio =
        DEFAULT_NOF_EVICTION_HIGH_WATERMARK_RATIO;
    config.client_active_ttl_sec = FLAGS_ha_supervisor_client_live_ttl_sec;
    config.client_suspicion_ttl_sec =
        FLAGS_ha_supervisor_client_suspicion_ttl_sec;
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
    config.enable_oplog = FLAGS_ha_supervisor_enable_oplog;

    // Arm the shutdown bound and the handlers like main() does in HA mode.
    if (!StartShutdownWatchdog(ha::kHaShutdownDeadline) ||
        !InstallShutdownSignalHandlers()) {
        return 1;
    }
    std::function<bool()> shutdown_requested = ShutdownRequested;
    if (!FLAGS_ha_supervisor_shutdown_file.empty()) {
        shutdown_requested = [path = FLAGS_ha_supervisor_shutdown_file] {
            std::error_code ec;
            return ShutdownRequested() || std::filesystem::exists(path, ec);
        };
    }
    ha::MasterServiceSupervisor supervisor(config, shutdown_requested);
    const int result = supervisor.Start();
    if (ShutdownRequested() || ShutdownLatched()) {
        ExitAfterShutdown(result);
    }
    return result;
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
    supervisor_config.metrics = MetricsBootstrapConfig{
        .enabled = false,
        .port = 0,
        .host = std::string(MetricsBootstrapConfig::kDefaultHost),
    };
    supervisor_config.eviction_ratio = DEFAULT_EVICTION_RATIO;
    supervisor_config.eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    supervisor_config.nof_eviction_ratio = DEFAULT_NOF_EVICTION_RATIO;
    supervisor_config.nof_eviction_high_watermark_ratio =
        DEFAULT_NOF_EVICTION_HIGH_WATERMARK_RATIO;
    supervisor_config.client_active_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    supervisor_config.client_suspicion_ttl_sec =
        DEFAULT_CLIENT_SUSPICION_TTL_SEC;
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

TEST_F(HighAvailabilityTest, InitialDiscoveryWaitsForReadyView) {
    FakeLeaderCoordinator coordinator(/*view_version=*/42);
    const ha::MasterView ready_view{
        .leader_address = "ready-leader",
        .view_version = 43,
    };
    coordinator.SetWaitResult(ha::ViewChangeResult{
        .changed = true,
        .timed_out = false,
        .current_view = ready_view,
    });

    constexpr auto kTimeout = std::chrono::milliseconds(1234);
    auto result = ha::ReadCurrentViewOrWaitForReady(coordinator, kTimeout);

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(result->value().leader_address, ready_view.leader_address);
    EXPECT_EQ(result->value().view_version, ready_view.view_version);
    EXPECT_EQ(coordinator.wait_calls(), 1u);
    EXPECT_EQ(coordinator.last_wait_timeout(), kTimeout);
}

TEST_F(HighAvailabilityTest, InitialDiscoveryReturnsEmptyAfterTimeout) {
    FakeLeaderCoordinator coordinator(/*view_version=*/42);
    coordinator.SetWaitResult(ha::ViewChangeResult{
        .changed = false,
        .timed_out = true,
        .current_view = std::nullopt,
    });

    auto result = ha::ReadCurrentViewOrWaitForReady(
        coordinator, std::chrono::milliseconds(25));

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
    EXPECT_EQ(coordinator.wait_calls(), 1u);
}

TEST_F(HighAvailabilityTest, ServingStateLossDoesNotWaitForPublication) {
    ha::detail::ServingStateGate serving_state;
    std::promise<void> publication_started;
    std::promise<void> finish_publication;
    auto finish_publication_future = finish_publication.get_future();
    std::atomic<bool> activated{false};
    std::atomic<bool> stopped{false};

    auto publication = std::async(std::launch::async, [&]() {
        publication_started.set_value();
        finish_publication_future.wait();
        return serving_state.RunIfActive(
            [&]() { activated.store(true, std::memory_order_release); });
    });
    publication_started.get_future().wait();

    auto leadership_loss = std::async(std::launch::async, [&]() {
        serving_state.RequestShutdown(
            [&]() { stopped.store(true, std::memory_order_release); });
    });

    EXPECT_EQ(leadership_loss.wait_for(std::chrono::milliseconds(500)),
              std::future_status::ready);
    EXPECT_TRUE(stopped.load(std::memory_order_acquire));

    finish_publication.set_value();
    EXPECT_FALSE(publication.get());
    leadership_loss.get();
    EXPECT_FALSE(activated.load(std::memory_order_acquire));
}

TEST_F(HighAvailabilityTest, ServingStateSerializesActivationAndLoss) {
    ha::detail::ServingStateGate serving_state;
    std::promise<void> activation_started;
    std::promise<void> finish_activation;
    auto finish_activation_future = finish_activation.get_future();
    std::promise<void> shutdown_started;
    std::atomic<bool> activated{false};
    std::atomic<bool> stopped{false};

    auto activation = std::async(std::launch::async, [&]() {
        return serving_state.RunIfActive([&]() {
            activation_started.set_value();
            finish_activation_future.wait();
            activated.store(true, std::memory_order_release);
        });
    });
    activation_started.get_future().wait();

    auto leadership_loss = std::async(std::launch::async, [&]() {
        shutdown_started.set_value();
        return serving_state.RequestShutdown(
            [&]() { stopped.store(true, std::memory_order_release); });
    });
    shutdown_started.get_future().wait();

    EXPECT_EQ(leadership_loss.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_TRUE(serving_state.IsActive());

    finish_activation.set_value();
    EXPECT_TRUE(activation.get());
    EXPECT_TRUE(leadership_loss.get());
    EXPECT_TRUE(activated.load(std::memory_order_acquire));
    EXPECT_TRUE(stopped.load(std::memory_order_acquire));
    EXPECT_FALSE(serving_state.IsActive());
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

namespace {

using namespace std::chrono_literals;

constexpr std::string_view kWarmingPrefix = "__mooncake_service_warming__:";

#ifdef MOONCAKE_ENABLE_TEST_FAILPOINTS
constexpr bool kFailPointsEnabled = true;
#else
constexpr bool kFailPointsEnabled = false;
#endif

// File-handshake fail points (see TestFailPoint::Wait): the test arms a point,
// waits for the process under test to hit it, and releases it. Without
// MOONCAKE_ENABLE_TEST_FAILPOINTS the points are no-ops and every method here
// does nothing, so cases that need them skip first.
class ScopedFailPointDir {
   public:
    ScopedFailPointDir() {
        if (!kFailPointsEnabled) {
            return;
        }
        std::string pattern =
            ::testing::TempDir() + "mooncake-shutdown-fp-XXXXXX";
        if (mkdtemp(pattern.data()) == nullptr) {
            ADD_FAILURE() << "mkdtemp failed for " << pattern;
            return;
        }
        dir_ = pattern;
        setenv("MOONCAKE_TEST_FAILPOINT_DIR", dir_.c_str(), 1);
        setenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC", "20", 1);
    }

    ~ScopedFailPointDir() {
        if (dir_.empty()) {
            return;
        }
        unsetenv("MOONCAKE_TEST_FAILPOINT_DIR");
        unsetenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC");
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    ScopedFailPointDir(const ScopedFailPointDir&) = delete;
    ScopedFailPointDir& operator=(const ScopedFailPointDir&) = delete;

    void Arm(const std::string& name) { Touch(name + ".arm"); }

    bool WaitForHit(const std::string& name,
                    std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (std::filesystem::exists(dir_ + "/" + name + ".hit")) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    void Release(const std::string& name) { Touch(name + ".release"); }

   private:
    void Touch(const std::string& file) {
        if (!dir_.empty()) {
            std::ofstream(dir_ + "/" + file).put('\n');
        }
    }

    std::string dir_;
};

std::string TempPath(const std::string& name, const std::string& suffix) {
    return ::testing::TempDir() + name + "-" + std::to_string(getpid()) +
           suffix;
}

std::string MasterViewKey(const std::string& cluster_id) {
    return "mooncake-store/" + cluster_id + "/master_view";
}

std::optional<std::string> ReadEtcdKey(const std::string& key,
                                       EtcdRevisionId* revision = nullptr) {
    std::string value;
    EtcdRevisionId rev = 0;
    const ErrorCode err = EtcdHelper::Get(key.c_str(), key.size(), value, rev);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return std::nullopt;
    }
    if (err != ErrorCode::OK) {
        ADD_FAILURE() << "etcd Get(" << key << ") failed: " << toString(err);
        return std::nullopt;
    }
    if (revision != nullptr) {
        *revision = rev;
    }
    return value;
}

bool WaitForEtcdKeyGone(const std::string& key,
                        std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (!ReadEtcdKey(key).has_value()) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool WaitForHealthSubstring(SupervisorChildProcess& child, int admin_port,
                            const std::string& needle,
                            std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && child.IsRunning()) {
        const auto health = HttpGet(admin_port, "/health");
        if (health.status == 200 &&
            health.body.find(needle) != std::string::npos) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

std::string ReadTextFile(const std::string& path) {
    std::ifstream input(path);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

bool WaitForLogLine(const std::string& path, const std::string& needle,
                    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (ReadTextFile(path).find(needle) != std::string::npos) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

// True if the child logged "finishing from phase=<phase>" exactly, so that
// "serve_start" does not match "serve_start_failure".
bool LogHasPhase(const std::string& log, std::string_view phase) {
    return log.find("finishing from phase=" + std::string(phase) + "\n") !=
           std::string::npos;
}

bool LogContains(const std::string& log, std::string_view needle) {
    return log.find(needle) != std::string::npos;
}

void ExpectExitedWith(const std::optional<int>& status, int code) {
    ASSERT_TRUE(status.has_value()) << "child did not exit in time";
    ASSERT_TRUE(WIFEXITED(*status))
        << "child was terminated by signal "
        << (WIFSIGNALED(*status) ? WTERMSIG(*status) : 0);
    EXPECT_EQ(WEXITSTATUS(*status), code);
}

int64_t MillisSince(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

std::optional<SegmentMountOp> FindDurableSegmentMount(
    const std::string& cluster_id, const std::string& segment_name) {
    auto backend =
        CreateHaKvBackend(ha::HABackendSpec{.type = ha::HABackendType::ETCD,
                                            .connstring = FLAGS_etcd_endpoints,
                                            .cluster_namespace = cluster_id});
    if (!backend) {
        ADD_FAILURE() << "CreateHaKvBackend failed: "
                      << toString(backend.error());
        return std::nullopt;
    }
    OpLogBatchStorage storage(cluster_id, *backend.value());
    DurablePrefix prefix;
    if (const auto err = storage.ReadDurablePrefix(prefix);
        err != ErrorCode::OK) {
        ADD_FAILURE() << "ReadDurablePrefix failed: " << toString(err);
        return std::nullopt;
    }
    for (uint64_t batch_id = 1; batch_id <= prefix.batch_id; ++batch_id) {
        OpLogBatchRecord batch;
        if (const auto err = storage.ReadBatch(batch_id, batch);
            err != ErrorCode::OK) {
            ADD_FAILURE() << "ReadBatch(" << batch_id
                          << ") failed: " << toString(err);
            return std::nullopt;
        }
        for (const auto& entry : batch.entries) {
            if (entry.op_type != OpType::SEGMENT_MOUNT) {
                continue;
            }
            SegmentMountOp op;
            if (struct_pack::deserialize_to(op, entry.payload) !=
                    struct_pack::errc::ok ||
                op.segment_name != segment_name) {
                continue;
            }
            return op;
        }
    }
    return std::nullopt;
}

constexpr size_t kTestSegmentSize = 16 * 1024 * 1024;

tl::expected<void, ErrorCode> MountTestSegment(MasterClient& client,
                                               const std::string& name) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = name;
    segment.base = 0x300000000;
    segment.size = kTestSegmentSize;
    segment.te_endpoint = "127.0.0.1:1";
    return client.MountSegment(segment);
}

// Reads the lease ID that the child publishes in the warming value of
// master_view, which is the decimal owner token.
std::optional<int64_t> WaitForWarmingLeaseId(
    SupervisorChildProcess& child, const std::string& key,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && child.IsRunning()) {
        const auto value = ReadEtcdKey(key);
        if (value.has_value() && value->starts_with(kWarmingPrefix)) {
            try {
                return std::stoll(value->substr(kWarmingPrefix.size()));
            } catch (const std::exception&) {
                ADD_FAILURE() << "unparsable warming value: " << *value;
                return std::nullopt;
            }
        }
        std::this_thread::sleep_for(20ms);
    }
    return std::nullopt;
}

struct PortPair {
    int rpc;
    int admin;
};

std::vector<PortPair> FreePortPairs(size_t count) {
    const auto ports = getFreeTcpPorts(static_cast<int>(count * 2));
    std::vector<PortPair> pairs;
    for (size_t i = 0; i + 1 < ports.size(); i += 2) {
        pairs.push_back({ports[i], ports[i + 1]});
    }
    return pairs;
}

std::string ClusterIdFor(const std::string& name) {
    return name + "-" + std::to_string(getpid());
}

}  // namespace

TEST_F(HighAvailabilityTest, ServingLeaderSigtermDrainsAndKeepsLease) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("sigterm-serving");
    const std::string log_path = TempPath("sigterm-serving", ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 20s));
    const std::string key = MasterViewKey(cluster_id);
    ASSERT_TRUE(ReadEtcdKey(key).has_value());

    const auto sent = std::chrono::steady_clock::now();
    ASSERT_TRUE(child.Signal(SIGTERM));
    const auto status = child.WaitForExit(10s);
    LOG(INFO) << "SIGTERM to exit: " << MillisSince(sent) << " ms";
    ExpectExitedWith(status, 0);

    EXPECT_TRUE(ReadEtcdKey(key).has_value())
        << "no deliberate release: the key is left to TTL expiry";
    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogContains(log, "reason=shutdown requested")) << log;
    EXPECT_TRUE(LogHasPhase(log, "serving")) << log;
    EXPECT_TRUE(LogContains(log, "no release attempted")) << log;
    EXPECT_TRUE(WaitForEtcdKeyGone(key, 8s)) << "the lease must expire by TTL";
}

TEST_F(HighAvailabilityTest, StandbySigtermLeavesLeaderUntouched) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto ports = FreePortPairs(2);
    ASSERT_EQ(ports.size(), 2u);
    const std::string cluster_id = ClusterIdFor("sigterm-standby");
    const std::string log_path_a = TempPath("sigterm-standby-a", ".log");
    const std::string log_path_b = TempPath("sigterm-standby-b", ".log");
    SupervisorChildProcess leader(ports[0].rpc, ports[0].admin, cluster_id,
                                  /*enable_oplog=*/false, log_path_a);
    ASSERT_TRUE(leader.Start());
    ASSERT_TRUE(WaitForServing(leader, ports[0].admin, 20s));
    SupervisorChildProcess standby(ports[1].rpc, ports[1].admin, cluster_id,
                                   /*enable_oplog=*/false, log_path_b);
    ASSERT_TRUE(standby.Start());
    ASSERT_TRUE(WaitForHealthSubstring(
        standby, ports[1].admin,
        "\"leader_address\":\"127.0.0.1:" + std::to_string(ports[0].rpc) + "\"",
        20s));

    const std::string key = MasterViewKey(cluster_id);
    EtcdRevisionId revision_before = 0;
    const auto value_before = ReadEtcdKey(key, &revision_before);
    ASSERT_TRUE(value_before.has_value());

    const auto sent = std::chrono::steady_clock::now();
    ASSERT_TRUE(standby.Signal(SIGTERM));
    const auto status = standby.WaitForExit(10s);
    LOG(INFO) << "standby SIGTERM to exit: " << MillisSince(sent) << " ms";
    ExpectExitedWith(status, 0);

    EtcdRevisionId revision_after = 0;
    const auto value_after = ReadEtcdKey(key, &revision_after);
    EXPECT_EQ(value_after, value_before);
    EXPECT_EQ(revision_after, revision_before);
    EXPECT_TRUE(WaitForServing(leader, ports[0].admin, 2s));
    EXPECT_TRUE(LogContains(ReadTextFile(log_path_b), "no lease held"));
}

TEST_F(HighAvailabilityTest, WarmupSigtermKeepsLease) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("sigterm-warmup");
    const std::string log_path = TempPath("sigterm-warmup", ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path);
    ASSERT_TRUE(child.Start());
    // The initial warmup lasts the lease TTL, so the signal lands inside it.
    ASSERT_TRUE(WaitForLogLine(log_path, "Entering warmup phase...", 20s));

    const auto sent = std::chrono::steady_clock::now();
    ASSERT_TRUE(child.Signal(SIGTERM));
    const auto status = child.WaitForExit(10s);
    LOG(INFO) << "warmup SIGTERM to exit: " << MillisSince(sent) << " ms";
    ExpectExitedWith(status, 0);

    const auto value = ReadEtcdKey(MasterViewKey(cluster_id));
    ASSERT_TRUE(value.has_value());
    EXPECT_TRUE(value->starts_with(kWarmingPrefix)) << *value;
    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogContains(log, "no release attempted")) << log;
    EXPECT_TRUE(LogHasPhase(log, "warmup")) << log;
    EXPECT_FALSE(LogContains(log, "Master runtime state -> recovering"))
        << "the request must be seen in the initial warmup, not after restore";
}

TEST_F(HighAvailabilityTest, OplogLeaderCommitsQueuedRecordsBeforeExit) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    ScopedFailPointDir failpoints;
    const auto ports = FreePortPairs(2);
    ASSERT_EQ(ports.size(), 2u);
    const std::string cluster_id = ClusterIdFor("sigterm-oplog");
    const std::string log_path = TempPath("sigterm-oplog", ".log");
    const std::string segment_name =
        "ha-shutdown-d-" + std::to_string(getpid());
    constexpr int64_t kLiveTtlSec = 120;
    constexpr int64_t kSuspicionTtlSec = 240;
    {
        SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                     /*enable_oplog=*/true, log_path, "",
                                     kLiveTtlSec, kSuspicionTtlSec);
        ASSERT_TRUE(child.Start());
        ASSERT_TRUE(WaitForServing(child, ports[0].admin, 30s));
        if (kFailPointsEnabled) {
            failpoints.Arm("batch_before_txn");
        }

        MasterClient client(generate_uuid());
        ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(ports[0].rpc)),
                  ErrorCode::OK);
        auto mounted = MountTestSegment(client, segment_name);
        ASSERT_TRUE(mounted.has_value()) << toString(mounted.error());

        const auto sent = std::chrono::steady_clock::now();
        if (kFailPointsEnabled) {
            // Hold the writer before its etcd transaction so the entry is
            // queued but not durable while the drain starts.
            ASSERT_TRUE(failpoints.WaitForHit("batch_before_txn", 5s));
            ASSERT_TRUE(child.Signal(SIGTERM));
            ASSERT_TRUE(
                WaitForLogLine(log_path, "reason=shutdown requested", 5s));
            std::this_thread::sleep_for(1s);
            EXPECT_TRUE(child.IsRunning())
                << "the drain must wait for the writer";
            EXPECT_FALSE(
                LogContains(ReadTextFile(log_path), "Shutdown complete"));
            failpoints.Release("batch_before_txn");
        } else {
            ASSERT_TRUE(child.Signal(SIGTERM));
        }
        const auto status = child.WaitForExit(10s);
        LOG(INFO) << "OpLog SIGTERM to exit: " << MillisSince(sent) << " ms";
        ExpectExitedWith(status, 0);
    }

    EXPECT_TRUE(ReadEtcdKey(MasterViewKey(cluster_id)).has_value());
    const auto durable = FindDurableSegmentMount(cluster_id, segment_name);
    ASSERT_TRUE(durable.has_value())
        << "the mounted segment is missing from the durable OpLog";
    EXPECT_EQ(durable->capacity, kTestSegmentSize);
    EXPECT_EQ(durable->transport_endpoint, "127.0.0.1:1");

    // A successor waits out the lease TTL and replays the log. A restored
    // segment is a placeholder until its client remounts, so the admin
    // segment list cannot show it; the restore log line counts it instead.
    const std::string successor_log =
        TempPath("sigterm-oplog-successor", ".log");
    SupervisorChildProcess successor(ports[1].rpc, ports[1].admin, cluster_id,
                                     /*enable_oplog=*/true, successor_log, "",
                                     kLiveTtlSec, kSuspicionTtlSec);
    ASSERT_TRUE(successor.Start());
    ASSERT_TRUE(WaitForServing(successor, ports[1].admin, 40s));
    const std::string successor_text = ReadTextFile(successor_log);
    EXPECT_TRUE(LogContains(successor_text,
                            "Restored from standby: 0 objects, "
                            "1 segments"))
        << successor_text;
}

// Revokes the leader's lease and signals it back to back, in either order.
void RunSigtermRacingLeaseLoss(bool revoke_first) {
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string name =
        revoke_first ? "sigterm-revoke-first" : "sigterm-signal-first";
    const std::string cluster_id = ClusterIdFor(name);
    const std::string log_path = TempPath(name, ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path);
    ASSERT_TRUE(child.Start());
    const std::string key = MasterViewKey(cluster_id);
    const auto lease_id = WaitForWarmingLeaseId(child, key, 20s);
    ASSERT_TRUE(lease_id.has_value());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 20s));

    if (revoke_first) {
        ASSERT_EQ(EtcdHelper::RevokeLease(*lease_id), ErrorCode::OK);
        ASSERT_TRUE(child.Signal(SIGTERM));
    } else {
        ASSERT_TRUE(child.Signal(SIGTERM));
        ASSERT_EQ(EtcdHelper::RevokeLease(*lease_id), ErrorCode::OK);
    }
    ExpectExitedWith(child.WaitForExit(10s), 0);

    EXPECT_TRUE(LogContains(ReadTextFile(log_path), "Shutdown complete"));
    EXPECT_FALSE(ReadEtcdKey(key).has_value()) << "the lease was revoked";
}

TEST_F(HighAvailabilityTest, SigtermRacingLeaseLossExitsZeroRevokeFirst) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    RunSigtermRacingLeaseLoss(/*revoke_first=*/true);
}

TEST_F(HighAvailabilityTest, SigtermRacingLeaseLossExitsZeroSignalFirst) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    RunSigtermRacingLeaseLoss(/*revoke_first=*/false);
}

TEST_F(HighAvailabilityTest, SigtermBeforeServeStartStopsWriter) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    failpoints.Arm("supervisor_before_serve_start");
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("sigterm-before-serve");
    const std::string log_path = TempPath("sigterm-before-serve", ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/true, log_path);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(failpoints.WaitForHit("supervisor_before_serve_start", 30s));

    const auto sent = std::chrono::steady_clock::now();
    ASSERT_TRUE(child.Signal(SIGTERM));
    ASSERT_TRUE(WaitForLogLine(log_path, "reason=shutdown requested", 5s));
    EXPECT_TRUE(WaitForHealthSubstring(child, ports[0].admin,
                                       "\"service_ready\":false", 5s));
    failpoints.Release("supervisor_before_serve_start");
    ExpectExitedWith(child.WaitForExit(10s), 0);
    LOG(INFO) << "pre-listener SIGTERM to exit: " << MillisSince(sent) << " ms";

    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogHasPhase(log, "serve_start")) << log;
    const auto stopped = log.find(
        "Shutdown stopped the term's workers, monitor and OpLog "
        "writer");
    ASSERT_NE(stopped, std::string::npos) << log;
    EXPECT_LT(stopped, log.find("Shutdown complete")) << log;
    const auto value = ReadEtcdKey(MasterViewKey(cluster_id));
    ASSERT_TRUE(value.has_value());
    EXPECT_TRUE(value->starts_with(kWarmingPrefix)) << *value;
}

TEST_F(HighAvailabilityTest, SigtermBeforeServeActivationNeverServes) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    failpoints.Arm("supervisor_before_serve_activation");
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("sigterm-before-activation");
    const std::string log_path = TempPath("sigterm-before-activation", ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(
        failpoints.WaitForHit("supervisor_before_serve_activation", 30s));

    ASSERT_TRUE(child.Signal(SIGTERM));
    ASSERT_TRUE(WaitForLogLine(log_path, "reason=shutdown requested", 5s));
    failpoints.Release("supervisor_before_serve_activation");
    ExpectExitedWith(child.WaitForExit(10s), 0);

    const std::string log = ReadTextFile(log_path);
    EXPECT_FALSE(LogContains(log, "Master runtime state -> serving")) << log;
    EXPECT_TRUE(LogHasPhase(log, "serving")) << log;
    const auto value = ReadEtcdKey(MasterViewKey(cluster_id));
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "127.0.0.1:" + std::to_string(ports[0].rpc))
        << "published, then left to TTL";
}

TEST_F(HighAvailabilityTest, InjectedPredicateDrivesServingShutdown) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("injected-predicate");
    const std::string log_path = TempPath("injected-predicate", ".log");
    const std::string shutdown_file =
        TempPath("injected-predicate", ".shutdown");
    std::filesystem::remove(shutdown_file);
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path,
                                 shutdown_file);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 20s));

    const auto sent = std::chrono::steady_clock::now();
    std::ofstream(shutdown_file).put('\n');
    const auto status = child.WaitForExit(10s);
    LOG(INFO) << "injected request to exit: " << MillisSince(sent) << " ms";
    std::filesystem::remove(shutdown_file);
    ExpectExitedWith(status, 0);

    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogContains(log, "reason=shutdown requested")) << log;
    EXPECT_TRUE(LogHasPhase(log, "serving")) << log;
    EXPECT_TRUE(ReadEtcdKey(MasterViewKey(cluster_id)).has_value());
}

TEST_F(HighAvailabilityTest, SecondSigtermDuringDrainExits143) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    failpoints.Arm("supervisor_before_serve_start");
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("second-sigterm");
    const std::string log_path = TempPath("second-sigterm", ".log");
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/true, log_path);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(failpoints.WaitForHit("supervisor_before_serve_start", 30s));
    ASSERT_TRUE(child.Signal(SIGTERM));
    ASSERT_TRUE(WaitForLogLine(log_path, "reason=shutdown requested", 5s));

    const auto sent = std::chrono::steady_clock::now();
    ASSERT_TRUE(child.Signal(SIGTERM));
    ExpectExitedWith(child.WaitForExit(2s), 143);
    LOG(INFO) << "second SIGTERM to exit: " << MillisSince(sent) << " ms";
    EXPECT_TRUE(ReadEtcdKey(MasterViewKey(cluster_id)).has_value());
}

TEST_F(HighAvailabilityTest, SigtermWaitsForAdmittedHandler) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("sigterm-admitted");
    const std::string log_path = TempPath("sigterm-admitted", ".log");
    const std::string segment_name =
        "ha-shutdown-k-" + std::to_string(getpid());
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/true, log_path);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 30s));

    failpoints.Arm("rpc_mount_segment_before_service");
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(ports[0].rpc)),
              ErrorCode::OK);
    // The response may be lost when the server closes the socket, so the
    // result is ignored.
    std::jthread mount([&] { (void)MountTestSegment(client, segment_name); });
    ASSERT_TRUE(failpoints.WaitForHit("rpc_mount_segment_before_service", 5s))
        << "no handler was admitted";

    ASSERT_TRUE(child.Signal(SIGTERM));
    ASSERT_TRUE(WaitForLogLine(log_path, "reason=shutdown requested", 5s));
    std::this_thread::sleep_for(1s);
    EXPECT_TRUE(child.IsRunning()) << "the drain must wait for the handler";
    EXPECT_FALSE(LogContains(ReadTextFile(log_path), "finishing from phase="));

    const auto released = std::chrono::steady_clock::now();
    failpoints.Release("rpc_mount_segment_before_service");
    const auto status = child.WaitForExit(10s);
    LOG(INFO) << "release to exit: " << MillisSince(released) << " ms";
    mount.join();
    ExpectExitedWith(status, 0);

    EXPECT_TRUE(LogHasPhase(ReadTextFile(log_path), "serving"));
    EXPECT_TRUE(FindDurableSegmentMount(cluster_id, segment_name).has_value())
        << "the admitted handler's entry must be durable before exit";
}

TEST_F(HighAvailabilityTest, InjectedRequestBlockedInDrainEndsByWatchdog) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("blocked-drain");
    const std::string log_path = TempPath("blocked-drain", ".log");
    const std::string shutdown_file = TempPath("blocked-drain", ".shutdown");
    std::filesystem::remove(shutdown_file);
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path,
                                 shutdown_file);
    ASSERT_TRUE(child.Start());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 20s));

    failpoints.Arm("rpc_mount_segment_before_service");
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(ports[0].rpc)),
              ErrorCode::OK);
    std::jthread mount([&] {
        (void)MountTestSegment(client,
                               "ha-shutdown-l-" + std::to_string(getpid()));
    });
    ASSERT_TRUE(failpoints.WaitForHit("rpc_mount_segment_before_service", 5s));

    // No signal: only the latched request starts the watchdog deadline. The
    // handler is never released, so the drain cannot finish by itself.
    const auto requested = std::chrono::steady_clock::now();
    std::ofstream(shutdown_file).put('\n');
    const auto status = child.WaitForExit(25s);
    const auto elapsed_ms = MillisSince(requested);
    LOG(INFO) << "injected request to watchdog exit: " << elapsed_ms << " ms";
    mount.join();
    std::filesystem::remove(shutdown_file);
    ExpectExitedWith(status, 1);
    EXPECT_GE(elapsed_ms, 14000);
    EXPECT_LE(elapsed_ms, 20000);

    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogContains(log, "reason=shutdown requested")) << log;
    EXPECT_FALSE(LogContains(log, "Shutdown complete")) << log;
}

TEST_F(HighAvailabilityTest, LateRequestAfterOrdinaryReleaseKeepsResult) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }
    if (!kFailPointsEnabled) {
        GTEST_SKIP() << "needs MOONCAKE_ENABLE_TEST_FAILPOINTS";
    }

    ScopedFailPointDir failpoints;
    const auto ports = FreePortPairs(1);
    ASSERT_EQ(ports.size(), 1u);
    const std::string cluster_id = ClusterIdFor("late-request");
    const std::string log_path = TempPath("late-request", ".log");
    const std::string shutdown_file = TempPath("late-request", ".shutdown");
    std::filesystem::remove(shutdown_file);
    SupervisorChildProcess child(ports[0].rpc, ports[0].admin, cluster_id,
                                 /*enable_oplog=*/false, log_path,
                                 shutdown_file);
    ASSERT_TRUE(child.Start());
    const std::string key = MasterViewKey(cluster_id);
    const auto lease_id = WaitForWarmingLeaseId(child, key, 20s);
    ASSERT_TRUE(lease_id.has_value());
    ASSERT_TRUE(WaitForServing(child, ports[0].admin, 20s));

    // Losing the lease stops the listener, and the serving exit releases
    // through the supervisor's ordinary path; hold it right after that release.
    failpoints.Arm("supervisor_after_term_release");
    ASSERT_EQ(EtcdHelper::RevokeLease(*lease_id), ErrorCode::OK);
    ASSERT_TRUE(failpoints.WaitForHit("supervisor_after_term_release", 10s));
    std::ofstream(shutdown_file).put('\n');
    failpoints.Release("supervisor_after_term_release");
    ExpectExitedWith(child.WaitForExit(10s), 0);
    std::filesystem::remove(shutdown_file);

    const std::string log = ReadTextFile(log_path);
    EXPECT_TRUE(LogHasPhase(log, "serving")) << log;
    EXPECT_TRUE(
        LogContains(log, "Shutdown complete after leadership release, result="))
        << log;
    EXPECT_FALSE(LogContains(log, "no lease held")) << log;
    EXPECT_FALSE(LogContains(log, "no release attempted")) << log;
    EXPECT_FALSE(LogContains(log, "Release leadership: "))
        << "the exit must come from the check right after the release";
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

TEST_F(HighAvailabilityTest, WaitForViewChangeObservesWarmingLeaderReady) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:2222");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto warming_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(warming_view.has_value());
    ASSERT_FALSE(warming_view->has_value());

    auto publisher = std::async(std::launch::async, [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return coordinator->PublishServiceReady(session);
    });

    auto ready_view =
        coordinator->WaitForViewChange(std::nullopt, std::chrono::seconds(5));
    EXPECT_EQ(ErrorCode::OK, publisher.get());

    ASSERT_TRUE(ready_view.has_value());
    EXPECT_TRUE(ready_view->changed);
    ASSERT_TRUE(ready_view->current_view.has_value());
    EXPECT_EQ(ready_view->current_view->leader_address,
              session.view.leader_address);
    EXPECT_EQ(ready_view->current_view->view_version,
              session.view.view_version);

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

    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::ResetEtcdStoreClient(FLAGS_etcd_endpoints));
    EXPECT_EQ(loss_future.wait_for(std::chrono::seconds(1)),
              std::future_status::timeout);

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

TEST_F(HighAvailabilityTest, EtcdStoreClientResetKeepsOldKeepAlive) {
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

    auto keep_alive_status = future.wait_for(std::chrono::seconds(1));
    if (keep_alive_status != std::future_status::timeout) {
        keep_alive_thread.join();
    }
    ASSERT_EQ(keep_alive_status, std::future_status::timeout);

    auto cancel_err = EtcdHelper::CancelKeepAlive(lease_id);
    if (cancel_err != ErrorCode::OK) {
        cleanup_keep_alive();
    }
    ASSERT_EQ(ErrorCode::OK, cancel_err);
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(ErrorCode::ETCD_CTX_CANCELLED, future.get());
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
