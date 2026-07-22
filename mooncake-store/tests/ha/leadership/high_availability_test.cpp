#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
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
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(etcd_endpoints, "127.0.0.1:2379", "Etcd endpoints");
DEFINE_string(etcd_test_key_prefix, "mooncake-store/test/",
              "The prefix of the test keys in ETCD");

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

}  // namespace

#ifdef STORE_USE_ETCD

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

    // Check the master view is correctly set
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

// Regression guard for the steady-state watch-churn fix (#3059): WaitForViewChange
// must arm at most one prefix watch per call (not one per loop iteration) and
// tear it down cleanly on return, so repeated calls on a stable leader do not
// accumulate watch state or violate the single-watcher-per-prefix limitation.
// After a sequence of stable-leader timeouts, a real view change must still be
// detected by the long-lived watch. The final leg releases leadership from a
// concurrent thread while WaitForViewChange is blocked on the watch (not
// before it, which the synchronous loop-top read would catch regardless of
// whether the watch is armed), so the change is delivered as a watch DELETE
// event. The detection latency is then asserted strictly below
// kViewChangeFallbackPollInterval (200ms): a poll-only regression (watch never
// armed) would be stranded in a 200ms poll sleep and could not reliably meet
// the bound, while the event-driven watch delivers in tens of ms. See
// WaitForViewChangeRearmsAndStillDetectsAfterWatchBroken for the WATCH_BROKEN
// -> re-arm path.
TEST_F(HighAvailabilityTest,
       WaitForViewChangeRepeatedStableCallsDoNotBreakChangeDetection) {
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

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    // Repeated WaitForViewChange calls on a stable leader must each time out
    // cleanly. Each call arms a single prefix watch and tears it down on
    // return; if the hoisted-state refactor regressed (watch not armed, not
    // torn down, or registration collided), these would error, hang, or miss.
    for (int i = 0; i < 6; ++i) {
        auto result = coordinator->WaitForViewChange(
            session.view.view_version, std::chrono::milliseconds(400));
        ASSERT_TRUE(result.has_value());
        EXPECT_FALSE(result->changed);
        EXPECT_TRUE(result->timed_out);
    }

    // Release leadership from a concurrent thread while the final
    // WaitForViewChange is blocked on the long-lived prefix watch, so the view
    // change is delivered as a watch DELETE event -- not caught by the
    // synchronous loop-top read that a pre-release would make trivial for any
    // implementation (watch or poll).
    std::chrono::steady_clock::time_point key_deleted;
    std::thread releaser([&]() {
        // Let WaitForViewChange arm the watch and block on the condition var.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        coordinator->ReleaseLeadership(session);
        // ReleaseLeadership returns after RevokeLease, which deletes the
        // master_view key synchronously on the etcd server.
        key_deleted = std::chrono::steady_clock::now();
    });
    auto changed = coordinator->WaitForViewChange(
        session.view.view_version, std::chrono::seconds(3));
    const auto detected = std::chrono::steady_clock::now();
    releaser.join();

    ASSERT_TRUE(changed.has_value());
    EXPECT_TRUE(changed->changed);
    EXPECT_FALSE(changed->current_view.has_value());
    // Detection latency from key deletion (RevokeLease return) to
    // WaitForViewChange return. The long-lived watch delivers the DELETE event
    // in tens of ms; a poll-only regression (watch never armed) would sleep
    // kViewChangeFallbackPollInterval (200ms) between reads and could not
    // reliably meet < 150ms. The 150ms bound sits below the 200ms poll floor
    // with slack for CI jitter on the watch-event delivery path.
    EXPECT_LT(detected - key_deleted, std::chrono::milliseconds(150));
}

// Coordinator-level guard for the WATCH_BROKEN -> re-arm + re-read path in
// WaitForViewChange (the `if (watch_broken)` block in the wait loop): while
// WaitForViewChange is blocked on the long-lived prefix watch, reset the etcd
// store client to force a WATCH_BROKEN (event_type == 2). The Go wrapper's
// cancelAllStorePrefixWatches marks the watch broken before cancelling its
// context, so the watch goroutine delivers exactly one WATCH_BROKEN callback
// (the same mechanism as EtcdStoreClientResetTriggersPrefixWatchBrokenAnd
// Reconnect, but driven through WaitForViewChange's own watch). The
// coordinator must re-arm the watch and re-read so a subsequent view change
// is still detected before the deadline -- proving the re-arm path does not
// drop the watch, deadlock, or crash. (The reset also errors out the
// keepalive goroutine; the lease remains valid for
// DEFAULT_MASTER_VIEW_LEASE_TTL_SEC=5s, enough headroom for the explicit
// ReleaseLeadership below.)
TEST_F(HighAvailabilityTest, WaitForViewChangeRearmsAndStillDetectsAfterWatchBroken) {
    if (auto skip_reason = GetEtcdSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    auto coordinator = CreateEtcdCoordinatorOrNull(FLAGS_etcd_endpoints);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("0.0.0.0:1111");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    // Run WaitForViewChange on a thread with a long timeout so it arms the
    // long-lived prefix watch and blocks on it. A flag carries the outcome
    // back across the thread boundary (read after join, which synchronizes).
    bool view_changed = false;
    std::thread waiter([&]() {
        auto result = coordinator->WaitForViewChange(
            session.view.view_version, std::chrono::seconds(15));
        view_changed = result.has_value() && result->changed;
    });

    // Let WaitForViewChange arm the watch and block. The watch is armed
    // before the loop-top read; give it time to establish server-side (matches
    // the 500ms settle used by the wrapper-level prefix-watch tests).
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Break the watch by resetting the etcd store client. The Go wrapper
    // delivers WATCH_BROKEN (event_type == 2) to the registered callback,
    // which flips watch_healthy=false and wakes WaitForViewChange to re-arm.
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::ResetEtcdStoreClient(FLAGS_etcd_endpoints));

    // Let the coordinator process WATCH_BROKEN, re-arm the watch, re-read
    // (view unchanged), reset the flags, and block again on the freshly
    // re-armed watch. The settle window covers WATCH_BROKEN delivery +
    // WaitWatchWithPrefixStopped + the new watch's server confirmation.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Now make a real view change: revoke the lease, deleting the master_view
    // key. The re-armed watch must deliver the DELETE event so WaitForViewChange
    // returns promptly with changed=true (not via the 15s deadline).
    const auto change_start = std::chrono::steady_clock::now();
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
    waiter.join();
    const auto elapsed = std::chrono::steady_clock::now() - change_start;

    EXPECT_TRUE(view_changed)
        << "WaitForViewChange did not detect the view change after re-arm; "
           "the WATCH_BROKEN -> re-arm path may have dropped the watch";
    // Detected well before the 15s deadline. A broken re-arm that left the
    // watch disarmed would still detect via the 200ms poll fallback within
    // ~200ms; a path that dropped the watch AND skipped the poll would hit the
    // 15s deadline. The bound proves neither hang nor crash occurred.
    EXPECT_LT(elapsed, std::chrono::seconds(3));
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

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}
