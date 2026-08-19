#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <hiredis/hiredis.h>

#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/common/redis/redis_test_utils.h"
#include "ha/leadership/high_availability_test_fixture.h"
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for HA integration tests, e.g. 127.0.0.1:6379");

namespace {

ha::HABackendSpec MakeRedisBackendSpec(const std::string& endpoint,
                                       const std::string& cluster_namespace) {
    return ha::HABackendSpec{
        .type = ha::HABackendType::REDIS,
        .connstring = endpoint,
        .cluster_namespace = cluster_namespace,
    };
}

std::unique_ptr<ha::LeaderCoordinator> CreateRedisCoordinatorOrNull(
    const std::string& endpoint, const std::string& cluster_namespace) {
    auto coordinator = ha::CreateLeaderCoordinator(
        MakeRedisBackendSpec(endpoint, cluster_namespace));
    if (!coordinator) {
        return nullptr;
    }
    return std::move(coordinator.value());
}

std::string MakeRedisTestClusterNamespace(const std::string& suffix) {
    return "ha-redis-test-" + suffix + "-" + UuidToString(generate_uuid());
}

}  // namespace

TEST_F(HighAvailabilityTest, RedisBasicMasterViewOperations) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("basic");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto initial_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(initial_view.has_value());
    EXPECT_FALSE(initial_view->has_value());

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:8899");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());

    auto current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_TRUE(current_view->has_value());
    EXPECT_EQ("127.0.0.1:8899", current_view->value().leader_address);
    EXPECT_EQ(acquire->session->view.view_version,
              current_view->value().view_version);

    auto renewed = coordinator->RenewLeadership(*acquire->session);
    ASSERT_TRUE(renewed.has_value());
    EXPECT_TRUE(renewed.value());

    auto contender =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(contender, nullptr);
    auto contended = contender->TryAcquireLeadership("127.0.0.1:9900");
    ASSERT_TRUE(contended.has_value());
    EXPECT_EQ(ha::AcquireLeadershipStatus::CONTENDED, contended->status);
    ASSERT_TRUE(contended->observed_view.has_value());
    EXPECT_EQ(acquire->session->view.view_version,
              contended->observed_view->view_version);

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(*acquire->session));

    auto released_view = contender->ReadCurrentView();
    ASSERT_TRUE(released_view.has_value());
    EXPECT_FALSE(released_view->has_value());
}

TEST_F(HighAvailabilityTest, RedisLeadershipMonitorReportsDeletedView) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("monitor");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:9911");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    std::promise<ha::LeadershipLossReason> loss_promise;
    auto loss_reported = std::make_shared<std::atomic<bool>>(false);
    auto loss_future = loss_promise.get_future();
    auto monitor = coordinator->StartLeadershipMonitor(
        session,
        [&loss_promise, loss_reported](ha::LeadershipLossReason reason) {
            bool expected = false;
            if (loss_reported->compare_exchange_strong(expected, true)) {
                loss_promise.set_value(reason);
            }
        });
    ASSERT_TRUE(monitor.has_value());

    auto redis = ConnectRedisForTest(FLAGS_redis_endpoint);
    ASSERT_TRUE(redis.has_value());
    const auto master_view_key = BuildRedisMasterViewKey(cluster_namespace);
    RedisReplyPtr delete_reply(static_cast<redisReply*>(
        redisCommand(redis->get(), "DEL %b", master_view_key.data(),
                     master_view_key.size())));
    ASSERT_NE(delete_reply, nullptr);
    ASSERT_NE(delete_reply->type, REDIS_REPLY_ERROR);

    ASSERT_EQ(loss_future.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    EXPECT_EQ(ha::LeadershipLossReason::kLostLeadership, loss_future.get());

    monitor.value()->Stop();
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
}

TEST_F(HighAvailabilityTest, RedisLeadershipMonitorIgnoresExplicitRelease) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("release");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:9922");
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
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_FALSE(callback_fired->load());
}

TEST_F(HighAvailabilityTest, RedisCanRestartRenewAfterExplicitRelease) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace =
        MakeRedisTestClusterNamespace("restart-renew");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto first_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9933");
    ASSERT_TRUE(first_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, first_acquire->status);
    ASSERT_TRUE(first_acquire->session.has_value());

    auto first_renew = coordinator->RenewLeadership(*first_acquire->session);
    ASSERT_TRUE(first_renew.has_value());
    ASSERT_TRUE(first_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*first_acquire->session));

    auto second_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9944");
    ASSERT_TRUE(second_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, second_acquire->status);
    ASSERT_TRUE(second_acquire->session.has_value());

    auto second_renew = coordinator->RenewLeadership(*second_acquire->session);
    ASSERT_TRUE(second_renew.has_value());
    ASSERT_TRUE(second_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*second_acquire->session));
}

}  // namespace testing
}  // namespace mooncake
