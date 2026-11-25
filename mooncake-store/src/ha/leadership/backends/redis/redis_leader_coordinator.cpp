#include "ha/leadership/backends/redis/redis_leader_coordinator.h"

#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include <glog/logging.h>

#include "types.h"
#include "config/ha_cluster_namespace_config.h"
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif
#include <ylt/util/tl/expected.hpp>

#include "ha/common/redis/redis_connection.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

namespace {

using common::redis::IsStringReply;
using common::redis::RedisReplyPtr;
using common::redis::SanitizeHashTagComponent;

#ifdef STORE_USE_REDIS

constexpr auto kViewChangePollInterval = std::chrono::milliseconds(200);
constexpr auto kMinimumRenewInterval = std::chrono::milliseconds(200);
constexpr auto kRedisLeaseTtl =
    std::chrono::seconds(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC);

constexpr char kLeaderAddressField[] = "leader_address";
constexpr char kViewVersionField[] = "view_version";
constexpr char kOwnerTokenField[] = "owner_token";

constexpr char kAcquireLeadershipScript[] = R"LUA(
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local view_version = redis.call('INCR', KEYS[2])
redis.call(
  'HSET',
  KEYS[1],
  'leader_address', ARGV[1],
  'view_version', tostring(view_version),
  'owner_token', ARGV[3]
)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, view_version}
)LUA";

constexpr char kRenewLeadershipScript[] = R"LUA(
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local owner_token = redis.call('HGET', KEYS[1], 'owner_token')
if not owner_token or owner_token ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
)LUA";

constexpr char kReleaseLeadershipScript[] = R"LUA(
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local owner_token = redis.call('HGET', KEYS[1], 'owner_token')
if not owner_token or owner_token ~= ARGV[1] then
  return -1
end
redis.call('DEL', KEYS[1])
return 1
)LUA";

class RedisLeadershipMonitorHandle final : public LeadershipMonitorHandle {
   public:
    explicit RedisLeadershipMonitorHandle(
        std::shared_ptr<std::atomic<bool>> armed)
        : armed_(std::move(armed)) {}

    void Stop() override {
        if (armed_ != nullptr) {
            armed_->store(false);
        }
    }

   private:
    std::shared_ptr<std::atomic<bool>> armed_;
};
tl::expected<std::string, ErrorCode> GetReplyString(const redisReply* reply) {
    if (!IsStringReply(reply) || reply->str == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return std::string(reply->str, reply->len);
}

tl::expected<long long, ErrorCode> ParseReplyInteger(const redisReply* reply) {
    if (reply == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    }
    if (!IsStringReply(reply) || reply->str == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    try {
        return std::stoll(std::string(reply->str, reply->len));
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

#endif

}  // namespace

#ifndef STORE_USE_REDIS

RedisLeaderCoordinator::RedisLeaderCoordinator(const HABackendSpec& spec)
    : spec_(spec) {}

RedisLeaderCoordinator::~RedisLeaderCoordinator() = default;

ErrorCode RedisLeaderCoordinator::Connect() {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

tl::expected<std::optional<MasterView>, ErrorCode>
RedisLeaderCoordinator::ReadCurrentView() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<AcquireLeadershipResult, ErrorCode>
RedisLeaderCoordinator::TryAcquireLeadership(
    const std::string& leader_address) {
    (void)leader_address;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<bool, ErrorCode> RedisLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    (void)session;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<ViewChangeResult, ErrorCode>
RedisLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    (void)known_version;
    (void)timeout;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
RedisLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    (void)session;
    (void)on_leadership_lost;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode RedisLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    (void)session;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ClusterNamespace RedisLeaderCoordinator::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisLeaderCoordinator::BuildMasterViewKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisLeaderCoordinator::BuildViewVersionKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

OwnerToken RedisLeaderCoordinator::MakeOwnerToken() { return {}; }

LeadershipLossReason RedisLeaderCoordinator::ClassifyLeadershipLossReason(
    ErrorCode err) {
    (void)err;
    return LeadershipLossReason::kRenewError;
}

ErrorCode RedisLeaderCoordinator::ConnectLocked() {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

void RedisLeaderCoordinator::DisconnectLocked() {}

ErrorCode RedisLeaderCoordinator::RenewLeadershipOnceLocked(
    const LeadershipSession& session, bool& renewed) {
    (void)session;
    renewed = false;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ErrorCode RedisLeaderCoordinator::ReleaseLeadershipOnceLocked(
    const OwnerToken& owner_token, bool& leadership_missing,
    bool& owner_mismatch) {
    (void)owner_token;
    leadership_missing = false;
    owner_mismatch = false;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ErrorCode RedisLeaderCoordinator::ShutdownRenewThread() {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

void RedisLeaderCoordinator::ClearLeadershipMonitorStateLocked() {}

std::chrono::milliseconds RedisLeaderCoordinator::ResolveRenewInterval(
    std::chrono::milliseconds lease_ttl) const {
    return lease_ttl;
}

bool RedisLeaderCoordinator::IsSameViewVersion(
    const std::optional<MasterView>& current_view,
    std::optional<ViewVersionId> known_version) const {
    return current_view.has_value() == known_version.has_value();
}

#else

RedisLeaderCoordinator::RedisLeaderCoordinator(const HABackendSpec& spec)
    : spec_(spec) {
    const auto resolved_namespace =
        ResolveClusterNamespace(spec.cluster_namespace);
    master_view_key_ = BuildMasterViewKey(resolved_namespace);
    view_version_key_ = BuildViewVersionKey(resolved_namespace);
}

RedisLeaderCoordinator::~RedisLeaderCoordinator() {
    auto err = ShutdownRenewThread();
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to shutdown Redis renew thread: "
                     << toString(err);
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    DisconnectLocked();
}

ErrorCode RedisLeaderCoordinator::Connect() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return ConnectLocked();
}

tl::expected<std::optional<MasterView>, ErrorCode>
RedisLeaderCoordinator::ReadCurrentView() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto err = ConnectLocked();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto reply = ExecuteLocked({"HMGET", master_view_key_, kLeaderAddressField,
                                kViewVersionField, kOwnerTokenField},
                               false);
    if (reply == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis HMGET returned error for key="
                     << master_view_key_ << ": "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 3) {
        LOG(WARNING) << "Redis HMGET returned unexpected reply for key="
                     << master_view_key_;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const auto* leader_reply = reply->element[0];
    const auto* version_reply = reply->element[1];
    const auto* owner_reply = reply->element[2];
    if (leader_reply->type == REDIS_REPLY_NIL &&
        version_reply->type == REDIS_REPLY_NIL &&
        owner_reply->type == REDIS_REPLY_NIL) {
        return std::optional<MasterView>{std::nullopt};
    }
    if (leader_reply->type == REDIS_REPLY_NIL ||
        version_reply->type == REDIS_REPLY_NIL ||
        owner_reply->type == REDIS_REPLY_NIL) {
        LOG(WARNING) << "Redis master view is partially populated for key="
                     << master_view_key_;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto leader_address = GetReplyString(leader_reply);
    if (!leader_address) {
        return tl::make_unexpected(leader_address.error());
    }
    auto view_version = ParseReplyInteger(version_reply);
    if (!view_version) {
        return tl::make_unexpected(view_version.error());
    }
    auto owner_token = GetReplyString(owner_reply);
    if (!owner_token || owner_token->empty()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return std::optional<MasterView>{MasterView{
        .leader_address = std::move(leader_address.value()),
        .view_version = static_cast<ViewVersionId>(view_version.value()),
    }};
}

tl::expected<AcquireLeadershipResult, ErrorCode>
RedisLeaderCoordinator::TryAcquireLeadership(
    const std::string& leader_address) {
    OwnerToken owner_token = MakeOwnerToken();
    ViewVersionId view_version = 0;
    bool acquired = false;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto err = ConnectLocked();
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }

        const auto ttl_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                kRedisLeaseTtl)
                .count();
        auto reply =
            ExecuteLocked({"EVAL", kAcquireLeadershipScript, "2",
                           master_view_key_, view_version_key_, leader_address,
                           std::to_string(ttl_ms), owner_token},
                          true);
        if (reply == nullptr) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            LOG(WARNING) << "Redis acquire leadership returned error: "
                         << (reply->str != nullptr ? reply->str : "unknown");
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        auto acquire_result = ParseReplyInteger(reply->element[0]);
        if (!acquire_result) {
            return tl::make_unexpected(acquire_result.error());
        }
        if (acquire_result.value() == 1) {
            if (reply->elements < 2) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            auto parsed_view_version = ParseReplyInteger(reply->element[1]);
            if (!parsed_view_version) {
                return tl::make_unexpected(parsed_view_version.error());
            }
            view_version =
                static_cast<ViewVersionId>(parsed_view_version.value());
            acquired = true;
        } else if (acquire_result.value() != 0) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    if (!acquired) {
        auto observed_view = ReadCurrentView();
        if (!observed_view) {
            return tl::make_unexpected(observed_view.error());
        }
        return AcquireLeadershipResult{
            .status = AcquireLeadershipStatus::CONTENDED,
            .session = std::nullopt,
            .observed_view = observed_view.value(),
        };
    }

    LeadershipSession session{
        .view = MasterView{.leader_address = leader_address,
                           .view_version = view_version},
        .owner_token = std::move(owner_token),
        .lease_ttl = kRedisLeaseTtl,
    };

    return AcquireLeadershipResult{
        .status = AcquireLeadershipStatus::ACQUIRED,
        .session = std::move(session),
        .observed_view = std::nullopt,
    };
}

tl::expected<bool, ErrorCode> RedisLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    if (session.owner_token.empty() ||
        session.lease_ttl <= std::chrono::milliseconds::zero()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::thread thread_to_join;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (shutdown_requested_) {
            return false;
        }

        if (renew_thread_.joinable()) {
            if (renew_owner_token_ != session.owner_token) {
                return false;
            }
            if (renew_stopped_) {
                thread_to_join = std::move(renew_thread_);
                renew_owner_token_.clear();
                renew_session_ = LeadershipSession{};
                ClearLeadershipMonitorStateLocked();
            } else {
                return true;
            }
        } else {
            auto err = ConnectLocked();
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            bool renewed = false;
            err = RenewLeadershipOnceLocked(session, renewed);
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            if (!renewed) {
                return false;
            }

            renew_session_ = session;
            renew_owner_token_ = session.owner_token;
            renew_stopped_ = false;
            renew_stop_requested_ = false;
            renew_result_ = ErrorCode::OK;
            ClearLeadershipMonitorStateLocked();
            renew_thread_ = std::thread([this, session]() {
                const auto renew_interval =
                    ResolveRenewInterval(session.lease_ttl);
                ErrorCode renew_result = ErrorCode::OK;
                LeadershipLossReason loss_reason =
                    LeadershipLossReason::kLostLeadership;

                while (true) {
                    std::unique_lock<std::mutex> lock(state_mutex_);
                    if (renew_cv_.wait_for(lock, renew_interval,
                                           [this, &session] {
                                               return shutdown_requested_ ||
                                                      renew_stop_requested_ ||
                                                      renew_owner_token_ !=
                                                          session.owner_token;
                                           })) {
                        break;
                    }

                    auto err = ConnectLocked();
                    if (err != ErrorCode::OK) {
                        renew_result = err;
                        loss_reason = ClassifyLeadershipLossReason(err);
                        break;
                    }

                    bool renewed = false;
                    err = RenewLeadershipOnceLocked(session, renewed);
                    if (err != ErrorCode::OK) {
                        renew_result = err;
                        loss_reason = ClassifyLeadershipLossReason(err);
                        break;
                    }
                    if (!renewed) {
                        renew_result = ErrorCode::OK;
                        loss_reason = LeadershipLossReason::kLostLeadership;
                        break;
                    }
                }

                std::shared_ptr<std::atomic<bool>> monitor_armed;
                LeadershipLostCallback on_leadership_lost;
                {
                    std::lock_guard<std::mutex> lock(state_mutex_);
                    renew_result_ = renew_result;
                    renew_stopped_ = true;
                    if (!shutdown_requested_ && !renew_stop_requested_ &&
                        renew_owner_token_ == leadership_monitor_owner_token_) {
                        monitor_armed = leadership_monitor_armed_;
                        on_leadership_lost =
                            std::move(leadership_monitor_callback_);
                        leadership_monitor_armed_.reset();
                        leadership_monitor_owner_token_.clear();
                    }
                }

                if (monitor_armed != nullptr &&
                    monitor_armed->exchange(false) &&
                    on_leadership_lost != nullptr) {
                    on_leadership_lost(loss_reason);
                }
            });
            return true;
        }
    }

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }
    return false;
}

tl::expected<ViewChangeResult, ErrorCode>
RedisLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        auto current_view = ReadCurrentView();
        if (!current_view) {
            return tl::make_unexpected(current_view.error());
        }

        if (!IsSameViewVersion(current_view.value(), known_version)) {
            return ViewChangeResult{
                .changed = true,
                .timed_out = false,
                .current_view = current_view.value(),
            };
        }

        if (timeout <= std::chrono::milliseconds::zero() ||
            std::chrono::steady_clock::now() >= deadline) {
            return ViewChangeResult{
                .changed = false,
                .timed_out = true,
                .current_view = std::nullopt,
            };
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
        std::this_thread::sleep_for(
            std::min(kViewChangePollInterval, remaining));
    }
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
RedisLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    if (!on_leadership_lost) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    auto err = ConnectLocked();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (shutdown_requested_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (renew_owner_token_ != session.owner_token ||
        !renew_thread_.joinable() || renew_stopped_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (leadership_monitor_armed_ != nullptr &&
        leadership_monitor_armed_->load()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    leadership_monitor_owner_token_ = session.owner_token;
    leadership_monitor_callback_ = std::move(on_leadership_lost);
    leadership_monitor_armed_ = std::make_shared<std::atomic<bool>>(true);
    return std::unique_ptr<LeadershipMonitorHandle>(
        std::make_unique<RedisLeadershipMonitorHandle>(
            leadership_monitor_armed_));
}

ErrorCode RedisLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    if (session.owner_token.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    std::thread thread_to_join;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (!renew_owner_token_.empty() &&
            renew_owner_token_ != session.owner_token) {
            return ErrorCode::INVALID_PARAMS;
        }

        renew_stop_requested_ = true;
        renew_cv_.notify_all();
        if (renew_thread_.joinable()) {
            thread_to_join = std::move(renew_thread_);
        }
        renew_owner_token_.clear();
        renew_session_ = LeadershipSession{};
        renew_stopped_ = true;
        ClearLeadershipMonitorStateLocked();
    }

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    bool leadership_missing = false;
    bool owner_mismatch = false;
    auto err = ReleaseLeadershipOnceLocked(session.owner_token,
                                           leadership_missing, owner_mismatch);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (owner_mismatch) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ClusterNamespace RedisLeaderCoordinator::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (!cluster_namespace.empty()) {
        return cluster_namespace;
    }

    return HaClusterNamespaceConfig::FromEnvironment().cluster_namespace;
}

std::string RedisLeaderCoordinator::BuildMasterViewKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/master_view";
}

std::string RedisLeaderCoordinator::BuildViewVersionKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/master_view_version";
}

OwnerToken RedisLeaderCoordinator::MakeOwnerToken() {
    return UuidToString(generate_uuid());
}

LeadershipLossReason RedisLeaderCoordinator::ClassifyLeadershipLossReason(
    ErrorCode err) {
    return err == ErrorCode::OK ? LeadershipLossReason::kLostLeadership
                                : LeadershipLossReason::kRenewError;
}

ErrorCode RedisLeaderCoordinator::ConnectLocked() {
    if (connected_ && client_) return ErrorCode::OK;
    auto client = std::make_unique<common::redis::RedisClusterClient>();
    const auto result = client->ConnectDetailed(
        spec_.connstring, spec_.ha_redis_expected_replicas_per_shard,
        spec_.redis_read_only);
    if (result.error != ErrorCode::OK) {
        LOG(ERROR) << "Redis leadership startup failed: error="
                   << toString(result.error) << " routing_error="
                   << common::redis::RedisClusterClient::RoutingErrorName(
                          result.routing_error);
        return result.error;
    }
    client_ = std::move(client);
    connected_ = true;
    return ErrorCode::OK;
}

void RedisLeaderCoordinator::DisconnectLocked() {
    client_.reset();
    connected_ = false;
}

RedisReplyPtr RedisLeaderCoordinator::ExecuteLocked(
    const std::vector<std::string>& command, bool write) {
    const auto started = std::chrono::steady_clock::now();
    if (write) reconciliation_ = OwnershipReconciliation::NotNeeded;
    {
        auto lease = client_->Execute(command, master_view_key_, write,
                                      started + std::chrono::seconds(2));
        if (lease.reply && lease.error == ErrorCode::OK) {
            if (!write || lease.reply->type == REDIS_REPLY_ERROR)
                return std::move(lease.reply);
            const auto ack =
                client_->Wait(lease, spec_.ha_redis_expected_replicas_per_shard,
                              std::chrono::milliseconds(1000));
            // Leave enough of the five-second lease for the next renewal.
            if (ack && *ack == spec_.ha_redis_expected_replicas_per_shard &&
                std::chrono::steady_clock::now() - started < kRedisLeaseTtl / 2)
                return std::move(lease.reply);
        }
    }
    if (write) {
        // Reconcile ownership on the current owner, but never turn a read-back
        // into replication confirmation or replay an uncertain lease mutation.
        auto observed = client_->Execute(
            {"HGET", master_view_key_, kOwnerTokenField}, master_view_key_,
            false, started + std::chrono::seconds(2));
        const auto& expected_owner = command[1] == kAcquireLeadershipScript
                                         ? command.back()
                                         : command[4];
        const auto owner = GetReplyString(observed.reply.get());
        auto state = OwnershipReconciliation::Unknown;
        if (observed.error == ErrorCode::OK && observed.reply) {
            if (owner)
                state = *owner == expected_owner
                            ? OwnershipReconciliation::MatchingUnconfirmed
                            : OwnershipReconciliation::Lost;
            else if (observed.reply->type == REDIS_REPLY_NIL)
                state = OwnershipReconciliation::Lost;
        }
        reconciliation_ = state;
        LOG(WARNING) << "Redis leadership write unconfirmed; reconciliation="
                     << (state == OwnershipReconciliation::MatchingUnconfirmed
                             ? "matching-unconfirmed"
                         : state == OwnershipReconciliation::Lost ? "lost"
                                                                  : "unknown");
    }
    return {};
}

ErrorCode RedisLeaderCoordinator::RenewLeadershipOnceLocked(
    const LeadershipSession& session, bool& renewed) {
    auto err = ConnectLocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    const auto ttl_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(session.lease_ttl)
            .count();
    auto reply =
        ExecuteLocked({"EVAL", kRenewLeadershipScript, "1", master_view_key_,
                       session.owner_token, std::to_string(ttl_ms)},
                      true);
    if (reply == nullptr) {
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis renew leadership returned error: "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return ErrorCode::INTERNAL_ERROR;
    }

    auto renew_result = ParseReplyInteger(reply.get());
    if (!renew_result) {
        return renew_result.error();
    }
    if (renew_result.value() == 0) {
        renewed = false;
        return ErrorCode::OK;
    }
    if (renew_result.value() == 1) {
        renewed = true;
        return ErrorCode::OK;
    }
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode RedisLeaderCoordinator::ReleaseLeadershipOnceLocked(
    const OwnerToken& owner_token, bool& leadership_missing,
    bool& owner_mismatch) {
    auto err = ConnectLocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    auto reply = ExecuteLocked(
        {"EVAL", kReleaseLeadershipScript, "1", master_view_key_, owner_token},
        true);
    if (reply == nullptr) {
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis release leadership returned error: "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return ErrorCode::INTERNAL_ERROR;
    }

    auto release_result = ParseReplyInteger(reply.get());
    if (!release_result) {
        return release_result.error();
    }
    leadership_missing = release_result.value() == 0;
    owner_mismatch = release_result.value() == -1;
    if (release_result.value() == -1 || release_result.value() == 0 ||
        release_result.value() == 1) {
        return ErrorCode::OK;
    }
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode RedisLeaderCoordinator::ShutdownRenewThread() {
    std::thread thread_to_join;
    OwnerToken owner_token_to_release;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        shutdown_requested_ = true;
        renew_stop_requested_ = true;
        renew_cv_.notify_all();
        if (renew_thread_.joinable()) {
            thread_to_join = std::move(renew_thread_);
        }
        owner_token_to_release = renew_owner_token_;
        renew_owner_token_.clear();
        renew_session_ = LeadershipSession{};
        renew_stopped_ = true;
        ClearLeadershipMonitorStateLocked();
    }

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }
    if (owner_token_to_release.empty()) {
        return ErrorCode::OK;
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    bool leadership_missing = false;
    bool owner_mismatch = false;
    auto err = ReleaseLeadershipOnceLocked(owner_token_to_release,
                                           leadership_missing, owner_mismatch);
    if (err != ErrorCode::OK) {
        return err;
    }
    return ErrorCode::OK;
}

void RedisLeaderCoordinator::ClearLeadershipMonitorStateLocked() {
    if (leadership_monitor_armed_ != nullptr) {
        leadership_monitor_armed_->store(false);
        leadership_monitor_armed_.reset();
    }
    leadership_monitor_callback_ = nullptr;
    leadership_monitor_owner_token_.clear();
}

std::chrono::milliseconds RedisLeaderCoordinator::ResolveRenewInterval(
    std::chrono::milliseconds lease_ttl) const {
    const auto candidate = lease_ttl / 3;
    if (candidate <= std::chrono::milliseconds::zero()) {
        return kMinimumRenewInterval;
    }
    return candidate < kMinimumRenewInterval ? kMinimumRenewInterval
                                             : candidate;
}

bool RedisLeaderCoordinator::IsSameViewVersion(
    const std::optional<MasterView>& current_view,
    std::optional<ViewVersionId> known_version) const {
    if (!current_view.has_value() && !known_version.has_value()) {
        return true;
    }
    if (!current_view.has_value() || !known_version.has_value()) {
        return false;
    }
    return current_view->view_version == known_version.value();
}

#endif

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
