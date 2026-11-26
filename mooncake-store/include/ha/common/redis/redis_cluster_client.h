#pragma once

#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "ha/common/redis/redis_connection.h"

namespace mooncake::ha::common::redis {

// Owns connections and serializes the entire command/confirmation exchange.
// A lease must outlive interpretation of the reply and WAIT/WAITAOF.
class RedisClusterClient {
   public:
    enum class RoutingError {
        None,
        InvalidConfiguration,
        Unsupported,
        Transport,
        ServerError,
        InvalidTopology,
        DurabilityUnsupported,
        InvalidDurabilityResponse,
        AofDisabled,
        PermissionDenied,
        ReplicaCountMismatch,
        RedirectLoop,
        RedirectLimit,
        MalformedRedirect,
        RedirectSlotMismatch,
        UnknownTarget,
        MembershipChanged,
    };
    struct ConnectResult {
        ErrorCode error = ErrorCode::OK;
        RoutingError routing_error = RoutingError::None;
    };
    struct Lease {
        std::unique_lock<std::mutex> lock;
        RedisReplyPtr reply;
        redisContext* context = nullptr;
        uint64_t connection_generation = 0;
        ErrorCode error = ErrorCode::OK;
        RoutingError routing_error = RoutingError::None;
        bool may_have_executed = false;
        std::string endpoint;
        std::string membership;
        uint16_t slot = 0;
        std::string slot_owner;
    };

    ErrorCode Connect(std::string connstring, size_t expected_replicas,
                      bool read_only = false, bool require_aof = false);
    ConnectResult ConnectDetailed(std::string connstring,
                                  size_t expected_replicas,
                                  bool read_only = false,
                                  bool require_aof = false);
    Lease Execute(const std::vector<std::string>& command, std::string_view key,
                  bool write,
                  std::chrono::steady_clock::time_point deadline =
                      std::chrono::steady_clock::time_point::max());
    tl::expected<size_t, ErrorCode> Wait(Lease& lease, size_t replicas,
                                         std::chrono::milliseconds timeout);
    tl::expected<std::pair<size_t, size_t>, ErrorCode> WaitAof(
        Lease& lease, size_t replicas, std::chrono::milliseconds timeout);
    static uint16_t HashSlot(std::string_view key);
    static const char* RoutingErrorName(RoutingError error);

   private:
    struct Shard {
        std::string primary;
        std::string membership;
        size_t replicas = 0;
    };
    struct ConnectionState {
        RedisContextPtr context;
        uint64_t generation = 0;
        // The complete shard membership binds capability to role, node ID
        // and every member endpoint, not merely a reusable server address.
        std::string validated_membership;
    };
    bool ValidateNode(const std::string& endpoint,
                      const std::string& membership, bool replica = false);
    bool ValidateShard(const Shard& shard);
    // Refresh only drops stale capability entries; the startup connect path
    // performs the one-time full-shard capability check.
    void InvalidateStaleCapabilities();
    bool ValidateAllShards();
    // Standalone deployments have no topology reply: discover replicas from
    // the primary's INFO replication and probe each one before serving.
    ErrorCode ValidateStandaloneReplicas();
    bool LeaseConnectionValid(const Lease& lease) const;
    bool ValidateAcknowledgement(Lease& lease);
    RedisReplyPtr Acknowledge(Lease& lease, std::vector<std::string> command,
                              std::chrono::milliseconds timeout);
    redisContext* Connection(const std::string& endpoint);
    RedisReplyPtr Run(redisContext* context,
                      const std::vector<std::string>& command);
    ErrorCode Refresh();
    ErrorCode ConnectLocked(std::string connstring, size_t expected_replicas,
                            bool read_only, bool require_aof);
    bool ParseShards(const redisReply* reply,
                     std::string_view discovery_endpoint);
    // A MOVED target remembered per slot so the next request starts there
    // instead of taking the same redirect again. Hints never replace slots_ or
    // shards_ as the authority: they only choose the first endpoint, are
    // validated against shards_ when used, expire with the topology refresh
    // interval, and are capped.
    struct RedirectHint {
        std::string endpoint;
        std::chrono::steady_clock::time_point expires_at;
    };
    std::string HintForSlot(uint16_t slot) const;
    void RememberRedirect(uint16_t slot, const std::string& endpoint);
    // CLUSTER SLOTS omits primaries without slots. Verify ASK targets through
    // CLUSTER NODES before permitting a write on such a primary.
    std::optional<Shard> SlotlessShard(const std::string& endpoint);
    std::mutex mutex_;
    std::chrono::steady_clock::time_point deadline_ =
        std::chrono::steady_clock::time_point::max();
    bool cluster_ = false;
    bool read_only_ = false;
    bool require_aof_ = false;
    size_t expected_replicas_ = 0;
    int command_timeout_ms_ = 3000;
    int topology_refresh_ms_ = 1000;
    int max_redirects_ = 5;
    bool topology_valid_ = false;
    RoutingError refresh_error_ = RoutingError::InvalidTopology;
    std::chrono::steady_clock::time_point refreshed_at_{};
    std::vector<std::string> seeds_;
    std::map<std::string, ConnectionState> connections_;
    std::array<Shard, 16384> slots_;
    std::map<std::string, Shard> shards_;
    std::map<uint16_t, RedirectHint> redirect_hints_;
    std::set<std::string> slots_only_endpoints_;
    bool shards_snapshot_ = false;
};

}  // namespace mooncake::ha::common::redis
