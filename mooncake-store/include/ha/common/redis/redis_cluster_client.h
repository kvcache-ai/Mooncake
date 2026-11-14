#pragma once

#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

#include "ha/common/redis/redis_connection.h"

namespace mooncake::ha::common::redis {

// Owns connections and serializes the entire command/confirmation exchange.
// A lease must outlive interpretation of the reply and any subsequent WAIT.
class RedisClusterClient {
   public:
    enum class RoutingError {
        None,
        InvalidConfiguration,
        Unsupported,
        Transport,
        ServerError,
        InvalidTopology,
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
        ErrorCode error = ErrorCode::OK;
        RoutingError routing_error = RoutingError::None;
        bool may_have_executed = false;
        std::string endpoint;
        std::string membership;
        uint16_t slot = 0;
        std::string slot_owner;
    };

    ErrorCode Connect(std::string connstring, size_t expected_replicas,
                      bool read_only = false);
    ConnectResult ConnectDetailed(std::string connstring,
                                  size_t expected_replicas,
                                  bool read_only = false);
    Lease Execute(const std::vector<std::string>& command, std::string_view key,
                  bool write,
                  std::chrono::steady_clock::time_point deadline =
                      std::chrono::steady_clock::time_point::max());
    tl::expected<size_t, ErrorCode> Wait(Lease& lease, size_t replicas,
                                         std::chrono::milliseconds timeout);
    static uint16_t HashSlot(std::string_view key);
    static const char* RoutingErrorName(RoutingError error);

   private:
    struct Shard {
        std::string primary;
        std::string membership;
        size_t replicas = 0;
    };
    redisContext* Connection(const std::string& endpoint);
    RedisReplyPtr Run(redisContext* context,
                      const std::vector<std::string>& command);
    ErrorCode Refresh();
    ErrorCode ConnectLocked(std::string connstring, size_t expected_replicas,
                            bool read_only);
    bool ParseShards(const redisReply* reply);
    // CLUSTER SLOTS omits primaries without slots. Verify ASK targets through
    // CLUSTER NODES before permitting a write on such a primary.
    std::optional<Shard> SlotlessShard(const std::string& endpoint);
    std::mutex mutex_;
    std::chrono::steady_clock::time_point deadline_ =
        std::chrono::steady_clock::time_point::max();
    bool cluster_ = false;
    bool read_only_ = false;
    size_t expected_replicas_ = 0;
    int command_timeout_ms_ = 3000;
    int topology_refresh_ms_ = 1000;
    int max_redirects_ = 5;
    bool topology_valid_ = false;
    RoutingError refresh_error_ = RoutingError::InvalidTopology;
    std::chrono::steady_clock::time_point refreshed_at_{};
    std::vector<std::string> seeds_;
    std::map<std::string, RedisContextPtr> connections_;
    std::array<Shard, 16384> slots_;
    std::map<std::string, Shard> shards_;
    std::set<std::string> slots_only_endpoints_;
    bool shards_snapshot_ = false;
};

}  // namespace mooncake::ha::common::redis
