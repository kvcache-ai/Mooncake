#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "types.h"

namespace mooncake {
class ClientManager {
   public:
    ClientManager(const int64_t client_live_ttl_sec);
    virtual ~ClientManager();

    void Start();

    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto ReMountSegment(const std::vector<Segment>& segments,
                        const UUID& client_id) -> tl::expected<void, ErrorCode>;
    virtual auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;
    virtual auto GetAllSegments()
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;
    virtual auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> = 0;
    virtual auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;
    virtual auto Ping(const UUID& client_id)
        -> tl::expected<ClientStatus, ErrorCode> = 0;

   protected:
    virtual auto InnerMountSegment(const Segment& segment,
                                   const UUID& client_id,
                                   std::function<ErrorCode()>& pre_func)
        -> tl::expected<void, ErrorCode> = 0;
    virtual auto InnerReMountSegment(const std::vector<Segment>& segments,
                                     const UUID& client_id,
                                     std::function<ErrorCode()>& pre_func)
        -> tl::expected<void, ErrorCode> = 0;
    // monitor each client's status
    virtual void ClientMonitorFunc() = 0;

   protected:
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    static constexpr size_t kClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first;
        uint64_t second;
    };

   protected:
    mutable SharedMutex client_mutex_;
    std::unordered_set<UUID, boost::hash<UUID>> GUARDED_BY(client_mutex_)
        ok_client_;  // client with ok status
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    boost::lockfree::queue<PodUUID> client_ping_queue_{kClientPingQueueSize};
    const int64_t client_live_ttl_sec_;
};

}  // namespace mooncake
