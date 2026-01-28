#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <shared_mutex>
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

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id);
    virtual ErrorCode UnmountSegment(const UUID& segment_id,
                                     const UUID& client_id) = 0;
    virtual ErrorCode GetAllSegments(
        std::vector<std::string>& all_segments) = 0;
    virtual ErrorCode QuerySegments(const std::string& segment, size_t& used,
                                    size_t& capacity) = 0;
    virtual ErrorCode QueryIp(const UUID& client_id,
                              std::vector<std::string>& result) = 0;
    virtual tl::expected<ClientStatus, ErrorCode> Ping(const UUID& client_id) = 0;

   protected:
    virtual ErrorCode InnerMountSegment(
        const Segment& segment, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) = 0;
    virtual ErrorCode InnerReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) = 0;
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
    std::shared_mutex client_mutex_;
    std::unordered_set<UUID, boost::hash<UUID>>
        ok_client_;  // client with ok status
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    boost::lockfree::queue<PodUUID> client_ping_queue_{kClientPingQueueSize};
    const int64_t client_live_ttl_sec_;
};

}  // namespace mooncake
