#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "conductor/zmq/event_type.h"
#include "conductor/zmq/zmq_client.h"

// forward declaration to keep coro_http out of this header
// (ylt's `coro_http` is a namespace alias for cinatra)
namespace cinatra {
class coro_http_server;
}

namespace conductor {
namespace kvevent {

class EventManager;

// KVEventHandler adapts the generic EventHandler interface for EventManager.
class KVEventHandler : public zmq::EventHandler {
   public:
    KVEventHandler(EventManager* manager, common::ServiceConfig svc);

    std::string HandleEvent(const zmq::KVEvent& event,
                            int64_t dp_rank) override;

   private:
    friend class KVEventHandlerTestPeer;

    std::string HandleBlockStored(const zmq::BlockStoredEvent& event,
                                  int64_t dp_rank);
    std::string HandleBlockRemoved(const zmq::BlockRemovedEvent& event,
                                   int64_t dp_rank);

    EventManager* manager_;
    common::ServiceConfig service_;
};

std::string MakeServiceKey(const std::string& instance_id,
                           const std::string& tenant_id, int dp_rank);

class EventManager {
   public:
    EventManager(std::vector<common::ServiceConfig> services,
                 int http_server_port);
    ~EventManager();
    EventManager(const EventManager&) = delete;
    EventManager& operator=(const EventManager&) = delete;

    // Subscribes to all statically configured services concurrently.
    // Individual failures are logged, not returned; Start
    // never fails as a whole.
    void Start();

    // Stops all ZMQ clients and shuts down the HTTP server. Idempotent.
    void Stop();

    // Starts the HTTP server on the configured port. Returns false when
    // the port cannot be bound.
    bool StartHTTPServer();

    prefixindex::PrefixCacheTable* GetIndexer() { return &indexer_; }

    // True once Stop() has begun; checked by KVEventHandler::HandleEvent
    // under mu_ (read).
    bool IsStopped();

   private:
    friend class KVEventHandler;
    friend class EventManagerTestPeer;

    // Requires mu_ held (exclusive). Returns {is_new, error}: is_new is
    // false for duplicates (idempotent registration).
    std::pair<bool, std::string> SubscribeToService(
        const common::ServiceConfig& svc);

    // Must be called WITHOUT mu_ held; stops the ZMQ client outside the
    // manager lock (see deadlock note in the implementation).
    std::pair<bool, std::string> UnsubscribeFromService(
        const std::string& instance_id, const std::string& tenant_id,
        int dp_rank);

    void RegisterHttpHandlers();

    prefixindex::PrefixCacheTable indexer_;
    std::vector<common::ServiceConfig> services_;
    int http_server_port_;

    // Concurrent-map fields, guarded by mu_ alongside services_.
    std::unordered_map<std::string, std::shared_ptr<zmq::ZMQClient>>
        subscribers_;
    std::map<std::string, common::ServiceConfig> active_configs_;
    std::set<std::string> unregistering_;

    std::shared_mutex mu_;
    bool stopped_ = false;

    std::unique_ptr<cinatra::coro_http_server> http_server_;
};

}  // namespace kvevent
}  // namespace conductor
