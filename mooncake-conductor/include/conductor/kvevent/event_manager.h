#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <compare>
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

    std::string HandleBatch(const zmq::DecodedBatch& batch,
                            const zmq::MessageMetadata& metadata) override;

    void MarkUnavailable();

    // Waits for callbacks admitted before MarkUnavailable() to finish. Must be
    // called without EventManager::mu_ held.
    void WaitForIdle();

    // Called only after the subscriber has stopped and joined.
    std::string InvalidateEndpoint();

   private:
    friend class KVEventHandlerTestPeer;

    struct PoolObjectKey {
        std::string source_endpoint;
        std::string backend_id;
        std::string tenant_id;
        std::string object_key;
        prefixindex::StorageTier tier = prefixindex::StorageTier::kCpu;

        auto operator<=>(const PoolObjectKey&) const = default;
    };

    struct PoolObjectBinding {
        prefixindex::ContextKey context;
        prefixindex::ProjectedPrefix prefix;
        std::string connector_block_hash;
        prefixindex::SharedObjectOwner owner;
        std::string tenant_id;
    };

    std::string HandleVllmBatch(const zmq::VllmEventBatch& batch,
                                const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeBatch(const zmq::MooncakeEventBatch& batch,
                                    const zmq::MessageMetadata& metadata);
    std::string HandleVllmStored(const zmq::VllmStoredEvent& event,
                                 const zmq::MessageMetadata& metadata);
    std::string HandleVllmRemoved(const zmq::VllmRemovedEvent& event,
                                  const zmq::MessageMetadata& metadata);
    std::string HandleVllmCleared(const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeStored(const zmq::MooncakeStoredEvent& event,
                                     const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeRemoved(const zmq::MooncakeRemovedEvent& event,
                                      const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeCleared(const zmq::MooncakeClearedEvent& event,
                                      const zmq::MessageMetadata& metadata);
    std::string ClearMooncakeBindings(
        const std::optional<std::string>& backend_id,
        const std::optional<std::string>& tenant_id);

    bool BeginDispatch();
    void EndDispatch();

    class DispatchLease {
       public:
        explicit DispatchLease(KVEventHandler* handler) : handler_(handler) {}
        ~DispatchLease() { handler_->EndDispatch(); }
        DispatchLease(const DispatchLease&) = delete;
        DispatchLease& operator=(const DispatchLease&) = delete;

       private:
        KVEventHandler* handler_;
    };

    EventManager* manager_;
    common::ServiceConfig service_;
    std::mutex lifecycle_mu_;
    std::condition_variable lifecycle_cv_;
    bool available_ = true;
    size_t in_flight_ = 0;
    std::mutex bindings_mu_;
    std::map<PoolObjectKey, PoolObjectBinding> pool_bindings_;
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

    // True once Stop() has begun; checked by KVEventHandler::HandleBatch
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
    std::unordered_map<std::string, std::shared_ptr<KVEventHandler>> handlers_;
    std::unordered_map<std::string, std::string> active_endpoints_;
    std::map<std::string, common::ServiceConfig> active_configs_;
    std::set<std::string> unregistering_;
    std::set<std::string> cleanup_quarantined_;

    std::shared_mutex mu_;
    bool stopped_ = false;
    bool stop_complete_ = false;
    std::condition_variable_any stop_cv_;

    std::unique_ptr<cinatra::coro_http_server> http_server_;
};

}  // namespace kvevent
}  // namespace conductor
