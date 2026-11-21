#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <cstdint>
#include <compare>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "conductor/prefixindex/cache_layout.h"
#include "conductor/zmq/event_type.h"
#include "conductor/zmq/zmq_client.h"

// forward declaration to keep coro_http out of this header
// (ylt's `coro_http` is a namespace alias for cinatra)
namespace cinatra {
class coro_http_server;
}

// Forward declarations keeping coro_rpc out of this header. `coro_rpc_server`
// is an alias (`coro_rpc_server_base<config_t>`), not a class, so the real
// declarations from ylt/coro_rpc are mirrored here; redeclaring an alias with
// the same target is legal, and the full header stays confined to the .cpp.
namespace coro_rpc {
struct config_t;
template <typename server_config>
class coro_rpc_server_base;
using coro_rpc_server = coro_rpc_server_base<config_t>;
}  // namespace coro_rpc

namespace mooncake::conductor::kvevent {

class EventManager;
class ConductorService;

// KVEventHandler adapts the generic EventHandler interface for EventManager.
class KVEventHandler : public zmq::EventHandler {
   public:
    KVEventHandler(EventManager* manager, common::ServiceConfig svc,
                   prefixindex::SourceLayout layout = {});

    std::string HandleBatch(const zmq::DecodedBatch& batch,
                            const zmq::MessageMetadata& metadata) override;

    // The source cannot be recovered to a gap-free sequence, so everything it
    // contributed is retracted: the handler stops admitting batches and
    // invalidates its index entries until a full resynchronization arrives.
    void OnSourceStale(const std::string& cache_pool_key,
                       const zmq::MessageMetadata& metadata,
                       const std::string& reason) override;

    void MarkUnavailable();

    // Waits for callbacks admitted before MarkUnavailable() to finish. Must be
    // called without EventManager::mu_ held.
    void WaitForIdle();

    // Called only after the subscriber has stopped and joined.
    std::string InvalidateEndpoint();
    // Wake a Store subscription whose initial events preceded the declaration.
    void RefreshDeployment();
    // 把同一 ContextKey 已激活的部署挂到本实例（登记 reader、继承可读存储域）。
    bool AttachDeployment(const prefixindex::SourceLayout& binding);

   private:
    friend class KVEventHandlerTestPeer;

    struct PoolObjectKey {
        std::string source_endpoint;
        std::string backend_id;
        std::string tenant_id;
        std::string object_key;
        prefixindex::StorageTier tier = prefixindex::StorageTier::kCpuShare;

        auto operator<=>(const PoolObjectKey&) const = default;
    };

    struct PoolObjectBinding {
        prefixindex::ContextKey context;
        prefixindex::ProjectedPrefix prefix;
        std::string connector_block_hash;
        prefixindex::SharedObjectOwner owner;
        std::string tenant_id;
        std::string group_id;
        // One logical SGLang hash may have several physical Mooncake
        // objects, such as separate key and value components.
        std::set<std::string> physical_components;
    };

    std::string HandleVllmBatch(const zmq::VllmEventBatch& batch,
                                const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeBatch(const zmq::MooncakeEventBatch& batch,
                                    const zmq::MessageMetadata& metadata);
    std::string HandleSglangBatch(const zmq::SglangEventBatch& batch,
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
    std::string HandleSglangMooncakeStored(
        const zmq::MooncakeStoredEvent& event,
        const zmq::MessageMetadata& metadata);
    std::string HandleSglangMooncakeRemoved(
        const zmq::MooncakeRemovedEvent& event,
        const zmq::MessageMetadata& metadata);
    std::string HandleMooncakeCleared(const zmq::MooncakeClearedEvent& event,
                                      const zmq::MessageMetadata& metadata);
    std::string HandleSglangStored(const zmq::SglangStoredEvent& event,
                                   const zmq::MessageMetadata& metadata);
    std::string HandleSglangRemoved(const zmq::SglangRemovedEvent& event,
                                    const zmq::MessageMetadata& metadata);
    std::string HandleSglangCleared(const zmq::MessageMetadata& metadata);
    std::string ClearMooncakeBindings(
        const std::optional<std::string>& backend_id,
        const std::optional<std::string>& tenant_id);

    std::string HandleGroupedVllmStored(const zmq::VllmStoredEvent& event);
    std::string HandleGroupedVllmRemoved(const zmq::VllmRemovedEvent& event);
    std::string HandleGroupedSglangStored(const zmq::SglangStoredEvent& event);
    std::string HandleGroupedSglangRemoved(
        const zmq::SglangRemovedEvent& event);
    std::string HandleGroupedMooncakeStored(
        const zmq::MooncakeStoredEvent& event);
    std::string HandleGroupedMooncakeRemoved(
        const zmq::MooncakeRemovedEvent& event);

    bool BeginDispatch();
    void EndDispatch();
    std::string QueuePending(const zmq::DecodedBatch& batch,
                             const zmq::MessageMetadata& metadata);
    void DrainPending();

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
    prefixindex::SourceLayout layout_;
    // layout 由事件中的 kv_cache_layout 声明决定，不能固定在构造期；
    // 因此统一用本函数实时判断是否走分组（多 cache group）路径。
    bool grouped_context() const { return layout_.layout != nullptr; }
    // 同一 ContextKey 下任意实例宣告的部署都应能被其他实例复用：
    // 本实例没有 layout 时，从索引取已激活的部署并登记自己的 reader。
    bool EnsureDeployment();
    std::mutex grouped_dispatch_mu_;
    std::string deployment_declaration_;
    std::deque<std::pair<zmq::DecodedBatch, zmq::MessageMetadata>> pending_;
    size_t pending_event_count_ = 0;
    size_t pending_bytes_ = 0;
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
    // max_blocks_per_context caps the prefix entries kept per index context;
    // zero disables capacity eviction.
    EventManager(
        std::vector<common::ServiceConfig> services, int http_server_port,
        int rpc_server_port = 0,
        size_t max_blocks_per_context = prefixindex::kDefaultMaxBlocks);
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

    // Starts the coro_rpc server on the configured port, sharing the same
    // ConductorService as the HTTP channel. rpc_server_port==0 disables the
    // channel: the call is a no-op and returns true.
    bool StartRPCServer();

    // Actual bound RPC port; 0 when the RPC server is not running.
    uint16_t RpcPort() const;

    prefixindex::PrefixCacheTable* GetIndexer() { return &indexer_; }

    ConductorService& GetService() { return *service_; }

    // True once Stop() has begun; checked by KVEventHandler::HandleBatch
    // under mu_ (read).
    bool IsStopped();
    void RefreshMooncakeDeployments(const prefixindex::ContextKey& context);
    // 把某个 ContextKey 上已激活的部署传播给该 ContextKey 下的所有订阅实例。
    void PropagateDeployment(const prefixindex::ContextKey& context,
                             const prefixindex::SourceLayout& binding);

   private:
    friend class KVEventHandler;
    friend class EventManagerTestPeer;
    friend class ConductorService;

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
    int rpc_server_port_;

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
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    std::unique_ptr<ConductorService> service_;
};

}  // namespace mooncake::conductor::kvevent
