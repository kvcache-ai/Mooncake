#pragma once

// Test-only accessors for EventManager internals. Shared by
// event_manager_test.cpp and concurrency_test.cpp.

#include <memory>
#include <string>
#include <utility>

#include "conductor/kvevent/event_manager.h"

namespace conductor {
namespace kvevent {

class EventManagerTestPeer {
   public:
    static std::pair<bool, std::string> Subscribe(
        EventManager& mgr, const common::ServiceConfig& svc) {
        std::unique_lock lock(mgr.mu_);
        return mgr.SubscribeToService(svc);
    }

    static std::pair<bool, std::string> Register(
        EventManager& mgr, const common::ServiceConfig& svc) {
        std::unique_lock lock(mgr.mu_);
        auto result = mgr.SubscribeToService(svc);
        if (result.first) {
            mgr.services_.push_back(svc);
        }
        return result;
    }

    static std::pair<bool, std::string> Unsubscribe(
        EventManager& mgr, const std::string& instance_id,
        const std::string& tenant_id, int dp_rank) {
        return mgr.UnsubscribeFromService(instance_id, tenant_id, dp_rank);
    }

    static void FakeSubscriber(EventManager& mgr, const std::string& key) {
        // A never-started client stands in as the subscriber; Stop() on
        // it is a no-op.
        auto dummy =
            std::make_shared<zmq::ZMQClient>(zmq::ZMQClientConfig{}, nullptr);
        std::unique_lock lock(mgr.mu_);
        mgr.subscribers_[key] = std::move(dummy);
    }

    static void FakeActiveService(EventManager& mgr, const std::string& key,
                                  const common::ServiceConfig& service) {
        auto dummy =
            std::make_shared<zmq::ZMQClient>(zmq::ZMQClientConfig{}, nullptr);
        auto handler = std::make_shared<KVEventHandler>(&mgr, service);
        std::unique_lock lock(mgr.mu_);
        mgr.subscribers_[key] = std::move(dummy);
        mgr.handlers_[key] = std::move(handler);
        mgr.active_configs_[key] = service;
        mgr.active_endpoints_[service.endpoint] = key;
    }

    static size_t ServicesLen(EventManager& mgr) {
        std::shared_lock lock(mgr.mu_);
        return mgr.services_.size();
    }

    static size_t SubscriberCount(EventManager& mgr) {
        std::shared_lock lock(mgr.mu_);
        return mgr.subscribers_.size();
    }

    static size_t ActiveConfigCount(EventManager& mgr) {
        std::shared_lock lock(mgr.mu_);
        return mgr.active_configs_.size();
    }

    static size_t CleanupQuarantinedCount(EventManager& mgr) {
        std::shared_lock lock(mgr.mu_);
        return mgr.cleanup_quarantined_.size();
    }

    static std::shared_ptr<KVEventHandler> HandlerFor(EventManager& mgr,
                                                      const std::string& key) {
        std::shared_lock lock(mgr.mu_);
        auto handler = mgr.handlers_.find(key);
        return handler == mgr.handlers_.end() ? nullptr : handler->second;
    }

    static void AppendService(EventManager& mgr,
                              const common::ServiceConfig& svc) {
        std::unique_lock lock(mgr.mu_);
        mgr.services_.push_back(svc);
    }

    static uint16_t HttpPort(EventManager& mgr);
};

class KVEventHandlerTestPeer {
   public:
    static size_t BindingCount(KVEventHandler& handler) {
        std::lock_guard lock(handler.bindings_mu_);
        return handler.pool_bindings_.size();
    }

    static bool IsAvailable(KVEventHandler& handler) {
        std::lock_guard lock(handler.lifecycle_mu_);
        return handler.available_;
    }

    static bool BeginDispatch(KVEventHandler& handler) {
        return handler.BeginDispatch();
    }

    static void EndDispatch(KVEventHandler& handler) { handler.EndDispatch(); }

    static std::string HandleVllmStored(KVEventHandler& handler,
                                        const zmq::VllmStoredEvent& event,
                                        const zmq::MessageMetadata& metadata) {
        return handler.HandleVllmStored(event, metadata);
    }

    static bool SetFirstBindingOwnerSource(KVEventHandler& handler,
                                           const std::string& source) {
        std::lock_guard lock(handler.bindings_mu_);
        if (handler.pool_bindings_.empty()) {
            return false;
        }
        handler.pool_bindings_.begin()->second.owner.source_stream = source;
        return true;
    }
};

}  // namespace kvevent
}  // namespace conductor
