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

    static void Unsubscribe(EventManager& mgr, const std::string& instance_id,
                            const std::string& tenant_id, int dp_rank) {
        mgr.UnsubscribeFromService(instance_id, tenant_id, dp_rank);
    }

    static void FakeSubscriber(EventManager& mgr, const std::string& key) {
        // A never-started client stands in as the subscriber; Stop() on
        // it is a no-op.
        auto dummy =
            std::make_shared<zmq::ZMQClient>(zmq::ZMQClientConfig{}, nullptr);
        std::unique_lock lock(mgr.mu_);
        mgr.subscribers_[key] = std::move(dummy);
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

    static void AppendService(EventManager& mgr,
                              const common::ServiceConfig& svc) {
        std::unique_lock lock(mgr.mu_);
        mgr.services_.push_back(svc);
    }

    static bool TenantHasInstance(EventManager& mgr, const std::string& tenant,
                                  const std::string& instance) {
        std::shared_lock lock(mgr.tenant_mutex_);
        auto it = mgr.tenant_instance_map_.find(tenant);
        return it != mgr.tenant_instance_map_.end() &&
               it->second.count(instance) != 0;
    }

    static uint16_t HttpPort(EventManager& mgr);
};

class KVEventHandlerTestPeer {
   public:
    static std::string BlockStored(KVEventHandler& handler,
                                   const zmq::BlockStoredEvent& event,
                                   int64_t dp_rank) {
        return handler.HandleBlockStored(event, dp_rank);
    }
};

}  // namespace kvevent
}  // namespace conductor
