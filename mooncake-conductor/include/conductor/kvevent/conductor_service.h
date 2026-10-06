#pragma once

#include <string>
#include <vector>

#include "conductor/client/types.h"

namespace mooncake::conductor::kvevent {

class EventManager;

// Transport-agnostic business operations shared by the HTTP and coro_rpc
// adapters. Thread-safety matches the underlying EventManager: Query and
// GetGlobalView rely on PrefixCacheTable's own locking; ListServices takes
// the manager's shared lock.
class ConductorService {
   public:
    explicit ConductorService(EventManager& manager) : manager_(manager) {}

    tl::expected<QueryResult, ErrorCode> Query(const QueryRequest& request);
    prefixindex::GlobalView GetGlobalView();
    std::vector<common::ServiceConfig> ListServices();

    tl::expected<RegisterResult, ErrorCode> Register(
        const common::ServiceConfig& config);
    tl::expected<UnregisterResult, ErrorCode> Unregister(
        const std::string& instance_id, const std::string& tenant_id,
        int dp_rank);

   private:
    EventManager& manager_;
};

// Returns an empty string when the service configuration is valid for
// subscription, otherwise a human-readable validation error. Shared by the
// ConductorService write path and the HTTP /register parsing layer.
std::string ValidateServiceConfig(const common::ServiceConfig& service);

// ServiceConfig → prefixindex conversion helpers. Defined once in
// conductor_service.cpp and shared with EventManager's subscribe/unsubscribe
// paths (ValidateServiceConfig depends on them too).
prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service);
prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service);
prefixindex::EngineRegistration RegistrationFromService(
    const common::ServiceConfig& service);

}  // namespace mooncake::conductor::kvevent
