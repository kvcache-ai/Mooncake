#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "conductor/client/types.h"

namespace mooncake::conductor {

struct ConductorClientConfig {
    std::string conductor_addr;  // RPC server address, "host:port"
    int64_t connect_timeout_ms = 1000;
    int64_t request_timeout_ms = 3000;
};

// Thread-safety: v1 serializes all calls on an internal mutex around a single
// coro_rpc_client. Sufficient for control-plane rates; a pooled variant
// (mirroring store's MasterClient client_accessor_) is the hot-path upgrade.
//
// The pimpl idiom keeps coro_rpc headers out of this public header; wire types
// come solely from conductor/client/types.h.
class ConductorClient {
   public:
    ConductorClient();
    ~ConductorClient();  // calls Close() internally
    ConductorClient(const ConductorClient&) = delete;
    ConductorClient& operator=(const ConductorClient&) = delete;

    tl::expected<void, ErrorCode> Setup(const ConductorClientConfig& config);
    void Close();        // idempotent; Setup() may be called again afterwards
    bool HealthCheck();  // healthy if one ListServices call succeeds

    tl::expected<QueryResult, ErrorCode> Query(const QueryRequest& request);
    tl::expected<RegisterResult, ErrorCode> Register(
        const common::ServiceConfig& config);
    tl::expected<UnregisterResult, ErrorCode> Unregister(
        const std::string& instance_id, const std::string& tenant_id,
        int dp_rank);
    tl::expected<prefixindex::GlobalView, ErrorCode> GetGlobalView();
    tl::expected<std::vector<common::ServiceConfig>, ErrorCode> ListServices();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::conductor
