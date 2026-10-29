#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "conductor/common/types.h"
#include "conductor/prefixindex/prefix_indexer.h"

namespace mooncake::conductor {

// Error codes shared by the service layer, both transports, and clients.
// Numeric values align with mooncake-store/include/types.h where meanings
// overlap (conductor does not link mooncake-store, so the shared subset is
// replicated by value); conductor-specific codes occupy the -2000 range.
enum class ErrorCode : int32_t {
    OK = 0,
    INTERNAL_ERROR = -1,
    INVALID_PARAMS = -600,
    RPC_FAIL = -900,
    RPC_TIMEOUT = -901,

    CONDUCTOR_UNAVAILABLE = -2000,  // client not set up / server stopping
    SERVICE_NOT_FOUND = -2001,      // unregister target not registered
};

const char* ErrorCodeName(ErrorCode code) noexcept;

// /query request, shared by both transports and the C++ client.
struct QueryRequest {
    prefixindex::ContextKey context;
    std::vector<int32_t> token_ids;
    std::optional<std::string> cache_salt;
    std::optional<std::string> instance_filter;

    bool operator==(const QueryRequest&) const = default;
};

struct QueryResult {
    std::map<std::string, prefixindex::CacheHitResult> instances;

    bool operator==(const QueryResult&) const = default;
};

struct RegisterResult {
    std::string instance_id;
    bool is_new = false;

    bool operator==(const RegisterResult&) const = default;
};

struct UnregisterResult {
    std::string removed_key;

    bool operator==(const UnregisterResult&) const = default;
};

}  // namespace mooncake::conductor
