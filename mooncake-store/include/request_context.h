#pragma once

#include <optional>
#include <string>
#include <utility>

namespace mooncake {

// Per-request context propagated through the store -> master path.
//
// This is independent of `client_id` (the stable lease/segment identity), which
// intentionally must NOT be reused as a per-request correlation id.
struct RequestContext {
    std::string request_id;       // application-level correlation id
    std::string trace_id;         // distributed trace id
    std::string span_id;
    std::string parent_span_id;
};

// Per-thread current request context. Set on the calling (Python) thread before
// a store operation and consumed synchronously by the master-client wrappers on
// the same thread. For coroutine/async paths, snapshot it at entry and forward it
// explicitly instead of reading this in continuations.
inline thread_local std::optional<RequestContext> g_current_ctx;

// RAII scope that sets the current request context and restores the previous one
// on destruction (handy for the hop A->B bridge and for test/Python helpers).
class CurrentCtxScope {
   public:
    CurrentCtxScope() = default;
    explicit CurrentCtxScope(RequestContext ctx) : saved_(g_current_ctx) {
        g_current_ctx = std::move(ctx);
    }
    ~CurrentCtxScope() { g_current_ctx = std::move(saved_); }
    CurrentCtxScope(const CurrentCtxScope&) = delete;
    CurrentCtxScope& operator=(const CurrentCtxScope&) = delete;

   private:
    std::optional<RequestContext> saved_;
};

inline void set_current_request_context(RequestContext ctx) {
    g_current_ctx = std::move(ctx);
}
inline void clear_current_request_context() { g_current_ctx.reset(); }
inline const std::optional<RequestContext>& get_current_request_context() {
    return g_current_ctx;
}

// Bypass (out-of-band) attachment helpers. Client side: this is snapshotted at
// the entry of the master-client invoke_rpc* templates and handed to
// coro_rpc_client::send_request_with_attachment so request_id rides the request
// framing rather than a struct field. Server side: read it back via
// ctx.get_context_info()->get_request_attachment() (a std::string_view); an
// empty view means no per-request id was supplied.
inline std::string current_request_id_attachment() {
    if (g_current_ctx) {
        return g_current_ctx->request_id;
    }
    return {};
}

}  // namespace mooncake
