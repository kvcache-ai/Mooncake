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

// Stamp the calling thread's current request_id (if any) into `out`. No-op when
// no context is set, which lets a downstream process preserve a request_id that
// was already carried in (e.g. across the dummy -> RealClient hop A).
inline void apply_current_request_id(std::optional<std::string>& out) {
    if (g_current_ctx) {
        out = g_current_ctx->request_id;
    }
}

}  // namespace mooncake
