#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <ylt/struct_pack.hpp>

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

// Enable struct_pack field-name-based serialization. Future fields appended
// as struct_pack::compatible<std::string> at the end are safely ignored by
// older binaries that lack the field in their YLT_REFL list.
YLT_REFL(RequestContext, request_id, trace_id, span_id, parent_span_id);

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
// ctx.get_context_info()->release_request_attachment() (a std::string, which drains the buffer); an
// empty view means no per-request id was supplied.
// Serialize the full RequestContext to wire bytes for out-of-band
// attachment (coro_rpc send_request_with_attachment / release_request_attachment).
inline std::string current_request_context_attachment() {
    if (g_current_ctx) {
        return struct_pack::serialize<std::string>(*g_current_ctx);
    }
    return {};
}

// Deserialize a RequestContext from wire bytes (received via
// release_request_attachment). Returns an empty RequestContext when
// data is empty or deserialization fails.
inline RequestContext deserialize_request_context(std::string_view data) {
    RequestContext ctx;
    if (!data.empty()) {
        struct_pack::deserialize_to(ctx, data.data(), data.size());
    }
    return ctx;
}

// --- Test/instrumentation seam (process-global; NOT a production hot path) ---
// Lets an in-process integration test observe the attachment request_id that a
// master handler actually received, instead of relying on VLOG. A test sets a
// RequestContext on the calling thread, performs a synchronous single-key read
// (invoke_rpc sends current_request_context_attachment() out-of-band), then reads
// LastObservedRequestId(). The mutex is held only across a short std::string
// copy, and only GetReplicaList/BatchGetReplicaList ever write here.
inline std::mutex& request_id_instrument_mutex() {
    static std::mutex m;
    return m;
}
inline std::string& request_id_instrument_store() {
    static std::string s;
    return s;
}
inline void RecordObservedRequestId(std::string_view id) {
    std::lock_guard<std::mutex> lk(request_id_instrument_mutex());
    request_id_instrument_store() = std::string(id);
}
inline std::string LastObservedRequestId() {
    std::lock_guard<std::mutex> lk(request_id_instrument_mutex());
    return request_id_instrument_store();
}
inline void ClearLastObservedRequestId() {
    std::lock_guard<std::mutex> lk(request_id_instrument_mutex());
    request_id_instrument_store().clear();
}

}  // namespace mooncake
