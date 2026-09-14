#pragma once

#include <cstddef>
#include <cstdint>
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
    std::string request_id;  // application-level correlation id
    std::string trace_id;    // distributed trace id
    std::string span_id;
    std::string parent_span_id;
    // Wire-compatible additions (struct_pack::compatible => optional semantics).
    // Appended at the end so older binaries lacking them safely ignore the
    // trailing compatible bytes, and a newer binary receiving old bytes sees
    // them unset; the type code stays stable (verified by a struct_pack
    // round-trip test). Distinct increasing version tags (1, 2) fix the wire
    // order and leave room for future compatible fields (use >=3). See
    // plan_trace.md §2.8.
    struct_pack::compatible<std::string, 1> caller_id;   // which dummy-client / TP rank issued the RPC
    struct_pack::compatible<std::string, 2> caller_role; // caller's thread role, e.g. prefetch / backup
};

// Enable struct_pack field-name-based serialization. Compatible fields appended
// at the end keep the type code stable: an older binary lacking them ignores
// the trailing compatible bytes (forward), and a newer binary receiving old
// bytes sees them unset (backward). Future compatible additions should use
// increasing version tags (>=3).
YLT_REFL(RequestContext, request_id, trace_id, span_id, parent_span_id,
         caller_id, caller_role);

// Per-thread current request context. Set on the calling (Python) thread before
// a store operation and consumed synchronously by the master-client wrappers on
// the same thread. For coroutine/async paths, snapshot it at entry and forward
// it explicitly instead of reading this in continuations.
inline thread_local std::optional<RequestContext> g_current_ctx;

// RAII scope that sets the current request context and restores the previous
// one on destruction (handy for the hop A->B bridge and for test/Python
// helpers).
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
// ctx.get_context_info()->release_request_attachment() (a std::string, which
// drains the buffer); an empty view means no per-request id was supplied.
// Serialize the full RequestContext to wire bytes for out-of-band
// attachment (coro_rpc send_request_with_attachment /
// release_request_attachment).
inline std::string current_request_context_attachment() {
    if (g_current_ctx) {
        return struct_pack::serialize<std::string>(*g_current_ctx);
    }
    return {};
}

// ---------------------------------------------------------------------------
// Trace id / span id derivation from a request id (header-only, no OTel dep).
//
// When an upstream caller supplies only a `request_id` but no trace context
// (the common case for UUID-tagged requests), the chain can still form a
// coherent trace by *using the request id as the trace id*. For a UUID request
// id the hyphens are simply stripped so the literal request id becomes the
// 32-hex trace id; for any other request id a deterministic FNV-1a hash is used
// so every hop seeds the same value. Pure, no OpenTelemetry dependency, and
// used both with tracing enabled (to seed the span's trace id) and disabled
// (to keep logs correlatable across hops).
// ---------------------------------------------------------------------------
inline char RequestContextLowHexChar(char c) {
    if (c >= 'A' && c <= 'F') return static_cast<char>(c - 'A' + 'a');
    return c;
}
inline bool RequestContextIsLowerHexChar(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}
inline std::uint64_t RequestContextFnv1a64(std::string_view s, std::uint64_t seed) {
    std::uint64_t h = seed;
    for (unsigned char c : s) {
        h ^= static_cast<std::uint64_t>(c);
        h *= 0x100000001b3ULL;
    }
    return h;
}
inline std::string RequestContextBytesToHex(const unsigned char* data, std::size_t n) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.resize(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out[2 * i] = kHex[(data[i] >> 4) & 0xf];
        out[2 * i + 1] = kHex[data[i] & 0xf];
    }
    return out;
}

// Derive a 32-hex trace id (16 bytes) from a request id. A hyphenated id that
// reduces to exactly 32 hex chars (a UUID, e.g. uuid4 minus its hyphens) is
// used verbatim so the trace id *is* the request id. Otherwise the request id
// is hashed deterministically; an all-zero id (invalid in OTel) is nudged.
inline std::string DeriveTraceIdFromRequestId(std::string_view request_id) {
    if (request_id.empty()) return {};
    std::string hex;
    hex.reserve(request_id.size());
    for (char c : request_id) {
        if (c == '-') continue;
        hex.push_back(RequestContextLowHexChar(c));
    }
    if (hex.size() == 32) {
        bool ok = true;
        for (char c : hex) {
            if (!RequestContextIsLowerHexChar(c)) {
                ok = false;
                break;
            }
        }
        if (ok) return hex;  // request id *is* the trace id (UUID case)
    }
    unsigned char bytes[16];
    std::uint64_t lo = RequestContextFnv1a64(request_id, 0xcbf29ce484222325ULL);
    std::uint64_t hi = RequestContextFnv1a64(request_id, 0x6c62272e07bb0142ULL);
    if (lo == 0 && hi == 0) lo = 1;  // all-zero TraceId is invalid
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<unsigned char>(hi >> (56 - 8 * i));
        bytes[8 + i] = static_cast<unsigned char>(lo >> (56 - 8 * i));
    }
    return RequestContextBytesToHex(bytes, 16);
}

// Derive a 16-hex span id (8 bytes) from a request id, using a distinct seed so
// it differs from the trace id halves. Used to synthesize a stable parent span
// id when the caller supplied only a request id (no real upstream span), so the
// started span can inherit the request-derived trace id (see
// MakeRemoteSpanContext).
inline std::string DeriveSpanIdFromRequestId(std::string_view request_id) {
    if (request_id.empty()) return {};
    std::uint64_t h = RequestContextFnv1a64(request_id, 0x9dc5d7e9c4b2f1a3ULL);
    if (h == 0) h = 1;  // all-zero SpanId is invalid
    unsigned char bytes[8];
    for (int i = 0; i < 8; ++i)
        bytes[i] = static_cast<unsigned char>(h >> (56 - 8 * i));
    return RequestContextBytesToHex(bytes, 8);
}

// When the upstream supplied only a request_id (no trace_id), use the request
// id as the trace id so the chain stays coherent even without an explicit
// trace context / OTel export. No-op when a trace id is already present.
inline void EnsureRequestIdAsTraceId(RequestContext& ctx) {
    if (!ctx.trace_id.empty() || ctx.request_id.empty()) return;
    ctx.trace_id = DeriveTraceIdFromRequestId(ctx.request_id);
}

// Read-only accessors for the (optional, compatible) caller-attribution
// fields, returning an empty view when unset. Lets call sites use them
// uniformly without dereferencing the optional; unset => empty => omitted from
// logs and span attributes.
inline std::string_view caller_id_of(const RequestContext& ctx) {
    return ctx.caller_id ? std::string_view(*ctx.caller_id) : std::string_view{};
}
inline std::string_view caller_role_of(const RequestContext& ctx) {
    return ctx.caller_role ? std::string_view(*ctx.caller_role) : std::string_view{};
}

// ---------------------------------------------------------------------------
// Caller-based virtual-root derivation (plan_trace.md §2.9). HiCache backup ops
// carry caller_id/caller_role but no request_id, so the chain would otherwise
// degenerate to a per-hop random root and become uncorrelatable. We synthesize
// a stable *virtual* trace/span id keyed on the caller using FNV-1a with seeds
// distinct from the request-id namespace above, so a literal collision (same
// string used as both a request id and a caller id) still maps to a different
// id, keeping the two namespaces disjoint. Pure, header-only, no OTel dep, and
// used both with tracing enabled (MakeRemoteSpanContext falls back to it) and
// disabled (so logs stay correlatable across hops for caller-keyed traffic).
inline std::string CallerDeriveKey(std::string_view caller_id,
                                  std::string_view caller_role) {
    std::string key;
    key.reserve(caller_id.size() + 1 + caller_role.size());
    key.append(caller_id);
    key.push_back('\x1f');  // separator so ("a","bc") != ("ab","c")
    key.append(caller_role);
    return key;
}

// Derive a 32-hex trace id from the caller attribution. Empty when neither
// field is set (no caller => no synthesized id; the SDK creates a genuine
// root). Distinct seeds keep this namespace disjoint from the request-id one.
inline std::string DeriveTraceIdFromCaller(std::string_view caller_id,
                                           std::string_view caller_role) {
    if (caller_id.empty() && caller_role.empty()) return {};
    const std::string key = CallerDeriveKey(caller_id, caller_role);
    unsigned char bytes[16];
    // Seeds intentionally differ from DeriveTraceIdFromRequestId's
    // {0xcbf29ce484222325, 0x6c62272e07bb0142}.
    std::uint64_t lo = RequestContextFnv1a64(key, 0x4242842e07bb0142ULL);
    std::uint64_t hi = RequestContextFnv1a64(key, 0x8c1dc8336afd6c62ULL);
    if (lo == 0 && hi == 0) lo = 1;  // all-zero TraceId is invalid
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<unsigned char>(hi >> (56 - 8 * i));
        bytes[8 + i] = static_cast<unsigned char>(lo >> (56 - 8 * i));
    }
    return RequestContextBytesToHex(bytes, 16);
}

// Derive a 16-hex span id from the caller attribution (distinct seed from the
// request-id span id and from the caller trace id halves).
inline std::string DeriveSpanIdFromCaller(std::string_view caller_id,
                                          std::string_view caller_role) {
    if (caller_id.empty() && caller_role.empty()) return {};
    const std::string key = CallerDeriveKey(caller_id, caller_role);
    // Seed intentionally differs from DeriveSpanIdFromRequestId's 0x9dc5d7e9c4b2f1a3.
    std::uint64_t h = RequestContextFnv1a64(key, 0x3a1c2b4e9d7c5debULL);
    if (h == 0) h = 1;  // all-zero SpanId is invalid
    unsigned char bytes[8];
    for (int i = 0; i < 8; ++i)
        bytes[i] = static_cast<unsigned char>(h >> (56 - 8 * i));
    return RequestContextBytesToHex(bytes, 8);
}

// Self-seed a virtual trace id from the caller attribution when there is
// neither an upstream trace_id nor a request_id (the latter is handled by
// EnsureRequestIdAsTraceId first, so the request-id path keeps precedence).
// Idempotent: a context already carrying a trace id is left untouched.
inline void EnsureTraceIdFromCaller(RequestContext& ctx) {
    if (!ctx.trace_id.empty()) return;            // upstream / already seeded
    if (!ctx.request_id.empty()) return;         // request-id path owns it
    auto cid = caller_id_of(ctx);
    auto crole = caller_role_of(ctx);
    if (cid.empty() && crole.empty()) return;     // nothing to key on -> root
    ctx.trace_id = DeriveTraceIdFromCaller(cid, crole);
}

// Deserialize a RequestContext from wire bytes (received via
// release_request_attachment). Returns an empty RequestContext when data is
// empty or deserialization fails.
inline RequestContext deserialize_request_context(std::string_view data) {
    RequestContext ctx;
    if (!data.empty()) {
        struct_pack::deserialize_to(ctx, data.data(), data.size());
    }
    // Self-seed the distributed trace id so the chain stays coherent (and
    // observable in logs) even when no explicit trace context / OpenTelemetry
    // export is configured. The request-id path (§2.7) runs first and takes
    // precedence; the caller-based virtual root (§2.9) only fills in when
    // there is no request_id either (e.g. HiCache backup ops). Both are
    // idempotent and deterministic, so every hop sees the same value.
    EnsureRequestIdAsTraceId(ctx);
    EnsureTraceIdFromCaller(ctx);
    return ctx;
}

}  // namespace mooncake
