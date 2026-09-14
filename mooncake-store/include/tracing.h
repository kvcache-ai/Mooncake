#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "request_context.h"

namespace mooncake {

// Lightweight OpenTelemetry tracing integration for the store binaries.
//
// Usage:
//   1. Call mooncake::InitTracing(endpoint, service_name) once at process
//      start, e.g. right after parsing --otlp-traces-endpoint. When
//      `endpoint` is empty, tracing stays disabled and every ScopedSpan below
//      is a cheap no-op.
//   2. In an RPC handler that already extracts the propagated RequestContext,
//      construct a `ScopedSpan`. It starts a SERVER span that is a child of
//      the incoming trace context (if any) and ends it (RAII) when the handler
//      returns.
//   3. On the hop-A bridge (real_client), call PopulateRequestContext(ctx)
//      before re-installing the per-thread context so the downstream hop-B
//      attachment carries *this* span as the parent.
//
// Build model: when MOONCAKE_ENABLE_OTEL_TRACING is defined, the spans are
// exported over OTLP/HTTP (via Mooncake's own coro_http transport) to the
// `endpoint` collector. When it is not defined (or tracing was never
// initialized), all of these are empty stubs, so the rest of Mooncake compiles
// and links without any OpenTelemetry dependency.

class ScopedSpanImpl;

class ScopedSpan {
   public:
    // `parent_ctx` may be nullptr (root span) or point to the RequestContext
    // deserialized from the incoming coro_rpc attachment; its trace_id/span_id
    // (when valid) become the remote parent of this span.
    ScopedSpan(const char* tracer_name, const char* span_name,
               const RequestContext* parent_ctx);
    ~ScopedSpan();
    ScopedSpan(const ScopedSpan&) = delete;
    ScopedSpan& operator=(const ScopedSpan&) = delete;

    // Whether a real span is being recorded. False when tracing is disabled or
    // uninitialized.
    bool active() const;

    // Overwrite `ctx`'s trace_id/span_id/parent_span_id with this span's
    // identity so that the next hop's attachment propagates this span as the
    // parent. No-op when inactive. `parent_span_id` is set to the incoming
    // parent ctx's span_id (or cleared) for completeness.
    void PopulateRequestContext(RequestContext& ctx) const;

    // --- OpenTelemetry span enrichment (all no-op when inactive) ---
    // The incoming RequestContext::request_id is recorded automatically as the
    // `request.id` span attribute, so a span can be correlated with
    // request-scoped application logs. Helpers below let a handler attach more
    // attributes and flag the outcome; they are cheap no-ops when tracing is
    // disabled, so call-sites need no compile-time guards.
    void AddAttribute(const char* key, std::string_view value);
    void AddAttribute(const char* key, std::int64_t value);
    void AddAttribute(const char* key, bool value);
    // Mark the span's status as ERROR with an optional description. Call this
    // from a handler when the store operation failed, so the trace surfaces the
    // failure without grepping duration.
    void SetError(std::string_view description);
    // Mark the span's status as OK.
    void SetOk();
    // Record a timestamped event on the span (e.g. "retry", "fallback").
    void AddEvent(std::string_view name);

   private:
    std::unique_ptr<ScopedSpanImpl> impl_;
};

// Initialize the global tracer provider. `otlp_endpoint` selects the OTLP
// traces collector, given WITHOUT a scheme: "host:port" (for gRPC) or
// "host:port/path" (for HTTP; /v1/traces is appended when only host:port is
// given). `protocol` chooses the transport: "http" (default, OTLP/HTTP) or
// "grpc" (OTLP/gRPC); anything other than "grpc" falls back to OTLP/HTTP. An
// empty endpoint disables tracing (no provider installed, ScopedSpan becomes
// no-op). Returns true if tracing was enabled. Headers/libs for both exporters
// are built by install_otel.sh (WITH_OTLP_HTTP=ON, WITH_OTLP_GRPC=ON).
bool InitTracing(const std::string& otlp_endpoint,
                 std::string service_name,
                 const std::string& protocol = "http");

// Force-flush pending spans and tear down the provider. Safe to call multiple
// times; safe no-op if tracing was never enabled. Intended for clean shutdown
// / tests.
void ShutdownTracing();

// True once InitTracing has successfully enabled tracing.
bool IsTracingEnabled();

}  // namespace mooncake
