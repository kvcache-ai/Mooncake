// Unit tests for request_context.h trace-id / span-id derivation, including the
// caller-based ("virtual prefetch/backup root") derivation added for HiCache
// backup ops that carry caller_id/caller_role but no request_id. These are
// header-only, no OpenTelemetry dependency, so they
// run in the default (tracing-OFF) build.

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <ylt/struct_pack.hpp>

#include "request_context.h"

namespace mooncake {
namespace {

TEST(RequestContextCallerDerive, IsDeterministicAndHexLengths) {
    auto trace = DeriveTraceIdFromCaller("sglang-tp0", "Prefetch");
    auto span = DeriveSpanIdFromCaller("sglang-tp0", "Prefetch");
    EXPECT_FALSE(trace.empty());
    EXPECT_EQ(trace.size(), 32u);
    EXPECT_EQ(span.size(), 16u);
    // Same input => same output (deterministic: same op reproducibly links).
    EXPECT_EQ(trace, DeriveTraceIdFromCaller("sglang-tp0", "Prefetch"));
    EXPECT_EQ(span, DeriveSpanIdFromCaller("sglang-tp0", "Prefetch"));
}

TEST(RequestContextCallerDerive, RoleAndCallerChangeIds) {
    auto pf = DeriveTraceIdFromCaller("sglang-tp0", "Prefetch");
    auto bk = DeriveTraceIdFromCaller("sglang-tp0", "Backup");
    auto other = DeriveTraceIdFromCaller("sglang-tp1", "Prefetch");
    EXPECT_NE(pf, bk);     // role distinguishes the virtual root
    EXPECT_NE(pf, other);  // caller (TP rank) distinguishes it
}

TEST(RequestContextCallerDerive, EmptyCallerReturnsEmpty) {
    // No caller attribution => no synthesized id (MakeRemoteSpanContext lets
    // the SDK create a genuine root).
    EXPECT_TRUE(DeriveTraceIdFromCaller("", "").empty());
    EXPECT_TRUE(DeriveSpanIdFromCaller("", "").empty());
}

TEST(RequestContextCallerDerive, DistinctFromRequestDerivedIds) {
    // Caller-derived ids must not accidentally collide with request-derived
    // ones for the same literal string (distinct seeds keep the namespaces
    // disjoint).
    auto ct = DeriveTraceIdFromCaller("abc", "Prefetch");
    auto cs = DeriveSpanIdFromCaller("abc", "Prefetch");
    EXPECT_NE(ct, DeriveTraceIdFromRequestId("abc"));
    EXPECT_NE(cs, DeriveSpanIdFromRequestId("abc"));
}

TEST(RequestContextEnsureTraceId, BackupSelfSeedsFromCaller) {
    // Backup carries caller_id/caller_role but no request_id: without a derived
    // trace id the chain would degenerate to a random root per hop. Ensure it
    // self-seeds a stable *virtual* trace id keyed on the caller.
    RequestContext ctx;
    ctx.caller_id = "sglang-tp0";
    ctx.caller_role = "Backup";
    EnsureTraceIdFromCaller(ctx);
    EXPECT_EQ(ctx.trace_id, DeriveTraceIdFromCaller("sglang-tp0", "Backup"));

    // Idempotent: a context that already carries a trace id is left untouched.
    auto before = ctx.trace_id;
    EnsureTraceIdFromCaller(ctx);
    EXPECT_EQ(ctx.trace_id, before);
}

TEST(RequestContextEnsureTraceId, RequestIdTakesPrecedenceOverCaller) {
    // Prefetch path keeps the existing request-id-derived trace id;
    // EnsureTraceIdFromCaller must not override it with a caller id.
    RequestContext ctx;
    ctx.request_id = "abcd1234-abcd-1234-abcd-1234abcd1234";  // UUID -> 32 hex
    ctx.caller_id = "sglang-tp0";
    ctx.caller_role = "Prefetch";
    EnsureRequestIdAsTraceId(ctx);
    EnsureTraceIdFromCaller(ctx);
    EXPECT_EQ(ctx.trace_id, DeriveTraceIdFromRequestId(ctx.request_id));
    EXPECT_NE(ctx.trace_id, DeriveTraceIdFromCaller("sglang-tp0", "Prefetch"));
}

TEST(RequestContextDeserialize, BackupAttachmentSelfSeedsCallerTraceId) {
    RequestContext raw;
    raw.caller_id = "sglang-tp0";
    raw.caller_role = "Backup";
    std::string att = struct_pack::serialize<std::string>(raw);
    auto got = deserialize_request_context(att);
    EXPECT_EQ(got.trace_id, DeriveTraceIdFromCaller("sglang-tp0", "Backup"));
    ASSERT_TRUE(got.caller_id);
    ASSERT_TRUE(got.caller_role);
    EXPECT_EQ(*got.caller_id, "sglang-tp0");
    EXPECT_EQ(*got.caller_role, "Backup");
    EXPECT_TRUE(got.request_id.empty());
}

TEST(RequestContextDeserialize, PrefetchAttachmentStillUsesRequestId) {
    RequestContext raw;
    raw.request_id = "abcd1234-abcd-1234-abcd-1234abcd1234";
    std::string att = struct_pack::serialize<std::string>(raw);
    auto got = deserialize_request_context(att);
    // UUID request_id used verbatim as the trace id.
    EXPECT_EQ(got.trace_id, "abcd1234abcd1234abcd1234abcd1234");
}

TEST(RequestContextDeserialize, PrefetchWithTraceIdPreservesUpstreamTrace) {
    // When sglang EXPORTS the hicache root span (scenario 2) it sends a real
    // trace_id; deserialize must NOT overwrite it with a derived value.
    RequestContext raw;
    raw.request_id = "abcd1234-abcd-1234-abcd-1234abcd1234";
    raw.trace_id = std::string(32, '7');  // explicit upstream trace id
    raw.span_id = std::string(16, '8');   // real upstream parent span id
    std::string att = struct_pack::serialize<std::string>(raw);
    auto got = deserialize_request_context(att);
    EXPECT_EQ(got.trace_id, std::string(32, '7'));  // preserved
    EXPECT_EQ(got.span_id, std::string(16, '8'));   // preserved
}

}  // namespace
}  // namespace mooncake
