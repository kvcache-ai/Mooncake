// Copyright 2026 Mooncake Authors
//
// Experimental, instance-scoped replica selection model. This API contains no
// signal collection or client integration: callers provide request context and
// a provider backed by cached signals, then receive a value-only decision.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

#include "replica.h"

namespace mooncake {

// LEGACY and SHADOW preserve the process-global V1 policy for the selected
// replica. SHADOW computes an instance-scoped V2 recommendation without
// applying it. ACTIVE is fully isolated from V1 and applies the V2 result.
enum class ReplicaSelectionMode : uint8_t {
    LEGACY = 0,
    SHADOW,
    ACTIVE,
};

enum class ReplicaSelectionDiagnostics : uint8_t {
    DISABLED = 0,
    ENABLED,
};

// Views are valid only for the duration of ReplicaScoreProvider::ScoreAll().
struct ReplicaSelectionContext {
    std::string_view tenant_id{};
    std::string_view key{};
    std::string_view requester_endpoint{};
    std::string_view destination_location{};
    uint64_t requested_bytes{0};
    uint64_t request_nonce{0};
};

// A stable value reference to a descriptor in the master-returned list.
// ResolveReplica also verifies replica_id, so reordering or replacement is
// detected instead of silently resolving the wrong descriptor.
struct ReplicaRef {
    size_t master_order{0};
    ReplicaID replica_id{0};

    friend bool operator==(const ReplicaRef&, const ReplicaRef&) = default;
};

struct ReplicaScoreCandidateView {
    ReplicaRef replica;
    std::string_view transport_endpoint;
    std::string_view protocol;
};

// Weighted, normalized cost contributions. Lower is better. Every component
// and their sum must be finite and non-negative.
struct ReplicaScoreBreakdown {
    double topology_cost{0.0};
    double load_cost{0.0};
    double health_cost{0.0};
};

struct ReplicaEligibleScore {
    ReplicaScoreBreakdown breakdown;
};

enum class ReplicaHardVetoReason : uint8_t {
    UNHEALTHY = 0,
    CIRCUIT_OPEN,
};

struct ReplicaHardVeto {
    ReplicaHardVetoReason reason{ReplicaHardVetoReason::UNHEALTHY};
};

// Signal unavailability is not proof that a replica is unreadable. It causes
// a structural fallback; it must never be treated as a hard veto.
enum class ReplicaScoreUnavailableReason : uint8_t {
    STALE = 0,
    MISSING_SIGNAL,
    SIGNAL_SOURCE_UNAVAILABLE,
};

struct ReplicaScoreUnavailable {
    ReplicaScoreUnavailableReason reason{
        ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE};
};

// monostate is an output sentinel. ScoreAll must replace every element.
using ReplicaScoreVerdict =
    std::variant<std::monostate, ReplicaEligibleScore, ReplicaHardVeto,
                 ReplicaScoreUnavailable>;

class ReplicaScoreProvider {
   public:
    virtual ~ReplicaScoreProvider() = default;

    // Select calls this method at most once. The provider must capture one
    // immutable request-scoped snapshot and evaluate every candidate from that
    // same snapshot. It must use cached state only: no blocking I/O and no
    // retaining candidate/context views after return. Expected stale/missing
    // data is returned as ReplicaScoreUnavailable; exceptions are reserved for
    // unexpected program failures. Concurrent Select calls require a
    // thread-safe implementation.
    virtual void ScoreAll(const ReplicaSelectionContext& context,
                          std::span<const ReplicaScoreCandidateView> candidates,
                          std::span<ReplicaScoreVerdict> verdicts) const = 0;
};

enum class ReplicaSelectionOutcome : uint8_t {
    LEGACY_MODE = 0,
    LOCAL_FAST_PATH,
    NO_REMOTE_MEMORY,
    SCORED_REMOTE_MEMORY,
    SCORED_STORAGE_FALLBACK,
    NO_SAFE_REPLICA,
    FALLBACK_NO_PROVIDER,
    FALLBACK_PROVIDER_EXCEPTION,
    FALLBACK_SIGNAL_STALE,
    FALLBACK_MISSING_SIGNAL,
    FALLBACK_SIGNAL_SOURCE_UNAVAILABLE,
    FALLBACK_INVALID_RESULT,
    FALLBACK_INVALID_SCORE,
};

enum class ReplicaSkipReason : uint8_t {
    INCOMPLETE = 0,
    NON_MEMORY,
    LOCAL,
};

struct ReplicaCandidateScore {
    ReplicaRef replica;
    ReplicaScoreBreakdown breakdown;
    double total_score{0.0};
};

struct ReplicaHardVetoObservation {
    ReplicaRef replica;
    ReplicaHardVetoReason reason{ReplicaHardVetoReason::UNHEALTHY};
};

struct ReplicaUnavailableObservation {
    ReplicaRef replica;
    ReplicaScoreUnavailableReason reason{
        ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE};
};

struct ReplicaSkipObservation {
    ReplicaRef replica;
    ReplicaSkipReason reason{ReplicaSkipReason::NON_MEMORY};
};

struct ReplicaSelectionTrace {
    std::vector<ReplicaCandidateScore> candidate_scores;
    std::vector<ReplicaHardVetoObservation> hard_vetoes;
    std::vector<ReplicaUnavailableObservation> unavailable_scores;
    std::vector<ReplicaSkipObservation> skipped;
};

struct ReplicaSelectionDecision {
    ReplicaSelectionMode mode{ReplicaSelectionMode::LEGACY};
    ReplicaSelectionOutcome outcome{ReplicaSelectionOutcome::LEGACY_MODE};
    // Scorer-free policy result used when V2 cannot produce a complete safe
    // ranking. Populated for SHADOW/ACTIVE. LEGACY leaves this empty to avoid
    // scanning the descriptor list once for diagnostics and again for V1.
    std::optional<ReplicaRef> structural_baseline;
    // Effective V2 counterfactual, including its structural fallback.
    std::optional<ReplicaRef> recommendation;
    // Replica the caller must actually use for I/O. In SHADOW this remains the
    // V1 incumbent; in ACTIVE it equals recommendation.
    std::optional<ReplicaRef> selected;
    // Empty when diagnostics are disabled. The core outcome never depends on
    // whether trace collection was requested.
    ReplicaSelectionTrace trace;
};

[[nodiscard]] const Replica::Descriptor* ResolveReplica(
    std::span<const Replica::Descriptor> replicas,
    const std::optional<ReplicaRef>& reference) noexcept;

// Immutable after construction. SHADOW reads V1 only to preserve the current
// online selection; its recommendation and every ACTIVE decision are isolated
// from V1. Diagnostics are opt-in so the common selection path does not
// allocate merely to build per-candidate trace vectors.
class ReplicaSelector final {
   public:
    explicit ReplicaSelector(
        ReplicaSelectionMode mode = ReplicaSelectionMode::LEGACY,
        std::shared_ptr<const ReplicaScoreProvider> provider = nullptr,
        ReplicaSelectionDiagnostics diagnostics =
            ReplicaSelectionDiagnostics::DISABLED);

    [[nodiscard]] ReplicaSelectionMode mode() const noexcept { return mode_; }

    [[nodiscard]] ReplicaSelectionDecision Select(
        const std::vector<Replica::Descriptor>& replicas,
        const std::unordered_set<std::string>& local_endpoints,
        const ReplicaSelectionContext& context = {}) const;

   private:
    ReplicaSelectionMode mode_;
    std::shared_ptr<const ReplicaScoreProvider> provider_;
    ReplicaSelectionDiagnostics diagnostics_;
};

}  // namespace mooncake
