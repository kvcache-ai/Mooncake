// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_destination_registration.h"
#include "ssd_read_protocol.h"
#include <chrono>
#include <string_view>

namespace mooncake::ssd_offload {

struct SsdGrantReferenceV1 {
    PeerIdentity requester;
    AttemptIdentity attempt;
    WriteCredential credential;
    bool operator==(const SsdGrantReferenceV1&) const = default;
};
struct SsdControlLimits {
    size_t max_records{128};
    size_t max_sessions{64};
    uint64_t max_metadata_bytes{16 * 1024 * 1024};
    uint64_t max_active_payload_bytes{1024ULL * 1024 * 1024};
};
struct SsdControlStats {
    size_t records{0};
    uint64_t metadata_bytes{0};
    uint64_t active_payload_bytes{0};
};
struct SsdGrantPreparation {
    DestinationError error{DestinationError::kInvalidArgument};
    SsdGrantReferenceV1 reference;
};
struct SsdGrantResolution {
    DestinationError error{DestinationError::kInvalidArgument};
    // Ordered by objects, ranges, destinations. Only the requester registry
    // may produce these views; callers must not resolve a raw RPC address.
    std::vector<ResolvedDestination> destinations;
};

class SsdRequesterGrants {
   public:
    SsdRequesterGrants(PeerIdentity requester,
                       std::shared_ptr<DestinationRegistration>,
                       SsdControlLimits = {}, SsdReadProtocolLimits = {});
    ~SsdRequesterGrants();
    SsdRequesterGrants(const SsdRequesterGrants&) = delete;
    SsdRequesterGrants& operator=(const SsdRequesterGrants&) = delete;
    // Local trusted preparation only. Do not expose Prepare as a remote RPC.
    SsdGrantPreparation Prepare(const SsdReadRequestV1&);
    SsdGrantResolution Resolve(const SsdGrantReferenceV1&,
                               const SsdReadRequestV1&);
    DestinationError Timeout(const SsdGrantReferenceV1&);
    DestinationError Complete(const SsdGrantReferenceV1&,
                              const SsdReadResultV1&);
    DestinationAttemptSnapshot Inspect(const SsdGrantReferenceV1&) const;
    DestinationError Forget(PeerIdentity owner, uint64_t request_id);
    void Close();
    SsdControlStats stats() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

enum class SsdOwnerRequestState { kAdmitted, kRunning, kQuarantined, kDrained };
struct SsdOwnerRequestSnapshot {
    DestinationError error{DestinationError::kNotFound};
    SsdOwnerRequestState state{SsdOwnerRequestState::kAdmitted};
    std::optional<SsdReadResultV1> result{};
};
class SsdOwnerRequestLedger {
    struct State;
    struct Record;

   public:
    class Operation {
       public:
        Operation() = default;
        Operation(Operation&&) noexcept;
        Operation& operator=(Operation&&) noexcept;
        ~Operation();
        Operation(const Operation&) = delete;
        Operation& operator=(const Operation&) = delete;
        DestinationError Start();
        // Owner monotonic budget includes admission/queue time. Zero is not a
        // drain proof; a worker must still stop refill and drain accepted I/O.
        std::chrono::milliseconds RemainingTimeout() const;
        // Trusted worker only: result.dma_drained must follow actual I/O drain.
        // Do not expose this method as an externally callable completion RPC.
        DestinationError Complete(const SsdReadResultV1&);

       private:
        friend class SsdOwnerRequestLedger;
        Operation(std::shared_ptr<State>, std::shared_ptr<Record>);
        void Abandon() noexcept;
        std::shared_ptr<State> state_;
        std::shared_ptr<Record> record_;
    };
    struct Admission {
        DestinationError error{DestinationError::kInvalidArgument};
        Operation operation;
    };
    SsdOwnerRequestLedger(PeerIdentity owner, SsdControlLimits = {},
                          SsdReadProtocolLimits = {},
                          bool bounded_terminal_cache = false);
    ~SsdOwnerRequestLedger();
    SsdOwnerRequestLedger(const SsdOwnerRequestLedger&) = delete;
    SsdOwnerRequestLedger& operator=(const SsdOwnerRequestLedger&) = delete;
    // received_at is a nonnegative timestamp from this local steady clock,
    // never a wire timestamp. Reject negative/future anchors before admission.
    Admission Admit(const SsdReadRequestV1&, const SsdGrantReferenceV1&,
                    std::chrono::steady_clock::time_point received_at =
                        std::chrono::steady_clock::now(),
                    std::string_view binding = {});
    SsdOwnerRequestSnapshot Query(const SsdGrantReferenceV1&) const;
    // Trusted direct endpoint only, after peer credential validation. Existing
    // records must match the exact request/binding. Absence fences delayed
    // admission and proves no writer remains for this attempt, never success.
    SsdOwnerRequestSnapshot Recover(const SsdReadRequestV1&,
                                    const SsdGrantReferenceV1&,
                                    std::string_view binding = {});
    // Explicit requester acknowledgment only; no TTL eviction or timer proof.
    DestinationError Forget(const SsdGrantReferenceV1&);
    void Close();
    SsdControlStats stats() const;

   private:
    std::shared_ptr<State> state_;
};
}  // namespace mooncake::ssd_offload
