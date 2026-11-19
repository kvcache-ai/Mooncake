// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mooncake::ssd_offload {

enum class DestinationError {
    kOk,
    kInvalidArgument,
    kOverflow,
    kNotFound,
    kStaleRegion,
    kUnauthorized,
    kOverlap,
    kLimit,
    kClosing,
    kBusy,
    kWrongAttempt,
    kNotDrained,
    kDuplicate,
    kStaleRequest,
    kNotSupported,
    kTransportError,
};

struct PeerIdentity {
    uint64_t client_id_high{0};
    uint64_t client_id_low{0};
    uint64_t boot_epoch{0};
    bool operator==(const PeerIdentity&) const = default;
};

// Issued by an authorized registration adapter. The registry does not generate
// secrets, and IDs/generations alone do not authorize a write or revoke DMA.
struct WriteCredential {
    uint64_t high{0};
    uint64_t low{0};
    bool operator==(const WriteCredential&) const = default;
};

struct DestinationRegion {
    PeerIdentity requester;
    uint64_t region_id{0};
    uint64_t generation{0};
    WriteCredential credential;
    bool operator==(const DestinationRegion&) const = default;
};

struct RegisteredDestination {
    DestinationRegion region;
    uint64_t offset{0};
    uint64_t length{0};
    bool operator==(const RegisteredDestination&) const = default;
};

enum class DestinationMemory { kHost, kDevice };
struct DestinationBinding {
    uint64_t logical_base{0};
    uint64_t logical_bytes{0};
    uint64_t registered_base{0};
    uint64_t registered_bytes{0};
    DestinationMemory memory{DestinationMemory::kHost};
};
struct ResolvedDestination {
    std::string transport_endpoint;
    uint64_t address{0};
    uint64_t length{0};
    DestinationMemory memory{DestinationMemory::kHost};
};

struct AttemptIdentity {
    PeerIdentity owner;
    uint64_t request_id{0};
    uint64_t attempt_id{0};
    bool operator==(const AttemptIdentity&) const = default;
};

enum class AttemptOutcome { kSucceeded, kFailed, kCancelled, kRejected };
struct DestinationDrainAck {
    AttemptIdentity attempt;
    AttemptOutcome outcome{AttemptOutcome::kFailed};
    uint64_t valid_bytes{0};
    bool dma_drained{false};
    PeerIdentity requester;
};
enum class DestinationAttemptState {
    kPrepared,
    kExposed,
    kQuarantined,
    kDrained
};
struct DestinationAttemptSnapshot {
    DestinationError error{DestinationError::kNotFound};
    DestinationAttemptState state{DestinationAttemptState::kPrepared};
    uint64_t requested_bytes{0};
    uint64_t valid_bytes{0};
    // Transfer completeness only; integrity still needs caller verification.
    // Timed-out or abandoned attempts never become publishable on a late ACK.
    bool publishable{false};
};

struct DestinationRegistryLimits {
    size_t max_regions{1024};
    size_t max_tracked_attempts{1024};
    size_t max_owner_sessions{64};
    size_t max_active_fragments{65536};
    uint64_t max_active_bytes{4ULL * 1024 * 1024 * 1024};
};
struct DestinationRegistryStats {
    size_t regions{0};
    size_t tracked_attempts{0};
    size_t active_attempts{0};
    size_t quarantined_attempts{0};
    size_t active_fragments{0};
    uint64_t active_bytes{0};
    // Reserved fragment-array storage, excluding map/string/allocator overhead.
    uint64_t fragment_array_bytes{0};
};

class DestinationRegistry {
    struct State;
    struct Record;

   public:
    class Attempt {
       public:
        Attempt() = default;
        Attempt(Attempt&&) noexcept;
        Attempt& operator=(Attempt&&) noexcept;
        ~Attempt();
        Attempt(const Attempt&) = delete;
        Attempt& operator=(const Attempt&) = delete;
        // Marks exposure before returning addresses that may be sent to an
        // owner. Keep the handle/registry alive for later drain confirmation.
        DestinationError Publish(
            std::vector<ResolvedDestination>& destinations);
        DestinationError Timeout();
        DestinationError Complete(const DestinationDrainAck& ack);
        DestinationAttemptSnapshot snapshot() const;

       private:
        friend class DestinationRegistry;
        Attempt(std::shared_ptr<State>, std::shared_ptr<Record>);
        void Abandon() noexcept;
        std::shared_ptr<State> state_;
        std::shared_ptr<Record> record_;
    };
    struct RegistrationResult {
        DestinationError error{DestinationError::kInvalidArgument};
        DestinationRegion region;
    };
    struct AcquireResult {
        DestinationError error{DestinationError::kInvalidArgument};
        Attempt attempt;
    };

    DestinationRegistry(PeerIdentity requester, std::string transport_endpoint,
                        DestinationRegistryLimits limits = {});
    ~DestinationRegistry();
    DestinationRegistry(const DestinationRegistry&) = delete;
    DestinationRegistry& operator=(const DestinationRegistry&) = delete;

    // Pins transport registration/parent mapping. The caller holds its actual
    // allocation until logical retirement AND transport deregistration succeed;
    // a token is not ownership
    // of an arbitrary application pointer. Logical aliases are rejected.
    RegistrationResult Register(DestinationBinding, WriteCredential,
                                std::shared_ptr<void> registration_lifetime);
    AcquireResult Acquire(
        const AttemptIdentity&,
        const std::vector<RegisteredDestination>& destinations);
    DestinationError BeginRevoke(const DestinationRegion&);
    // Call after no DMA remains; kBusy leaves the registration pinned. The
    // adapter must retain/quarantine backing if transport deregistration fails.
    DestinationError Retire(const DestinationRegion&);
    DestinationError Complete(const DestinationDrainAck&);
    DestinationAttemptSnapshot Inspect(const AttemptIdentity&) const;
    // Acknowledges a completed logical request and advances an owner-session
    // replay floor. New request IDs must increase; existing older attempts
    // remain drainable. No active/quarantined request may be forgotten.
    DestinationError ForgetRequest(PeerIdentity owner, uint64_t request_id);
    DestinationRegistryStats stats() const;
    bool HasPendingWrite(uint64_t address, uint64_t bytes) const;

   private:
    std::shared_ptr<State> state_;
};

}  // namespace mooncake::ssd_offload
