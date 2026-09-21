#pragma once

#include "client_session_registry.h"

namespace mooncake::test {

// The single test access boundary for ClientSessionRegistry. Keep test-only
// inspection and synchronous drivers here, not in the production registry API.
// Raw accessors take no registry lock, so callers must preserve its lock
// order, and synchronous drivers must not race its own expiry thread.
class ClientSessionRegistryTestPeer {
   public:
    using Clock = ClientSessionRegistry::Clock;
    using Record = ClientSessionRegistry::Record;
    using Records = ClientSessionRegistry::Records;
    using Slot = ClientSessionRegistry::Slot;

    // Lookup only: the returned record does not grant operation admission.
    // Use TryAcquire*Session for work that must exclude state transitions.
    static Record Find(const ClientSessionRegistry& registry,
                       const UUID& client_id) {
        const auto slot = registry.FindSlot(client_id);
        return slot ? slot->liveness : nullptr;
    }

    // Copies registry membership, not a frozen snapshot of session state.
    static Records SnapshotRecords(const ClientSessionRegistry& registry) {
        Records records;
        for (const auto& [client_id, slot] : registry.SnapshotSlots()) {
            records.emplace(client_id, slot->liveness);
        }
        return records;
    }

    // One deterministic expiry pass. Do not race the registry's expiry thread
    // or another pass.
    static void ExpireSessions(ClientSessionRegistry& registry,
                               Clock::time_point now) {
        registry.ExpireSessions(now);
    }

    // Drive the registry's own retirement listener directly, without waiting
    // for an expiry pass to produce the event.
    static void RetireSession(ClientSessionRegistry& registry,
                              const UUID& client_id,
                              const ClientSessionSharedPtr& session) {
        registry.OnSessionRetired(client_id, session);
    }

    // Deliver queued transitions on the caller's thread instead of the
    // registry's delivery thread.
    static void Drain(ClientSessionRegistry& registry) {
        registry.events_.Drain();
    }

    // One deterministic tick: evaluate every session, then deliver what it
    // published.
    static void Poll(ClientSessionRegistry& registry, Clock::time_point now) {
        registry.ExpireSessions(now);
        registry.events_.Drain();
    }

    static Slot FindSlot(const ClientSessionRegistry& registry,
                         const UUID& client_id) {
        return registry.FindSlot(client_id);
    }

    static bool AcquireExclusiveOperation(const ClientSessionRegistry& registry,
                                          const UUID& client_id,
                                          const Slot& slot,
                                          bool try_lock = false) {
        return registry.AcquireExclusiveOperation(client_id, slot, try_lock)
            .has_value();
    }

    static bool AcquireSharedOperation(const ClientSessionRegistry& registry,
                                       const UUID& client_id,
                                       const Slot& slot) {
        return registry.AcquireSharedOperation(client_id, slot).has_value();
    }
};

}  // namespace mooncake::test
