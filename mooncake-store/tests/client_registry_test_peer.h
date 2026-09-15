#pragma once

#include "client_registry.h"
#include "../src/client_offboarding_internal.h"

namespace mooncake {

// Deterministic lifecycle tests can drive one tick/attempt without sleeping for
// the monitor. No worker or queue operations are exposed by the production API.
class ClientRegistryTestPeer {
   public:
    static auto Evaluate(ClientRegistry& registry,
                         const ClientSessionPtr& session,
                         ClientSession::TimePoint now,
                         ClientSession::Clock::duration active_ttl,
                         ClientSession::Clock::duration suspicion_ttl) {
        return registry.Evaluate(session, now, active_ttl, suspicion_ttl);
    }
    static void StartWorker(ClientRegistry& registry,
                            ClientRegistry::CleanupResources cleanup) {
        registry.StartOffboarding(std::move(cleanup));
    }
    // These fixtures already contain prepared/pending region operations.
    static bool ProcessPreparedJob(ClientRegistry& registry,
                                   ClientOffboardingJob& job) {
        job.resources_prepared = true;
        return registry.ProcessClientOffboardingJob(job);
    }
    static auto RetryDelay(uint64_t retry_count) {
        return ClientOffboardingWorker::RetryDelay(retry_count);
    }
    static bool ShouldAlert(uint64_t retry_count) {
        return ClientOffboardingWorker::ShouldAlert(retry_count);
    }
};

}  // namespace mooncake
