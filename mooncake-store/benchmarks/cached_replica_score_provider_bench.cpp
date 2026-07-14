// Copyright 2026 Mooncake Authors

#include "cached_replica_score_provider.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

std::atomic<bool> g_count_allocations{false};
std::atomic<uint64_t> g_allocation_count{0};
std::atomic<uint64_t> g_allocated_bytes{0};

void RecordAllocation(std::size_t size) noexcept {
    if (!g_count_allocations.load(std::memory_order_relaxed)) return;
    g_allocation_count.fetch_add(1, std::memory_order_relaxed);
    g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
}

uint64_t ParseUnsigned(std::string_view argument, std::string_view prefix,
                       uint64_t fallback) {
    if (!argument.starts_with(prefix)) return fallback;
    const std::string value(argument.substr(prefix.size()));
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    return end && *end == '\0' ? parsed : fallback;
}

}  // namespace

void* operator new(std::size_t size) {
    RecordAllocation(size);
    if (void* allocation = std::malloc(size)) return allocation;
    throw std::bad_alloc();
}

void* operator new[](std::size_t size) { return ::operator new(size); }

void operator delete(void* allocation) noexcept { std::free(allocation); }
void operator delete[](void* allocation) noexcept { std::free(allocation); }
void operator delete(void* allocation, std::size_t) noexcept {
    std::free(allocation);
}
void operator delete[](void* allocation, std::size_t) noexcept {
    std::free(allocation);
}

namespace mooncake {
namespace {

enum class Scenario : uint8_t {
    FRESH = 0,
    REQUIRED_SOURCE_DOWN,
    FRESH_HEALTH_VETO,
};

const char* ScenarioName(Scenario scenario) {
    switch (scenario) {
        case Scenario::FRESH:
            return "fresh";
        case Scenario::REQUIRED_SOURCE_DOWN:
            return "required_source_down";
        case Scenario::FRESH_HEALTH_VETO:
            return "fresh_health_veto";
    }
    return "unknown";
}

ReplicaSignalSourcePolicy RequiredPolicy() {
    return {true, true, std::chrono::hours(1), 1.0};
}

ReplicaSignalProviderConfig Config() {
    ReplicaSignalProviderConfig config;
    config.topology_source = RequiredPolicy();
    config.load_source = RequiredPolicy();
    config.health_source = RequiredPolicy();
    return config;
}

ReplicaSignalSnapshot Snapshot(Scenario scenario,
                               const std::vector<std::string>& endpoints) {
    const auto observed_at = std::chrono::steady_clock::now();
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.topology_source = {scenario != Scenario::REQUIRED_SOURCE_DOWN,
                                observed_at};
    snapshot.load_source = {true, observed_at};
    snapshot.health_source = {true, observed_at};

    for (size_t i = 0; i < endpoints.size(); ++i) {
        const double fraction = static_cast<double>(i + 1) /
                                static_cast<double>(endpoints.size() + 1);
        if (snapshot.topology_source.ready) {
            snapshot.topology.emplace(
                ReplicaTopologyRouteKey{"requester", "rack-b", endpoints[i],
                                        "rdma"},
                fraction);
        }
        snapshot.load.emplace(ReplicaEndpointProtocolKey{endpoints[i], "rdma"},
                              ReplicaLoadSignal{static_cast<uint64_t>(i * 4096),
                                                1 << 20, fraction / 2.0});
        const auto state = scenario == Scenario::FRESH_HEALTH_VETO && i == 0
                               ? ReplicaHealthState::UNHEALTHY
                               : ReplicaHealthState::HEALTHY;
        snapshot.health.emplace(
            ReplicaEndpointProtocolKey{endpoints[i], "rdma"},
            ReplicaHealthSignal{state, fraction / 4.0});
    }
    return snapshot;
}

uint64_t Consume(std::span<const ReplicaScoreVerdict> verdicts) {
    uint64_t checksum = 0;
    for (const auto& verdict : verdicts) {
        if (const auto* eligible =
                std::get_if<ReplicaEligibleScore>(&verdict)) {
            const auto total = eligible->breakdown.topology_cost +
                               eligible->breakdown.load_cost +
                               eligible->breakdown.health_cost;
            checksum += static_cast<uint64_t>(total * 1000000.0) + 1;
        } else if (std::holds_alternative<ReplicaHardVeto>(verdict)) {
            checksum += 7;
        } else if (std::holds_alternative<ReplicaScoreUnavailable>(verdict)) {
            checksum += 11;
        } else {
            checksum += 13;
        }
    }
    return checksum;
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    using namespace mooncake;

    uint64_t iterations = 100000;
    uint64_t windows = 10;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        iterations = ParseUnsigned(argument, "--iterations=", iterations);
        windows = ParseUnsigned(argument, "--windows=", windows);
    }
    if (iterations == 0 || windows == 0) return 2;

    const ReplicaSelectionContext context{"tenant", "model/layer", "requester",
                                          "rack-b", 1 << 18,       7};
    std::cout << "scenario,candidates,window,ops,elapsed_ns,ns_per_op,"
                 "allocations,allocated_bytes,checksum\n";
    std::cout << std::fixed << std::setprecision(3);

    for (const size_t candidate_count : {1u, 2u, 4u, 8u, 16u}) {
        std::vector<std::string> endpoints;
        endpoints.reserve(candidate_count);
        for (size_t i = 0; i < candidate_count; ++i) {
            endpoints.push_back("store-" + std::to_string(i + 1));
        }

        std::vector<ReplicaScoreCandidateView> candidates;
        candidates.reserve(candidate_count);
        for (size_t i = 0; i < candidate_count; ++i) {
            candidates.push_back(
                {{i, static_cast<ReplicaID>(i + 1)}, endpoints[i], "rdma"});
        }
        std::vector<ReplicaScoreVerdict> verdicts(candidate_count);

        for (const Scenario scenario :
             {Scenario::FRESH, Scenario::REQUIRED_SOURCE_DOWN,
              Scenario::FRESH_HEALTH_VETO}) {
            CachedReplicaScoreProvider provider(Config());
            if (provider.PublishSnapshot(Snapshot(scenario, endpoints)) !=
                ReplicaSignalPublishStatus::PUBLISHED) {
                return 3;
            }

            uint64_t warmup_checksum = 0;
            for (size_t i = 0; i < 10000; ++i) {
                provider.ScoreAll(context, candidates, verdicts);
                warmup_checksum += Consume(verdicts);
            }
            if (warmup_checksum == std::numeric_limits<uint64_t>::max()) {
                return 4;
            }

            for (uint64_t window = 0; window < windows; ++window) {
                g_allocation_count.store(0, std::memory_order_relaxed);
                g_allocated_bytes.store(0, std::memory_order_relaxed);
                uint64_t checksum = 0;
                g_count_allocations.store(true, std::memory_order_release);
                const auto start = std::chrono::steady_clock::now();
                for (uint64_t operation = 0; operation < iterations;
                     ++operation) {
                    provider.ScoreAll(context, candidates, verdicts);
                    checksum += Consume(verdicts);
                }
                const auto end = std::chrono::steady_clock::now();
                g_count_allocations.store(false, std::memory_order_release);
                const auto elapsed =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                         start)
                        .count();

                std::cout << ScenarioName(scenario) << ',' << candidate_count
                          << ',' << window << ',' << iterations << ','
                          << elapsed << ','
                          << static_cast<double>(elapsed) /
                                 static_cast<double>(iterations)
                          << ','
                          << g_allocation_count.load(std::memory_order_relaxed)
                          << ','
                          << g_allocated_bytes.load(std::memory_order_relaxed)
                          << ',' << checksum << '\n';
            }
        }
    }
    return 0;
}
