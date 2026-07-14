// Copyright 2026 Mooncake Authors

#include "client_metric.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <string>
#include <string_view>

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

enum class Scenario : uint8_t {
    NOOP = 0,
    AGREE,
    DISAGREE,
    FALLBACK,
};

const char* ScenarioName(Scenario scenario) {
    switch (scenario) {
        case Scenario::NOOP:
            return "noop";
        case Scenario::AGREE:
            return "agree";
        case Scenario::DISAGREE:
            return "disagree";
        case Scenario::FALLBACK:
            return "fallback";
    }
    return "unknown";
}

mooncake::ReplicaSelectionDecision MakeDecision(Scenario scenario) {
    mooncake::ReplicaSelectionDecision decision;
    decision.mode = mooncake::ReplicaSelectionMode::SHADOW;
    if (scenario == Scenario::FALLBACK) {
        decision.outcome = mooncake::ReplicaSelectionOutcome::
            FALLBACK_SIGNAL_SOURCE_UNAVAILABLE;
        return decision;
    }

    decision.outcome = mooncake::ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY;
    decision.selected = mooncake::ReplicaRef{0, 1};
    decision.recommendation = scenario == Scenario::AGREE
                                  ? mooncake::ReplicaRef{0, 1}
                                  : mooncake::ReplicaRef{1, 2};
    return decision;
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

int main(int argc, char** argv) {
    uint64_t iterations = 200000;
    uint64_t windows = 20;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        iterations = ParseUnsigned(argument, "--iterations=", iterations);
        windows = ParseUnsigned(argument, "--windows=", windows);
    }
    if (iterations == 0 || windows == 0) return 2;

    mooncake::ReplicaSelectionMetric metrics;
    uint64_t noop_checksum = 0;
    std::cout << "scenario,window,ops,elapsed_ns,ns_per_op,allocations,"
                 "allocated_bytes,checksum\n";
    std::cout << std::fixed << std::setprecision(3);

    for (const Scenario scenario : {Scenario::NOOP, Scenario::AGREE,
                                    Scenario::DISAGREE, Scenario::FALLBACK}) {
        const auto decision = MakeDecision(scenario);
        for (uint64_t warmup = 0; warmup < 10000; ++warmup) {
            if (scenario == Scenario::NOOP) {
                noop_checksum += warmup & 1;
            } else {
                metrics.Observe(decision, 500);
            }
        }

        for (uint64_t window = 0; window < windows; ++window) {
            g_allocation_count.store(0, std::memory_order_relaxed);
            g_allocated_bytes.store(0, std::memory_order_relaxed);
            uint64_t checksum = noop_checksum;
            g_count_allocations.store(true, std::memory_order_release);
            const auto start = std::chrono::steady_clock::now();
            for (uint64_t operation = 0; operation < iterations; ++operation) {
                if (scenario == Scenario::NOOP) {
                    checksum += operation & 1;
                } else {
                    metrics.Observe(decision, 500);
                }
            }
            const auto end = std::chrono::steady_clock::now();
            g_count_allocations.store(false, std::memory_order_release);
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                    .count();

            if (scenario != Scenario::NOOP) {
                const char* comparison = scenario == Scenario::FALLBACK
                                             ? "neither_available"
                                             : ScenarioName(scenario);
                const char* outcome = scenario == Scenario::FALLBACK
                                          ? "fallback_signal_source_unavailable"
                                          : "scored_remote_memory";
                checksum = static_cast<uint64_t>(
                    metrics.decisions.value({"shadow", outcome, comparison}));
            }

            std::cout << ScenarioName(scenario) << ',' << window << ','
                      << iterations << ',' << elapsed << ','
                      << static_cast<double>(elapsed) /
                             static_cast<double>(iterations)
                      << ','
                      << g_allocation_count.load(std::memory_order_relaxed)
                      << ','
                      << g_allocated_bytes.load(std::memory_order_relaxed)
                      << ',' << checksum << '\n';
        }
    }

    return noop_checksum == std::numeric_limits<uint64_t>::max() ? 3 : 0;
}
