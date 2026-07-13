// Copyright 2026 Mooncake Authors

#include "replica_selector.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "replica_selection.h"

namespace {

std::atomic<bool> g_count_allocations{false};
std::atomic<uint64_t> g_allocation_count{0};
std::atomic<uint64_t> g_allocated_bytes{0};

void RecordAllocation(std::size_t size) noexcept {
    if (!g_count_allocations.load(std::memory_order_relaxed)) return;
    g_allocation_count.fetch_add(1, std::memory_order_relaxed);
    g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
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

enum class ProviderOutcome : uint8_t {
    SUCCESS = 0,
    UNAVAILABLE,
};

enum class BenchmarkMode : uint8_t {
    DIRECT = 0,
    LEGACY,
    SHADOW,
    ACTIVE,
};

class BenchmarkProvider final : public ReplicaScoreProvider {
   public:
    explicit BenchmarkProvider(ProviderOutcome outcome) : outcome_(outcome) {}

    void ScoreAll(const ReplicaSelectionContext&,
                  std::span<const ReplicaScoreCandidateView> candidates,
                  std::span<ReplicaScoreVerdict> verdicts) const override {
        ++calls_;
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (outcome_ == ProviderOutcome::UNAVAILABLE) {
                verdicts[i] = ReplicaScoreUnavailable{
                    ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE};
            } else {
                verdicts[i] = ReplicaEligibleScore{
                    {static_cast<double>(candidates.size() - i), 0.0, 0.0}};
            }
        }
    }

    void ResetCalls() const noexcept { calls_ = 0; }
    [[nodiscard]] uint64_t calls() const noexcept { return calls_; }

   private:
    ProviderOutcome outcome_;
    mutable uint64_t calls_{0};
};

Replica::Descriptor MakeMemory(ReplicaID id) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    MemoryDescriptor memory;
    memory.buffer_descriptor.size_ = 1024;
    memory.buffer_descriptor.buffer_address_ = 0x1000 + id * 0x1000;
    memory.buffer_descriptor.protocol_ = "rdma";
    memory.buffer_descriptor.transport_endpoint_ =
        "store-" + std::to_string(id);
    descriptor.descriptor_variant = std::move(memory);
    descriptor.status = ReplicaStatus::COMPLETE;
    return descriptor;
}

const char* ModeName(BenchmarkMode mode) {
    switch (mode) {
        case BenchmarkMode::DIRECT:
            return "direct";
        case BenchmarkMode::LEGACY:
            return "legacy";
        case BenchmarkMode::SHADOW:
            return "shadow";
        case BenchmarkMode::ACTIVE:
            return "active";
    }
    return "unknown";
}

ReplicaSelectionMode SelectorMode(BenchmarkMode mode) {
    switch (mode) {
        case BenchmarkMode::DIRECT:
        case BenchmarkMode::LEGACY:
            return ReplicaSelectionMode::LEGACY;
        case BenchmarkMode::SHADOW:
            return ReplicaSelectionMode::SHADOW;
        case BenchmarkMode::ACTIVE:
            return ReplicaSelectionMode::ACTIVE;
    }
    return ReplicaSelectionMode::LEGACY;
}

const char* OutcomeName(ProviderOutcome outcome) {
    return outcome == ProviderOutcome::SUCCESS ? "success" : "unavailable";
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
}  // namespace mooncake

int main(int argc, char** argv) {
    using namespace mooncake;
    uint64_t iterations = 100000;
    uint64_t windows = 10;
    ReplicaSelectionDiagnostics diagnostics =
        ReplicaSelectionDiagnostics::DISABLED;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        iterations = ParseUnsigned(argument, "--iterations=", iterations);
        windows = ParseUnsigned(argument, "--windows=", windows);
        if (argument == "--diagnostics") {
            diagnostics = ReplicaSelectionDiagnostics::ENABLED;
        }
    }
    if (iterations == 0 || windows == 0) return 2;

    SetRemoteReplicaScorer(nullptr);
    const std::unordered_set<std::string> local_endpoints;
    const ReplicaSelectionContext context{
        "tenant", "model/layer", "decode-0", "rack-b/gpu-0", 1 << 20, 7};

    std::cout
        << "mode,provider_outcome,diagnostics,candidates,window,ops,elapsed_ns,"
           "ns_per_op,allocations,allocated_bytes,provider_calls,checksum\n";
    std::cout << std::fixed << std::setprecision(3);

    for (const size_t candidate_count : {1u, 2u, 4u, 8u, 16u}) {
        std::vector<Replica::Descriptor> replicas;
        replicas.reserve(candidate_count);
        for (size_t i = 0; i < candidate_count; ++i) {
            replicas.push_back(MakeMemory(static_cast<ReplicaID>(i + 1)));
        }

        for (const BenchmarkMode mode :
             {BenchmarkMode::DIRECT, BenchmarkMode::LEGACY,
              BenchmarkMode::SHADOW, BenchmarkMode::ACTIVE}) {
            for (const ProviderOutcome provider_outcome :
                 {ProviderOutcome::SUCCESS, ProviderOutcome::UNAVAILABLE}) {
                auto provider =
                    std::make_shared<BenchmarkProvider>(provider_outcome);
                const ReplicaSelector selector(SelectorMode(mode), provider,
                                               diagnostics);
                uint64_t warmup_checksum = 0;
                for (size_t i = 0; i < 10000; ++i) {
                    if (mode == BenchmarkMode::DIRECT) {
                        if (const auto* selected =
                                SelectBestReplica(replicas, local_endpoints)) {
                            warmup_checksum += selected->id;
                        }
                    } else {
                        const auto decision =
                            selector.Select(replicas, local_endpoints, context);
                        if (const auto* selected =
                                ResolveReplica(replicas, decision.selected)) {
                            warmup_checksum += selected->id;
                        }
                    }
                }
                if (warmup_checksum == std::numeric_limits<uint64_t>::max()) {
                    return 3;
                }

                for (uint64_t window = 0; window < windows; ++window) {
                    provider->ResetCalls();
                    g_allocation_count.store(0, std::memory_order_relaxed);
                    g_allocated_bytes.store(0, std::memory_order_relaxed);
                    uint64_t checksum = 0;
                    g_count_allocations.store(true, std::memory_order_release);
                    const auto start = std::chrono::steady_clock::now();
                    for (uint64_t operation = 0; operation < iterations;
                         ++operation) {
                        if (mode == BenchmarkMode::DIRECT) {
                            if (const auto* selected = SelectBestReplica(
                                    replicas, local_endpoints)) {
                                checksum += selected->id;
                            }
                        } else {
                            const auto decision = selector.Select(
                                replicas, local_endpoints, context);
                            if (const auto* selected = ResolveReplica(
                                    replicas, decision.selected)) {
                                checksum += selected->id;
                            }
                        }
                    }
                    const auto end = std::chrono::steady_clock::now();
                    g_count_allocations.store(false, std::memory_order_release);
                    const auto elapsed =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            end - start)
                            .count();
                    const uint64_t allocations =
                        g_allocation_count.load(std::memory_order_relaxed);
                    const uint64_t allocated_bytes =
                        g_allocated_bytes.load(std::memory_order_relaxed);

                    std::cout
                        << ModeName(mode) << ','
                        << OutcomeName(provider_outcome) << ','
                        << (diagnostics == ReplicaSelectionDiagnostics::ENABLED
                                ? "full"
                                : "off")
                        << ',' << candidate_count << ',' << window << ','
                        << iterations << ',' << elapsed << ','
                        << static_cast<double>(elapsed) /
                               static_cast<double>(iterations)
                        << ',' << allocations << ',' << allocated_bytes << ','
                        << provider->calls() << ',' << checksum << '\n';
                }
            }
        }
    }
    return 0;
}
