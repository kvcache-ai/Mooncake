// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <vector>

#include "adaptive_congestion_control.h"

namespace mooncake::adaptive_congestion_control {
namespace {

constexpr uint64_t kBytesPerOperation = 64ULL << 10;
constexpr size_t kOperationsPerRound = 100'000;
constexpr size_t kMeasuredRounds = 41;

struct Counts {
    uint64_t allow = 0;
    uint64_t defer = 0;
    uint64_t avoid = 0;
};

Config configFor(Mode mode) {
    Config config;
    config.mode = mode;
    config.min_window_bytes = 64ULL << 20;
    config.max_window_bytes = 64ULL << 20;
    return config;
}

uint64_t mix(uint64_t value) {
    value ^= value << 13;
    value ^= value >> 7;
    return value ^ (value << 17);
}

double runBaseline(Mode mode, Counts& counts) {
    uint64_t checksum = 1;
    const auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kOperationsPerRound; ++i) {
        if (mode != Mode::kOff) ++counts.allow;
        checksum = mix(checksum + i);
    }
    const auto end = std::chrono::steady_clock::now();
    if (checksum == 0) std::abort();
    return std::chrono::duration<double, std::nano>(end - start).count() /
           kOperationsPerRound;
}

double runController(Mode mode, Counts& counts) {
    DomainState device(configFor(mode));
    DomainState route(configFor(mode));
    uint64_t checksum = 1;
    const auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kOperationsPerRound; ++i) {
        if (mode == Mode::kOff) {
            checksum = mix(checksum + i);
            continue;
        }
        Permit permit;
        PathHandle path{&device, &route, generation(device), generation(route)};
        switch (tryAcquire(path, kBytesPerOperation, permit)) {
            case Decision::kAllow:
                ++counts.allow;
                complete(permit, OutcomeClass::kSuccess,
                         FailureScope::kOperation);
                break;
            case Decision::kDefer:
                ++counts.defer;
                break;
            case Decision::kAvoid:
                ++counts.avoid;
                break;
        }
        checksum = mix(checksum + i);
    }
    const auto end = std::chrono::steady_clock::now();
    if (checksum == 0) std::abort();
    return std::chrono::duration<double, std::nano>(end - start).count() /
           kOperationsPerRound;
}

Mode parseMode(std::string_view value) {
    if (value == "off") return Mode::kOff;
    if (value == "observe") return Mode::kObserve;
    if (value == "enforce") return Mode::kEnforce;
    std::cerr << "mode must be baseline, off, observe, or enforce\n";
    std::exit(2);
}

double percentile(const std::vector<double>& samples, double fraction) {
    const size_t index = std::min(
        samples.size() - 1,
        static_cast<size_t>(fraction * static_cast<double>(samples.size())));
    return samples[index];
}

}  // namespace
}  // namespace mooncake::adaptive_congestion_control

int main(int argc, char** argv) {
    using namespace mooncake::adaptive_congestion_control;
    if (argc != 2) {
        std::cerr << "usage: adaptive_congestion_control_benchmark MODE\n";
        return 2;
    }

    const std::string_view name(argv[1]);
    const bool baseline = name == "baseline";
    const Mode mode = baseline ? Mode::kOff : parseMode(name);
    std::vector<double> samples;
    samples.reserve(kMeasuredRounds);
    Counts counts;
    runBaseline(Mode::kOff, counts);
    for (size_t round = 0; round < kMeasuredRounds; ++round) {
        samples.push_back(baseline ? runBaseline(mode, counts)
                                   : runController(mode, counts));
    }
    std::sort(samples.begin(), samples.end());

    std::cout << "mode,median_ns_per_op,p99_ns_per_op,allow,defer,avoid\n"
              << name << ',' << percentile(samples, 0.5) << ','
              << percentile(samples, 0.99) << ',' << counts.allow << ','
              << counts.defer << ',' << counts.avoid << '\n';
    return 0;
}
