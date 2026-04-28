// Cost-aware routing — RPC throughput benchmark.
//
// Spins up an in-process MasterService + a coro_rpc server, mounts N
// fake segments through the public API, then drives QueryCost RPCs from
// W worker threads at maximum rate, measuring p50/p99 latency and QPS.
//
// Minimal version: it does not require a real transfer engine because
// the master side does not transfer payloads for QueryCost.
//
// Usage:
//   ./query_cost_bench [--candidates=N] [--workers=W] [--seconds=S]

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "master_service.h"
#include "rpc_types.h"

using namespace mooncake;

namespace {

struct Options {
    int candidate_count = 32;
    int worker_threads = 4;
    int run_seconds = 5;
};

Options ParseArgs(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto eat = [&](const std::string& key, int& out) {
            if (a.rfind(key, 0) == 0) {
                out = std::atoi(a.c_str() + key.size());
                return true;
            }
            return false;
        };
        eat("--candidates=", o.candidate_count);
        eat("--workers=", o.worker_threads);
        eat("--seconds=", o.run_seconds);
    }
    return o;
}

}  // namespace

int main(int argc, char** argv) {
    Options opts = ParseArgs(argc, argv);

    MasterServiceConfig config;
    config.enable_cost_aware = true;
    MasterService master(config);

    // Pre-build a candidate set; segments don't need to be mounted because
    // QueryCost just measures the dispatch + sort cost. Unmounted
    // candidates are dropped from the response by default but still
    // exercise the BuildSegmentNameIndex + lookup paths.
    std::vector<std::string> candidates;
    candidates.reserve(opts.candidate_count);
    for (int i = 0; i < opts.candidate_count; ++i) {
        candidates.push_back("seg-" + std::to_string(i));
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_calls{0};
    std::vector<std::vector<double>> per_thread_latencies(opts.worker_threads);

    auto worker_fn = [&](int tid) {
        QueryCostRequest req;
        req.candidate_segment_names = candidates;
        req.client_host = "10.0.0.1";
        req.client_zone = "zone-a";
        req.request_size_bytes = 1024 * 1024;
        req.include_unmounted = true;

        auto& lat = per_thread_latencies[tid];
        lat.reserve(1 << 20);
        while (!stop.load(std::memory_order_relaxed)) {
            const auto t0 = std::chrono::steady_clock::now();
            auto resp = master.QueryCost(req);
            const auto t1 = std::chrono::steady_clock::now();
            (void)resp;
            lat.push_back(
                std::chrono::duration<double, std::micro>(t1 - t0).count());
            total_calls.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < opts.worker_threads; ++i) {
        threads.emplace_back(worker_fn, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(opts.run_seconds));
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : threads) t.join();

    // Aggregate latencies.
    std::vector<double> all;
    for (auto& v : per_thread_latencies) {
        all.insert(all.end(), v.begin(), v.end());
    }
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) {
        if (all.empty()) return 0.0;
        size_t idx = static_cast<size_t>(p * all.size());
        if (idx >= all.size()) idx = all.size() - 1;
        return all[idx];
    };

    const double qps =
        static_cast<double>(total_calls.load()) / opts.run_seconds;
    std::printf("=== query_cost_bench ===\n");
    std::printf("candidates per request : %d\n", opts.candidate_count);
    std::printf("worker threads         : %d\n", opts.worker_threads);
    std::printf("duration               : %d s\n", opts.run_seconds);
    std::printf("total RPCs             : %llu\n",
                static_cast<unsigned long long>(total_calls.load()));
    std::printf("aggregate QPS          : %.0f\n", qps);
    std::printf("latency p50            : %.2f us\n", pct(0.50));
    std::printf("latency p90            : %.2f us\n", pct(0.90));
    std::printf("latency p99            : %.2f us\n", pct(0.99));
    std::printf("latency p999           : %.2f us\n", pct(0.999));
    return 0;
}
