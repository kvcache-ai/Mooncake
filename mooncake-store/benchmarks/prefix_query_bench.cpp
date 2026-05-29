// Copyright 2025 Mooncake Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "master_service.h"

DEFINE_uint64(num_keys, 200000, "Number of keys to preload into Master");
DEFINE_uint64(num_prefix_groups, 100,
              "Distinct prefix groups to distribute the keys across");
DEFINE_uint64(query_iterations, 200,
              "Number of repeated prefix queries to execute");
DEFINE_uint64(value_size, 1024, "Logical object value size in bytes");
DEFINE_uint64(target_prefix_id, 42, "Prefix group id to query");

namespace {

using mooncake::ErrorCode;
using mooncake::generate_uuid;
using mooncake::MasterService;
using mooncake::ReplicateConfig;
using mooncake::ReplicaType;
using mooncake::Segment;
using mooncake::UUID;

constexpr uintptr_t kSegmentBase = 0x400000000ULL;

std::string FormatPrefixId(uint64_t id) {
    std::ostringstream oss;
    oss << std::setw(3) << std::setfill('0') << id;
    return oss.str();
}

Segment MakeSegment(uint64_t segment_size) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "prefix_query_bench_segment";
    segment.base = kSegmentBase;
    segment.size = segment_size;
    segment.te_endpoint = segment.name;
    return segment;
}

void CheckExpected(bool ok, const std::string& message) {
    if (!ok) {
        LOG(ERROR) << message;
        std::exit(1);
    }
}

void PreloadKeys(MasterService& service, const UUID& client_id,
                 uint64_t num_keys, uint64_t num_prefix_groups,
                 uint64_t value_size) {
    ReplicateConfig config;
    config.replica_num = 1;

    for (uint64_t i = 0; i < num_keys; ++i) {
        const uint64_t prefix_id = i % num_prefix_groups;
        const std::string key = "tenant_" + FormatPrefixId(prefix_id) +
                                "/session_" + std::to_string(i);
        auto put_start = service.PutStart(client_id, key, value_size, config);
        CheckExpected(put_start.has_value(), "PutStart failed for " + key);
        auto put_end = service.PutEnd(client_id, key, ReplicaType::MEMORY);
        CheckExpected(put_end.has_value(), "PutEnd failed for " + key);
    }
}

struct QueryStats {
    double total_ms{0.0};
    double avg_ms{0.0};
    size_t match_count{0};
};

QueryStats RunPrefixQueryBenchmark(MasterService& service,
                                   const std::string& pattern,
                                   uint64_t iterations, bool disable_fastpath) {
    setenv("MOONCAKE_STORE_DISABLE_PREFIX_FASTPATH",
           disable_fastpath ? "1" : "0", 1);

    // Warm up query-plan creation and cache state before timing.
    for (int i = 0; i < 10; ++i) {
        auto result = service.GetReplicaListByRegex(pattern);
        CheckExpected(result.has_value(), "Warmup query failed");
    }

    QueryStats stats;
    const auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < iterations; ++i) {
        auto result = service.GetReplicaListByRegex(pattern);
        CheckExpected(result.has_value(), "Measured query failed");
        stats.match_count = result->size();
    }
    const auto end = std::chrono::steady_clock::now();
    stats.total_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    stats.avg_ms = stats.total_ms / static_cast<double>(iterations);
    return stats;
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    CheckExpected(FLAGS_num_prefix_groups > 0,
                  "num_prefix_groups must be positive");
    const uint64_t segment_size =
        FLAGS_num_keys * FLAGS_value_size * 2 + (64ULL << 20);

    auto service_config = mooncake::MasterServiceConfig::builder()
                              .set_client_live_ttl_sec(3600)
                              .build();
    MasterService service(service_config);
    const UUID client_id = generate_uuid();
    Segment segment = MakeSegment(segment_size);
    auto mount_result = service.MountSegment(segment, client_id);
    CheckExpected(mount_result.has_value(),
                  "Failed to mount benchmark segment");

    std::cout << "Preloading " << FLAGS_num_keys << " keys across "
              << FLAGS_num_prefix_groups << " prefix groups..." << std::endl;
    PreloadKeys(service, client_id, FLAGS_num_keys, FLAGS_num_prefix_groups,
                FLAGS_value_size);

    const std::string prefix =
        "tenant_" +
        FormatPrefixId(FLAGS_target_prefix_id % FLAGS_num_prefix_groups) +
        "/session_";
    const std::string pattern = "^" + prefix;

    auto baseline =
        RunPrefixQueryBenchmark(service, pattern, FLAGS_query_iterations, true);
    auto fastpath = RunPrefixQueryBenchmark(service, pattern,
                                            FLAGS_query_iterations, false);

    const double speedup = baseline.avg_ms / fastpath.avg_ms;
    const double latency_reduction =
        (baseline.avg_ms - fastpath.avg_ms) / baseline.avg_ms * 100.0;

    std::cout << "\n=== Prefix Query Benchmark ===\n";
    std::cout << "Pattern: " << pattern << "\n";
    std::cout << "Matches/query: " << baseline.match_count << "\n";
    std::cout << "Iterations: " << FLAGS_query_iterations << "\n";
    std::cout << "\n[Regex fallback]\n";
    std::cout << "  Total: " << baseline.total_ms << " ms\n";
    std::cout << "  Avg:   " << baseline.avg_ms << " ms/query\n";
    std::cout << "\n[Prefix fast path]\n";
    std::cout << "  Total: " << fastpath.total_ms << " ms\n";
    std::cout << "  Avg:   " << fastpath.avg_ms << " ms/query\n";
    std::cout << "\n[Delta]\n";
    std::cout << "  Speedup:            " << speedup << "x\n";
    std::cout << "  Latency reduction:  " << latency_reduction << "%\n";

    return 0;
}
