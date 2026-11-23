// Copyright 2026 Alibaba Cloud and its affiliates
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

// In-process micro-benchmark for BatchExistKey: measures the serial path
// (parallel=false) against the chunked parallel path (parallel=true) on a
// single MasterService, with no network or client in between. Sweeps batch
// size; the parallel worker count is fixed per process and controlled by
// MOONCAKE_BATCH_EXIST_WORKERS (defaults to std::thread::hardware_concurrency).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_service.h"
#include "types.h"

DEFINE_uint64(num_objects, 200000, "Number of completed objects to prefill");
DEFINE_string(key_sizes, "1000,10000,100000,200000",
              "Comma-separated batch sizes (num keys) to sweep");
DEFINE_double(hit_ratio, 0.5, "Fraction of queried keys that exist (0.0-1.0)");
DEFINE_uint64(repetitions, 10, "Timed repetitions per batch size");
DEFINE_uint64(warmup, 3, "Untimed warmup iterations per batch size");

namespace mooncake::benchmarks {
namespace {

constexpr uint64_t kObjectSize = 1024;
constexpr const char* kSegmentName = "batch_exist_bench_segment";
constexpr uintptr_t kSegmentBase = 0x200000000ULL;

MasterServiceConfig MakeConfig() {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(3600)
        .build();
}

size_t SegmentSizeFor(size_t num_objects) {
    constexpr size_t kMinSegmentSize = 16 * 1024 * 1024;
    const size_t needed = num_objects * kObjectSize;
    const size_t headroom = needed / 8 + 1024 * kObjectSize;
    return std::max(kMinSegmentSize, needed + headroom);
}

Segment MakeSegment(size_t num_objects) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = kSegmentName;
    segment.base = kSegmentBase;
    segment.size = SegmentSizeFor(num_objects);
    segment.te_endpoint = segment.name;
    return segment;
}

std::string ExistingKey(size_t i) {
    return "batch_exist_bench_key_" + std::to_string(i);
}

std::string MissingKey(size_t i) {
    return "batch_exist_bench_missing_" + std::to_string(i);
}

bool Populate(MasterService& service, const UUID& client_id, size_t n) {
    if (!service.MountSegment(MakeSegment(n), client_id).has_value()) {
        LOG(ERROR) << "MountSegment failed";
        return false;
    }
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = kSegmentName;
    for (size_t i = 0; i < n; ++i) {
        const std::string key = ExistingKey(i);
        auto put_start = service.PutStart(client_id, key, TenantId::Default(),
                                          kObjectSize, config);
        if (!put_start.has_value()) {
            LOG(ERROR) << "PutStart failed for i=" << i
                       << ", error=" << toString(put_start.error());
            return false;
        }
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        if (!put_end.has_value()) {
            LOG(ERROR) << "PutEnd failed for i=" << i
                       << ", error=" << toString(put_end.error());
            return false;
        }
    }
    return true;
}

std::vector<std::string> BuildKeys(size_t n, size_t num_objects,
                                   double hit_ratio) {
    std::vector<std::string> keys;
    keys.reserve(n);
    const size_t hit_mod = static_cast<size_t>(hit_ratio * 100.0);
    for (size_t i = 0; i < n; ++i) {
        if ((i % 100) < hit_mod) {
            keys.push_back(ExistingKey(i % num_objects));
        } else {
            keys.push_back(MissingKey(i));
        }
    }
    return keys;
}

uint64_t Percentile(std::vector<uint64_t> values, double p) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const size_t rank =
        std::max<size_t>(1, static_cast<size_t>(std::ceil(p * values.size())));
    return values[std::min(rank - 1, values.size() - 1)];
}

uint64_t Mean(const std::vector<uint64_t>& values) {
    if (values.empty()) {
        return 0;
    }
    return std::accumulate(values.begin(), values.end(), 0ULL) / values.size();
}

std::vector<size_t> ParseSizes(const std::string& csv) {
    std::vector<size_t> out;
    size_t start = 0;
    while (start <= csv.size()) {
        const size_t comma = csv.find(',', start);
        const std::string tok =
            csv.substr(start, comma == std::string::npos ? std::string::npos
                                                         : comma - start);
        if (!tok.empty()) {
            out.push_back(static_cast<size_t>(std::stoull(tok)));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

}  // namespace
}  // namespace mooncake::benchmarks

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    using namespace mooncake;
    using namespace mooncake::benchmarks;

    MasterService service(MakeConfig());
    const UUID client_id = generate_uuid();
    if (!Populate(service, client_id, static_cast<size_t>(FLAGS_num_objects))) {
        LOG(ERROR) << "Populate failed";
        return 1;
    }

    const char* workers_env = std::getenv("MOONCAKE_BATCH_EXIST_WORKERS");
    std::cout << "# prefilled=" << FLAGS_num_objects
              << " workers=" << (workers_env ? workers_env : "(hardware)")
              << "\n";

    const std::vector<size_t> sizes = ParseSizes(FLAGS_key_sizes);
    const TenantId tenant = TenantId::Default();

    std::cout << "num_keys,hit_ratio,serial_p50_us,serial_mean_us,"
                 "parallel_p50_us,parallel_mean_us,speedup,"
                 "serial_keys_per_s,parallel_keys_per_s\n";

    for (size_t n : sizes) {
        if (n > FLAGS_num_objects) {
            LOG(WARNING) << "num_keys " << n << " > num_objects "
                         << FLAGS_num_objects << ", skipping";
            continue;
        }
        const auto keys = BuildKeys(n, static_cast<size_t>(FLAGS_num_objects),
                                    FLAGS_hit_ratio);

        for (uint64_t w = 0; w < FLAGS_warmup; ++w) {
            (void)service.BatchExistKey(keys, tenant, /*parallel=*/false);
            (void)service.BatchExistKey(keys, tenant, /*parallel=*/true);
        }

        std::vector<uint64_t> serial_us;
        std::vector<uint64_t> parallel_us;
        serial_us.reserve(FLAGS_repetitions);
        parallel_us.reserve(FLAGS_repetitions);
        for (uint64_t r = 0; r < FLAGS_repetitions; ++r) {
            const auto t0 = std::chrono::steady_clock::now();
            (void)service.BatchExistKey(keys, tenant, /*parallel=*/false);
            const auto t1 = std::chrono::steady_clock::now();
            (void)service.BatchExistKey(keys, tenant, /*parallel=*/true);
            const auto t2 = std::chrono::steady_clock::now();
            serial_us.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                    .count());
            parallel_us.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
                    .count());
        }

        const uint64_t s_p50 = Percentile(serial_us, 0.5);
        const uint64_t p_p50 = Percentile(parallel_us, 0.5);
        const uint64_t s_mean = Mean(serial_us);
        const uint64_t p_mean = Mean(parallel_us);
        const double speedup =
            p_p50 > 0 ? static_cast<double>(s_p50) / p_p50 : 0.0;
        const double s_kps = s_p50 > 0 ? n * 1e6 / s_p50 : 0.0;
        const double p_kps = p_p50 > 0 ? n * 1e6 / p_p50 : 0.0;

        std::cout << n << "," << FLAGS_hit_ratio << "," << s_p50 << ","
                  << s_mean << "," << p_p50 << "," << p_mean << "," << speedup
                  << "," << s_kps << "," << p_kps << "\n";
    }
    return 0;
}
