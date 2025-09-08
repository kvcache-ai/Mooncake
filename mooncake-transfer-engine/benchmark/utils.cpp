// Copyright 2025 KVCache.AI
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

#include "utils.h"

#include <gflags/gflags.h>
#include <iostream>

DEFINE_string(seg_name, "", "Memory segment name for the local side");
DEFINE_string(seg_type, "DRAM",
              "Memory segment type for the target side: DRAM|VRAM");
DEFINE_string(target_seg_name, "", "Memory segment name for the target side");
DEFINE_string(op_type, "read", "Operation type to benchmark: read|write|mixed");
DEFINE_bool(check_consistency, false,
            "Enable data consistency check after transfer.");
DEFINE_uint64(total_buffer_size, 1UL << 30,
              "Total buffer size for testing (in bytes).");
DEFINE_uint64(start_block_size, 4096, "Start block size (in bytes).");
DEFINE_uint64(max_block_size, 1UL << 26, "Maximum block size (in bytes).");
DEFINE_uint64(start_batch_size, 1, "Start batch size (number of requests).");
DEFINE_uint64(max_batch_size, 1, "Maximum batch size (number of requests).");
DEFINE_int32(duration, 5, "Number of duration per test case.");
DEFINE_int32(num_threads, 2, "Number of concurrent worker threads.");
DEFINE_string(metadata_type, "p2p",
              "Type of metadata service: p2p|etcd|redis|http");
DEFINE_string(metadata_url_list, "",
              "List of metadata service URLs, comma-separated.");
DEFINE_string(xport_type, "", "Transport type: rdma|shm|mnnvl|gds|iouring");

namespace mooncake {
namespace v1 {
std::string XferBenchConfig::seg_name;
std::string XferBenchConfig::seg_type;
std::string XferBenchConfig::target_seg_name;
std::string XferBenchConfig::op_type;
bool XferBenchConfig::check_consistency = false;

size_t XferBenchConfig::total_buffer_size = 0;
size_t XferBenchConfig::start_block_size = 0;
size_t XferBenchConfig::max_block_size = 0;
size_t XferBenchConfig::start_batch_size = 0;
size_t XferBenchConfig::max_batch_size = 0;
int XferBenchConfig::duration = 0;
int XferBenchConfig::num_threads = 0;

std::string XferBenchConfig::metadata_type;
std::string XferBenchConfig::metadata_url_list;
std::string XferBenchConfig::xport_type;

void XferBenchConfig::loadFromFlags() {
    seg_type = FLAGS_seg_type;
    seg_name = FLAGS_seg_name;
    target_seg_name = FLAGS_target_seg_name;
    op_type = FLAGS_op_type;
    check_consistency = FLAGS_check_consistency;

    total_buffer_size = FLAGS_total_buffer_size;
    start_block_size = FLAGS_start_block_size;
    max_block_size = FLAGS_max_block_size;
    start_batch_size = FLAGS_start_batch_size;
    max_batch_size = FLAGS_max_batch_size;
    duration = FLAGS_duration;
    num_threads = FLAGS_num_threads;

    metadata_type = FLAGS_metadata_type;
    metadata_url_list = FLAGS_metadata_url_list;

    xport_type = FLAGS_xport_type;
}

double XferMetricStats::percentile(double p) {
    if (samples.empty()) return 0.0;
    if (p <= 0) return min();
    if (p >= 100) return max();
    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    double rank = (p / 100.0) * (sorted.size() - 1);
    size_t idx = static_cast<size_t>(rank);
    double frac = rank - idx;
    if (idx + 1 < sorted.size()) {
        return sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac;
    } else {
        return sorted[idx];
    }
}

void printStatsHeader() {
    // clang-format off
    std::cout << std::left
                << std::setw(20) << "Block Size (B)"
                << std::setw(15) << "Batch Size"
                << std::setw(15) << "B/W (GB/Sec)"
                << std::setw(15) << "Avg Lat. (us)"
                << std::setw(15) << "Avg Tx (us)"
                << std::setw(15) << "P99 Tx (us)"
                << std::setw(15) << "P999 Tx (us)"
                << std::endl;
    std::cout << std::string(160, '-') << std::endl;
    // clang-format on
}

void printStats(size_t block_size, size_t batch_size, XferBenchStats &stats) {
    size_t total_data_transferred = 0;
    double avg_latency = 0, throughput_gb = 0;
    auto num_ops = stats.transfer_duration.count();
    double total_duration = stats.total_duration.avg();
    total_data_transferred = ((block_size * batch_size) * num_ops);
    avg_latency = (total_duration * XferBenchConfig::num_threads / num_ops);
    throughput_gb = (((double)total_data_transferred / (1000 * 1000 * 1000)) /
                     (total_duration / 1e6));  // In GB/Sec

    // Tabulate print with fixed width for each string
    // clang-format off
    std::cout << std::left << std::fixed << std::setprecision(6)
              << std::setw(20) << block_size
              << std::setw(15) << batch_size
              << std::setw(15) << throughput_gb
              << std::setprecision(1)
              << std::setw(15) << avg_latency
              << std::setw(15) << stats.transfer_duration.avg()
              << std::setw(15) << stats.transfer_duration.p99()
              << std::setw(15) << stats.transfer_duration.p999()
              << std::endl;
    // clang-format on
}

}  // namespace v1
}  // namespace mooncake