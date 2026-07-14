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
DEFINE_string(op_type, "read", "Operation type to benchmark: read|write|mix");
DEFINE_bool(check_consistency, false,
            "Enable data consistency check after transfer.");
DEFINE_uint64(total_buffer_size, 1UL << 30,
              "Total buffer size for testing (in bytes).");
DEFINE_uint64(start_block_size, 4096, "Start block size (in bytes).");
DEFINE_uint64(max_block_size, 1UL << 26, "Maximum block size (in bytes).");
DEFINE_uint64(start_batch_size, 1, "Start batch size (number of requests).");
DEFINE_uint64(max_batch_size, 1, "Maximum batch size (number of requests).");
DEFINE_int32(duration, 5, "Number of duration per test case.");
DEFINE_int32(start_num_threads, 1,
             "Start number of concurrent worker threads.");
DEFINE_int32(max_num_threads, 1,
             "Maximum number of concurrent worker threads.");
DEFINE_uint64(deadline_us, 0,
              "tent only: relative per-transfer deadline in microseconds for "
              "tight worker threads (0 disables deadline tagging).");
DEFINE_int32(deadline_tight_threads, 0,
             "tent only: workers [0, N) that carry --deadline_us; remaining "
             "workers have no deadline.");
DEFINE_bool(deadline_bw_arbitration, false,
            "tent only: enable deadline-aware RDMA bandwidth arbitration.");
DEFINE_int32(local_gpu_id, 0, "Local GPU ID to be used, -1 for all GPUs");
DEFINE_int32(target_gpu_id, 0, "Target GPU ID to be used, -1 for all GPUs");
DEFINE_string(metadata_type, "p2p",
              "Type of metadata service: p2p|etcd|redis|http");
DEFINE_string(metadata_url_list, "",
              "List of metadata service URLs, comma-separated.");
DEFINE_int32(
    rpc_server_port, 0,
    "RPC server port used for p2p metadata service (0 = auto-select).");
DEFINE_string(xport_type, "",
              "Transport type: rdma|shm|mnnvl|gds|iouring|sunrise_link");
DEFINE_string(backend, "tent", "Transport backend: classic|tent");
DEFINE_bool(notifi, false,
            "Enable RDMA notification for performance measurement.");
DEFINE_string(
    tent_transport_hint, "unspec",
    "tent only: per-request transport_hint. "
    "unspec|rdma|tcp|shm|nvlink|gds|io_uring|mnnvl|ascend|sunrise_link");
DEFINE_string(tent_intent_type, "unspec",
              "tent only: intent_type attached to every benchmark request. "
              "unspec|foreground_get|background_prefetch|migration|checkpoint|"
              "weight_loading|staging_internal");

namespace mooncake {
namespace tent {
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
int XferBenchConfig::max_num_threads = 0;
int XferBenchConfig::start_num_threads = 0;
uint64_t XferBenchConfig::deadline_us = 0;
int XferBenchConfig::deadline_tight_threads = 0;
bool XferBenchConfig::deadline_bw_arbitration = false;

std::string XferBenchConfig::metadata_type;
std::string XferBenchConfig::metadata_url_list;
int XferBenchConfig::rpc_server_port = 0;
std::string XferBenchConfig::xport_type;
std::string XferBenchConfig::backend;
bool XferBenchConfig::notifi = false;
std::string XferBenchConfig::tent_transport_hint;
std::string XferBenchConfig::tent_intent_type;

int XferBenchConfig::local_gpu_id = 0;
int XferBenchConfig::target_gpu_id = 0;

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
    start_num_threads = FLAGS_start_num_threads;
    max_num_threads = FLAGS_max_num_threads;
    deadline_us = FLAGS_deadline_us;
    deadline_tight_threads = FLAGS_deadline_tight_threads;
    deadline_bw_arbitration = FLAGS_deadline_bw_arbitration;
    duration = FLAGS_duration;

    metadata_type = FLAGS_metadata_type;
    metadata_url_list = FLAGS_metadata_url_list;
    rpc_server_port = FLAGS_rpc_server_port;

    xport_type = FLAGS_xport_type;
    backend = FLAGS_backend;
    notifi = FLAGS_notifi;
    tent_transport_hint = FLAGS_tent_transport_hint;
    tent_intent_type = FLAGS_tent_intent_type;

    local_gpu_id = FLAGS_local_gpu_id;
    target_gpu_id = FLAGS_target_gpu_id;
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
              << std::setw(14) << "BlkSize (B)"
              << std::setw(8) << "Batch"
              << std::setw(14) << "BW (GB/S)"
              << std::setw(14) << "Avg Lat (us)"
              << std::setw(14) << "Avg Tx (us)"
              << std::setw(14) << "P99 Tx (us)"
              << std::setw(14) << "P999 Tx (us)"
              << std::endl;
    std::cout << std::string(160, '-') << std::endl;
    // clang-format on
}

void printStats(size_t block_size, size_t batch_size, XferBenchStats& stats,
                int num_threads) {
    size_t total_data_transferred = 0;
    double avg_latency = 0, throughput_gb = 0;
    auto num_ops = stats.transfer_duration.count();
    double total_duration = stats.total_duration.avg();
    total_data_transferred = ((block_size * batch_size) * num_ops);
    avg_latency = (total_duration * num_threads / num_ops);
    throughput_gb = (((double)total_data_transferred / (1000 * 1000 * 1000)) /
                     (total_duration / 1e6));  // In GB/Sec

    // Tabulate print with fixed width for each string
    // clang-format off
    std::cout << std::left << std::fixed << std::setprecision(6)
              << std::setw(14) << block_size
              << std::setw(8)  << batch_size
              << std::setw(14) << throughput_gb
              << std::setprecision(1)
              << std::setw(14) << avg_latency
              << std::setw(14) << stats.transfer_duration.avg()
              << std::setw(14) << stats.transfer_duration.p99()
              << std::setw(14) << stats.transfer_duration.p999()
              << std::endl;
    // clang-format on
}

void printDeadlineGroupStats(const char* group, size_t block_size,
                             size_t batch_size, XferBenchStats& stats,
                             int num_threads, uint64_t deadline_us) {
    if (num_threads <= 0 || stats.transfer_duration.count() == 0) return;
    const double duration_s = stats.total_duration.avg() / 1e6;
    const double bytes = static_cast<double>(block_size) * batch_size *
                         stats.transfer_duration.count();
    const double throughput_gbs = bytes / 1e9 / duration_s;
    std::cout << "  [deadline-" << group << "] threads=" << num_threads;
    if (deadline_us != 0) std::cout << " deadline_us=" << deadline_us;
    std::cout << " operations=" << stats.transfer_duration.count()
              << " throughput=" << std::fixed << std::setprecision(6)
              << throughput_gbs << " GB/s" << std::endl;
}

}  // namespace tent
}  // namespace mooncake
