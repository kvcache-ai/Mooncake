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

#ifndef TENT_PARAMS_H
#define TENT_PARAMS_H

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {

struct DeviceParams {
    int num_cq_list = 6;  // Derived from RdmaParams::num_lanes.
    int num_comp_channels = 1;
    uint8_t port = 1;
    int gid_index = -1;  // -1 means auto-select, >= 0 means explicit index
    int auto_gid_max_retries = 2;
    int max_cqe = 4096;
};

// One named QP pool. When SelectionPolicy entries declare a `qp_pool`, each
// distinct pool gets its own contiguous run of data QPs inside every endpoint,
// handshaked with that pool's SL/TC. Transfers routed to a pool only use its
// QPs, giving link-layer isolation between traffic classes (RFC #2568 step 2).
//
// Layering note: the wire handshake is unchanged. Both peers derive the same
// pool layout from the same SelectionPolicy config, so the flat qp_num list is
// still paired positionally — pool P's i-th QP on one side lines up with pool
// P's i-th QP on the other. This keeps BootstrapDesc byte-compatible with peers
// that don't know about pools (they simply run a single default pool).
struct QpPoolSegment {
    std::string name;
    int num_qp = 0;          // Number of data QPs dedicated to this pool.
    int begin = 0;           // Index into qp_list_ where this pool's QPs start.
    int service_level = -1;  // -1 = fall back to EndPointParams::service_level.
    int traffic_class = -1;  // -1 = fall back to EndPointParams::traffic_class.
};

struct EndPointParams {
    int endpoint_store_cap = 65536;
    int conn_pause_ttl_ms = 1000;
    int qp_mul_factor = 6;  // Derived from RdmaParams::num_lanes.
    int max_sge = 4;
    int max_qp_wr = 256;
    int max_inline_bytes = 64;
    ibv_mtu path_mtu = IBV_MTU_4096;

    // Named QP pools. Empty (default) = today's behavior: a single homogeneous
    // run of qp_mul_factor data QPs, all handshaked with the global SL/TC. When
    // non-empty, the segments partition the data QPs by pool; total QP count is
    // the sum of per-pool num_qp. Both peers must derive the same layout.
    std::vector<QpPoolSegment> qp_pools;

    // Advanced parameters, do not change unless you understand them
    // INIT State
    uint16_t pkey_index = 0;
    // RTR State
    uint8_t hop_limit = 16;
    uint32_t flow_label = 0;
    uint8_t traffic_class = 0;
    uint8_t service_level = 0;
    uint8_t src_path_bits = 0;
    uint8_t static_rate = 0;
    uint32_t rq_psn = 0;
    uint8_t max_dest_rd_atomic = 16;
    uint8_t min_rnr_timer = 12;
    // Optional mlx5 direct-verbs tuning. Empty/false keeps default driver
    // behavior. Requires USE_MLX5DV at build time; otherwise ignored with a
    // one-time warning.
    std::vector<uint16_t> mlx5_qp_udp_sports;
    bool mlx5_qp_lag_port_balance = false;
    // RTS State
    uint32_t sq_psn = 0;
    uint8_t send_timeout = 14;
    uint8_t send_retry_count = 7;
    uint8_t send_rnr_count = 7;
    uint8_t max_rd_atomic = 16;
};

// Result of resolving the QP-pool layout: the concrete per-pool segments (each
// with its [begin, begin+num_qp) span filled in) and the total data-QP count.
struct QpPoolLayout {
    std::vector<QpPoolSegment> segments;
    int total_qp = 0;
    bool valid = false;  // false = invalid config (non-positive total).
};

// Pure resolver used by RdmaEndPoint::construct(). Kept free-standing (no RDMA
// handles) so the layout math can be unit-tested. Empty `pools` reproduces the
// historical single homogeneous run of `qp_mul_factor` data QPs (segments left
// empty, meaning "one default pool"); a non-empty config lays out one
// contiguous segment per pool and the total is the sum of per-pool num_qp.
inline QpPoolLayout computeQpPoolSegments(
    const std::vector<QpPoolSegment>& pools, int qp_mul_factor) {
    QpPoolLayout layout;
    if (pools.empty()) {
        layout.total_qp = qp_mul_factor;
    } else {
        for (const auto& pool : pools) {
            // Every pool must claim at least one QP; a non-positive num_qp
            // would produce an empty/negative [begin, begin+num_qp) span and
            // break the router. Reject the whole layout so the caller falls
            // back to the default single-pool behavior.
            if (pool.num_qp <= 0) {
                layout.segments.clear();
                layout.total_qp = 0;
                layout.valid = false;
                return layout;
            }
            QpPoolSegment seg = pool;
            seg.begin = layout.total_qp;
            layout.segments.push_back(seg);
            layout.total_qp += pool.num_qp;
        }
    }
    layout.valid = layout.total_qp > 0;
    return layout;
}

// Pure QP router used by RdmaEndPoint::submitSlices (RFC #2568 step 3). Given
// the resolved segments, the pool a transfer asked for, and a caller-provided
// candidate index (the worker lane), return the QP index the transfer should
// use. When the pool is named and found, the candidate is folded into that
// pool's [begin, begin+num_qp) span so the transfer only ever touches its
// pool's QPs. When no pool is named, or the name is unknown, or no pools are
// configured, the candidate passes through unchanged (default spray behavior).
// total_qp must be > 0; the result is always in [0, total_qp).
inline int selectQpInPool(const std::vector<QpPoolSegment>& segments,
                          const std::string& pool_name, int candidate,
                          int total_qp) {
    if (candidate < 0) candidate = 0;
    if (!pool_name.empty()) {
        for (const auto& seg : segments) {
            if (seg.name == pool_name && seg.num_qp > 0)
                return seg.begin + (candidate % seg.num_qp);
        }
    }
    return candidate % total_qp;
}

struct WorkerParams {
    int num_workers = 6;  // Derived from RdmaParams::num_lanes.
    int max_retry_count = 9;
    int block_size = 65536;
    uint64_t grace_period_ns = 5000000;  // 5ms
    std::string rail_topo_path;
    bool show_latency_info = false;
    bool track_posted_slices = false;
};

struct RdmaParams {
    int num_lanes = 6;  // Unified worker/QP/CQ parallelism.
    DeviceParams device;
    EndPointParams endpoint;
    WorkerParams workers;
    bool verbose = false;
    bool log_slice_affinity = false;
};
}  // namespace tent
}  // namespace mooncake
#endif  // TENT_PARAMS_H
