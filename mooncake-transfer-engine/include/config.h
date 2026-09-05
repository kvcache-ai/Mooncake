// Copyright 2024 KVCache.AI
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

#ifndef CONFIG_H
#define CONFIG_H

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mooncake {

enum class EndpointStoreType {
    FIFO = 0,
    SIEVE = 1,
};

// Which NICs the EFA transport registers a buffer on.
enum class EfaNicSelection {
    ALL = 0,    // every NIC, the historical behavior
    LOCAL = 1,  // device memory only on that GPU's topology-local NICs
};

struct GlobalConfig {
    size_t num_cq_per_ctx = 1;
    size_t num_comp_channels_per_ctx = 1;
    uint8_t port = 1;
    int gid_index = -1;       // -1 for auto-selection, >=0 for user-specified
    uint16_t pkey_index = 0;  // QP attr.pkey_index; override via MC_PKEY_INDEX
    uint64_t max_mr_size = 0x10000000000;
    size_t max_cqe = 4096;
    int max_ep_per_ctx = 65536;
    size_t num_qp_per_ep = 2;
    size_t max_sge = 4;
    size_t max_wr = 256;
    // Set when MC_MAX_WR was given explicitly.  EFA's transmit depth is a
    // per-device attribute (2048 on p6-b300, 4096 on p5), so with no override
    // the EFA transport adopts the provider's depth rather than max_wr; with
    // one it honors the operator's value, clamped to the hardware.  The RDMA
    // transport passes max_wr to ibv_create_qp() and is unaffected.
    bool max_wr_from_env = false;
    size_t max_inline = 64;
    ibv_mtu mtu_length = IBV_MTU_4096;
    uint16_t handshake_port = 12001;
    int workers_per_ctx = 2;
    size_t slice_size = 65536;
    int retry_cnt = 9;
    int auto_gid_max_retries = 2;
    int handshake_listen_backlog = 128;
    // Connect timeout (seconds) for outbound handshake-port RPCs (QP
    // handshake, probe, notify, metadata exchange). A plain blocking
    // connect() has no deadline: to an unroutable address (e.g. a
    // torn-down pod IP) it stalls for the kernel's full SYN-retry cycle,
    // which is minutes. Override via MC_HANDSHAKE_CONNECT_TIMEOUT.
    int handshake_connect_timeout = 5;
    // Cooldown before retrying a failed RDMA peer rail. Override via
    // MC_RDMA_RAIL_PAUSE_SECONDS.
    uint64_t rdma_rail_pause_seconds = 30;
    bool metacache = true;
    // Periodically refresh Transfer Engine metadata-derived local caches. 0
    // disables the background poller and preserves the manual
    // syncSegmentCache() behavior. Currently refreshes cached remote segment
    // descriptors. Override via MC_TE_METADATA_REFRESH_INTERVAL_SECONDS.
    uint64_t te_metadata_refresh_interval_seconds = 0;
    int log_level = google::INFO;
    bool trace = false;
    int64_t slice_timeout = -1;
    // Active-connect circuit-breaker. After an endpoint to a peer is torn down
    // (path failure / QP fatal), pause active reconnection to that peer's
    // address for this many milliseconds, so the posting worker is not
    // blocked re-handshaking a likely-gone peer (a k8s rolling restart brings
    // the pod back at a different podIP:port, so the old address is dead). The
    // not-yet-posted slices fail/redispatch instead of hanging. 0 disables.
    // Override via MC_CONN_PAUSE_TTL_MS.
    int conn_pause_ttl_ms = 0;
    // Context-level circuit-breaker pause. When the WorkerPool breaker trips
    // (kContextFailureThreshold consecutive all-rails-failed submit batches
    // spanning >= context_failure_min_peers distinct peers), the local RNIC
    // context is deactivated for this many milliseconds and then automatically
    // reactivated (half-open: the failure streak resets, so a genuinely dead
    // NIC re-trips after another threshold's worth of failed batches while a
    // false positive self-heals). 0 = never auto-reactivate, i.e. the legacy
    // latch that only IBV_EVENT_PORT_ACTIVE clears. Override via
    // MC_CONTEXT_PAUSE_TTL_MS.
    int context_pause_ttl_ms = 5000;
    // Minimum number of DISTINCT peer servers a consecutive all-rails-failed
    // submit streak must span before the context breaker may trip. A submit
    // batch targets a single peer, so a streak against one dead/restarting
    // peer is a peer problem, not evidence of local RNIC failure, and must
    // never deactivate the context. Direct local completion faults remain
    // strong local evidence and are not subject to this minimum. 1 = legacy
    // single-peer submit tripping. Range [1, 64]. Override via
    // MC_CONTEXT_FAILURE_MIN_PEERS.
    int context_failure_min_peers = 2;
    uint16_t rpc_min_port = 15000;
    uint16_t rpc_max_port = 17000;
    bool use_ipv6 = false;
    size_t fragment_limit = 16384;
    bool enable_dest_device_affinity = false;
    bool enable_hca_peer_affinity = false;
    std::unordered_map<std::string, std::vector<std::string>> nic_peer_affinity;
    bool log_rdma_slice_affinity = false;
    bool track_rdma_posted_slices = false;
    int parallel_reg_mr = -1;
    // Cap on concurrent buffer registrations in registerLocalMemoryBatch().
    // 0 (default) = unbounded, one thread per buffer. Set via
    // MC_MAX_CONCURRENT_REG_MR; the best value is platform-specific, see the
    // measured tables in efa_transport.cpp before choosing one.
    size_t max_concurrent_reg_mr = 0;
    // Which NICs a buffer is registered on in the EFA transport. ALL (default)
    // registers every buffer on every NIC; LOCAL restricts device memory to the
    // NICs the topology reports as closest to that GPU. Set via
    // MC_EFA_NIC_SELECTION=all|local; see efa_transport.cpp for the trade-off.
    EfaNicSelection efa_nic_selection = EfaNicSelection::ALL;
    size_t eic_max_block_size = 64UL * 1024 * 1024;
    EndpointStoreType endpoint_store_type = EndpointStoreType::SIEVE;
    int ib_traffic_class = -1;
    // InfiniBand Service Level (SL), 0-15. -1 = use default (0).
    // Maps to a Virtual Lane on the switch for QoS isolation, e.g. to
    // steer KV-cache traffic into a different VL than EP all-to-all.
    int ib_service_level = -1;
    // mlx5 QP UDP source ports for ECMP path diversification.
    // Empty = no modification. QP at index i uses
    // mlx5_qp_udp_sports[i % size]. Requires mlx5 device + RoCEv2,
    // and the binary must be built with USE_MLX5DV.
    std::vector<uint16_t> mlx5_qp_udp_sports;
    // mlx5 QP LAG port balancing. When enabled, QPs are distributed across
    // physical LAG ports: QP at index i is pinned to port (i % num_lag_ports)
    // + 1. num_lag_ports is queried from hardware; if the device is not in LAG
    // mode the setting is a no-op. Requires USE_MLX5DV.
    bool mlx5_qp_lag_port_balance = false;
    // Install RdmaTwoSidedTransport (CtrlChannel notify) instead of classic
    // one-sided RdmaTransport. MC_USE_RDMA_TWOSIDED.
    bool use_rdma_twosided = false;
    // RDMA CtrlChannel notify path. MC_RDMA_NOTIFY_ENABLED.
    bool rdma_notify_enabled = true;
    // Ctrl recv/send slot count and slot size. MC_RDMA_NOTIFY_RECV_COUNT /
    // MC_RDMA_NOTIFY_BUFFER_SIZE.
    size_t rdma_notify_recv_count = 64;
    size_t rdma_notify_buffer_size = 4096;
    // Local pending SEND cap; actual cap is min(this, peer notify_rq_depth).
    // MC_RDMA_NOTIFY_MAX_PENDING_SENDS.
    size_t rdma_notify_max_pending_sends = 64;
    // Fall back to OOB RPC notify when CtrlChannel is unavailable.
    // MC_RDMA_NOTIFY_OOB_FALLBACK.
    bool rdma_notify_oob_fallback = true;
    // Upper bound for waiting on an in-flight CtrlChannel connect to the same
    // peer. On expiry the notify falls back to OOB instead of blocking on a
    // handshake that may never complete.
    // MC_RDMA_NOTIFY_CONNECT_TIMEOUT_MS.
    uint32_t rdma_notify_connect_timeout_ms = 10000;
    // ib_pci_relaxed_ordering_mode: 0: off, 1: on if supported, 2: auto
    int ib_pci_relaxed_ordering_mode = 1;
    bool ascend_use_fabric_mem = false;
    bool ascend_agent_mode = false;
    bool sunrise_use_device_mem = false;
    // Transient flag scoped to a single TE init: set true by the Store entry
    // (Client::InitTransferEngine) before installing the ascend transport, and
    // reset to false right after. Lets ascend_direct distinguish a Store-init
    // TE from a normal/P2P TE so each can resolve its own
    // ASCEND_GLOBAL_RESOURCE_CONFIG (e.g. Store=RoCE, P2P=HCCS). Assumes TE
    // inits are serialized within the process.
    bool ascend_store_te_init = false;
    // ub config parameters
    size_t num_jfc_per_ctx = 2;
    size_t num_jfce_per_ctx = 2;
    int eid_index = 0;
    uint64_t max_seg_size = 0x10000000000;
    size_t max_jfc_e = 4096;  // urma is temporarily using this default value.
    size_t num_jetty_per_ep = 1;
};

struct RpcCommunicatorConfig {
    std::string listen_address;
    size_t thread_count = 0;
    size_t timeout_seconds = 30;
    // Maximum number of cached RPC client connections per target endpoint.
    // RPC client I/O threads are configured by
    // MC_TE_RPC_CLIENT_IO_THREADS/MC_RPC_CLIENT_IO_THREADS.
    size_t pool_size = 100;
};

void loadGlobalConfig(GlobalConfig& config);

void dumpGlobalConfig();

void updateGlobalConfig(ibv_device_attr& device_attr);

GlobalConfig& globalConfig();

uint16_t getDefaultHandshakePort();

// Validates a port range. Returns {default_min, default_max} on invalid input.
// Rejects: min > max, well-known ports (0-1023), ephemeral ports (32768-60999).
std::pair<int, int> ValidatePortRange(int min_port, int max_port,
                                      int default_min, int default_max);

}  // namespace mooncake

#endif  // CONFIG_H
