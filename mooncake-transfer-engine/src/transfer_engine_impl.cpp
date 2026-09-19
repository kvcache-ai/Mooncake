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

#include "transfer_engine_impl.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <cmath>
#include <string>
#include <thread>
#include <sys/resource.h>
#include <unistd.h>
#ifdef WITH_METRICS
#include <iomanip>
#include <sstream>
#endif

#include "transfer_metadata_plugin.h"
#include "common.h"
#include "ib_link_speed.h"
#include "transport/transport.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_twosided/rdma_twosided_transport.h"
#include "transport/shm_transport/shm_transport.h"
#ifdef USE_BAREX
#include "transport/barex_transport/barex_transport.h"
#endif

namespace mooncake {

namespace {
constexpr uint8_t kTransferCommandVersion = 1;
constexpr uint8_t kScatterGatherCommand = 1;
constexpr uint8_t kScatterRelative32Encoding = 1;
constexpr uint8_t kScatterRelative32FixedLengthEncoding = 2;
constexpr uint32_t kMaxScatterSpans = 131072;
constexpr uint64_t kMaxScatterBytes = 512ULL << 20;
constexpr uint32_t kMaxScatterChunkBytes = 16U << 20;
constexpr size_t kScatterCommandHeaderBytes =
    3 * sizeof(uint8_t) + 2 * sizeof(uint16_t) + 2 * sizeof(uint32_t) +
    2 * sizeof(uint64_t);
constexpr size_t kScatterRelativeHeaderBytes =
    sizeof(uint8_t) + sizeof(uint32_t) + 2 * sizeof(uint64_t);
constexpr double kScatterRttSeconds = 10e-6;
constexpr double kScatterControlSeconds = 120e-6;
constexpr double kScatterPostRate = 3.0e6;
constexpr double kScatterCopyRate = 30.0e6;
constexpr double kScatterCopyBandwidth = 20.0e9;
constexpr double kScatterSafetyMargin = 1.10;
constexpr double kScatterChunkAmortizationSeconds = 250e-6;
constexpr size_t kScatterChunkAlignment = 64ULL << 10;

template <typename T>
void appendUnsigned(std::string& output, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        output.push_back(static_cast<char>(value >> (i * 8)));
}

template <typename T>
void writeUnsigned(char*& output, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        *output++ = static_cast<char>(value >> (i * 8));
}

template <typename T>
bool readUnsigned(std::string_view input, size_t& offset, T& value) {
    if (input.size() - std::min(input.size(), offset) < sizeof(T)) return false;
    value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(static_cast<unsigned char>(input[offset + i]))
                 << (i * 8);
    }
    offset += sizeof(T);
    return true;
}

bool hostAccessibleLocation(const std::string& location) {
    return location == kWildcardLocation || location == "cpu" ||
           location.rfind("cpu:", 0) == 0 ||
           location.rfind(kSegmentsLocationPrefix, 0) == 0;
}

bool overlapWithRegion(uintptr_t addr, uint64_t length, void* region_addr,
                       uint64_t region_length) {
    return overlap(reinterpret_cast<void*>(addr), length, region_addr,
                   region_length);
}

Transport* tryInstallShmTransport(MultiTransport* multi_transports,
                                  std::shared_ptr<Topology> topology) {
    if (!multi_transports) return nullptr;
    if (Transport* existing = multi_transports->getTransport("shm")) {
        return existing;
    }
    return multi_transports->installTransport("shm", topology);
}

int maybeInstallShmTransport(MultiTransport* multi_transports,
                             std::shared_ptr<Topology> topology) {
#ifdef ENABLE_MULTI_PROTOCOL
    if (!multi_transports || !envFlagEnabled("MC_FORCE_SHM")) return 0;
    Transport* shm = tryInstallShmTransport(multi_transports, topology);
    if (!shm) {
        LOG(WARNING) << "MC_FORCE_SHM is set but failed to install SHM "
                        "transport; continuing without it";
        return 0;
    }
    LOG(INFO) << "SHM transport installed for same-host DRAM copies";
    return 0;
#else
    (void)multi_transports;
    (void)topology;
    return 0;
#endif
}
}  // namespace

static bool setFilesLimit() {
    struct rlimit filesLimit;
    if (getrlimit(RLIMIT_NOFILE, &filesLimit) != 0) {
        LOG(ERROR) << "getrlimit failed: " << strerror(errno);
        return false;
    }
    rlim_t target_limit = filesLimit.rlim_max;
    // Skip if already sufficient
    if (filesLimit.rlim_cur >= target_limit) {
        return true;
    }
    filesLimit.rlim_cur = target_limit;
    if (setrlimit(RLIMIT_NOFILE, &filesLimit) != 0) {
        LOG(ERROR) << "setrlimit failed: " << strerror(errno);
        return false;
    }
    return true;
}

static std::string loadTopologyJsonFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    file.close();
    return content;
}

int TransferEngineImpl::init(const std::string& metadata_conn_string,
                             const std::string& local_server_name,
                             const std::string& ip_or_host_name,
                             uint64_t rpc_port) {
    TransferMetadata::RpcMetaDesc desc;
    std::string rpc_binding_method;

    if (!setFilesLimit()) {
        LOG(WARNING) << "Failed to set file descriptor limit. Continuing "
                        "initialization, but this may cause issues if too many "
                        "files are opened.";
    }
    // Set resources to the maximum value
#ifdef USE_BAREX
    const char* use_barex_env = std::getenv("USE_BAREX");
    if (use_barex_env) {
        int val = atoi(use_barex_env);
        if (val != 0) {
            use_barex_ = true;
        }
    }
#endif

#ifdef USE_ASCEND
    // The only difference in initializing the Ascend Transport is that the
    // `local_server_name` must include the physical NPU card ID. The format
    // changes from `ip:port` to `ip:port:npu_x`, e.g., `"0.0.0.0:12345:npu_2"`.
    // While the desc_name stored in the metadata remains in the format of
    // ip:port.
    int devicePhyId = -1;
    auto [host_name, port] =
        parseHostNameWithPortAscend(local_server_name, &devicePhyId);
    LOG(INFO) << "Transfer Engine parseHostNameWithPortAscend. server_name: "
              << host_name << " port: " << port
              << " devicePhyId: " << devicePhyId;
    local_server_name_ = host_name + ":" + std::to_string(port);
#else
    auto [host_name, port] = parseHostNameWithPort(local_server_name);
    LOG(INFO) << "Transfer Engine parseHostNameWithPort. server_name: "
              << host_name << " port: " << port;
    local_server_name_ = local_server_name;
#endif

    if (getenv("MC_LEGACY_RPC_PORT_BINDING") ||
        metadata_conn_string == P2PHANDSHAKE) {
        rpc_binding_method = "legacy/P2P";
        desc.ip_or_host_name = host_name;
        desc.rpc_port = port;
        desc.sockfd = -1;
#ifdef USE_BAREX
        if (use_barex_) {
            int tmp_fd = -1;
            desc.barex_port = findAvailableTcpPort(tmp_fd, true);
            if (desc.barex_port == 0) {
                LOG(ERROR)
                    << "Barex: No valid port found for local barex service.";
                return -1;
            }
            close(tmp_fd);
            tmp_fd = -1;
        }
#endif
        if (metadata_conn_string == P2PHANDSHAKE) {
            rpc_binding_method = "P2P handshake";
            desc.rpc_port = findAvailableTcpPort(desc.sockfd);
            if (desc.rpc_port == 0) {
                LOG(ERROR) << "P2P: No valid port found for local TCP service.";
                return -1;
            }
#if defined(USE_ASCEND)
            // The current version of Ascend Transport does not support IPv6,
            // but it will be added in a future release.
            local_server_name_ =
                desc.ip_or_host_name + ":" + std::to_string(desc.rpc_port);
#else
            local_server_name_ = maybeWrapIpV6(desc.ip_or_host_name) + ":" +
                                 std::to_string(desc.rpc_port);
#endif
        }
    } else {
        rpc_binding_method = "new RPC mapping";
        (void)(ip_or_host_name);
        auto* ip_address = getenv("MC_TCP_BIND_ADDRESS");
        if (ip_address)
            desc.ip_or_host_name = ip_address;
        else {
            auto ip_list = findLocalIpAddresses();
            if (ip_list.empty()) {
                LOG(ERROR) << "not valid LAN address found";
                return -1;
            } else {
                desc.ip_or_host_name = ip_list[0];
            }
        }

        // In the new rpc port mapping, it is randomly selected to prevent
        // port conflict
        (void)(rpc_port);
        desc.rpc_port = findAvailableTcpPort(desc.sockfd);
        if (desc.rpc_port == 0) {
            LOG(ERROR) << "not valid port for serving local TCP service";
            return -1;
        }
    }

    LOG(INFO) << "Transfer Engine RPC using " << rpc_binding_method
              << ", listening on " << desc.ip_or_host_name << ":"
              << desc.rpc_port
#ifdef USE_BAREX
              << (use_barex_
                      ? ", barex use port:" + std::to_string(desc.barex_port)
                      : "")
#endif
              << "";

    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
    metadata_->registerOnCommandCallBack([this](const std::string& peer,
                                                const std::string& request,
                                                std::string& response) {
        handleTransferCommand(peer, request, response);
    });
#ifdef USE_ASCEND
    std::string mutable_server_name =
        local_server_name_ + ":npu_" + std::to_string(devicePhyId);
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, mutable_server_name);
#else
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, local_server_name_);
#endif
    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

    // Universal TCP force mechanism: if MC_FORCE_TCP is set, skip all other
    // transport installation logic and use TCP transport only. This allows
    // running metadata-only instances without requiring specialized hardware
    // (e.g., NPU for Ascend Direct, RDMA HCAs, etc.).
    if (getenv("MC_FORCE_TCP")) {
#ifdef USE_TCP
        Transport* tcp_transport =
            multi_transports_->installTransport("tcp", nullptr);
        if (!tcp_transport) {
            LOG(ERROR)
                << "MC_FORCE_TCP is set but failed to install TCP transport";
            return -1;
        }
        LOG(INFO) << "MC_FORCE_TCP is set, using TCP transport only";
        return 0;
#else
        LOG(ERROR) << "MC_FORCE_TCP is set but USE_TCP is not compiled in";
        return -1;
#endif
    }

    // MC_FORCE_SHM:
    // - Without ENABLE_MULTI_PROTOCOL: SHM-only (skip RDMA/TCP), like
    //   MC_FORCE_TCP.
    // - With ENABLE_MULTI_PROTOCOL: fall through so auto-discover still
    //   installs RDMA/TCP, then maybeInstallShmTransport appends SHM.
#ifndef ENABLE_MULTI_PROTOCOL
    if (envFlagEnabled("MC_FORCE_SHM")) {
        Transport* shm_transport =
            tryInstallShmTransport(multi_transports_.get(), local_topology_);
        if (!shm_transport) {
            LOG(ERROR)
                << "MC_FORCE_SHM is set but failed to install SHM transport";
            return -1;
        }
        LOG(INFO) << "MC_FORCE_SHM is set, using SHM transport only";
        return 0;
    }
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    Transport* ascend_transport =
        multi_transports_->installTransport("ascend", local_topology_);
    if (!ascend_transport) {
        LOG(ERROR) << "Failed to install Ascend transport";
        return -1;
    }
#else

#ifdef USE_UBSHMEM
    Transport* ubshmem_transport =
        multi_transports_->installTransport("ubshmem", local_topology_);
    if (!ubshmem_transport) {
        LOG(ERROR) << "Failed to install UBShmem transport";
        return -1;
    }
    auto_discover_config_.enabled = false;
#endif

#if defined(USE_CXL) && !defined(USE_ASCEND) && \
    !defined(USE_ASCEND_HETEROGENEOUS)
    if (std::getenv("MC_CXL_DEV_PATH") != nullptr) {
        Transport* cxl_transport =
            multi_transports_->installTransport("cxl", local_topology_);
        if (!cxl_transport) {
            LOG(ERROR) << "Failed to install CXL transport";
            return -1;
        }
    }
#endif

    if (auto_discover_config_.enabled) {
        LOG(INFO) << "Auto-discovering topology...";
        if (getenv("MC_CUSTOM_TOPO_JSON")) {
            auto path = getenv("MC_CUSTOM_TOPO_JSON");
            LOG(INFO) << "Using custom topology from: " << path;
            auto topo_json = loadTopologyJsonFile(path);
            if (!topo_json.empty()) {
                local_topology_->parse(topo_json);
            } else {
                LOG(WARNING) << "Failed to load custom topology from " << path
                             << ", falling back to auto-detect.";
                local_topology_->discover(filter_);
            }
        } else {
            local_topology_->discover(filter_);
        }
        LOG(INFO) << "Topology discovery complete. Found "
                  << local_topology_->getHcaList().size() << " HCAs.";

#ifdef USE_UB
        Transport* ub_transport =
            multi_transports_->installTransport("ub", local_topology_);
        if (!ub_transport) {
            LOG(ERROR) << "Failed to install ub transport";
            return -1;
        }
#endif

#ifdef USE_ASCEND_HETEROGENEOUS
        Transport* ascend_transport =
            multi_transports_->installTransport("ascend", local_topology_);
        if (!ascend_transport) {
            LOG(ERROR) << "Failed to install Ascend transport";
            return -1;
        }
#elif defined(USE_MACA)

        if (getenv("MC_MACA_HOST_TRANSPORT")) {
            if ((local_topology_->getHcaList().size() > 0 &&
                 !getenv("MC_FORCE_TCP")) ||
                getenv("MC_FORCE_HCA")) {
                Transport* t = multi_transports_->installTransport(
                    "rdma", local_topology_);
                if (!t) {
                    LOG(ERROR) << "Failed to install RDMA transport for MACA";
                    return -1;
                }
                LOG(INFO) << "Using RDMA host transport for MACA";
            } else {
#ifdef USE_TCP
                Transport* t =
                    multi_transports_->installTransport("tcp", nullptr);
                if (!t) {
                    LOG(ERROR) << "Failed to install TCP transport for MACA";
                    return -1;
                }
                LOG(INFO) << "Using TCP host transport for MACA";
#else
                LOG(ERROR)
                    << "MC_MACA_HOST_TRANSPORT requires RDMA HCAs or USE_TCP";
                return -1;
#endif
            }
        } else {
            Transport* t = multi_transports_->installTransport("maca", nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install MACA transport";
                return -1;
            }
            LOG(INFO) << "Using MACA transport";
        }

#elif defined(USE_MNNVL) || defined(USE_INTRA_NVLINK) || defined(USE_MUSA)

        const char* force_mnnvl = getenv("MC_FORCE_MNNVL");
        const char* intra_env = getenv("MC_INTRANODE_NVLINK");
#ifdef USE_MUSA
        const char* gpu_p2p_protocol = "musa";
        const char* gpu_p2p_name = "MUSA";
        const bool force_gpu_p2p = force_mnnvl || getenv("MC_FORCE_MUSA");
#else
        const char* gpu_p2p_protocol = "nvlink";
        const char* gpu_p2p_name = "NVLink";
        const bool force_gpu_p2p = force_mnnvl;
#endif
        // The cross-node GPU P2P transport is only constructible when its own
        // build flag is set, so a build that enables USE_INTRA_NVLINK alone
        // must keep the RDMA/TCP fallback instead of requesting a protocol
        // MultiTransport cannot create.
#if defined(USE_MNNVL) || defined(USE_MUSA)
        constexpr bool kGpuP2PCompiled = true;
#else
        constexpr bool kGpuP2PCompiled = false;
#endif
        const bool no_hca = local_topology_->getHcaList().empty();
        // MC_FORCE_HCA keeps the same meaning as on the non-NVLink path:
        // install RDMA even when topology discovery found no HCA.
        const bool force_hca = getenv("MC_FORCE_HCA") != nullptr;
        if (force_gpu_p2p && !kGpuP2PCompiled) {
            LOG(WARNING) << gpu_p2p_name
                         << " transport was requested but is not compiled in, "
                            "falling back to RDMA/TCP";
        }
        // Explicit env var overrides take priority over HCA auto-detection
        if (intra_env) {
            Transport* t =
                multi_transports_->installTransport("nvlink_intra", nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install Intra-Node NVLink transport";
                return -1;
            }
            LOG(INFO) << "Using Intra-Node NVLink transport "
                         "(MC_INTRANODE_NVLINK set)";
        } else if (kGpuP2PCompiled &&
                   (force_gpu_p2p || (no_hca && !force_hca))) {
            // MC_FORCE_HCA suppresses automatic no-HCA fallback, but keeps
            // explicitly requested GPU P2P transport precedence unchanged.
            Transport* t =
                multi_transports_->installTransport(gpu_p2p_protocol, nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install " << gpu_p2p_name
                           << " transport";
                return -1;
            }
            LOG(INFO) << "Using " << gpu_p2p_name << " transport "
                      << "(forced or no HCA detected)";
        } else if (!no_hca || force_hca) {
            Transport* t =
                multi_transports_->installTransport("rdma", local_topology_);
            if (!t) {
                LOG(ERROR) << "Failed to install RDMA transport";
                return -1;
            }
            LOG(INFO) << "Using RDMA transport (RoCE/iWARP)";
        } else {
#ifdef USE_TCP
            Transport* t = multi_transports_->installTransport("tcp", nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install TCP transport";
                return -1;
            }
            LOG(INFO) << "Using TCP transport (no HCA detected)";
#else
            LOG(ERROR) << "No HCA detected and neither " << gpu_p2p_name
                       << " nor TCP transport is compiled in";
            return -1;
#endif
        }

#elif !defined(USE_SUNRISE)
        // Sunrise classic installs its transport explicitly from tebench after
        // benchmark-specific setup, so it skips the default auto transport
        // path.
        if ((local_topology_->getHcaList().size() > 0 &&
             !getenv("MC_FORCE_TCP")) ||
            getenv("MC_FORCE_HCA")) {
            const std::string transport_type = autoDiscoverTransport();
            Transport* transport = nullptr;
            if (transport_type == "barex") {
#ifdef USE_BAREX
                transport = multi_transports_->installTransport(
                    "barex", local_topology_);
#else
                LOG(ERROR) << "Set USE BAREX while barex not compiled";
                return -1;
#endif
            } else {
                transport = multi_transports_->installTransport(
                    transport_type, local_topology_);
            }
            if (transport == nullptr) {
                LOG(ERROR) << "Failed to install transport, type="
                           << transport_type;
                return -1;
            } else {
                LOG(INFO) << "installTransport, type=" << transport_type;
            }
        } else {
            Transport* tcp_transport =
                multi_transports_->installTransport("tcp", nullptr);
            if (!tcp_transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return -1;
            }
        }
#endif
        // TODO: install other transports automatically

#ifdef USE_HIP
        // HIP transport handles intra-node GPU P2P via XGMI/IPC and can
        // coexist with the cross-node transport (RDMA/TCP) selected above.
        {
            Transport* hip_transport =
                multi_transports_->installTransport("hip", nullptr);
            if (!hip_transport) {
                LOG(WARNING) << "Failed to install HIP transport "
                                "(intra-node GPU P2P unavailable)";
            } else {
                LOG(INFO) << "HIP transport installed for intra-node GPU P2P";
            }
        }
#endif
    }
#endif

    maybeInstallShmTransport(multi_transports_.get(), local_topology_);
    return 0;
}

int TransferEngineImpl::freeEngine() {
    if (metadata_) {
        metadata_->registerOnCommandCallBack({});
        setScatterStagingAllocator({});
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
    return 0;
}

void TransferEngineImpl::setScatterStagingAllocator(
    TransferEngine::ScatterStagingAllocator allocator) {
    std::unique_lock<std::mutex> lock(scatter_staging_mutex_);
    scatter_staging_allocator_ = std::move(allocator);
    if (!scatter_staging_allocator_) {
        scatter_staging_cv_.wait(
            lock, [this] { return active_scatter_commands_ == 0; });
    }
}

size_t TransferEngineImpl::scatterCommandSpanBudget(
    const std::string& peer_server_name) const {
    if (!metadata_) return 0;
    TransferMetadata::RpcMetaDesc peer;
    if (metadata_->getRpcMetaEntry(peer_server_name, peer) != 0 ||
        peer.command_capability.empty())
        return 0;

    size_t remaining = kMaxTransferCommandLength;
    const auto consume = [&remaining](size_t bytes) {
        if (bytes > remaining) return false;
        remaining -= bytes;
        return true;
    };
    const auto& source_ip = metadata_->localRpcMeta().ip_or_host_name;
    if (!consume(sizeof(uint8_t)) || !consume(peer.command_capability.size()) ||
        !consume(kScatterCommandHeaderBytes + kScatterRelativeHeaderBytes) ||
        !consume(local_server_name_.size()) || !consume(source_ip.size()))
        return 0;
    return remaining;
}

TransferEngineImpl::ScatterTransportProfile
TransferEngineImpl::scatterTransportProfile() const {
    double bytes_per_second = 12.5e9;
    size_t context_count = 1;
    if (multi_transports_) {
        auto* transport = multi_transports_->getTransport("rdma");
        if (auto* rdma = dynamic_cast<RdmaTransport*>(transport)) {
            double total_gbps = 0;
            context_count = std::max<size_t>(1, rdma->getContextList().size());
            for (const auto& context : rdma->getContextList()) {
                total_gbps += ibLinkSpeedGbps(context->activeSpeed(),
                                              context->activeWidth());
            }
            if (total_gbps > 0) bytes_per_second = total_gbps * 1e9 / 8.0;
        }
    }
    const auto& config = globalConfig();
    return {
        .link_bytes_per_second = bytes_per_second,
        .queue_depth = std::max<size_t>(
            1, config.max_wr * config.num_qp_per_ep * context_count),
        .pipeline_width =
            std::clamp<size_t>(std::thread::hardware_concurrency(), 1, 4),
    };
}

size_t TransferEngineImpl::scatterSmallFragmentLimit(
    const ScatterTransportProfile& profile) {
    if (profile.queue_depth == 0) return 0;
    const double budget = 1.0 / kScatterPostRate +
                          kScatterRttSeconds / profile.queue_depth -
                          1.0 / kScatterCopyRate;
    return budget <= 0 ? 0
                       : static_cast<size_t>(kScatterCopyBandwidth * budget);
}

TransferEngineImpl::ScatterPlan TransferEngineImpl::planScatter(
    size_t fragments, size_t spans, uint64_t bytes,
    const ScatterTransportProfile& profile) {
    if (fragments == 0 || spans == 0 || bytes == 0 ||
        profile.link_bytes_per_second <= 0 || profile.queue_depth == 0)
        return {};
    const double wire = bytes / profile.link_bytes_per_second;
    const double direct = kScatterRttSeconds +
                          std::max({wire, fragments / kScatterPostRate,
                                    std::ceil(static_cast<double>(fragments) /
                                              profile.queue_depth) *
                                        kScatterRttSeconds});

    ScatterPlan best;
    double best_cost = std::numeric_limits<double>::infinity();
    for (uint8_t depth : {1, 2, 4}) {
        if (depth > profile.pipeline_width) continue;
        const double bdp = profile.link_bytes_per_second * kScatterRttSeconds;
        // Cover the BDP and amortize batch/status handling. Keep the transfer
        // within one wave of staging slots so all slots can pack in parallel.
        const double target = std::clamp(
            std::max({2.0 * bdp / depth,
                      profile.link_bytes_per_second *
                          kScatterChunkAmortizationSeconds,
                      static_cast<double>(bytes) / depth}),
            static_cast<double>(1ULL << 20), static_cast<double>(16ULL << 20));
        const size_t chunk = std::min<size_t>(
            16ULL << 20,
            static_cast<size_t>(std::ceil(target / kScatterChunkAlignment)) *
                kScatterChunkAlignment);
        const double chunks = std::ceil(static_cast<double>(bytes) / chunk);
        const double pack =
            bytes / kScatterCopyBandwidth + spans / kScatterCopyRate;
        const double bulk = kScatterRttSeconds +
                            std::max({wire, chunks / kScatterPostRate,
                                      std::ceil(chunks / profile.queue_depth) *
                                          kScatterRttSeconds});
        const double fill =
            std::min<uint64_t>(bytes, chunk) / kScatterCopyBandwidth;
        const double drain =
            std::min<uint64_t>(bytes, chunk) / profile.link_bytes_per_second;
        const double gather = kScatterControlSeconds + fill +
                              std::max(pack, bulk) +
                              std::min(pack, bulk) / depth + drain;
        if (gather < best_cost) {
            best_cost = gather;
            best.pipeline_depth = depth;
            best.chunk_bytes = chunk;
        }
    }
    best.gather = best_cost * kScatterSafetyMargin < direct;
    return best;
}

Status TransferEngineImpl::transferDirect(
    const std::vector<TransferRequest>& requests) {
    if (requests.empty()) return Status::OK();

    MultiTransport::ScatterSubmission submission;
    Status aggregate = submitScatter(requests, submission);
    if (submission.batch_id == INVALID_BATCH_ID) return aggregate;

    for (size_t task_id = 0; task_id < submission.task_sizes.size();
         ++task_id) {
        TransferStatus transfer_status;
        Status status;
        while (true) {
            status = getTransferStatus(submission.batch_id, task_id,
                                       transfer_status);
            if (!status.ok()) break;
            if (transfer_status.s == TransferStatusEnum::COMPLETED) break;
            if (transfer_status.s != TransferStatusEnum::WAITING &&
                transfer_status.s != TransferStatusEnum::PENDING) {
                status = Status::Socket("direct scatter transfer failed");
                break;
            }
            PAUSE();
        }
        if (!status.ok() && aggregate.ok()) aggregate = status;
    }
    auto free_status = freeBatchID(submission.batch_id);
    if (!free_status.ok() && aggregate.ok()) aggregate = free_status;
    return aggregate;
}

Status TransferEngineImpl::requestScatterGather(
    const std::string& peer_server_name, uint64_t destination_address,
    const std::vector<ScatterSpan>& spans, uint64_t source_base,
    uint64_t source_size, uint64_t total_bytes, size_t chunk_bytes,
    uint8_t pipeline_depth) {
    if (!metadata_)
        return Status::NotImplemented(
            "peer does not support transfer commands");
    if (spans.empty() || spans.size() > kMaxScatterSpans || total_bytes == 0 ||
        total_bytes > kMaxScatterBytes || chunk_bytes == 0 ||
        chunk_bytes > kMaxScatterChunkBytes || pipeline_depth == 0 ||
        pipeline_depth > 8)
        return Status::InvalidArgument("invalid scatter gather request");

    const std::string destination_endpoint = local_server_name_;
    const std::string source_ip = metadata_->localRpcMeta().ip_or_host_name;
    if (destination_endpoint.size() > UINT16_MAX ||
        source_ip.size() > UINT16_MAX)
        return Status::InvalidArgument("scatter destination is too long");
    const size_t span_budget = scatterCommandSpanBudget(peer_server_name);
    if (span_budget == 0)
        return Status::TooManyRequests("scatter command envelope is too large");
    bool valid_source_window = source_size > 0 && source_size <= UINT32_MAX &&
                               source_base <= UINT64_MAX - source_size;
    uint32_t fixed_length = spans.front().length;
    for (const auto& span : spans) {
        if (!valid_source_window) break;
        valid_source_window =
            span.source_address >= source_base &&
            span.source_address - source_base <= UINT32_MAX &&
            span.length <= source_size &&
            span.source_address - source_base <= source_size - span.length;
        if (span.length != fixed_length) fixed_length = 0;
    }
    if (!valid_source_window)
        return Status::InvalidArgument("scatter source window is too large");
    const bool fixed_relative32 = fixed_length != 0;

    std::string request;
    request.reserve(kScatterCommandHeaderBytes + kScatterRelativeHeaderBytes +
                    destination_endpoint.size() + source_ip.size() +
                    spans.size() * (fixed_relative32 ? 4 : 8));
    appendUnsigned<uint8_t>(request, kTransferCommandVersion);
    appendUnsigned<uint8_t>(request, kScatterGatherCommand);
    appendUnsigned<uint8_t>(request, pipeline_depth);
    appendUnsigned<uint16_t>(
        request, static_cast<uint16_t>(destination_endpoint.size()));
    appendUnsigned<uint16_t>(request, static_cast<uint16_t>(source_ip.size()));
    appendUnsigned<uint32_t>(request, static_cast<uint32_t>(spans.size()));
    appendUnsigned<uint32_t>(request, static_cast<uint32_t>(chunk_bytes));
    appendUnsigned<uint64_t>(request, destination_address);
    appendUnsigned<uint64_t>(request, total_bytes);
    appendUnsigned<uint8_t>(request, fixed_relative32
                                         ? kScatterRelative32FixedLengthEncoding
                                         : kScatterRelative32Encoding);
    appendUnsigned<uint64_t>(request, source_base);
    appendUnsigned<uint64_t>(request, source_size);
    if (fixed_relative32) appendUnsigned<uint32_t>(request, fixed_length);
    request.append(destination_endpoint);
    request.append(source_ip);
    const size_t spans_begin = request.size();
    const size_t words_per_span = fixed_relative32 ? 1 : 2;
    request.resize(request.size() +
                   spans.size() * words_per_span * sizeof(uint32_t));
    char* output = request.data() + spans_begin;
    for (const auto& span : spans) {
        writeUnsigned<uint32_t>(
            output, static_cast<uint32_t>(span.source_address - source_base));
        if (!fixed_relative32) writeUnsigned<uint32_t>(output, span.length);
    }
    if (request.size() - spans_begin > span_budget)
        return Status::TooManyRequests("scatter gather plan is too large");
    std::string response;
    const int rc = metadata_->sendCommand(peer_server_name, request, response);
    if (rc != 0) return Status::Socket("scatter gather command failed");
    if (response.size() < 2 ||
        static_cast<uint8_t>(response[0]) != kTransferCommandVersion)
        return Status::Context("invalid scatter gather response");
    if (response[1] != 0) return Status::Context(response.substr(2));
    return Status::OK();
}

void TransferEngineImpl::handleTransferCommand(const std::string& peer_address,
                                               const std::string& request,
                                               std::string& response) {
    const Status status = executeScatterGather(peer_address, request);
    response.clear();
    response.push_back(static_cast<char>(kTransferCommandVersion));
    response.push_back(status.ok() ? 0 : 1);
    if (!status.ok()) response.append(status.ToString());
}

Status TransferEngineImpl::executeScatterGather(const std::string& peer_address,
                                                std::string_view request) {
    TransferEngine::ScatterStagingAllocator allocator;
    {
        std::lock_guard<std::mutex> lock(scatter_staging_mutex_);
        if (!scatter_staging_allocator_)
            return Status::NotImplemented("scatter staging pool unavailable");
        allocator = scatter_staging_allocator_;
        ++active_scatter_commands_;
    }
    struct ActiveGuard {
        std::function<void()> release;
        ~ActiveGuard() { release(); }
    } active_guard{[this] {
        std::lock_guard<std::mutex> lock(scatter_staging_mutex_);
        --active_scatter_commands_;
        scatter_staging_cv_.notify_all();
    }};

    size_t offset = 0;
    uint8_t version = 0, command = 0, pipeline_depth = 0;
    uint16_t endpoint_size = 0, source_ip_size = 0;
    uint32_t span_count = 0, chunk_bytes = 0;
    uint64_t destination_address = 0, total_bytes = 0;
    if (!readUnsigned(request, offset, version) ||
        !readUnsigned(request, offset, command) ||
        !readUnsigned(request, offset, pipeline_depth) ||
        !readUnsigned(request, offset, endpoint_size) ||
        !readUnsigned(request, offset, source_ip_size) ||
        !readUnsigned(request, offset, span_count) ||
        !readUnsigned(request, offset, chunk_bytes) ||
        !readUnsigned(request, offset, destination_address) ||
        !readUnsigned(request, offset, total_bytes) ||
        version != kTransferCommandVersion ||
        command != kScatterGatherCommand || pipeline_depth == 0 ||
        pipeline_depth > 8 || span_count == 0 ||
        span_count > kMaxScatterSpans || chunk_bytes == 0 ||
        chunk_bytes > kMaxScatterChunkBytes || total_bytes == 0 ||
        total_bytes > kMaxScatterBytes) {
        return Status::InvalidArgument("invalid scatter gather command");
    }

    uint8_t span_encoding = 0;
    uint64_t source_base = 0, source_size = 0;
    if (!readUnsigned(request, offset, span_encoding) ||
        !readUnsigned(request, offset, source_base) ||
        !readUnsigned(request, offset, source_size) ||
        (span_encoding != kScatterRelative32Encoding &&
         span_encoding != kScatterRelative32FixedLengthEncoding) ||
        source_size == 0 || source_size > UINT32_MAX ||
        source_base > UINT64_MAX - source_size)
        return Status::InvalidArgument("invalid scatter source window");
    uint32_t fixed_length = 0;
    if (span_encoding == kScatterRelative32FixedLengthEncoding &&
        (!readUnsigned(request, offset, fixed_length) || fixed_length == 0 ||
         fixed_length > chunk_bytes || fixed_length > source_size)) {
        return Status::InvalidArgument(
            "invalid fixed-length relative scatter command");
    }
    const size_t remaining = request.size() - std::min(request.size(), offset);
    if (remaining < endpoint_size || remaining - endpoint_size < source_ip_size)
        return Status::InvalidArgument("invalid scatter gather command");

    const std::string destination_endpoint(
        request.substr(offset, endpoint_size));
    offset += endpoint_size;
    const std::string source_ip(request.substr(offset, source_ip_size));
    offset += source_ip_size;
    const auto peer_host = parseHostNameWithPort(peer_address).first;
    if (peer_host.empty() || source_ip.empty() || peer_host != source_ip)
        return Status::RejectHandshake(
            "scatter destination does not match command peer");

    std::vector<ScatterSpan> spans;
    spans.reserve(span_count);
    uint64_t decoded_bytes = 0;
    // Keep unregister from returning while gather workers still dereference
    // validated source addresses.
    std::shared_lock<std::shared_mutex> source_registration_lock(mutex_);
    const MemoryRegion* cached_region = nullptr;
    auto contains = [&](uint64_t address, uint64_t length) {
        auto in_region = [&](const MemoryRegion& region) {
            const auto begin = reinterpret_cast<uintptr_t>(region.addr);
            return address >= begin && length <= region.length &&
                   address - begin <= region.length - length &&
                   region.remote_accessible &&
                   hostAccessibleLocation(region.location);
        };
        if (cached_region && in_region(*cached_region)) return true;
        auto next = local_memory_regions_.upper_bound(address);
        if (next == local_memory_regions_.begin()) return false;
        const auto& region = std::prev(next)->second;
        if (!in_region(region)) return false;
        cached_region = &region;
        return true;
    };
    if (!contains(source_base, source_size)) {
        return Status::AddressNotRegistered(
            "scatter source window is not registered host memory");
    }
    for (uint32_t i = 0; i < span_count; ++i) {
        uint64_t source = 0, length = 0;
        uint32_t relative_offset = 0, relative_length = 0;
        if (!readUnsigned(request, offset, relative_offset) ||
            (span_encoding == kScatterRelative32Encoding &&
             !readUnsigned(request, offset, relative_length)))
            return Status::InvalidArgument("invalid scatter span");
        if (span_encoding == kScatterRelative32FixedLengthEncoding)
            relative_length = fixed_length;
        if (relative_length == 0 || relative_length > chunk_bytes ||
            relative_length > source_size ||
            relative_offset > source_size - relative_length)
            return Status::InvalidArgument("invalid scatter span");
        source = source_base + relative_offset;
        length = relative_length;
        if (decoded_bytes > total_bytes ||
            length > total_bytes - decoded_bytes) {
            return Status::InvalidArgument("invalid scatter byte count");
        }
        spans.push_back({source, static_cast<uint32_t>(length)});
        decoded_bytes += length;
    }
    if (offset != request.size() || decoded_bytes != total_bytes ||
        destination_address > UINT64_MAX - total_bytes)
        return Status::InvalidArgument("scatter gather size mismatch");
    struct Chunk {
        size_t begin;
        size_t end;
        size_t bytes;
        size_t destination_offset;
    };
    std::vector<Chunk> chunks;
    size_t begin = 0, bytes = 0, destination_offset = 0;
    for (size_t i = 0; i < spans.size(); ++i) {
        if (bytes != 0 && bytes + spans[i].length > chunk_bytes) {
            chunks.push_back({begin, i, bytes, destination_offset});
            begin = i;
            destination_offset += bytes;
            bytes = 0;
        }
        bytes += spans[i].length;
    }
    chunks.push_back({begin, spans.size(), bytes, destination_offset});

    std::vector<TransferEngine::ScatterStagingBuffer> staging;
    const size_t depth = std::min<size_t>(pipeline_depth, chunks.size());
    for (size_t i = 0; i < depth; ++i) {
        auto buffer = allocator(chunk_bytes);
        if (!buffer || buffer.capacity < chunk_bytes) break;
        staging.push_back(std::move(buffer));
    }
    if (staging.empty())
        return Status::Memory("scatter staging pool exhausted");
    const auto segment = openSegment(destination_endpoint);
    if (segment == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT))
        return Status::Endpoint("failed to open scatter destination");
    struct Slot {
        BatchID batch = INVALID_BATCH_ID;
        std::vector<TransferRequest> requests{1};
    };
    std::vector<Slot> slots(staging.size());
    auto drain = [&](Slot& slot) {
        if (slot.batch == INVALID_BATCH_ID) return Status::OK();
        TransferStatus transfer_status;
        Status aggregate = Status::OK();
        uint32_t polls = 0;
        while (true) {
            const auto status =
                getTransferStatus(slot.batch, 0, transfer_status);
            bool terminal = false;
            if (!status.ok()) {
                if (aggregate.ok()) aggregate = status;
                terminal = true;
            } else if (transfer_status.s == TransferStatusEnum::COMPLETED) {
                terminal = true;
            } else if (transfer_status.s != TransferStatusEnum::WAITING &&
                       transfer_status.s != TransferStatusEnum::PENDING) {
                if (aggregate.ok())
                    aggregate =
                        Status::Socket("scatter gather RDMA write failed");
                terminal = true;
            }
            if (terminal) {
                const auto free_status = freeBatchID(slot.batch);
                if (!free_status.IsBatchBusy()) {
                    slot.batch = INVALID_BATCH_ID;
                    if (!free_status.ok() && aggregate.ok())
                        aggregate = free_status;
                    return aggregate;
                }
            }
            if (++polls < 64)
                PAUSE();
            else
                std::this_thread::yield();
        }
    };

    std::vector<Status> slot_status(slots.size(), Status::OK());
    auto run_slot = [&](size_t slot_index) {
        auto& slot = slots[slot_index];
        auto* output = static_cast<char*>(staging[slot_index].data);
        for (size_t i = slot_index; i < chunks.size(); i += slots.size()) {
            auto status = drain(slot);
            if (!status.ok()) {
                slot_status[slot_index] = status;
                return;
            }

            size_t packed = 0;
            for (size_t j = chunks[i].begin; j < chunks[i].end; ++j) {
                std::memcpy(
                    output + packed,
                    reinterpret_cast<const void*>(spans[j].source_address),
                    spans[j].length);
                packed += spans[j].length;
            }
            slot.batch = allocateBatchID(1);
            if (slot.batch == INVALID_BATCH_ID) {
                slot_status[slot_index] =
                    Status::Memory("failed to allocate scatter batch");
                return;
            }
            slot.requests[0] = TransferRequest{
                .opcode = TransferRequest::WRITE,
                .source = output,
                .target_id = segment,
                .target_offset =
                    destination_address + chunks[i].destination_offset,
                .length = chunks[i].bytes,
            };
            status = submitTransfer(slot.batch, slot.requests);
            if (!status.ok()) {
                drain(slot);
                slot_status[slot_index] = status;
                return;
            }
        }
        const auto status = drain(slot);
        if (!status.ok()) slot_status[slot_index] = status;
    };

    if (slots.size() == 1) {
        run_slot(0);
    } else {
        std::vector<std::thread> workers;
        workers.reserve(slots.size());
        Status launch_status = Status::OK();
        try {
            for (size_t i = 0; i < slots.size(); ++i)
                workers.emplace_back(run_slot, i);
        } catch (...) {
            launch_status =
                Status::Memory("failed to launch scatter gather workers");
        }
        for (auto& worker : workers) worker.join();
        if (!launch_status.ok()) {
            closeSegment(segment);
            return launch_status;
        }
    }
    Status aggregate = Status::OK();
    for (const auto& status : slot_status)
        if (!status.ok() && aggregate.ok()) aggregate = status;
    if (closeSegment(segment) != 0 && aggregate.ok())
        aggregate = Status::Endpoint("failed to close scatter destination");
    return aggregate;
}

// Only for testing
Transport* TransferEngineImpl::installTransport(const std::string& proto,
                                                void** args) {
    Transport* transport = multi_transports_->getTransport(proto);
    if (transport) {
        LOG(WARNING) << "Transport " << proto << " already installed";
        return transport;
    }
#ifdef USE_NCCL_HOST
    if (proto == "nccl" && !local_memory_regions_.empty()) {
        LOG(ERROR) << "Install NCCL before registering local memory so peer "
                      "buffer order remains deterministic";
        return nullptr;
    }
#endif

    if (args != nullptr && args[0] != nullptr) {
        const std::string nic_priority_matrix = static_cast<char*>(args[0]);
        int ret = local_topology_->parse(nic_priority_matrix);
        if (ret) {
            LOG(ERROR) << "Failed to parse NIC priority matrix";
            return nullptr;
        }
    }

    transport = multi_transports_->installTransport(proto, local_topology_);
    if (!transport) return nullptr;

    // Since installTransport() is only called once during initialization
    // and is not expected to be executed concurrently, we do not acquire a
    // shared lock here. If future modifications allow installTransport() to be
    // invoked concurrently, a std::shared_lock<std::shared_mutex> should be
    // added to ensure thread safety.
    for (auto& [_, entry] : local_memory_regions_) {
        int ret = transport->registerLocalMemory(
            entry.addr, entry.length, entry.location, entry.remote_accessible);
        if (ret < 0) return nullptr;
    }
    return transport;
}

int TransferEngineImpl::uninstallTransport(const std::string& proto) {
    return 0;
}

void* TransferEngineImpl::allocateSharedMemory(size_t length) {
    return allocateSharedMemory(length, SharedMemoryOptions{});
}

void* TransferEngineImpl::allocateSharedMemory(size_t length,
                                               const SharedMemoryOptions& opt) {
    auto* shm =
        dynamic_cast<ShmTransport*>(multi_transports_->getTransport("shm"));
    if (!shm) {
        LOG(ERROR) << "allocateSharedMemory requires ShmTransport "
                      "(set MC_FORCE_SHM=1 or installTransport(\"shm\"))";
        return nullptr;
    }
    return shm->allocateSharedMemory(length, opt);
}

int TransferEngineImpl::freeSharedMemory(void* addr) {
    if (!addr) return ERR_INVALID_ARGUMENT;
    auto* shm =
        dynamic_cast<ShmTransport*>(multi_transports_->getTransport("shm"));
    if (!shm) return ERR_INVALID_ARGUMENT;
    std::string shm_name;
    if (!shm->getShmName(addr, &shm_name)) return ERR_INVALID_ARGUMENT;
    int uret = unregisterLocalMemory(addr, true);
    if (uret && uret != ERR_ADDRESS_NOT_REGISTERED) {
        LOG(WARNING) << "unregisterLocalMemory failed before freeSharedMemory, "
                        "ret="
                     << uret;
    }
    return shm->freeSharedMemory(addr);
}

#if (defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)) && \
    !defined(USE_CXI)
device::P2pTransport* TransferEngineImpl::getOrCreateP2pTransport(
    int num_ranks) {
    if (!p2p_transport_) {
        p2p_transport_ = device::createP2pDeviceTransport(num_ranks);
    }
    return p2p_transport_.get();
}

device::RdmaTransport* TransferEngineImpl::getOrCreateRdmaTransport(
    const std::vector<std::string>& device_filter) {
    if (!rdma_transport_) {
        rdma_transport_ = device::createIbgdaDeviceTransport(device_filter);
    }
    return rdma_transport_.get();
}
#endif

#ifdef USE_NCCL_DEVICE
device::NcclTransport* TransferEngineImpl::getOrCreateNcclTransport() {
    if (!nccl_transport_) {
        nccl_transport_ = device::createNcclDeviceTransport();
    }
    return nccl_transport_.get();
}
#endif

int TransferEngineImpl::getRpcPort() {
    return metadata_->localRpcMeta().rpc_port;
}

std::string TransferEngineImpl::getLocalIpAndPort() {
    return metadata_->localRpcMeta().ip_or_host_name + ":" +
           std::to_string(metadata_->localRpcMeta().rpc_port);
}

int TransferEngineImpl::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    return metadata_->getNotifies(notifies);
}

int TransferEngineImpl::sendNotifyByID(
    SegmentID target_id, TransferMetadata::NotifyDesc notify_msg) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        LOG(ERROR) << "sendNotifyByID: invalid segment ID " << target_id;
        return ERR_METADATA;
    }
    return sendNotifyByName(desc->name, std::move(notify_msg));
}

int TransferEngineImpl::sendNotifyByName(
    std::string remote_agent, TransferMetadata::NotifyDesc notify_msg) {
    if (globalConfig().rdma_notify_enabled) {
        Transport* transport = getTransport("rdma_twosided");
        if (!transport) transport = getTransport("rdma");
        auto* rdma_twosided = dynamic_cast<RdmaTwoSidedTransport*>(transport);
        if (rdma_twosided) {
            int ret = rdma_twosided->sendRdmaNotify(remote_agent, notify_msg);
            if (ret == 0) return 0;
            if (!globalConfig().rdma_notify_oob_fallback) {
                LOG(ERROR) << "sendNotifyByName: RDMA notify failed for "
                           << remote_agent << " ret=" << ret
                           << " (OOB fallback disabled)";
                return ret;
            }
            VLOG(1) << "sendNotifyByName: RDMA notify unavailable for "
                    << remote_agent << ", falling back to OOB";
        }
    }
    Transport::NotifyDesc peer_desc;
    return metadata_->sendNotify(remote_agent, notify_msg, peer_desc);
}

int TransferEngineImpl::probePeerAliveByID(SegmentID target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        return ERR_METADATA;
    }
    return metadata_->sendProbe(desc->name);
}

Transport::SegmentHandle TransferEngineImpl::openSegment(
    const std::string& segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    SegmentID sid = metadata_->getSegmentID(trimmed_segment_name);
#ifdef USE_BAREX
    if (use_barex_) {
        Transport* transport = multi_transports_->getTransport("barex");
        if (!transport) {
            LOG(ERROR) << "Barex proto not installed";
            return (Transport::SegmentHandle)-1;
        }
        Status s = transport->OpenChannel(segment_name, sid);
        if (!s.ok()) {
            LOG(ERROR) << "openSegment, OpenChannel failed";
            return (Transport::SegmentHandle)-1;
        }
    }
#endif
    return sid;
}

Status TransferEngineImpl::CheckSegmentStatus(SegmentID sid) {
#ifdef USE_BAREX
    if (use_barex_) {
        Transport* transport = multi_transports_->getTransport("barex");
        BarexTransport* barex_transport =
            dynamic_cast<BarexTransport*>(transport);
        return barex_transport->CheckStatus(sid);
    } else {
        return Status::OK();
    }
#else
    return Status::OK();
#endif
}

int TransferEngineImpl::closeSegment(Transport::SegmentHandle handle) {
    return 0;
}

int TransferEngineImpl::removeLocalSegment(const std::string& segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->removeLocalSegment(trimmed_segment_name);
}

bool TransferEngineImpl::checkOverlap(void* addr, uint64_t length) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return hasOverlapLocked(reinterpret_cast<uintptr_t>(addr), length);
}

int TransferEngineImpl::registerLocalMemory(void* addr, size_t length,
                                            const std::string& location,
                                            bool remote_accessible,
                                            bool update_metadata) {
    if (length == 0) {
        LOG(ERROR)
            << "Transfer Engine does not support zero length memory region";
        return ERR_INVALID_ARGUMENT;
    }

    std::vector<MemoryRegion> regions = {
        {addr, length, location, remote_accessible}};
    if (!tryReserveMemoryRegions(regions)) {
        LOG(ERROR)
            << "Transfer Engine does not support overlapped memory region";
        return ERR_ADDRESS_OVERLAPPED;
    }

    std::vector<Transport*> attempted_transports;
    for (auto transport : multi_transports_->listTransports()) {
        attempted_transports.push_back(transport);
        int ret = transport->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
        if (ret < 0) {
            // Roll back the transports that already registered so a partial
            // failure doesn't leave the region registered on some of them.
            // Mirrors registerLocalMemoryBatch (#2869).
            for (auto it = attempted_transports.rbegin();
                 it != attempted_transports.rend(); ++it) {
                int rollback_ret =
                    (*it)->unregisterLocalMemory(addr, update_metadata);
                if (rollback_ret != 0 &&
                    rollback_ret != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(WARNING)
                        << "Failed to roll back registration for "
                        << (*it)->getName() << ", ret=" << rollback_ret;
                }
            }
            releaseMemoryRegions(regions);
            return ret;
        }
    }

    commitMemoryRegions(regions);
    return 0;
}

int TransferEngineImpl::unregisterLocalMemory(void* addr,
                                              bool update_metadata) {
    // Best-effort: try every transport so one failure can't leave the region
    // registered on the others; mirrors unregisterLocalMemoryBatch (#2869).
    int first_error = 0;
    for (auto& transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemory(addr, update_metadata);
        if (ret && !first_error) first_error = ret;
    }
    if (first_error) return first_error;

    std::unique_lock<std::shared_mutex> lock(mutex_);
    eraseMemoryRegionLocked(addr);
    return 0;
}

#ifdef ENABLE_MULTI_PROTOCOL
// Multi-protocol API (only available when ENABLE_MULTI_PROTOCOL is defined)
// Supports registering memory for multiple protocols (CXL, TCP / RDMA)
int TransferEngineImpl::mp_registerLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    // ========== Phase 1: Pre-check ==========
    for (const auto& entry : buffer_map) {
        for (const auto& buffer : entry.second) {
            if (checkOverlap(buffer.addr, buffer.length)) {
                LOG(ERROR) << "Transfer Engine does not support overlapped "
                              "memory region";
                return ERR_ADDRESS_OVERLAPPED;
            }
            if (buffer.length == 0) {
                LOG(ERROR) << "Transfer Engine does not support zero length "
                              "memory region";
                return ERR_INVALID_ARGUMENT;
            }
        }
    }

    // ========== Phase 2: Prepare rollback records ==========
    std::vector<TransferEngineImpl::RegisteredRecord> success_records;

    // Reserve space to reduce reallocations
    size_t total_buffers = 0;
    for (const auto& entry : buffer_map) {
        total_buffers += entry.second.size();
    }
    success_records.reserve(total_buffers);

    // ========== Phase 3: Execute registration ==========
    for (const auto& entry : buffer_map) {
        const std::string& protocol = entry.first;
        const auto& buffer_list = entry.second;

        auto transport = multi_transports_->getTransport(protocol);
        if (!transport) {
            LOG(ERROR) << "Transport " << protocol << " not found";
            rollbackAllRegistrations(success_records);
            return -1;
        }

        for (const auto& buffer : buffer_list) {
            int ret = transport->registerLocalMemory(
                buffer.addr, buffer.length, buffer.location,
                buffer.remote_accessible, buffer.update_metadata);

            if (ret < 0) {
                LOG(ERROR) << "Failed to register memory with transport "
                           << protocol << " addr=" << buffer.addr
                           << " length=" << buffer.length;

                // ========== Phase 4: Rollback on failure ==========
                rollbackAllRegistrations(success_records);
                return ret;
            }

            // Record successful registration for potential rollback
            success_records.push_back(TransferEngineImpl::RegisteredRecord{
                transport, buffer.addr, buffer.length, buffer.location,
                buffer.remote_accessible});
        }
    }

    // ========== Phase 5: Commit to system state ==========
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (const auto& record : success_records) {
            insertMemoryRegionLocked({record.addr, record.length,
                                      record.location,
                                      record.remote_accessible});
        }
    }

    return 0;
}

void TransferEngineImpl::rollbackAllRegistrations(
    const std::vector<RegisteredRecord>& records) {
    LOG(INFO) << "Rolling back " << records.size() << " registered regions";

    for (const auto& record : records) {
        if (record.transport) {
            record.transport->unregisterLocalMemory(record.addr, true);
        }
    }
}

int TransferEngineImpl::mp_unregisterLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    for (const auto& buffer_entry : buffer_map) {
        const std::string& protocol = buffer_entry.first;
        const std::vector<RegisteredBuffer>& buffer_list = buffer_entry.second;

        auto transport = multi_transports_->getTransport(protocol);
        if (!transport) {
            LOG(ERROR) << "Transport " << protocol << " not found";
            return -1;
        }

        for (const auto& buffer : buffer_list) {
            int ret = transport->unregisterLocalMemory(buffer.addr,
                                                       buffer.update_metadata);
            if (ret) {
                return ret;
            }
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (const auto& buffer : buffer_list) {
            eraseMemoryRegionLocked(buffer.addr);
        }
    }
    return 0;
}
#endif

int TransferEngineImpl::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    std::vector<BufferEntry> sorted_buffers = buffer_list;
    std::sort(sorted_buffers.begin(), sorted_buffers.end(),
              [](const BufferEntry& lhs, const BufferEntry& rhs) {
                  return reinterpret_cast<uintptr_t>(lhs.addr) <
                         reinterpret_cast<uintptr_t>(rhs.addr);
              });

    for (size_t i = 0; i < sorted_buffers.size(); ++i) {
        const auto& buffer = sorted_buffers[i];
        if (buffer.length == 0) {
            LOG(ERROR)
                << "Transfer Engine does not support zero length memory region";
            return ERR_INVALID_ARGUMENT;
        }

        if (i > 0) {
            const auto& previous = sorted_buffers[i - 1];
            auto address = reinterpret_cast<uintptr_t>(buffer.addr);
            auto previous_address = reinterpret_cast<uintptr_t>(previous.addr);
            if (address - previous_address < previous.length) {
                LOG(ERROR) << "Transfer Engine does not support overlapped "
                              "memory region";
                return ERR_ADDRESS_OVERLAPPED;
            }
        }
    }

    std::vector<MemoryRegion> regions;
    std::vector<void*> addr_list;
    regions.reserve(buffer_list.size());
    addr_list.reserve(buffer_list.size());
    for (const auto& buffer : buffer_list) {
        regions.push_back({buffer.addr, buffer.length, location, true});
        addr_list.push_back(buffer.addr);
    }
    if (!tryReserveMemoryRegions(regions)) {
        LOG(ERROR)
            << "Transfer Engine does not support overlapped memory region";
        return ERR_ADDRESS_OVERLAPPED;
    }

    std::vector<Transport*> attempted_transports;
    for (auto transport : multi_transports_->listTransports()) {
        attempted_transports.push_back(transport);
        int ret = transport->registerLocalMemoryBatch(buffer_list, location);
        if (ret) {
            for (auto it = attempted_transports.rbegin();
                 it != attempted_transports.rend(); ++it) {
                int rollback_ret = (*it)->unregisterLocalMemoryBatch(addr_list);
                if (rollback_ret != 0 &&
                    rollback_ret != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(WARNING)
                        << "Failed to roll back batch registration for "
                        << (*it)->getName() << ", ret=" << rollback_ret;
                }
            }
            releaseMemoryRegions(regions);
            return ret;
        }
    }

    commitMemoryRegions(regions);
    return 0;
}

int TransferEngineImpl::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemoryBatch(addr_list);
        if (ret && !first_error) first_error = ret;
    }
    if (first_error) return first_error;

    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto& addr : addr_list) {
        eraseMemoryRegionLocked(addr);
    }
    return 0;
}

bool TransferEngineImpl::hasOverlapLocked(uintptr_t addr,
                                          uint64_t length) const {
    return hasOverlapInMapLocked(local_memory_regions_, addr, length) ||
           hasOverlapInMapLocked(registering_memory_regions_, addr, length);
}

bool TransferEngineImpl::hasOverlapInMapLocked(const MemoryRegionMap& regions,
                                               uintptr_t addr,
                                               uint64_t length) const {
    if (length == 0) {
        return false;
    }

    auto next = regions.lower_bound(addr);
    if (next != regions.end() &&
        overlapWithRegion(addr, length, next->second.addr,
                          next->second.length)) {
        return true;
    }

    if (next != regions.begin()) {
        auto prev = std::prev(next);
        if (overlapWithRegion(addr, length, prev->second.addr,
                              prev->second.length)) {
            return true;
        }
    }

    return false;
}

bool TransferEngineImpl::tryReserveMemoryRegions(
    const std::vector<MemoryRegion>& regions) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<uintptr_t> reserved;
    reserved.reserve(regions.size());

    for (const auto& region : regions) {
        auto addr = reinterpret_cast<uintptr_t>(region.addr);
        if (hasOverlapLocked(addr, region.length)) {
            for (auto reserved_addr : reserved) {
                registering_memory_regions_.erase(reserved_addr);
            }
            return false;
        }
        registering_memory_regions_[addr] = region;
        reserved.push_back(addr);
    }
    return true;
}

void TransferEngineImpl::commitMemoryRegions(
    const std::vector<MemoryRegion>& regions) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto& region : regions) {
        registering_memory_regions_.erase(
            reinterpret_cast<uintptr_t>(region.addr));
        insertMemoryRegionLocked(region);
    }
}

void TransferEngineImpl::releaseMemoryRegions(
    const std::vector<MemoryRegion>& regions) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto& region : regions) {
        registering_memory_regions_.erase(
            reinterpret_cast<uintptr_t>(region.addr));
    }
}

void TransferEngineImpl::insertMemoryRegionLocked(const MemoryRegion& region) {
    local_memory_regions_[reinterpret_cast<uintptr_t>(region.addr)] = region;
}

void TransferEngineImpl::eraseMemoryRegionLocked(void* addr) {
    local_memory_regions_.erase(reinterpret_cast<uintptr_t>(addr));
}

#ifdef WITH_METRICS
// Helper function to convert string to lowercase for case-insensitive
// comparison
static std::string toLower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

void TransferEngineImpl::InitializeMetricsConfig() {
    // Check if metrics reporting is enabled via environment variable
    const char* metric_env = getenv("MC_TE_METRIC");
    if (metric_env) {
        std::string value = toLower(metric_env);
        metrics_enabled_ = (value == "1" || value == "true" || value == "yes" ||
                            value == "on");
    }

    // Check for custom reporting interval
    const char* interval_env = getenv("MC_TE_METRIC_INTERVAL_SECONDS");
    if (interval_env) {
        try {
            int interval = std::stoi(interval_env);
            if (interval > 0) {
                metrics_interval_seconds_ = static_cast<uint64_t>(interval);
                LOG(INFO) << "Metrics reporting interval set to "
                          << metrics_interval_seconds_ << " seconds";
            } else {
                LOG(WARNING)
                    << "Invalid MC_TE_METRIC_INTERVAL_SECONDS value: "
                    << interval_env << ", must be positive. Using default: "
                    << metrics_interval_seconds_;
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to parse MC_TE_METRIC_INTERVAL_SECONDS: "
                         << interval_env
                         << ", using default: " << metrics_interval_seconds_;
        }
    }
}

void TransferEngineImpl::StartMetricsReportingThread() {
    // Only start the metrics thread if metrics are enabled
    if (!metrics_enabled_) {
        LOG(INFO)
            << "Metrics reporting is disabled (set MC_TE_METRIC=1 to enable)";
        return;
    }

    should_stop_metrics_thread_ = false;

    // Initialize previous bucket counts
    {
        std::lock_guard<std::mutex> lock(metrics_snapshot_mutex_);
        auto bucket_counts = task_completion_latency_us_.get_bucket_counts();
        prev_bucket_counts_.resize(bucket_counts.size(), 0);
    }

    metrics_reporting_thread_ = std::thread([this]() {
        LOG(INFO) << "Metrics reporting thread started (interval: "
                  << metrics_interval_seconds_ << "s)";
        constexpr double kBytesPerMegabyte = 1024.0 * 1024.0;

        while (!should_stop_metrics_thread_) {
            // Sleep for the interval, checking periodically for stop signal
            for (uint64_t i = 0;
                 i < metrics_interval_seconds_ && !should_stop_metrics_thread_;
                 ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (should_stop_metrics_thread_) {
                break;  // Exit if stopped during sleep
            }

            auto bytes_transferred_in_interval =
                transferred_bytes_counter_.value();
            transferred_bytes_counter_
                .reset();  // Reset counter for the next interval

            // Calculate throughput
            bool has_throughput = (bytes_transferred_in_interval > 0);
            double throughput_megabytes_per_second = 0.0;
            if (has_throughput) {
                throughput_megabytes_per_second =
                    static_cast<double>(bytes_transferred_in_interval) /
                    (metrics_interval_seconds_ * kBytesPerMegabyte);
            }

            // Calculate task completion latency statistics for this interval
            auto bucket_counts =
                task_completion_latency_us_.get_bucket_counts();

            // Compute interval counts (delta from previous snapshot)
            std::vector<int64_t> interval_counts;
            int64_t total_task_count = 0;
            {
                std::lock_guard<std::mutex> lock(metrics_snapshot_mutex_);
                interval_counts.resize(bucket_counts.size());

                for (size_t i = 0; i < bucket_counts.size(); ++i) {
                    int64_t current_count = bucket_counts[i]->value();
                    interval_counts[i] = current_count - prev_bucket_counts_[i];
                    total_task_count += interval_counts[i];
                    prev_bucket_counts_[i] = current_count;
                }
            }

            bool has_latency = (total_task_count > 0);

            // Skip if no data to report
            if (!has_throughput && !has_latency) {
                continue;
            }

            // Build metrics log message
            std::stringstream log_msg;
            log_msg << "[Metrics] Transfer Engine Stats (over last "
                    << metrics_interval_seconds_ << "s):";

            if (has_throughput) {
                log_msg << " Throughput: " << std::fixed << std::setprecision(2)
                        << throughput_megabytes_per_second << " MB/s";
            }

            if (!has_latency) {
                LOG(INFO) << log_msg.str();
                continue;
            }

            // Append latency distribution
            log_msg << " | Latency Distribution (count=" << total_task_count
                    << "): ";

            bool first = true;
            for (size_t i = 0; i < interval_counts.size(); ++i) {
                int64_t count = interval_counts[i];
                if (count <= 0) continue;

                double percentage = (count * 100.0) / total_task_count;
                constexpr double kMinPercentageThreshold = 0.1;
                if (percentage < kMinPercentageThreshold) continue;

                // Add separator between entries
                if (!first) {
                    log_msg << ", ";
                }
                first = false;

                // Format and append bucket range with percentage
                bool is_overflow_bucket = (i >= kTaskLatencyBuckets.size());
                if (is_overflow_bucket) {
                    int threshold =
                        static_cast<int>(kTaskLatencyBuckets.back());
                    log_msg << ">" << threshold << "μs";
                } else {
                    int lower = 0;
                    if (i > 0) {
                        lower = static_cast<int>(kTaskLatencyBuckets[i - 1]);
                    }
                    int upper = static_cast<int>(kTaskLatencyBuckets[i]);
                    log_msg << lower << "-" << upper << "μs";
                }

                log_msg << ":" << std::fixed << std::setprecision(1)
                        << percentage << "%";
            }

            LOG(INFO) << log_msg.str();
        }
        LOG(INFO) << "Metrics reporting thread stopped";
    });
}

void TransferEngineImpl::StopMetricsReportingThread() {
    should_stop_metrics_thread_ = true;  // Signal the thread to stop
    if (metrics_reporting_thread_.joinable()) {
        LOG(INFO) << "Waiting for metrics reporting thread to join...";
        metrics_reporting_thread_.join();  // Wait for the thread to finish
        LOG(INFO) << "Metrics reporting thread joined";
    }
}
#endif

}  // namespace mooncake
