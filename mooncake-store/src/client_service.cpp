#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"
#include "nof/nof_runtime.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <limits>
#if defined(__linux__)
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "common.h"
#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.h"
#include "common/network.h"
#include "config/client_host_identity_config.h"
#include "config/client_object_checksum_config.h"
#include "replica_selection.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "config/client_auto_discovery_config.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "config/client_numa_config.h"
#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {

namespace {

constexpr size_t kObjectChecksumD2HChunkSize = 8 * 1024 * 1024;
constexpr auto kInitialLeaderReadyTimeout = std::chrono::seconds(30);

class ScopedObjectChecksumBuffer {
   public:
    ScopedObjectChecksumBuffer(PinnedBufferPool& pool, size_t size)
        : pool_(pool), buffer_(pool_.Acquire(size)) {}

    ~ScopedObjectChecksumBuffer() { pool_.Release(std::move(buffer_)); }

    char* data() const { return buffer_.data; }

   private:
    PinnedBufferPool& pool_;
    PinnedBufferPool::Buffer buffer_;
};

int GetCurrentNumaSocketId() {
#if defined(__linux__)
    if (numa_available() < 0) {
        return 0;
    }
    int cpu = sched_getcpu();
    if (cpu < 0) {
        return 0;
    }
    // numa_node_of_cpu() fills libnuma's per-node cpumask cache on first use;
    // see numaNodeCpuCacheMutex().
    int node;
    {
        std::lock_guard<std::mutex> guard(numaNodeCpuCacheMutex());
        node = numa_node_of_cpu(cpu);
    }
    return node < 0 ? 0 : node;
#else
    // libnuma is Linux-only; default to socket 0 elsewhere.
    return 0;
#endif
}

// Backing slot for Client::SetDefaultNofInitiatorFactoryForTesting.
std::function<std::shared_ptr<NVMeoFInitiator>()>&
DefaultNofInitiatorFactoryForTestingSlot() {
    static std::function<std::shared_ptr<NVMeoFInitiator>()> factory;
    return factory;
}

}  // namespace

std::optional<size_t> GetTransportRegistrationLimit(
    const std::string& protocol) {
    if (protocol == "efa" || protocol == "cxi") {
        return globalConfig().max_mr_size;
    }
    if (protocol == "ub") {
        return globalConfig().max_seg_size;
    }
    return std::nullopt;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               const std::map<std::string, std::string>& labels,
               const std::string& tenant_id)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr,
                     tenant_id),
      local_hostname_(local_hostname),
      host_id_(
          ClientHostIdentityConfig::FromEnvironment(local_hostname).host_id),
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      object_checksum_enabled_(
          ClientObjectChecksumConfig::IsEnabledAtFirstUse()),
      pinned_buffer_pool_(std::make_unique<PinnedBufferPool>()),
      write_thread_pool_(2),
      task_thread_pool_(4) {
    LOG(INFO) << "client_id=" << client_id_;
    if (!host_id_.empty()) {
        LOG(INFO) << "client_id=" << client_id_ << ", host_id=" << host_id_;
    }
    if (object_checksum_enabled_) {
        LOG(INFO) << "Object checksum validation is enabled";
    }

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

Client::~Client() {
    task_poll_running_ = false;
    if (task_poll_thread_.joinable()) {
        task_poll_thread_.join();
    }

    storage_heartbeat_running_ = false;
    if (storage_heartbeat_thread_.joinable()) {
        storage_heartbeat_thread_.join();
    }
    // The heartbeat thread has drained any in-flight gather refresh.
    if (!gather_endpoint_.empty()) {
        (void)master_client_.PublishGatherEndpoint(GetSegmentEndpoint(),
                                                   gather_endpoint_, true);
    }

    leader_monitor_running_ = false;
    if (leader_monitor_thread_.joinable()) {
        leader_monitor_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(graceful_unmount_timer_mutex_);
        graceful_unmount_timer_stopping_ = true;
    }
    graceful_unmount_timer_cv_.notify_all();

    // Stop queued timer/task callbacks before tearing down segment state.
    task_running_ = false;
    task_thread_pool_.stop();

    // Unmount callbacks must finish before the service and sources are drained.
    gather_service_.reset();
    gather_clients_.clear();

    // Make copies to avoid modifying while iterating
    std::vector<Segment> mounted_segments_copy;
    std::vector<Segment> gracefully_unmounting_segments_copy;
    std::vector<std::pair<UUID, std::function<void(const UUID&)>>>
        graceful_cleanup_callbacks;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_copy.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            mounted_segments_copy.emplace_back(entry.second);
        }
        for (auto& entry : gracefully_unmounting_segments_) {
            gracefully_unmounting_segments_copy.emplace_back(entry.second);
        }
        graceful_cleanup_callbacks.reserve(
            graceful_unmount_cleanup_callbacks_.size());
        for (auto& entry : graceful_unmount_cleanup_callbacks_) {
            graceful_cleanup_callbacks.emplace_back(entry.first,
                                                    std::move(entry.second));
        }
        graceful_unmount_cleanup_callbacks_.clear();
    }

    // Unmount mounted segments: notify master + local cleanup
    for (auto& segment : mounted_segments_copy) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(result.error());
        }
    }

    // Unregister gracefully unmounting segments: master already has timer
    for (auto& segment : gracefully_unmounting_segments_copy) {
        int rc = transfer_engine_->unregisterLocalMemory(
            reinterpret_cast<void*>(segment.base));
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
            LOG(ERROR) << "Failed to unregister transfer buffer in destructor: "
                       << rc;
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
        gracefully_unmounting_segments_.clear();
    }

    for (auto& entry : graceful_cleanup_callbacks) {
        if (entry.second) {
            entry.second(entry.first);
        }
    }

    // Stop hot cache handler before unregistering/freeing its backing memory.
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
}

ReplicateConfig Client::AttachHostId(const ReplicateConfig& config) const {
    ReplicateConfig client_cfg = config;
    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs()) {
        // KVCS is represented by one Mooncake DFS descriptor. The adapter
        // independently handles target placement and provider replicas, so a
        // Mooncake replica count greater than one must not create a policy the
        // single target descriptor cannot satisfy.
        client_cfg.dfs_replica_num = 1;
    }
    if (!host_id_.empty()) {
        client_cfg.host_id = host_id_;
    }
    return client_cfg;
}

std::optional<SegmentLocality> Client::DiscoverSegmentLocality(
    const Topology& topology, const std::string& location) {
    if (!location.starts_with("cpu:")) return std::nullopt;
    int32_t node = -1;
    const auto value = std::string_view(location).substr(4);
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), node);
    if (error != std::errc{} || end != value.data() + value.size() || node < 0)
        return std::nullopt;
    const auto matrix = topology.getMatrix();
    const auto entry = matrix.find(location);
    return SegmentLocality{node, entry == matrix.end()
                                     ? std::vector<std::string>{}
                                     : entry->second.preferred_hca};
}

Client::HcaAffinity Client::MakeHcaAffinity(
    const Topology& topology, std::vector<SegmentBufferInfo> buffers) {
    auto matrix = topology.getMatrix();
    for (auto& [location, entry] : matrix) {
        auto& hcas = entry.preferred_hca;
        std::sort(hcas.begin(), hcas.end());
        hcas.erase(std::unique(hcas.begin(), hcas.end()), hcas.end());
    }
    return
        [matrix = std::move(matrix), buffers = std::move(buffers)](
            std::span<const Slice> slices) -> const std::vector<std::string>& {
            static const std::vector<std::string> empty;
            const std::vector<std::string>* common = nullptr;
            for (const auto& slice : slices) {
                if (slice.size == 0) continue;
                if (!slice.ptr) return empty;
                const auto address = reinterpret_cast<uintptr_t>(slice.ptr);
                std::string location;
                for (const auto& buffer : buffers) {
                    if (address < buffer.addr) continue;
                    const auto offset = address - buffer.addr;
                    if (offset >= buffer.length ||
                        slice.size > buffer.length - offset)
                        continue;
                    location = buffer.location;
                    SegmentsLocationInfo segments;
                    if (parseSegmentsLocation(location, segments)) {
                        location = resolveSegmentsLocation(
                            segments, buffer.length, offset);
                        if (location !=
                            resolveSegmentsLocation(segments, buffer.length,
                                                    offset + slice.size - 1))
                            return empty;
                    }
                    break;
                }
                const auto entry = matrix.find(location);
                if (entry == matrix.end() ||
                    entry->second.preferred_hca.empty())
                    return empty;
                if (!common)
                    common = &entry->second.preferred_hca;
                else if (*common != entry->second.preferred_hca)
                    return empty;
            }
            return common ? *common : empty;
        };
}

Client::HcaAffinity Client::MakeHcaAffinity() const {
    if (protocol_ != "rdma" || !StoreHcaAffinityEnabled() || !transfer_engine_)
        return {};
    const auto topology = transfer_engine_->getLocalTopology();
    std::vector<SegmentBufferInfo> buffers;
    if (!topology ||
        transfer_engine_->getSegmentBuffers(LOCAL_SEGMENT_ID, buffers) != 0)
        return {};
    return MakeHcaAffinity(*topology, std::move(buffers));
}

void Client::AttachHcaAffinity(ReplicateConfig& config,
                               const std::vector<Slice>& slices) const {
    if (config.preferred_hca_names) return;
    if (auto infer = MakeHcaAffinity()) {
        auto hcas = infer(slices);
        if (!hcas.empty()) config.preferred_hca_names = std::move(hcas);
    }
}

ErrorCode Client::ReadReplicaSelector::operator()(
    const std::vector<Replica::Descriptor>& replicas,
    std::span<const Slice> destinations, Replica::Descriptor& result) {
    // No topology copies, registration lookups or callback allocations for
    // disabled affinity, non-RDMA clients or single-copy reads.
    if (!StoreHcaAffinityEnabled() || client_->protocol_ != "rdma" ||
        replicas.size() <= 1) {
        return client_->FindFirstCompleteReplica(replicas, result);
    }
    if (!initialized_) {
        initialized_ = true;
        if (auto infer = client_->MakeHcaAffinity()) {
            context_ = std::make_unique<Context>(std::move(infer),
                                                 client_->GetLocalEndpoints());
        }
    }
    if (!context_) return client_->FindFirstCompleteReplica(replicas, result);
    const auto& hcas = context_->infer(destinations);
    if (hcas.empty())
        return client_->FindFirstCompleteReplica(replicas, result);
    const auto* selected =
        SelectBestReplica(replicas, context_->endpoints, hcas);
    if (!selected) return ErrorCode::INVALID_REPLICA;
    result = *selected;
    return ErrorCode::OK;
}

static std::vector<std::string> ParseDeviceNames(std::string_view value) {
    std::vector<std::string> devices;
    boost::split(devices, std::string(value), boost::is_any_of(","),
                 boost::token_compress_on);
    for (auto& device : devices) {
        device = std::string(TrimAsciiWhitespace(device));
    }
    return devices;
}

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry) {
    const auto delimiter_pos = master_server_entry.find("://");
    if (delimiter_pos == std::string::npos) {
        return std::optional<ha::HABackendSpec>{std::nullopt};
    }

    auto backend_type =
        ha::ParseHABackendType(master_server_entry.substr(0, delimiter_pos));
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ha::ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
        .type = backend_type.value(),
        .connstring = master_server_entry.substr(delimiter_pos + 3),
        .cluster_namespace = "",
    }};
}

tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
    const void* addr, size_t length, const std::string& protocol) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (auto limit = GetTransportRegistrationLimit(protocol);
        limit.has_value() && length > *limit) {
        LOG(ERROR) << "length " << length << " exceeds registration limit "
                   << *limit << " for protocol " << protocol;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    auto ha_backend_spec = ParseHABackendSpec(master_server_entry);
    if (!ha_backend_spec) {
        LOG(ERROR) << "Invalid HA backend entry: " << master_server_entry;
        return ha_backend_spec.error();
    }

    if (ha_backend_spec.value().has_value()) {
        auto coordinator =
            ha::CreateLeaderCoordinator(ha_backend_spec.value().value());
        if (!coordinator) {
            LOG(ERROR) << "Failed to create HA backend coordinator: "
                       << toString(coordinator.error());
            return coordinator.error();
        }

        auto master_view = ha::ReadCurrentViewOrWaitForReady(
            *coordinator.value(), kInitialLeaderReadyTimeout);
        if (!master_view) {
            LOG(ERROR) << "Failed to discover a ready master view: "
                       << toString(master_view.error());
            return master_view.error();
        }
        if (!master_view->has_value()) {
            // An absent ready view can mean either no elected leader or an
            // elected leader whose endpoint is still hidden during recovery.
            LOG(ERROR) << "No master became ready within "
                       << kInitialLeaderReadyTimeout.count() << " seconds";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }

        auto err = SwitchLeader(master_view->value());
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        leader_coordinator_ = std::move(coordinator.value());
        direct_master_address_.clear();
        if (!leader_monitor_running_.exchange(true)) {
            leader_monitor_thread_ =
                std::thread([this]() { this->LeaderMonitorThreadMain(); });
        }
        return ErrorCode::OK;
    } else {
        auto err = ConnectMasterEndpoint(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        direct_master_address_ = master_server_entry;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            current_master_view_.reset();
        }
        last_ping_success_.store(true);
        return ErrorCode::OK;
    }
}

void Client::EnterHaRuntimeMode() {
    if (!leader_coordinator_) {
        return;
    }

    // Foreground RPCs keep their normal retry policy. Only the HA heartbeat
    // and leader-readiness probes use the separately configured fast-fail
    // control pool once initialization has completed.
    master_client_.EnableHaConnectionPolicy();
}

ErrorCode Client::ConnectMasterEndpoint(const std::string& address) {
    if (!metrics_) return master_client_.Connect(address);

    return metrics_->master_heartbeat_metric.ObserveConnect(
        [this, &address] { return master_client_.Connect(address); });
}

ErrorCode Client::SwitchLeader(const ha::MasterView& target_view) {
    std::lock_guard<std::mutex> lock(leader_switch_mutex_);

    if (current_master_view_.has_value()) {
        const auto& current_view = current_master_view_.value();
        if (target_view.view_version < current_view.view_version) {
            return ErrorCode::OK;
        }

        if (target_view.view_version == current_view.view_version &&
            target_view.leader_address == current_view.leader_address &&
            last_ping_success_.load()) {
            return ErrorCode::OK;
        }
    }

    auto err = ConnectMasterEndpoint(target_view.leader_address);
    if (err != ErrorCode::OK) {
        last_ping_success_.store(false);
        return err;
    }

    current_master_view_ = target_view;
    last_ping_success_.store(true);
    return ErrorCode::OK;
}

void Client::LeaderMonitorThreadMain() {
    // WaitForViewChange now blocks on a backend watch and returns as soon as
    // leadership actually changes, so this timeout no longer drives polling
    // cadence -- it is only a periodic re-sync safety net that re-reads the
    // current view in case a watch event was missed (e.g. a connection blip).
    // Keeping it long (30s instead of 1s) collapses steady-state backend load
    // from ~1 read/sec/client to ~1 read/30s/client without affecting failover
    // responsiveness, which is driven by the watch.
    constexpr auto kViewChangeTimeout = std::chrono::milliseconds(30000);
    constexpr auto kErrorRetryInterval = std::chrono::milliseconds(1000);

    while (leader_monitor_running_.load()) {
        std::optional<ViewVersionId> known_version;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            if (current_master_view_.has_value()) {
                known_version = current_master_view_->view_version;
            }
        }

        auto view_change = leader_coordinator_->WaitForViewChange(
            known_version, kViewChangeTimeout);
        if (!view_change) {
            LOG(WARNING) << "Failed to wait for leader view change: "
                         << toString(view_change.error());
            std::this_thread::sleep_for(kErrorRetryInterval);
            continue;
        }

        if (!view_change->changed || !view_change->current_view.has_value()) {
            // A warming leader owns master_view without exposing a routable
            // endpoint. Acknowledge the empty observation so the next wait
            // blocks on the ready-value update instead of spinning on the old
            // view version.
            if (view_change->changed &&
                !view_change->current_view.has_value()) {
                std::lock_guard<std::mutex> lock(leader_switch_mutex_);
                // Do not let a stale empty watch result erase a newer view
                // installed concurrently by the heartbeat recovery path.
                const bool still_on_observed_view =
                    known_version.has_value()
                        ? current_master_view_.has_value() &&
                              current_master_view_->view_version ==
                                  known_version.value()
                        : !current_master_view_.has_value();
                if (still_on_observed_view) {
                    current_master_view_.reset();
                }
            }
            continue;
        }

        auto err = SwitchLeader(view_change->current_view.value());
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to switch to leader "
                         << view_change->current_view->leader_address << ": "
                         << toString(err);
            std::this_thread::sleep_for(kErrorRetryInterval);
        }
    }
}

void Client::EnsureStorageControlPlaneStarted() {
    if (!storage_heartbeat_running_.exchange(true)) {
        storage_heartbeat_thread_ =
            std::thread([this]() { this->StorageHeartbeatThreadMain(); });
    }

    if (!task_poll_running_.exchange(true)) {
        task_poll_thread_ =
            std::thread([this]() { this->TaskPollThreadMain(); });
    }
}

ErrorCode Client::InitTransferEngine(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    // TEs created through the Store entry (Client::Create ->
    // InitTransferEngine) are tagged so ascend_direct can resolve a per-role
    // link config (e.g. Store=RoCE/D2H, P2P=HCCS/D2D). The flag only needs to
    // be live while the ascend transport is installed below; reset it on every
    // exit path. Assumes TE inits are serialized within the process.
    struct StoreTeInitGuard {
        ~StoreTeInitGuard() { globalConfig().ascend_store_te_init = false; }
    } store_te_init_guard;
    if (protocol == "ascend") {
        globalConfig().ascend_store_te_init = true;
    }

    // Check if using TENT mode - TENT handles transport configuration
    // internally. Use the engine's own check rather than the raw env var:
    // if Mooncake was built without USE_TENT, the env var has no effect on
    // the TransferEngine, so the store must still install transports.
    bool use_tent = transfer_engine_->isUsingTent();

    bool auto_discover = false;
    if (!use_tent) {
        auto config = ClientAutoDiscoveryConfig::FromEnvironment(
            protocol, device_names.has_value());
        auto_discover = config.enabled;
        transfer_engine_->setAutoDiscover(
            {.enabled = auto_discover, .protocol = protocol});

        // Honor filters when auto-discovery is enabled; otherwise warn once
        if (auto_discover) {
            LOG(INFO)
                << "Transfer engine auto discovery is enabled for protocol: "
                << protocol;
        }

        config.LoadFiltersFromEnvironment();
        if (auto_discover) {
            transfer_engine_->setWhitelistFilters(std::move(config.filters));
        }
    }

    if (protocol == "ascend" || protocol == "ubshmem") {
        const char* ascend_use_fabric_mem =
            std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
        if (ascend_use_fabric_mem) {
            globalConfig().ascend_use_fabric_mem = true;
        }
    }
    if (protocol == "sunrise_link") {
        const char* sunrise_use_device_mem_env =
            std::getenv("SUNRISE_USE_DEVICE_MEM");
        if (sunrise_use_device_mem_env) {
            globalConfig().sunrise_use_device_mem = true;
            LOG(INFO) << "SUNRISE_USE_DEVICE_MEM enabled: segments will be "
                      << "allocated in device memory via tangMalloc";
        }
    }
    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_->init(metadata_connstring, local_hostname,
                                    hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    // TENT mode: Skip manual transport installation - TENT handles this
    // internally
    if (use_tent) {
        LOG(INFO)
            << "Using TENT mode - transport configuration handled internally";
        if (device_names.has_value()) {
            LOG(INFO)
                << "Note: device_names parameter is ignored in TENT mode. "
                << "Configure devices via TENT config file or environment "
                   "variables.";
        }
        return ErrorCode::OK;
    }

    if (!auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is disabled for protocol: "
                  << protocol;

        Transport* transport = nullptr;

        if (protocol == "rdma" || protocol == "efa") {
            if (!device_names.has_value() || device_names->empty()) {
                LOG(ERROR) << "RDMA protocol requires device names when auto "
                              "discovery is disabled";
                return ErrorCode::INVALID_PARAMS;
            }

            LOG(INFO) << "Using specified RDMA devices: "
                      << device_names.value();

            std::vector<std::string> devices =
                ParseDeviceNames(device_names.value());

            // Manually discover topology with specified devices only
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }

            transport = transfer_engine_->installTransport(protocol, nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install RDMA transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "tcp") {
            if (device_names.has_value()) {
                LOG(WARNING)
                    << "TCP protocol does not use device names, ignoring";
            }

            try {
                transport = transfer_engine_->installTransport("tcp", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "nvlink" || protocol == "ascend" ||
                   protocol == "ubshmem" || protocol == "sunrise_link") {
            if (device_names.has_value()) {
                LOG(WARNING) << protocol
                             << " protocol does not use device names, ignoring";
            }
            try {
                transport =
                    transfer_engine_->installTransport(protocol, nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << protocol
                           << "_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install " << protocol << " transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxl") {
            if (device_names.has_value()) {
                LOG(WARNING) << "CXL protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport = transfer_engine_->installTransport("cxl", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxl_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install CXL transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxi") {
            // Auto-discovery is RDMA-only, so populate the topology with the
            // CXI NICs before installing the transport (an empty device list
            // means "all NICs"); otherwise CxiTransport finds no devices.
            std::vector<std::string> devices;
            if (device_names.has_value() && !device_names->empty()) {
                devices = ParseDeviceNames(device_names.value());
            }
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete for CXI. Found "
                          << topology->getHcaList().size() << " HCAs";
            }
            try {
                transport = transfer_engine_->installTransport("cxi", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxi_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install CXI transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ub") {
            auto deviceName = device_names.value_or("bonding_dev_0");
            LOG(ERROR) << "ub protocol entable device names is " << deviceName;
            auto devices = ParseDeviceNames(deviceName);
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }
            transport = transfer_engine_->installTransport("ub", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install ub transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
            LOG(INFO) << "Using intra-NVLink protocol.";
            transport =
                transfer_engine_->installTransport("nvlink_intra", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install nvlink_intra transport.";
                return ErrorCode::INTERNAL_ERROR;
            }
#else
            LOG(ERROR)
                << "--protocol=nvlink_intra requires USE_INTRA_NVLINK=ON, "
                   "please rebuild mooncake from source.";
            return ErrorCode::INVALID_PARAMS;
#endif
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

void Client::InitTransferSubmitter() {
    // Initialize TransferSubmitter after transfer engine is ready
    // Keep using logical local_hostname for name-based behaviors; endpoint is
    // used separately where needed.
    // Unconditional; no NofWorkerPool is created when nof_initiator_ is null.
    const int numa_socket_id =
        ClientNumaConfig::FromEnvironment().socket_id.value_or(
            GetCurrentNumaSocketId());
    // ClientMetric::nof_metric exists only in USE_NOF builds.
    NofMetric* nof_metric = nullptr;
#ifdef USE_NOF
    nof_metric = metrics_ ? &metrics_->nof_metric : nullptr;
#endif
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr, numa_socket_id,
        nof_initiator_, nof_metric);
}

void Client::SetDefaultNofInitiatorFactoryForTesting(
    std::function<std::shared_ptr<NVMeoFInitiator>()> factory) {
    DefaultNofInitiatorFactoryForTestingSlot() = std::move(factory);
}

std::shared_ptr<NVMeoFInitiator> Client::GetNofInitiatorForTesting() const {
    return nof_initiator_;
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::optional<std::string>& device_names,
    const std::string& master_server_entry,
    const std::shared_ptr<TransferEngine>& transfer_engine,
    std::map<std::string, std::string> labels, const std::string& tenant_id,
    std::shared_ptr<NVMeoFInitiator> nof_initiator) {
    // Reused engines retain their metadata mode. In particular, P2PHANDSHAKE
    // must publish the engine's actual endpoint rather than the Store hostname.
    const auto& resolved_metadata =
        transfer_engine && metadata_connstring.empty()
            ? transfer_engine->getMetadataConnectionString()
            : metadata_connstring;
    auto client = std::shared_ptr<Client>(new Client(
        local_hostname, resolved_metadata, protocol, labels, tenant_id));
    if (!nof_initiator) {
        // nullptr means "use the process default", not "disable NoF" —
        // existing callers keep the pre-parameter behavior. Explicit disable
        // is MC_NOF_BACKEND=none; explicit injection is a non-null initiator.
        auto& factory = DefaultNofInitiatorFactoryForTestingSlot();
        nof_initiator = factory ? factory() : CreateNofRuntime().initiator;
    }
    client->nof_initiator_ = std::move(nof_initiator);

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize storage backend if storage_root_dir is valid
    auto config_response = client->master_client_.GetStorageConfig();
    if (!config_response) {
        LOG(ERROR) << "Failed to get storage config from master";
        // Fallback to GetFsdir for backward compatibility
        auto response = client->master_client_.GetFsdir();
        if (!response) {
            LOG(ERROR) << "Failed to get fsdir from master";
        } else if (response.value().empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            auto dir_string = response.value();
            size_t pos = dir_string.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = dir_string.substr(0, pos);
                std::string fs_subdir = dir_string.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                // Initialize storage backend with default eviction settings
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, true, 0);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via fsdir but is "
                           "unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
                return std::nullopt;
            }
        }
    } else {
        auto config = config_response.value();
        if (config.fsdir.empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            size_t pos = config.fsdir.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = config.fsdir.substr(0, pos);
                std::string fs_subdir = config.fsdir.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                LOG(INFO) << "Disk eviction enabled: "
                          << config.enable_disk_eviction;
                LOG(INFO) << "Quota bytes: " << config.quota_bytes;
                // Initialize storage backend with config from master
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, config.enable_disk_eviction,
                    config.quota_bytes);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via storage config "
                           "but is unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
                return std::nullopt;
            }
        }
    }

    client->EnterHaRuntimeMode();

    // this only performs RPC calls
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return client;
    }

    // Initialize transfer engine
    if (transfer_engine == nullptr) {
        client->transfer_engine_ = std::make_shared<TransferEngine>();
        err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                         protocol, device_names);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return std::nullopt;
        }
    } else {
        client->transfer_engine_ = transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }

    client->InitTransferSubmitter();
    // Initialize local hot cache
    err = client->InitLocalHotCache();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize local hot cache";
    }

    return client;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
Client::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto result = master_client_.BatchQueryIp(client_ids);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
Client::QueryByRegex(const std::string& str) {
    auto result = master_client_.GetReplicaListByRegex(str);
    return result;
}

tl::expected<QueryResult, ErrorCode> Client::Query(
    const std::string& object_key) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaList(object_key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return tl::expected<QueryResult, ErrorCode>(
        tl::in_place, std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(result.value().lease_ttl_ms),
        result.value().object_checksum, result.value().resolved_tenant_id);
}

tl::expected<QueryResult, ErrorCode> Client::QueryReadOnly(
    const std::string& object_key) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaListReadOnly(object_key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    // The admin read-only path grants no lease: pin the expiry to now so the
    // caller cannot mistake this metadata for a lease-protected one.
    return QueryResult(std::move(result.value().replicas), start_time,
                       result.value().object_checksum);
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys) {
    return BatchQuery(object_keys, master_client_.tenant_id());
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys, const std::string& tenant_id) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_.BatchGetReplicaList(object_keys, tenant_id);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            results.emplace_back(
                tl::in_place, std::move(response[i].value().replicas),
                start_time +
                    std::chrono::milliseconds(response[i].value().lease_ttl_ms),
                response[i].value().object_checksum,
                response[i].value().resolved_tenant_id);
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQueryReadOnly(
    const std::vector<std::string>& object_keys) {
    return BatchQueryReadOnly(object_keys, master_client_.tenant_id());
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQueryReadOnly(
    const std::vector<std::string>& object_keys, const std::string& tenant_id) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response =
        master_client_.BatchGetReplicaListReadOnly(object_keys, tenant_id);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQueryReadOnly response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            // No lease granted on the read-only path: expiry pinned to now.
            results.emplace_back(
                QueryResult(std::move(response[i].value().replicas), start_time,
                            response[i].value().object_checksum));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<uint64_t, ErrorCode> Client::ComputeObjectChecksumForSlices(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size) {
    CrcChecksum checksum;
    size_t remaining = object_size;
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    std::unique_ptr<ScopedObjectChecksumBuffer> staging;

    for (const auto& slice : slices) {
        const size_t bytes = std::min(slice.size, remaining);
        if (bytes == 0) {
            continue;
        }
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "object_checksum_null_slice key=" << object_key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (!runtime_accelerator.FindDeviceForPointer(slice.ptr)) {
            checksum.Update(slice.ptr, bytes);
            remaining -= bytes;
            continue;
        }

        if (!staging) {
            staging = std::make_unique<ScopedObjectChecksumBuffer>(
                *pinned_buffer_pool_,
                std::min(kObjectChecksumD2HChunkSize, object_size));
        }
        size_t offset = 0;
        while (offset < bytes) {
            const size_t chunk =
                std::min(kObjectChecksumD2HChunkSize, bytes - offset);
            const auto* source = static_cast<const char*>(slice.ptr) + offset;
            if (!runtime_accelerator.CopyToHost(staging->data(), source,
                                                chunk)) {
                LOG(ERROR) << "object_checksum_d2h_failed key=" << object_key
                           << " size=" << chunk;
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
            checksum.Update(staging->data(), chunk);
            offset += chunk;
        }
        remaining -= bytes;
    }

    if (remaining != 0) {
        LOG(ERROR) << "object_checksum_slices_too_small key=" << object_key
                   << " missing_bytes=" << remaining;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return checksum.Finalize();
}

tl::expected<void, ErrorCode> Client::VerifyObjectChecksum(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size, std::optional<uint64_t> expected_checksum) {
    if (!object_checksum_enabled_) {
        return {};
    }
    if (!expected_checksum.has_value()) {
        VLOG(1) << "object_checksum_absent key=" << object_key;
        return {};
    }

    auto actual_checksum =
        ComputeObjectChecksumForSlices(object_key, slices, object_size);
    if (!actual_checksum) {
        return tl::unexpected(actual_checksum.error());
    }
    if (*actual_checksum != *expected_checksum) {
        LOG(ERROR) << "object_checksum_mismatch key=" << object_key
                   << " expected=" << *expected_checksum
                   << " actual=" << *actual_checksum;
        return tl::unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

ErrorCode Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                        const std::string& fsdir,
                                        bool enable_eviction,
                                        uint64_t quota_bytes) {
    // Initialize storage backend
    auto backend_result =
        StorageBackend::Create(storage_root_dir, fsdir, enable_eviction);
    if (!backend_result) {
        LOG(ERROR) << "Failed to create storage backend: "
                   << backend_result.error();
        return backend_result.error();
    }
    // Initialize into a local first and only publish to storage_backend_
    // after a successful Init(): users of storage_backend_ only null-check
    // it, so it must never point to a backend whose Init() failed (using it
    // before successful Init() is undefined behavior). If Init() throws,
    // stack unwinding destroys the local and storage_backend_ stays clean.
    auto backend = std::move(backend_result.value());
    auto init_result = backend->Init(quota_bytes);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize StorageBackend. Error: "
                   << init_result.error() << ". The backend will be unusable.";
        return init_result.error();
    }
    storage_backend_ = std::move(backend);
    return ErrorCode::OK;
}
tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs()) {
        auto results = BatchRemove({key}, force);
        if (results.size() != 1)
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        return std::move(results.front());
    }

    if (hot_cache_) {
        hot_cache_->BumpKeyGeneration(key);
    }
    auto result = master_client_.Remove(key, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveFile(key);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }
    return {};
}

tl::expected<long, ErrorCode> Client::RemoveByRegex(const ObjectKey& str,
                                                    bool force) {
    // KVCS has no provider-side key listing or conditional bulk delete yet.
    // Removing only Master metadata would skip provider cleanup.
    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs())
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveByRegex(str, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveByRegex(str);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    if (result.value() > 0 && hot_cache_) {
        hot_cache_->BumpCacheEpoch();
        hot_cache_->RemoveHotKeysByRegex(str);
    }
    return result.value();
}

tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs())
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveAll(force);
    // The master can retain protected objects. Local disk cleanup must use
    // per-object eviction, not an unconditional wipe of this client's files.
    if (result && result.value() > 0 && hot_cache_) {
        hot_cache_->RemoveAllHotKeys();
    }
    return result;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    if (hot_cache_) {
        hot_cache_->BumpKeyGenerations(keys);
    }

    auto results = master_client_.BatchRemove(keys, force);

    const size_t result_count = std::min(keys.size(), results.size());
    std::vector<std::string> removed_keys;
    std::vector<size_t> removed_indices;
    removed_keys.reserve(result_count);
    removed_indices.reserve(result_count);
    for (size_t i = 0; i < result_count; ++i) {
        if (results[i]) {
            removed_keys.emplace_back(keys[i]);
            removed_indices.push_back(i);
        }
    }

    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs() &&
        !removed_keys.empty()) {
        const auto deleted =
            dfs_storage_backend_->BatchDeleteObjects(removed_keys);
        if (deleted.size() != removed_keys.size()) {
            for (size_t index : removed_indices) {
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        } else {
            for (size_t i = 0; i < deleted.size(); ++i) {
                if (!deleted[i]) results[removed_indices[i]] = deleted[i];
            }
        }
    }

    if (hot_cache_ && !removed_keys.empty())
        hot_cache_->RemoveHotKeys(removed_keys);

    return results;
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, replica_type);
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, tenant_id, replica_type);
}

tl::expected<bool, ErrorCode> Client::EvictDiskReplicaIfCurrent(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type, ReplicaID expected_replica_id) {
    return master_client_.EvictDiskReplicaIfCurrent(
        key, tenant_id, replica_type, expected_replica_id);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, tenant_id, replica_type);
}

std::vector<int> Client::GetNicNumaNodes() const {
    std::set<int> nodes;
    if (!transfer_engine_) return {};
    auto topo = transfer_engine_->getLocalTopology();
    if (!topo) return {};
    for (auto& [name, entry] : topo->getMatrix()) {
        if (name.rfind("cpu:", 0) != 0 || entry.preferred_hca.empty()) continue;
        int node = std::stoi(name.substr(4));
        nodes.insert(node);
    }
    return {nodes.begin(), nodes.end()};
}

tl::expected<void, ErrorCode> Client::MountSegment(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto result = MountSegmentAndGetId(buffer, size, protocol, location);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegmentImpl(
    std::unordered_map<UUID, Segment, boost::hash<UUID>>::iterator it) {
    auto unmount_result = master_client_.UnmountSegment(it->second.id);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    if (gather_service_)
        gather_service_->removeSource(reinterpret_cast<void*>(it->second.base));
    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(it->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer
        // engine, we can continue
    }

    mounted_segments_.erase(it);
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return UnmountSegmentImpl(segment);
}

tl::expected<UUID, ErrorCode> Client::MountSegmentAndGetId(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto check_result = CheckRegisterMemoryParams(buffer, size, protocol);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    UUID segment_id;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

        // Check if the segment overlaps with any existing segment
        for (auto& it : mounted_segments_) {
            auto& mtseg = it.second;
            uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
            uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
            uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
            uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
            if (std::max(l1, l2) < std::min(r1, r2)) {
                LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                           << " size1=" << mtseg.size << " base2=" << buffer
                           << " size2=" << size;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
        }

        int rc = transfer_engine_->registerLocalMemory((void*)buffer, size,
                                                       location, true, true);
        if (rc != 0) {
            LOG(ERROR) << "register_local_memory_failed base=" << buffer
                       << " size=" << size << ", error=" << rc;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        Segment segment;
        segment.id = generate_uuid();
        segment.name = local_hostname_;
        segment.base = reinterpret_cast<uintptr_t>(buffer);
        segment.size = size;
        segment.protocol = protocol;
        segment.host_id = host_id_;
        if (protocol == "rdma" && StoreHcaAffinityEnabled()) {
            if (auto topology = transfer_engine_->getLocalTopology()) {
                segment.locality = DiscoverSegmentLocality(*topology, location);
            }
        }
        if (metadata_connstring_ == P2PHANDSHAKE) {
            segment.te_endpoint = transfer_engine_->getLocalIpAndPort();
        } else {
            segment.te_endpoint = local_hostname_;
        }

        // Only CPU memory on classic RDMA owners enables gather.
        // Service setup failure leaves the existing direct path available.
        if (SupportsGatherRead() && protocol == "rdma" &&
            !device::GetAcceleratorRegistry()
                 .RuntimeAccelerators()
                 .FindDeviceForPointer(buffer)) {
            try {
                if (!gather_service_) {
                    auto service = std::make_unique<store::GatherReadService>(
                        transfer_engine_, store::GatherReadOptions{},
                        GetSegmentEndpoint());
                    service->start("0.0.0.0");
                    auto gather_endpoint = buildHostNameWithPort(
                        getHostNameWithoutPort(
                            transfer_engine_->getLocalIpAndPort()),
                        service->port());
                    // Let the initial publish finish before heartbeats can
                    // see the endpoint and schedule a refresh.
                    (void)master_client_.PublishGatherEndpoint(
                        GetSegmentEndpoint(), gather_endpoint);
                    gather_service_ = std::move(service);
                    std::lock_guard<std::mutex> endpoint_lock(
                        gather_endpoint_mutex_);
                    gather_endpoint_ = std::move(gather_endpoint);
                }
                gather_service_->addSource(const_cast<void*>(buffer), size);
            } catch (const std::exception& e) {
                LOG(WARNING) << "Store gather unavailable: " << e.what();
            }
        }
        auto mount_result = master_client_.MountSegment(segment);
        if (!mount_result) {
            ErrorCode err = mount_result.error();
            if (gather_service_)
                gather_service_->removeSource(const_cast<void*>(buffer));
            LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                       << " size=" << size << ", error=" << err;
            return tl::unexpected(err);
        }

        segment_id = segment.id;
        mounted_segments_[segment_id] = segment;
    }

    EnsureStorageControlPlaneStarted();
    return segment_id;
}

tl::expected<void, ErrorCode> Client::UnmountSegmentById(
    const UUID& segment_id, uint64_t grace_period_ms,
    std::function<void(const UUID&)> cleanup_callback) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.find(segment_id);
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found id=" << UuidToString(segment_id);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (grace_period_ms == 0) {
        return UnmountSegmentImpl(segment);
    }

    auto result =
        master_client_.GracefulUnmountSegment(segment_id, grace_period_ms);
    if (!result) {
        ErrorCode err = result.error();
        LOG(ERROR) << "Failed to graceful unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    gracefully_unmounting_segments_.emplace(segment->first, segment->second);
    if (cleanup_callback) {
        graceful_unmount_cleanup_callbacks_[segment->first] =
            std::move(cleanup_callback);
    }
    mounted_segments_.erase(segment);
    StartGracefulUnmountTimer(segment_id, grace_period_ms);
    return {};
}

bool Client::WaitForGracefulUnmountDelay(std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(graceful_unmount_timer_mutex_);
    return graceful_unmount_timer_cv_.wait_for(
        lock, delay, [this]() { return graceful_unmount_timer_stopping_; });
}

void Client::StartGracefulUnmountTimer(const UUID& segment_id,
                                       uint64_t grace_period_ms) {
    auto delay =
        std::chrono::milliseconds(grace_period_ms) + std::chrono::seconds(10);
    task_thread_pool_.enqueue([this, segment_id, delay]() {
        if (this->WaitForGracefulUnmountDelay(delay)) {
            return;
        }
        this->OnGracefulUnmountTimer(segment_id, /*retry_left=*/3);
    });
}

void Client::OnGracefulUnmountTimer(const UUID& segment_id, int retry_left) {
    // Query master to confirm the segment has been removed
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        auto it = gracefully_unmounting_segments_.find(segment_id);
        if (it == gracefully_unmounting_segments_.end()) {
            // Already cleaned up (e.g. by destructor)
            return;
        }
    }

    auto status = master_client_.QuerySegmentStatusById(segment_id);
    bool removed = false;
    if (!status) {
        if (status.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            removed = true;
        } else {
            LOG(WARNING) << "Failed to query graceful unmount segment status: "
                         << toString(status.error());
        }
    } else if (status.value() == SegmentStatus::UNDEFINED) {
        removed = true;
    }

    if (removed) {
        std::function<void(const UUID&)> cleanup_callback;
        {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            auto it = gracefully_unmounting_segments_.find(segment_id);
            if (it != gracefully_unmounting_segments_.end()) {
                if (gather_service_)
                    gather_service_->removeSource(
                        reinterpret_cast<void*>(it->second.base));
                int rc = transfer_engine_->unregisterLocalMemory(
                    reinterpret_cast<void*>(it->second.base));
                if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(ERROR)
                        << "Failed to unregister TE MR for graceful unmount: "
                        << rc;
                }
                gracefully_unmounting_segments_.erase(it);
            }
            auto callback_it =
                graceful_unmount_cleanup_callbacks_.find(segment_id);
            if (callback_it != graceful_unmount_cleanup_callbacks_.end()) {
                cleanup_callback = std::move(callback_it->second);
                graceful_unmount_cleanup_callbacks_.erase(callback_it);
            }
        }
        if (cleanup_callback) {
            cleanup_callback(segment_id);
        }
        return;
    }

    if (retry_left > 0) {
        try {
            task_thread_pool_.enqueue([this, segment_id, retry_left]() {
                if (this->WaitForGracefulUnmountDelay(
                        std::chrono::seconds(10))) {
                    return;
                }
                this->OnGracefulUnmountTimer(segment_id, retry_left - 1);
            });
        } catch (const std::runtime_error& e) {
            VLOG(1) << "Skip graceful unmount retry enqueue: " << e.what();
        }
    } else {
        LOG(WARNING) << "Graceful unmount cleanup timeout for segment "
                     << UuidToString(segment_id);
    }
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length, protocol_);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    const int rc =
        transfer_engine_->unregisterLocalMemory(addr, update_metadata);
    if (rc != 0) {
        // Distinguish "not registered" from real failures so callers can
        // tell an already-cleaned buffer apart from a live registration
        // whose deregistration failed.
        return tl::unexpected(rc == ERR_ADDRESS_NOT_REGISTERED
                                  ? ErrorCode::NOT_REGISTERED
                                  : ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
    return result;
}

tl::expected<bool, ErrorCode> Client::ProbeKey(const std::string& key) {
    auto result = master_client_.ProbeKey(key);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct
    // format
    return response;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchProbeKey(
    const std::vector<std::string>& keys, const GrantLeasePolicy& policy) {
    auto response = master_client_.BatchProbeKey(keys, policy);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchProbeKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    return response;
}

WeightRpcResult<WeightRevisionMetadata> Client::BeginWeightImport(
    const BeginWeightImportRequest& request) {
    return master_client_.BeginWeightImport(request);
}

WeightRpcResult<WeightRevisionMetadata> Client::CommitWeightImport(
    const CommitWeightImportRequest& request) {
    return master_client_.CommitWeightImport(request);
}

WeightRpcResult<WeightRevisionMetadata> Client::AbortWeightImport(
    const AbortWeightImportRequest& request) {
    return master_client_.AbortWeightImport(request);
}

WeightRpcResult<WeightRevisionView> Client::GetWeightRevision(
    const GetWeightRevisionRequest& request) {
    return master_client_.GetWeightRevision(request);
}

WeightRpcResult<ListWeightRevisionsResponse> Client::ListWeightRevisions(
    const ListWeightRevisionsRequest& request) {
    return master_client_.ListWeightRevisions(request);
}

WeightRpcResult<WeightRevisionLease> Client::AcquireWeightRevisionLease(
    const AcquireWeightRevisionLeaseRequest& request) {
    return master_client_.AcquireWeightRevisionLease(request);
}

WeightRpcResult<WeightRevisionLease> Client::RenewWeightRevisionLease(
    const RenewWeightRevisionLeaseRequest& request) {
    return master_client_.RenewWeightRevisionLease(request);
}

WeightRpcResult<void> Client::ReleaseWeightRevisionLease(
    const ReleaseWeightRevisionLeaseRequest& request) {
    return master_client_.ReleaseWeightRevisionLease(request);
}

WeightRpcResult<WeightResidencyOperation> Client::StartWeightResidencyOperation(
    const StartWeightResidencyOperationRequest& request) {
    return master_client_.StartWeightResidencyOperation(request);
}

WeightRpcResult<WeightResidencyOperation> Client::QueryWeightOperation(
    const QueryWeightOperationRequest& request) {
    return master_client_.QueryWeightOperation(request);
}

WeightRpcResult<WeightRevisionMetadata> Client::DeleteWeightRevision(
    const DeleteWeightRevisionRequest& request) {
    return master_client_.DeleteWeightRevision(request);
}

void* Client::GetBaseAddr() { return transfer_engine_->getBaseAddr(); }

void Client::StorageHeartbeatThreadMain() {
    // How many failed pings before reconnecting via the HA coordinator
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is
        // unmounted successfully first, and then remounted again in
        // this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.emplace_back(segment);
        }
        auto remount_result = master_client_.ReMountSegment(segments);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
        // Re-publish Transfer Engine segment descriptors to the HTTP
        // metadata server.  When Master (which hosts the HTTP metadata
        // server in the same process) is killed and restarted, all
        // in-memory KV entries are lost.  ReMountSegment above only
        // restores Master-side allocation state; it does NOT write back
        // the transport-level segment descriptors.  Without this, remote
        // peers get HTTP 404 when querying our segment descriptor and
        // data transfers fail.
        auto metadata = transfer_engine_->getMetadata();
        if (metadata) {
            int rc = metadata->updateLocalSegmentDesc();
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish segment descriptor "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                segment_desc_publish_pending_.store(true);
            } else {
                segment_desc_publish_pending_.store(false);
            }
            // Also re-publish RPC meta entry (mooncake/rpc_meta/<hostname>).
            // Remote peers need this to locate our RDMA RPC port for
            // handshake.  Like segment descriptors, this entry is lost
            // when the HTTP metadata server is cleared on Master restart.
            rc = metadata->rePublishRpcMetaEntry(local_hostname_);
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish RPC meta entry "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                rpc_meta_publish_pending_.store(true);
            } else {
                rpc_meta_publish_pending_.store(false);
            }
        }
        // Note: LOCAL_DISK segment remount is NOT done here.
        // It is handled by FileStorage::Heartbeat() when it detects
        // SEGMENT_NOT_FOUND, which also triggers ScanMeta to
        // re-register offloaded object metadata.
    };
    // Remounts and metadata retries share one slot.
    std::future<void> remount_segment_future;
    std::atomic<bool> gather_refresh_done{true};
    std::thread gather_refresh_thread;

    auto retry_metadata = [this]() {
        auto metadata = transfer_engine_->getMetadata();
        if (!metadata) return;

        if (segment_desc_publish_pending_.load()) {
            int rc = metadata->updateLocalSegmentDesc();
            if (rc != 0) {
                LOG(ERROR) << "Retry: failed to re-publish segment "
                           << "descriptor to metadata server, rc=" << rc;
            } else {
                LOG(INFO) << "Retry: successfully re-published segment "
                          << "descriptor to metadata server";
                segment_desc_publish_pending_.store(false);
            }
        }
        if (rpc_meta_publish_pending_.load()) {
            int rc = metadata->rePublishRpcMetaEntry(local_hostname_);
            if (rc != 0) {
                LOG(ERROR) << "Retry: failed to re-publish RPC "
                           << "meta entry to metadata server, rc=" << rc;
            } else {
                LOG(INFO) << "Retry: successfully re-published "
                          << "RPC meta entry to metadata server";
                rpc_meta_publish_pending_.store(false);
            }
        }
    };

    while (storage_heartbeat_running_.load()) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }
        auto ping_result = metrics_
                               ? metrics_->master_heartbeat_metric.ObservePing(
                                     [this] { return master_client_.Ping(); })
                               : master_client_.Ping();
        if (ping_result) {
            // Skip refreshes while a publication is still in flight.
            if (gather_refresh_done.load()) {
                if (gather_refresh_thread.joinable()) {
                    gather_refresh_thread.join();
                }
                std::string endpoint;
                {
                    std::lock_guard<std::mutex> lock(gather_endpoint_mutex_);
                    endpoint = gather_endpoint_;
                }
                if (!endpoint.empty()) {
                    gather_refresh_done.store(false);
                    gather_refresh_thread =
                        std::thread([this, endpoint = std::move(endpoint),
                                     &gather_refresh_done] {
                            try {
                                (void)master_client_.PublishGatherEndpoint(
                                    GetSegmentEndpoint(), endpoint);
                            } catch (const std::exception& e) {
                                LOG(WARNING)
                                    << "Failed to refresh gather endpoint: "
                                    << e.what();
                            }
                            gather_refresh_done.store(true);
                        });
                }
            }
            // Reset ping failure count
            ping_fail_count = 0;
            last_ping_success_.store(true);
            auto& ping_response = ping_result.value();
            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            } else if ((segment_desc_publish_pending_.load() ||
                        rpc_meta_publish_pending_.load()) &&
                       !remount_segment_future.valid()) {
                // Serialize retries with remount so an older attempt cannot
                // clear the pending status of a newer publication.
                remount_segment_future =
                    std::async(std::launch::async, retry_metadata);
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        last_ping_success_.store(false);
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Exceeded ping failure threshold. Reconnect based on mode.
        if (leader_coordinator_) {
            LOG(ERROR)
                << "Failed to ping master for " << ping_fail_count
                << " times; fetching latest master view and reconnecting";
            auto current_view = leader_coordinator_->ReadCurrentView();
            if (!current_view) {
                LOG(ERROR) << "Failed to get new master view: "
                           << toString(current_view.error());
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            if (!current_view.value().has_value()) {
                LOG(WARNING) << "No active master view is published yet";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            const auto& next_view = current_view.value().value();
            auto err = SwitchLeader(next_view);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to master "
                           << next_view.leader_address << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            LOG(INFO) << "Reconnected to master " << next_view.leader_address;
            ping_fail_count = 0;
        } else {
            const std::string current_master_address = direct_master_address_;
            LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                       << " times (non-HA); reconnecting to "
                       << current_master_address;
            auto err = ConnectMasterEndpoint(current_master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Reconnect failed to " << current_master_address
                           << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            LOG(INFO) << "Reconnected to master " << current_master_address;
            last_ping_success_.store(true);
            ping_fail_count = 0;
        }
    }
    if (gather_refresh_thread.joinable()) {
        gather_refresh_thread.join();
    }
    // Finish remounts and metadata retries before destroying the client.
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) const {
    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            replica = replica_list[i];
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

tl::expected<Replica::Descriptor, ErrorCode> Client::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list) {
    if (replica_list.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted_segments_.empty() || replica_list.size() == 1) {
        return replica_list[0];
    }

    std::unordered_set<std::string> local_endpoints;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        for (const auto& [segment_id, segment] : mounted_segments_) {
            local_endpoints.insert(segment.te_endpoint);
        }
    }

    // Prefer local MEMORY replicas first
    for (const auto& rep : replica_list) {
        if (rep.is_memory_replica()) {
            const auto& mem_desc = rep.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    // Then prefer local NOF_SSD replicas
    for (const auto& rep : replica_list) {
        if (rep.is_nof_replica()) {
            const auto& nof_desc = rep.get_nof_descriptor();
            const std::string& endpoint =
                nof_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    return replica_list[0];
}

ErrorCode Client::InitLocalHotCache() {
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
    admission_sketch_.reset();

    if (object_checksum_enabled_) {
        return ErrorCode::OK;
    }

    const auto config = LocalHotCacheConfig::FromEnvironment();
    if (config.total_size_bytes == 0) {
        return ErrorCode::OK;
    }

    size_t thread_num = 2;

    // Enable hot cache
    {
        hot_cache_ = std::make_shared<LocalHotCache>(
            config.total_size_bytes, config.block_size_bytes, config.use_shm);
        // Check if cache initialization was successful
        if (hot_cache_->GetCacheSize() == 0) {
            LOG(ERROR)
                << "Local hot cache creation failed: no blocks allocated. "
                << "total_cache=" << config.total_size_bytes;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            return ErrorCode::INVALID_PARAMS;
        }

        // For shm-backed (HugeTLB) hot caches, ShmHelper::allocate rounds the
        // mapping up to hugepage granularity while GetTotalSize() reports the
        // logical size, so registering the logical size could end an MR inside
        // the last huge page (madvise MADV_DONTFORK then fails with EINVAL).
        // Register the full segment instead, same rule as
        // RealClient::register_buffer_internal.
        size_t hot_cache_reg_size = hot_cache_->GetTotalSize();
        if (auto shm_seg = hot_cache_->GetShmSegment()) {
            hot_cache_reg_size = shm_seg->size;
        }
        int rc = transfer_engine_->registerLocalMemory(
            hot_cache_->GetBaseAddress(), hot_cache_reg_size, kWildcardLocation,
            true, true);
        if (rc != 0) {
            LOG(ERROR)
                << "Failed to register local hot cache memory with transfer "
                   "engine, base="
                << hot_cache_->GetBaseAddress()
                << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            hot_cache_memory_registered_ = false;
            return ErrorCode::INVALID_PARAMS;
        }
        hot_cache_memory_registered_ = true;

        LOG(INFO) << "Local hot cache enabled with cache size="
                  << config.total_size_bytes
                  << ", block size=" << config.block_size_bytes
                  << ", block amount=" << hot_cache_->GetCacheSize()
                  << ", shm=" << (config.use_shm ? "on" : "off")
                  << ", transfer engine registered=on";
        // Create async handler with 2 worker threads
        hot_cache_handler_ =
            std::make_unique<LocalHotCacheHandler>(hot_cache_, thread_num);
        admission_sketch_ = std::make_unique<CountMinSketch>();
        admission_threshold_ = config.admission_threshold;
    }
    return ErrorCode::OK;
}

void Client::UnregisterLocalHotCacheMemory() {
    if (!(hot_cache_ && hot_cache_memory_registered_)) {
        return;
    }
    if (!transfer_engine_) {
        hot_cache_memory_registered_ = false;
        return;
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        hot_cache_->GetBaseAddress(), true);
    if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
        LOG(ERROR)
            << "Failed to unregister local hot cache memory from transfer "
               "engine, base="
            << hot_cache_->GetBaseAddress()
            << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
    }
    hot_cache_memory_registered_ = false;
}

}  // namespace mooncake
