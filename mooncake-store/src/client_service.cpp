#include "client_service.h"

#include <glog/logging.h>

#include "allocator.h"
#include "segment.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iomanip>
#include <limits>
#ifdef USE_NOF
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <ylt/struct_json/json_reader.h>

#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.hpp"
#include "utils.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "gpu_staging_utils.h"

namespace mooncake {

using gpu_staging::CopyDeviceToHost;
using gpu_staging::IsDevicePointer;
using gpu_staging::SetDevice;

namespace {

#ifdef USE_NOF
std::optional<int> GetConfiguredNumaSocketId() {
    const char* raw_value = std::getenv("MC_STORE_NUMA_SOCKET_ID");
    if (!raw_value || raw_value[0] == '\0') {
        return std::nullopt;
    }

    char* end_ptr = nullptr;
    errno = 0;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed < 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid MC_STORE_NUMA_SOCKET_ID=" << raw_value
                     << ", falling back to auto-detect";
        return std::nullopt;
    }

    return static_cast<int>(parsed);
}

int GetCurrentNumaSocketId() {
    if (numa_available() < 0) {
        return 0;
    }
    int cpu = sched_getcpu();
    if (cpu < 0) {
        return 0;
    }
    int node = numa_node_of_cpu(cpu);
    return node < 0 ? 0 : node;
}
#endif

struct ContiguousSliceRange {
    void* ptr = nullptr;
    size_t size = 0;
};

std::optional<ContiguousSliceRange> GetContiguousSliceRange(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }

    uintptr_t expected_ptr = 0;
    uintptr_t start_ptr = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) {
            return std::nullopt;
        }

        const auto current_ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        if (i == 0) {
            start_ptr = current_ptr;
            expected_ptr = current_ptr;
        } else if (current_ptr != expected_ptr) {
            return std::nullopt;
        }

        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return std::nullopt;
        }
        if (slice.size > std::numeric_limits<uintptr_t>::max() - current_ptr) {
            return std::nullopt;
        }

        total_size += slice.size;
        expected_ptr = current_ptr + slice.size;
    }

    if (total_size == 0) {
        return std::nullopt;
    }

    return ContiguousSliceRange{.ptr = reinterpret_cast<void*>(start_ptr),
                                .size = total_size};
}

struct ReplicaTransferSummary {
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    size_t successful_memory_transfers = 0;
    size_t successful_nof_transfers = 0;
    size_t failed_memory_transfers = 0;
    size_t failed_nof_transfers = 0;
    ErrorCode first_error = ErrorCode::OK;

    void RecordAllocatedReplica(const Replica::Descriptor& replica) {
        if (replica.is_memory_replica()) {
            ++allocated_memory_replicas;
        } else if (replica.is_nof_replica()) {
            ++allocated_nof_replicas;
        }
    }

    void RecordSuccess(ReplicaType replica_type) {
        if (replica_type == ReplicaType::MEMORY) {
            ++successful_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++successful_nof_transfers;
        }
    }

    void RecordFailure(ReplicaType replica_type, ErrorCode error) {
        if (replica_type == ReplicaType::MEMORY) {
            ++failed_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++failed_nof_transfers;
        }
        if (first_error == ErrorCode::OK) {
            first_error = error;
        }
    }
};

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  const ReplicaTransferSummary& summary) {
    if (config.nof_replica_num == 0) {
        return summary.allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return summary.allocated_memory_replicas +
                   summary.allocated_nof_replicas >
               0;
    }
    return summary.allocated_memory_replicas == config.replica_num &&
           summary.allocated_nof_replicas == config.nof_replica_num;
}

// success describes whether the overall put should succeed. Reliable modes
// require all allocated replicas to complete. Flexible dual-replica mode only
// requires one replica type to succeed, so success may be true while
// revoke_type is also set for the failed side.
struct FinalizeDecision {
    std::optional<ReplicaType> end_type;
    std::optional<ReplicaType> revoke_type;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

FinalizeDecision DetermineFinalizeDecision(
    const ReplicateConfig& config, const ReplicaTransferSummary& summary) {
    const auto write_mode = DetermineReplicaWriteMode(config);
    const bool allocation_satisfied =
        HasExpectedReplicaAllocation(config, summary);

    if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        const bool all_transfers_succeeded =
            summary.successful_memory_transfers ==
                summary.allocated_memory_replicas &&
            summary.successful_nof_transfers ==
                summary.allocated_nof_replicas &&
            summary.failed_memory_transfers == 0 &&
            summary.failed_nof_transfers == 0;
        if (allocation_satisfied && all_transfers_succeeded) {
            return {.end_type = ReplicaType::ALL,
                    .revoke_type = std::nullopt,
                    .success = true,
                    .error = ErrorCode::OK};
        }
        return {.end_type = std::nullopt,
                .revoke_type = ReplicaType::ALL,
                .success = false,
                .error = allocation_satisfied
                             ? (summary.first_error == ErrorCode::OK
                                    ? ErrorCode::TRANSFER_FAIL
                                    : summary.first_error)
                             : ErrorCode::NO_AVAILABLE_HANDLE};
    }

    const bool memory_succeeded = summary.successful_memory_transfers > 0;
    const bool nof_succeeded = summary.successful_nof_transfers > 0;

    if (memory_succeeded && nof_succeeded) {
        return {.end_type = ReplicaType::ALL,
                .revoke_type = std::nullopt,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (memory_succeeded) {
        return {.end_type = ReplicaType::MEMORY,
                .revoke_type = ReplicaType::NOF_SSD,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (nof_succeeded) {
        return {.end_type = ReplicaType::NOF_SSD,
                .revoke_type = ReplicaType::MEMORY,
                .success = true,
                .error = ErrorCode::OK};
    }

    return {.end_type = std::nullopt,
            .revoke_type = ReplicaType::ALL,
            .success = false,
            .error = summary.first_error == ErrorCode::OK
                         ? ErrorCode::NO_AVAILABLE_HANDLE
                         : summary.first_error};
}

}  // namespace

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
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
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      pinned_buffer_pool_(std::make_unique<PinnedBufferPool>()),
      write_thread_pool_(2),
      task_thread_pool_(4) {
    LOG(INFO) << "client_id=" << client_id_;

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

static std::optional<bool> get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        int iv = std::stoi(ev_ad);
        if (iv == 1) {
            LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
            return true;
        } else if (iv == 0) {
            LOG(INFO) << "auto discovery not set by env MC_MS_AUTO_DISC";
            return false;
        } else {
            LOG(WARNING)
                << "invalid MC_MS_AUTO_DISC value: " << ev_ad
                << ", should be 0 or 1, using default: auto discovery not set";
        }
    }
    return std::nullopt;
}

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

static std::vector<std::string> get_auto_discover_filters() {
    std::vector<std::string> whitelst_filters;
    char* ev_ad = std::getenv("MC_MS_FILTERS");
    if (ev_ad) {
        LOG(INFO) << "whitelist filters: " << ev_ad;
        char delimiter = ',';
        char* end = ev_ad + std::strlen(ev_ad);
        char *start = ev_ad, *pos = ev_ad;
        while ((pos = std::find(start, end, delimiter)) != end) {
            std::string str(start, pos);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
            start = pos + 1;
        }
        if (start != (end + 1)) {
            std::string str(start, end);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
        }
    }
    return whitelst_filters;
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

tl::expected<void, ErrorCode> CheckRegisterMemoryParams(const void* addr,
                                                        size_t length) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Tcp is not limited by max_mr_size, but we ignore it for now.
    auto max_mr_size = globalConfig().max_mr_size;  // Max segment size
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
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

        auto current_view = coordinator.value()->ReadCurrentView();
        if (!current_view) {
            LOG(ERROR) << "Failed to read current master view: "
                       << toString(current_view.error());
            return current_view.error();
        }
        if (!current_view.value().has_value()) {
            LOG(ERROR) << "No master is available in HA backend";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }

        const auto& master_view = current_view.value().value();
        auto err = SwitchLeader(master_view);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        leader_coordinator_ = std::move(coordinator.value());
        direct_master_address_.clear();

        leader_monitor_running_ = true;
        leader_monitor_thread_ =
            std::thread([this]() { this->LeaderMonitorThreadMain(); });

        return ErrorCode::OK;
    } else {
        auto err = master_client_.Connect(master_server_entry);
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

    auto err = master_client_.Connect(target_view.leader_address);
    if (err != ErrorCode::OK) {
        last_ping_success_.store(false);
        return err;
    }

    current_master_view_ = target_view;
    last_ping_success_.store(true);
    return ErrorCode::OK;
}

void Client::LeaderMonitorThreadMain() {
    constexpr auto kViewChangeTimeout = std::chrono::milliseconds(1000);
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
    // Check if using TENT mode - TENT handles transport configuration
    // internally
    bool use_tent = (std::getenv("MC_USE_TENT") != nullptr) ||
                    (std::getenv("MC_USE_TEV1") != nullptr);

    bool auto_discover = false;
    if (!use_tent) {
        // Get auto_discover and filters from env (non-TENT only)
        std::optional<bool> env_auto_discover = get_auto_discover();
        if (env_auto_discover.has_value()) {
            // Use user-specified auto-discover setting
            auto_discover = env_auto_discover.value();
        } else {
            // Enable auto-discover for RDMA if no devices are specified
            if ((protocol == "rdma" || protocol == "efa") &&
                !device_names.has_value()) {
                LOG(INFO)
                    << "Set auto discovery ON by default for RDMA protocol, "
                       "since no "
                       "device names provided";
                auto_discover = true;
            }
        }
        transfer_engine_->setAutoDiscover(auto_discover);

        // Honor filters when auto-discovery is enabled; otherwise warn once
        if (auto_discover) {
            LOG(INFO)
                << "Transfer engine auto discovery is enabled for protocol: "
                << protocol;
            auto filters = get_auto_discover_filters();
            transfer_engine_->setWhitelistFilters(std::move(filters));
        } else {
            const char* env_filters = std::getenv("MC_MS_FILTERS");
            if (env_filters && *env_filters != '\0') {
                LOG(WARNING)
                    << "MC_MS_FILTERS is set but auto discovery is disabled; "
                    << "ignoring whitelist: " << env_filters;
            }
        }
    }

    if (protocol == "ascend" || protocol == "ubshmem") {
        const char* ascend_use_fabric_mem =
            std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
        if (ascend_use_fabric_mem) {
            globalConfig().ascend_use_fabric_mem = true;
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
                splitString(device_names.value(), ',', /*skip_empty=*/true);

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
        } else if (protocol == "ascend" || protocol == "ubshmem") {
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
        } else if (protocol == "ub") {
            auto deviceName = device_names.value_or("bonding_dev_0");
            LOG(ERROR) << "ub protocol entable device names is " << deviceName;
            auto devices = splitString(deviceName, ',', true);
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
#ifdef USE_NOF
    int numa_socket_id =
        GetConfiguredNumaSocketId().value_or(GetCurrentNumaSocketId());
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr, numa_socket_id);
#else
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr);
#endif
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::optional<std::string>& device_names,
    const std::string& master_server_entry,
    const std::shared_ptr<TransferEngine>& transfer_engine,
    std::map<std::string, std::string> labels, const std::string& tenant_id) {
    auto client = std::shared_ptr<Client>(new Client(
        local_hostname, metadata_connstring, protocol, labels, tenant_id));

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
                client->PrepareStorageBackend(storage_root_dir, fs_subdir, true,
                                              0);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
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
                client->PrepareStorageBackend(storage_root_dir, fs_subdir,
                                              config.enable_disk_eviction,
                                              config.quota_bytes);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
            }
        }
    }

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

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto query_result = Query(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return Get(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto batched_query_results = BatchQuery(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<QueryResult> valid_query_results;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_query_results.emplace_back(batched_query_results[i].value());
            valid_indices.emplace_back(i);
            valid_keys.emplace_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGet(valid_keys, valid_query_results, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
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
    return QueryResult(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(result.value().lease_ttl_ms));
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
            results.emplace_back(QueryResult(
                std::move(response[i].value().replicas),
                start_time + std::chrono::milliseconds(
                                 response[i].value().lease_ttl_ms)));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices) {
    // Find the first complete replica
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    // Check local hot cache and update replica descriptor if cache hit
    bool cache_used = false;
    if (hot_cache_ && replica.is_memory_replica()) {
        cache_used = RedirectToHotCache(object_key, replica);
    }

    auto t0_get = std::chrono::steady_clock::now();
    err = TransferRead(replica, slices);

    // Release the cache block after transfer completes (memcpy is done)
    if (hot_cache_ && cache_used) {
        hot_cache_->ReleaseHotKey(object_key);
    }

    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }

    // Frequency admission: only promote frequently accessed keys to hot cache.
    // Skip when cache_used — data was already served from local cache, no need
    // to re-promote or increment the CMS counter.
    if (ShouldAdmitToHotCache(object_key, cache_used)) {
        ProcessSlicesAsync(object_key, slices, replica);
    }

    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    // Log cache hit statistics
    if (hot_cache_ && replica.is_memory_replica()) {
        VLOG(1) << "Get completed: key=" << object_key
                << " cache_hit=" << (cache_used ? 1 : 0);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices,
                                          uint64_t src_offset) {
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }
    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "Range read only supported for memory replicas, key="
                   << object_key;
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto t0_get = std::chrono::steady_clock::now();
    err = TransferReadRange(replica, slices, src_offset);
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_range_failed key=" << object_key;
        return tl::unexpected(err);
    }
    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

struct BatchGetOperation {
    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<size_t> key_indexes;
    std::vector<bool> cache_used;  // Track which keys used cache
    std::vector<TransferFuture> futures;
};

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i].replicas;
        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }
        if (!replica.is_memory_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Check local hot cache and update replica descriptor if cache hit
        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
        }

        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        auto& op = seg_to_op_map[seg];
        op.replicas.emplace_back(replica);
        op.batched_slices.emplace_back(slices_it->second);
        op.key_indexes.emplace_back(i);
        op.cache_used.emplace_back(cache_used);
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        auto future = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::READ);
        if (!future) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
            continue;
        }
        op.futures.emplace_back(std::move(*future));
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        if (op.futures.empty()) {
            continue;
        }
        ErrorCode result = op.futures[0].get();
        if (result != ErrorCode::OK) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                // Release cache block even on failure (memcpy may have started)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
        } else {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                VLOG(1) << "Transfer completed successfully for key: "
                        << object_keys[index];
                results[index] = {};

                // Release the cache block after transfer completes (memcpy is
                // done)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }

                // Frequency admission: only promote frequently accessed keys.
                // Skip when cache was used (data served from local cache).
                if (idx < op.replicas.size() &&
                    idx < op.batched_slices.size() &&
                    ShouldAdmitToHotCache(
                        object_keys[index],
                        idx < op.cache_used.size() && op.cache_used[idx])) {
                    ProcessSlicesAsync(object_keys[index],
                                       op.batched_slices[idx],
                                       op.replicas[idx]);
                }
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool prefer_alloc_in_same_node) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Validate input size consistency
    if (query_results.size() != object_keys.size()) {
        LOG(ERROR) << "Query results size (" << query_results.size()
                   << ") doesn't match object keys size (" << object_keys.size()
                   << ")";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }
    if (prefer_alloc_in_same_node) {
        return BatchGetWhenPreferSameNode(object_keys, query_results, slices);
    }

    // Collect all transfer operations for parallel execution
    // Tuple: (index, key, future, replica, cache_used)
    std::vector<std::tuple<size_t, std::string, TransferFuture,
                           Replica::Descriptor, bool>>
        pending_transfers;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
    // Record batch get transfer latency (Submit + Wait)
    auto t0_batch_get = std::chrono::steady_clock::now();

    // Collect cache hit statistics for the entire batch
    size_t total_cache_hits = 0;

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& query_result = query_results[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        Replica::Descriptor replica;
        ErrorCode err =
            FindFirstCompleteReplica(query_result.replicas, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
            if (cache_used) {
                total_cache_hits++;
            }
        }

        // Submit transfer operation asynchronously
        std::optional<TransferFuture> future;
        if (replica.is_nof_replica()) {
            auto contiguous_range = GetContiguousSliceRange(slices_it->second);
            if (!contiguous_range.has_value()) {
                LOG(ERROR) << "NoF transfer requires contiguous slices";
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            future = transfer_submitter_->submit(
                replica, slices_it->second, TransferRequest::READ,
                contiguous_range->ptr, contiguous_range->size);
        } else {
            future = transfer_submitter_->submit(replica, slices_it->second,
                                                 TransferRequest::READ);
        }
        if (!future) {
            // Release cache block if submit failed
            if (hot_cache_ && cache_used) {
                hot_cache_->ReleaseHotKey(key);
            }
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future), replica,
                                       cache_used);
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future, stored_replica, cache_used] :
         pending_transfers) {
        ErrorCode result = future.get();

        // Release the cache block after transfer completes (memcpy is done)
        if (hot_cache_ && cache_used) {
            hot_cache_->ReleaseHotKey(key);
        }
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;
            results[index] = {};

            // Frequency admission: only promote frequently accessed keys.
            // Skip when cache was used (data served from local cache).
            if (hot_cache_) {
                auto slices_it = slices.find(key);
                if (slices_it != slices.end() &&
                    ShouldAdmitToHotCache(key, cache_used)) {
                    ProcessSlicesAsync(key, slices_it->second, stored_replica);
                }
            }
        }
    }

    // As lease expired is a rare case, we check all the results with the same
    // time_point to avoid too many syscalls
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    for (size_t i = 0; i < object_keys.size(); ++i) {
        if (results[i].has_value() && query_results[i].IsLeaseExpired(now)) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_keys[i];
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
    }

    auto us_batch_get = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0_batch_get)
                            .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us_batch_get);
    }

    // Log overall cache hit statistics for the entire batch
    if (hot_cache_) {
        VLOG(1) << "BatchGet completed: num_keys=" << object_keys.size()
                << " total_cache_hits=" << total_cache_hits;
    } else {
        VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    }
    return results;
}

bool Client::RedirectToHotCache(const std::string& key,
                                Replica::Descriptor& replica) {
    if (!replica.is_memory_replica() || !hot_cache_) {
        return false;
    }

    auto& mem_desc = replica.get_memory_descriptor();
    HotMemBlock* blk = hot_cache_->GetHotKey(key);
    if (blk == nullptr) {
        return false;
    }

    if (mem_desc.buffer_descriptor.size_ != blk->size) {
        LOG(ERROR) << "Cache hit but size mismatch for key: " << key;
        return false;
    }

    mem_desc.buffer_descriptor.transport_endpoint_ =
        (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                               : local_hostname_;
    mem_desc.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(blk->addr);
    return true;
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    // Start put operation
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start put operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start put operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
    }

    // Record Put transfer latency (all replicas)
    auto t0_put = std::chrono::steady_clock::now();

    // We must deal with disk replica first, then the disk putrevoke/putend can
    // be called surely
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // Store to local file if storage backend is available
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;  // Only one disk replica is needed
            }
        }
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            // Transfer data using allocated handles from all replicas
            const auto replica_type = replica.is_memory_replica()
                                          ? ReplicaType::MEMORY
                                          : ReplicaType::NOF_SSD;
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
        }
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(config, transfer_summary);

    if (finalize_decision.end_type.has_value()) {
        auto end_result =
            master_client_.PutEnd(key, *finalize_decision.end_type);
        if (!end_result) {
            ErrorCode err = end_result.error();
            LOG(ERROR) << "Failed to end put operation: " << err;
            return tl::unexpected(err);
        }
    }

    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result =
            master_client_.PutRevoke(key, *finalize_decision.revoke_type);
        if (!revoke_result) {
            LOG(ERROR) << "Failed to revoke put operation";
            return tl::unexpected(revoke_result.error());
        }
    }

    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Upsert(const ObjectKey& key,
                                             std::vector<Slice>& slices,
                                             const ReplicateConfig& config) {
    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    // Start upsert operation
    auto start_result =
        master_client_.UpsertStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start upsert operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start upsert operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    // Record transfer latency
    auto t0 = std::chrono::steady_clock::now();

    // Handle disk replicas first
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;
            }
        }
    }

    // Transfer to memory replicas
    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                auto revoke_result =
                    master_client_.UpsertRevoke(key, ReplicaType::MEMORY);
                if (!revoke_result) {
                    LOG(ERROR) << "Failed to revoke upsert operation";
                    return tl::unexpected(revoke_result.error());
                }
                return tl::unexpected(transfer_err);
            }
        }
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us);
    }

    // End upsert operation
    auto end_result = master_client_.UpsertEnd(key, ReplicaType::MEMORY);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end upsert operation: " << err;
        return tl::unexpected(err);
    }

    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    if (client_cfg.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported for upsert";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    StartBatchUpsert(ops, client_cfg);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchUpsert(ops);
    return CollectResults(ops);
}

// TODO: `client.cpp` is too long, consider split it into multiple files
enum class PutOperationState {
    PENDING,
    MASTER_FAILED,
    TRANSFER_FAILED,
    FINALIZE_FAILED,
    SUCCESS
};

class PutOperation {
   public:
    struct PendingTransferRecord {
        ReplicaType replica_type;
        TransferFuture future;

        PendingTransferRecord(ReplicaType type,
                              TransferFuture&& transfer_future)
            : replica_type(type), future(std::move(transfer_future)) {}
    };

    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    std::vector<std::vector<Slice>> batched_slices;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<PendingTransferRecord> pending_transfers;

    size_t requested_memory_replicas = 0;
    size_t requested_nof_replicas = 0;
    ReplicaTransferSummary transfer_summary;

    // Error context for debugging
    std::optional<std::string> failure_context;

    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetTerminalError(ErrorCode error, PutOperationState terminal_state,
                          const std::string& context = "") {
        state = terminal_state;
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = context;
        }
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = toString(error) + ": " + context + "; " +
                              failure_context.value_or("");
        }

        // Update state based on current processing stage
        if (replicas.empty()) {
            state = PutOperationState::MASTER_FAILED;
        } else if (pending_transfers.empty()) {
            state = PutOperationState::TRANSFER_FAILED;
        } else {
            state = PutOperationState::FINALIZE_FAILED;
        }
    }

    void AppendFailureContext(const std::string& context) {
        if (context.empty()) {
            return;
        }
        if (!failure_context.has_value()) {
            failure_context = context;
            return;
        }
        failure_context = *failure_context + "; " + context;
    }

    void InitializeRequestedReplicas(const ReplicateConfig& config) {
        requested_memory_replicas = config.replica_num;
        requested_nof_replicas = config.nof_replica_num;
    }

    ReplicateConfig ToReplicateConfig() const {
        ReplicateConfig config;
        config.replica_num = requested_memory_replicas;
        config.nof_replica_num = requested_nof_replicas;
        return config;
    }

    void RecordAllocatedReplicas() {
        transfer_summary = ReplicaTransferSummary{};
        for (const auto& replica : replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
        }
    }

    bool IsResolved() const { return state != PutOperationState::PENDING; }

    bool IsSuccessful() const {
        return state == PutOperationState::SUCCESS && result.has_value();
    }
};

std::vector<PutOperation> Client::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());

    for (const auto& op : ops) {
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != ops.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << ops.size() << ", got " << start_responses.size();
        for (auto& op : ops) {
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < ops.size(); ++i) {
        ops[i].InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            ops[i].SetTerminalError(start_responses[i].error(),
                                    PutOperationState::MASTER_FAILED,
                                    "Master failed to start put operation");
        } else {
            ops[i].replicas = start_responses[i].value();
            ops[i].RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config,
                                              ops[i].transfer_summary)) {
                ops[i].SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                        PutOperationState::MASTER_FAILED,
                                        "Allocated replicas do not satisfy "
                                        "requested replica policy");
                continue;
            }
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << ops[i].key
                    << " with " << ops[i].replicas.size() << " replicas";
        }
    }
}

void Client::StartBatchUpsert(std::vector<PutOperation>& ops,
                              const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());

    for (const auto& op : ops) {
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    auto start_responses =
        master_client_.BatchUpsertStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != ops.size()) {
        LOG(ERROR) << "BatchUpsertStart response size mismatch: expected "
                   << ops.size() << ", got " << start_responses.size();
        for (auto& op : ops) {
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchUpsertStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < ops.size(); ++i) {
        if (!start_responses[i]) {
            ops[i].SetError(start_responses[i].error(),
                            "Master failed to start upsert operation");
        } else {
            ops[i].replicas = start_responses[i].value();
            VLOG(1) << "Successfully started upsert for key " << ops[i].key
                    << " with " << ops[i].replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        for (auto& op : ops) {
            op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                PutOperationState::TRANSFER_FAILED,
                                "TransferSubmitter not initialized");
        }
        return;
    }

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "No replicas available for transfer");
            continue;
        }

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                 ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(op.key, op.slices, disk_descriptor);
                    break;  // Only one disk replica is needed
                }
            }
        }

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];
            if (replica.is_memory_replica() || replica.is_nof_replica()) {
                const auto replica_type = replica.is_memory_replica()
                                              ? ReplicaType::MEMORY
                                              : ReplicaType::NOF_SSD;
                std::optional<TransferFuture> submit_result;
                if (replica.is_nof_replica()) {
                    auto contiguous_range = GetContiguousSliceRange(op.slices);
                    if (!contiguous_range.has_value()) {
                        std::string failure_context =
                            "NoF transfer requires contiguous slices for "
                            "replica " +
                            std::to_string(replica_idx);
                        op.transfer_summary.RecordFailure(
                            replica_type, ErrorCode::INVALID_PARAMS);
                        op.AppendFailureContext(failure_context);
                        continue;
                    }
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE,
                        contiguous_range->ptr, contiguous_range->size);
                } else {
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE);
                }

                if (!submit_result) {
                    std::string failure_context =
                        "Failed to submit transfer for replica " +
                        std::to_string(replica_idx);
                    op.transfer_summary.RecordFailure(replica_type,
                                                      ErrorCode::TRANSFER_FAIL);
                    op.AppendFailureContext(failure_context);
                    continue;
                }

                op.pending_transfers.emplace_back(
                    replica_type, std::move(submit_result.value()));
            }
        }

        VLOG(1) << "Submitted " << op.pending_transfers.size()
                << " transfers for key " << op.key;
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            auto& pending_transfer = op.pending_transfers[i];
            ErrorCode transfer_result = pending_transfer.future.get();
            if (transfer_result != ErrorCode::OK) {
                op.transfer_summary.RecordFailure(pending_transfer.replica_type,
                                                  transfer_result);
                std::string error_context =
                    "Transfer " + std::to_string(i) + " failed";
                op.AppendFailureContext(error_context);
            } else {
                op.transfer_summary.RecordSuccess(
                    pending_transfer.replica_type);
            }
        }

        VLOG(1) << "Transfers finished for key " << op.key << ", success(mem="
                << op.transfer_summary.successful_memory_transfers
                << ", nof=" << op.transfer_summary.successful_nof_transfers
                << "), fail(mem=" << op.transfer_summary.failed_memory_transfers
                << ", nof=" << op.transfer_summary.failed_nof_transfers << ")";
    }
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops) {
    struct BatchFinalizeGroup {
        std::vector<std::string> keys;
        std::vector<size_t> indices;
    };

    BatchFinalizeGroup end_all_group;
    BatchFinalizeGroup end_memory_group;
    BatchFinalizeGroup end_nof_group;
    BatchFinalizeGroup revoke_all_group;
    BatchFinalizeGroup revoke_memory_group;
    BatchFinalizeGroup revoke_nof_group;

    std::vector<size_t> pending_finalize_actions(ops.size(), 0);
    std::vector<bool> should_succeed(ops.size(), false);
    std::vector<ErrorCode> terminal_errors(ops.size(), ErrorCode::OK);
    std::vector<std::optional<ErrorCode>> finalize_rpc_errors(ops.size());

    auto add_group_entry = [](BatchFinalizeGroup& group, const std::string& key,
                              size_t index) {
        group.keys.emplace_back(key);
        group.indices.emplace_back(index);
    };

    auto add_finalize_action =
        [&](const std::optional<ReplicaType>& replica_type, bool is_end,
            const std::string& key, size_t index) {
            if (!replica_type.has_value()) {
                return;
            }
            switch (*replica_type) {
                case ReplicaType::ALL:
                    add_group_entry(is_end ? end_all_group : revoke_all_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::MEMORY:
                    add_group_entry(
                        is_end ? end_memory_group : revoke_memory_group, key,
                        index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::NOF_SSD:
                    add_group_entry(is_end ? end_nof_group : revoke_nof_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                default:
                    LOG(ERROR) << "Unexpected replica type in batch finalize: "
                               << *replica_type;
                    finalize_rpc_errors[index] = ErrorCode::INVALID_PARAMS;
                    break;
            }
        };

    auto complete_finalize_action = [&](size_t index) {
        if (pending_finalize_actions[index] > 0) {
            --pending_finalize_actions[index];
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful() && !op.replicas.empty()) {
                terminal_errors[i] = op.result.has_value()
                                         ? ErrorCode::INTERNAL_ERROR
                                         : op.result.error();
                add_finalize_action(ReplicaType::ALL, false, op.key, i);
            }
            continue;
        }
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "Operation has no replicas to finalize");
            continue;
        }

        const auto finalize_decision = DetermineFinalizeDecision(
            op.ToReplicateConfig(), op.transfer_summary);
        should_succeed[i] = finalize_decision.success;
        terminal_errors[i] = finalize_decision.error;
        add_finalize_action(finalize_decision.end_type, true, op.key, i);
        add_finalize_action(finalize_decision.revoke_type, false, op.key, i);
    }

    auto process_end_group = [&](BatchFinalizeGroup& group,
                                 ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        auto responses = master_client_.BatchPutEnd(group.keys, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutEnd key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    auto process_revoke_group = [&](BatchFinalizeGroup& group,
                                    ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        auto responses =
            master_client_.BatchPutRevoke(group.keys, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutRevoke key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    process_end_group(end_all_group, ReplicaType::ALL);
    process_end_group(end_memory_group, ReplicaType::MEMORY);
    process_end_group(end_nof_group, ReplicaType::NOF_SSD);
    process_revoke_group(revoke_all_group, ReplicaType::ALL);
    process_revoke_group(revoke_memory_group, ReplicaType::MEMORY);
    process_revoke_group(revoke_nof_group, ReplicaType::NOF_SSD);

    auto append_finalize_error_context = [&](PutOperation& op, size_t index) {
        if (finalize_rpc_errors[index].has_value()) {
            op.AppendFailureContext("Batch finalization RPC failed: " +
                                    toString(*finalize_rpc_errors[index]));
        }
        if (pending_finalize_actions[index] != 0) {
            op.AppendFailureContext(
                "Operation has unfinished finalize actions");
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful()) {
                append_finalize_error_context(op, i);
            }
            continue;
        }
        if (finalize_rpc_errors[i].has_value() ||
            pending_finalize_actions[i] != 0) {
            if (!should_succeed[i] && terminal_errors[i] != ErrorCode::OK) {
                append_finalize_error_context(op, i);
                op.SetTerminalError(
                    terminal_errors[i], PutOperationState::TRANSFER_FAILED,
                    op.failure_context.value_or(
                        "Replica transfer failed before finalize"));
            } else if (finalize_rpc_errors[i].has_value()) {
                op.SetTerminalError(*finalize_rpc_errors[i],
                                    PutOperationState::FINALIZE_FAILED,
                                    "Batch finalization RPC failed");
            } else {
                op.SetTerminalError(
                    ErrorCode::INTERNAL_ERROR,
                    PutOperationState::FINALIZE_FAILED,
                    "Operation has unfinished finalize actions");
            }
            continue;
        }
        if (should_succeed[i]) {
            op.SetSuccess();
            continue;
        }
        op.SetTerminalError(terminal_errors[i],
                            PutOperationState::TRANSFER_FAILED,
                            op.failure_context.value_or(
                                "Replica transfer failed before finalize"));
    }
}

void Client::FinalizeBatchUpsert(std::vector<PutOperation>& ops) {
    std::vector<std::string> successful_keys;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    successful_keys.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];

        if (!op.IsResolved() && !op.replicas.empty() &&
            !op.pending_transfers.empty()) {
            successful_keys.emplace_back(op.key);
            successful_indices.emplace_back(i);
        } else if (op.state != PutOperationState::PENDING &&
                   !op.replicas.empty()) {
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
    }

    // Process successful operations
    if (!successful_keys.empty()) {
        auto end_responses = master_client_.BatchUpsertEnd(successful_keys);
        if (end_responses.size() != successful_keys.size()) {
            LOG(ERROR) << "BatchUpsertEnd response size mismatch: expected "
                       << successful_keys.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertEnd response size mismatch");
            }
        } else {
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize upsert for key "
                               << successful_keys[i] << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchUpsertEnd failed");
                } else {
                    ops[op_idx].SetSuccess();
                    VLOG(1) << "Successfully completed upsert for key "
                            << successful_keys[i];
                }
            }
        }
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses = master_client_.BatchUpsertRevoke(failed_keys);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchUpsertRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertRevoke response size mismatch");
            }
        } else {
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke upsert for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed upsert for key "
                              << failed_keys[i];
                }
            }
        }
    }

    // Ensure all operations have definitive results
    for (auto& op : ops) {
        if (!op.IsResolved()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "Operation not resolved after finalization");
            LOG(ERROR) << "Operation for key " << op.key
                       << " was not properly resolved";
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
    const std::vector<PutOperation>& ops) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());

    int no_available_handle_count = 0;
    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);

        // Additional validation and logging for debugging
        if (!op.result.has_value()) {
            // If error == object already exist, consider as ok (Put semantics).
            // UpsertStart never returns this error, so this branch is
            // unreachable for BatchUpsert — kept for BatchPut compatibility.
            if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                results.back() = {};
                continue;
            }
            if (op.result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "Operation for key " << op.key
                           << " failed: " << toString(op.result.error())
                           << (op.failure_context
                                   ? (" (" + *op.failure_context + ")")
                                   : "");
            }
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }
    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutWhenPreferSameNode(
    std::vector<PutOperation>& ops) {
    auto t0 = std::chrono::steady_clock::now();
    std::unordered_map<std::string, PutOperation> seg_to_ops{};
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }
        auto replica = op.replicas[0];
        if (!replica.is_memory_replica()) {
            op.SetError(ErrorCode::INVALID_PARAMS, "only memory is supported.");
            continue;
        }
        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            op.SetError(ErrorCode::INVALID_PARAMS, "buffer size is 0.");
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        if (seg_to_ops.find(seg) == seg_to_ops.end()) {
            seg_to_ops.emplace(seg, PutOperation(op.key, op.slices));
        }
        // multiple replica-slices
        seg_to_ops.at(seg).batched_slices.emplace_back(op.slices);
        seg_to_ops.at(seg).replicas.emplace_back(replica);
    }
    std::vector<PutOperation> merged_ops;
    merged_ops.reserve(seg_to_ops.size());
    for (auto& seg_to_op : seg_to_ops) {
        auto& op = seg_to_op.second;
        bool all_transfers_submitted = true;
        std::string failure_context;
        merged_ops.emplace_back(op.key, op.slices);
        auto& merged_op = merged_ops.back();
        merged_op.replicas = op.replicas;
        merged_op.transfer_summary.allocated_memory_replicas = 1;
        auto submit_result = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::WRITE);
        if (!submit_result) {
            failure_context = "Failed to submit batch transfer";
            all_transfers_submitted = false;
        } else {
            merged_op.pending_transfers.emplace_back(
                ReplicaType::MEMORY, std::move(submit_result.value()));
        }
        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            merged_op.transfer_summary.RecordFailure(ReplicaType::MEMORY,
                                                     ErrorCode::TRANSFER_FAIL);
            merged_op.failure_context = failure_context;
            merged_op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted "
                    << merged_op.pending_transfers.size()
                    << " transfers for key " << merged_ops.back().key;
        }
    }
    WaitForTransfers(merged_ops);
    for (auto& op : merged_ops) {
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        seg_to_ops.at(seg).transfer_summary = op.transfer_summary;
        seg_to_ops.at(seg).failure_context = op.failure_context;
    }
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        op.transfer_summary.successful_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.successful_memory_transfers > 0
                ? 1
                : 0;
        op.transfer_summary.failed_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.failed_memory_transfers > 0 ? 1
                                                                            : 0;
        op.transfer_summary.first_error =
            seg_to_ops.at(seg).transfer_summary.first_error;
        op.failure_context = seg_to_ops.at(seg).failure_context;
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }
    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (client_cfg.nof_replica_num > 0) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "NoF replicas";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        if (client_cfg.replica_num != 1) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "replica_num != 1";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        StartBatchPut(ops, client_cfg);
        return BatchPutWhenPreferSameNode(ops);
    }
    StartBatchPut(ops, client_cfg);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    auto result = master_client_.Remove(key, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveFile(key);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> Client::RemoveByRegex(const ObjectKey& str,
                                                    bool force) {
    auto result = master_client_.RemoveByRegex(str, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveByRegex(str);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
    auto result = master_client_.RemoveAll(force);
    if (result && storage_backend_) {
        storage_backend_->RemoveAll();
    }
    return result;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    return master_client_.BatchRemove(keys, force);
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
    auto check_result = CheckRegisterMemoryParams(buffer, size);
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
        if (metadata_connstring_ == P2PHANDSHAKE) {
            segment.te_endpoint = transfer_engine_->getLocalIpAndPort();
        } else {
            segment.te_endpoint = local_hostname_;
        }

        auto mount_result = master_client_.MountSegment(segment);
        if (!mount_result) {
            ErrorCode err = mount_result.error();
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
    auto check_result = CheckRegisterMemoryParams(addr, length);
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
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
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

void* Client::GetBaseAddr() { return transfer_engine_->getBaseAddr(); }

tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
    bool enable_offloading) {
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
        return response;
    }

    EnsureStorageControlPlaneStarted();
    return response;
}

tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
    bool enable_offloading, std::vector<OffloadTaskItem>& offloading_objects) {
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<void, ErrorCode> Client::ReportSsdCapacity(
    int64_t ssd_total_capacity_bytes) {
    auto response =
        master_client_.ReportSsdCapacity(client_id_, ssd_total_capacity_bytes);
    if (!response) {
        LOG(ERROR) << "ReportSsdCapacity failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    return master_client_.NotifyOffloadSuccess(client_id_, tasks, metadatas);
}

tl::expected<void, ErrorCode> Client::PromotionObjectHeartbeat(
    std::vector<PromotionTaskItem>& promotion_objects) {
    auto response = master_client_.PromotionObjectHeartbeat(client_id_);
    if (!response) {
        return tl::make_unexpected(response.error());
    }
    promotion_objects = std::move(response.value());
    return {};
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, size,
                                              preferred_segments);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, tenant_id, size,
                                              preferred_segments);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key) {
    return master_client_.NotifyPromotionSuccess(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionSuccess(client_id_, key, tenant_id);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key) {
    return master_client_.NotifyPromotionFailure(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionFailure(client_id_, key, tenant_id);
}

ErrorCode Client::PromotionWrite(const Replica::Descriptor& memory_descriptor,
                                 std::vector<Slice>& slices) {
    return TransferWrite(memory_descriptor, slices);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, tenant_id, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return master_client_.CreateMoveTask(key, source, target);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    return master_client_.CreateMoveTask(key, tenant_id, source, target);
}

tl::expected<void, ErrorCode> Client::ExecuteReplicaTransfer(
    const std::string& key, const std::string& action_name,
    std::function<tl::expected<void, ErrorCode>()> end_fn,
    std::function<tl::expected<void, ErrorCode>()> revoke_fn,
    const Replica::Descriptor& source,
    const std::vector<Replica::Descriptor>& targets) {
    auto revoke_lambda = [&]() {
        auto revoke_result = revoke_fn();
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_" << action_name << "_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
    };

    // currently only memory source replica is supported
    if (!source.is_memory_replica()) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key << ", error=invalid_replica_type";
        revoke_lambda();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate that source replica is in local memory
    if (!IsReplicaOnLocalMemory(source)) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key
                   << ", error=source_replica_not_in_local_memory";
        revoke_lambda();
        return tl::unexpected(ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
    }

    // Split the source replica into slices for transfer
    // This avoids data copy because the source replica is already in memory
    const auto& buffer_descriptor =
        source.get_memory_descriptor().buffer_descriptor;
    void* buffer = reinterpret_cast<void*>(buffer_descriptor.buffer_address_);
    auto slices = split_into_slices(buffer, buffer_descriptor.size_);

    // Transfer to each target
    for (const auto& target : targets) {
        if (TransferWrite(target, slices) != ErrorCode::OK) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
    }

    // Call end function to finalize
    auto end_result = end_fn();
    if (!end_result.has_value()) {
        revoke_lambda();
        return tl::unexpected(end_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& source,
    const std::vector<std::string>& targets) {
    return Copy(key, master_client_.tenant_id(), source, targets);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets) {
    LOG(INFO) << "action=replica_copy_start" << ", key=" << key
              << ", targets_count=" << targets.size();

    // Call CopyStart first - it validates existence and allocates replicas
    auto start_result =
        master_client_.CopyStart(key, tenant_id, source, targets);
    if (!start_result.has_value()) {
        ErrorCode error = start_result.error();
        LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                   << ", source=" << source << ", error=copy_start_failed"
                   << ", error_code=" << error;
        return tl::unexpected(error);
    }

    const auto& response = start_result.value();
    if (response.targets.empty()) {
        LOG(INFO) << "action=replica_copy_skipped" << ", key=" << key
                  << ", info=target_replicas_already_exist";
        // Target replicas already exist, consider it success
        auto copy_end_result = master_client_.CopyEnd(key, tenant_id);
        if (!copy_end_result.has_value()) {
            ErrorCode error = copy_end_result.error();
            LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                       << ", error=copy_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    auto result = ExecuteReplicaTransfer(
        key, "copy", [&]() { return master_client_.CopyEnd(key, tenant_id); },
        [&]() { return master_client_.CopyRevoke(key, tenant_id); },
        response.source, response.targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_copy_success" << ", key=" << key
                  << ", target_count=" << response.targets.size();
    }

    return result;
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& source,
                                           const std::string& target) {
    return Move(key, master_client_.tenant_id(), source, target);
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& tenant_id,
                                           const std::string& source,
                                           const std::string& target) {
    LOG(INFO) << "action=replica_move_start" << ", key=" << key
              << ", source_segment=" << source << ", target_segment=" << target;

    // Call MoveStart first - it validates existence and allocates replica if
    // needed
    auto move_start_result =
        master_client_.MoveStart(key, tenant_id, source, target);
    if (!move_start_result.has_value()) {
        ErrorCode error = move_start_result.error();
        LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                   << ", error=move_start_failed" << ", error_code=" << error;
        // MoveStart already validated existence, so we just return the error
        return tl::unexpected(error);
    }

    const auto& response = move_start_result.value();
    if (!response.target.has_value()) {
        LOG(INFO) << "action=replica_move_skipped" << ", key=" << key
                  << ", info=target_replica_already_exists";
        // Target already exists, consider it success
        auto move_end_result = master_client_.MoveEnd(key, tenant_id);
        if (!move_end_result.has_value()) {
            ErrorCode error = move_end_result.error();
            LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                       << ", error=move_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    std::vector<Replica::Descriptor> targets = {response.target.value()};

    auto result = ExecuteReplicaTransfer(
        key, "move", [&]() { return master_client_.MoveEnd(key, tenant_id); },
        [&]() { return master_client_.MoveRevoke(key, tenant_id); },
        response.source, targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_move_success" << ", key=" << key
                  << ", source_segment=" << source
                  << ", target_segment=" << target;
    }

    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
    const UUID& task_id) {
    return master_client_.QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> Client::FetchTasks(
    size_t batch_size) {
    return master_client_.FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_.MarkTaskToComplete(update_request);
}

void Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                   const std::string& fsdir,
                                   bool enable_eviction, uint64_t quota_bytes) {
    // Initialize storage backend
    storage_backend_ =
        StorageBackend::Create(storage_root_dir, fsdir, enable_eviction);
    if (!storage_backend_) {
        LOG(INFO) << "Failed to initialize storage backend";
    }
    auto init_result = storage_backend_->Init(quota_bytes);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize StorageBackend. Error: "
                   << init_result.error() << ". The backend will be unusable.";
    }
}

void Client::PutToLocalFile(const std::string& key,
                            const std::vector<Slice>& slices,
                            const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string path = disk_descriptor.file_path;

    // Synchronous D2H staging + copy into std::string.
    // Done on the calling thread to guarantee GPU buffers are still valid
    // (BatchPut has not yet returned to Python, so blocks are not reused).
    std::string value;
    value.reserve(total_size);

    for (const auto& slice : slices) {
        int device_id = -1;
        if (IsDevicePointer(slice.ptr, &device_id)) {
            SetDevice(device_id);
            auto buf = pinned_buffer_pool_->Acquire(slice.size);
            if (!CopyDeviceToHost(buf.data, slice.ptr, slice.size)) {
                LOG(ERROR) << "D2H copy failed for key: " << key
                           << ", triggering PutRevoke for disk replica";
                pinned_buffer_pool_->Release(buf);
                // Must revoke to avoid phantom replica in master
                auto revoke_result =
                    master_client_.PutRevoke(key, ReplicaType::DISK);
                if (!revoke_result) {
                    LOG(ERROR)
                        << "Failed to revoke put operation for key: " << key;
                }
                return;
            }
            value.append(buf.data, slice.size);
            pinned_buffer_pool_->Release(buf);
        } else {
            value.append(static_cast<char*>(slice.ptr), slice.size);
        }
    }

    // Async StoreObject + PutEnd (unchanged from original)
    write_thread_pool_.enqueue([this, backend = storage_backend_, key,
                                value = std::move(value), path] {
        // Store the object
        auto store_result = backend->StoreObject(path, value, key);
        ReplicaType replica_type = ReplicaType::DISK;

        if (!store_result) {
            // If storage failed, revoke the put operation
            LOG(ERROR) << "Failed to store object for key: " << key;
            auto revoke_result = master_client_.PutRevoke(key, replica_type);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation for key: " << key;
            }
            return;
        }

        // Notify master about any evicted disk replicas (batch)
        if (!store_result.value().empty()) {
            const auto& evicted_keys = store_result.value();
            auto evict_results = master_client_.BatchEvictDiskReplica(
                evicted_keys, replica_type);
            for (size_t i = 0; i < evict_results.size(); ++i) {
                if (!evict_results[i]) {
                    LOG(WARNING)
                        << "Failed to notify master about evicted key: "
                        << evicted_keys[i]
                        << ", error: " << evict_results[i].error();
                }
            }
        }

        // If storage succeeded, end the put operation
        auto end_result = master_client_.PutEnd(key, replica_type);
        if (!end_result) {
            LOG(ERROR) << "Failed to end put operation for key: " << key;
        }
    });
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    std::optional<TransferFuture> future;
    if (replica_descriptor.is_nof_replica()) {
        auto contiguous_range = GetContiguousSliceRange(slices);
        if (!contiguous_range.has_value()) {
            LOG(ERROR) << "NoF transfer requires contiguous slices";
            return ErrorCode::INVALID_PARAMS;
        }
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, contiguous_range->ptr,
                                             contiguous_range->size);
    } else {
        future =
            transfer_submitter_->submit(replica_descriptor, slices, op_code);
    }
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferReadInternal(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    auto future = transfer_submitter_->submitRangeRead(replica_descriptor,
                                                       slices, src_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range read operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_nof_replica()) {
        auto& nof_desc = replica_descriptor.get_nof_descriptor();
        total_size = nof_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_local_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_local_disk_descriptor();
        total_size = disk_desc.object_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

ErrorCode Client::TransferReadRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    return TransferReadInternal(replica_descriptor, slices, src_offset);
}

void Client::PollAndDispatchTasks() {
    if (task_running_.load()) {
        auto fetch_result = FetchTasks(kTaskBatchSize);
        if (fetch_result.has_value()) {
            const auto& tasks = fetch_result.value();
            if (!tasks.empty()) {
                LOG(INFO) << "action=task_poll_success"
                          << ", task_count=" << tasks.size();
                for (const auto& task_assignment : tasks) {
                    SubmitTask(task_assignment);
                }
            }
        } else {
            ErrorCode error = fetch_result.error();
            // Only log if it's not an RPC failure (which is expected
            // during connection failures)
            if (error != ErrorCode::RPC_FAIL) {
                LOG(WARNING)
                    << "action=task_poll_failed" << ", error_code=" << error;
            }
        }
    }
}

void Client::TaskPollThreadMain() {
    const auto poll_interval = std::chrono::milliseconds(1000);

    while (task_poll_running_.load()) {
        PollAndDispatchTasks();
        std::this_thread::sleep_for(poll_interval);
    }
}

void Client::SubmitTask(const TaskAssignment& assignment) {
    if (!task_running_.load()) {
        LOG(WARNING) << "action=task_rejected" << ", task_id=" << assignment.id
                     << ", reason=executor_stopped";
        return;
    }

    // Construct ClientTask from TaskAssignment
    ClientTask client_task;
    client_task.assignment = assignment;
    client_task.retry_count = 0;

    task_thread_pool_.enqueue(
        [this, client_task]() { ExecuteTask(client_task); });
}

void Client::ExecuteTask(const ClientTask& client_task) {
    const auto& assignment = client_task.assignment;
    ErrorCode result = ErrorCode::OK;

    try {
        switch (assignment.type) {
            case TaskType::REPLICA_COPY: {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto copy_result = Copy(payload.key, payload.tenant_id,
                                        payload.source, payload.targets);
                if (copy_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = copy_result.error();
                }
                break;
            }
            case TaskType::REPLICA_MOVE: {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto move_result = Move(payload.key, payload.tenant_id,
                                        payload.source, payload.target);
                if (move_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = move_result.error();
                }
                break;
            }
            default:
                LOG(ERROR) << "action=task_execution_failed"
                           << ", task_id=" << assignment.id
                           << ", error=unknown_task_type"
                           << ", task_type=" << assignment.type;
                result = ErrorCode::INVALID_PARAMS;
                break;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "action=task_execution_failed"
                   << ", task_id=" << assignment.id << ", error=exception"
                   << ", exception=" << e.what();
        result = ErrorCode::INTERNAL_ERROR;
    }

    if (result == ErrorCode::OK) {
        TaskCompleteRequest complete_request;
        complete_request.id = assignment.id;
        complete_request.status = TaskStatus::SUCCESS;
        complete_request.message = "Task completed successfully";
        auto complete_result =
            master_client_.MarkTaskToComplete(complete_request);
        if (!complete_result.has_value()) {
            LOG(WARNING) << "action=task_complete_failed"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << complete_result.error();
        }
    } else {
        uint32_t current_retry_count = client_task.retry_count;
        // Only retry on allocation failures (NO_AVAILABLE_HANDLE)
        // Other errors (e.g., OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should
        // not be retried
        bool should_retry =
            (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
            (current_retry_count < assignment.max_retry_attempts);

        if (should_retry) {
            ClientTask retry_task = client_task;
            retry_task.increment_retry();

            const auto retry_delay =
                std::chrono::milliseconds(50 * (current_retry_count + 1));
            std::this_thread::sleep_for(retry_delay);

            LOG(WARNING) << "action=task_execution_failed_retry"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << result
                         << ", retry_count=" << current_retry_count
                         << ", max_retry_count="
                         << assignment.max_retry_attempts << ", will_retry=true"
                         << ", retry_delay=" << retry_delay.count() << "ms";

            task_thread_pool_.enqueue(
                [this, retry_task]() { ExecuteTask(retry_task); });
        } else {
            LOG(ERROR) << "action=task_execution_failed"
                       << ", task_id=" << assignment.id
                       << ", error_code=" << result
                       << ", retry_count=" << current_retry_count
                       << ", max_retry_count=" << assignment.max_retry_attempts;
            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message =
                toString(result) + " (max retries reached: " +
                std::to_string(assignment.max_retry_attempts) + ")";
            auto complete_result =
                master_client_.MarkTaskToComplete(complete_request);
            if (!complete_result.has_value()) {
                LOG(WARNING) << "action=task_complete_failed"
                             << ", task_id=" << assignment.id
                             << ", error_code=" << complete_result.error();
            }
        }
    }
}

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
    // Use another thread to remount segments to avoid blocking the ping
    // thread
    std::future<void> remount_segment_future;

    while (storage_heartbeat_running_.load()) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping();
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            last_ping_success_.store(true);
            auto& ping_response = ping_result.value();
            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            } else if (segment_desc_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but updateLocalSegmentDesc()
                // failed (e.g. transient HTTP error).  Retry it directly
                // without re-running ReMountSegment.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->updateLocalSegmentDesc();
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish segment "
                            << "descriptor to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "segment descriptor to metadata server";
                        segment_desc_publish_pending_.store(false);
                    }
                }
            } else if (rpc_meta_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but rePublishRpcMetaEntry()
                // failed.  Retry it directly.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->rePublishRpcMetaEntry(local_hostname_);
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish RPC "
                            << "meta entry to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "RPC meta entry to metadata server";
                        rpc_meta_publish_pending_.store(false);
                    }
                }
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
            auto err = master_client_.Connect(current_master_address);
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
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
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

size_t Client::GetLocalHotCacheSizeFromEnv() {
    if (const char* ev_size = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE")) {
        std::string ev_size_str(ev_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_CACHE_SIZE='" +
                                ev_size_str + "', disable local hot cache";
        // Check for negative values
        if (!ev_size_str.empty() && ev_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return 0;
        }
        try {
            unsigned long long v = std::stoull(ev_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return 0;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return 0;
        }
    }
    return 0;
}

size_t Client::GetLocalHotBlockSizeFromEnv(size_t default_value) {
    if (const char* ev_block_size =
            std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE")) {
        std::string ev_block_size_str(ev_block_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_BLOCK_SIZE='" +
                                ev_block_size_str +
                                "', using default block size";
        // Check for negative values
        if (!ev_block_size_str.empty() && ev_block_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return default_value;
        }
        try {
            unsigned long long v = std::stoull(ev_block_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return default_value;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return default_value;
        }
    }
    return default_value;
}

ErrorCode Client::InitLocalHotCache() {
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
    admission_sketch_.reset();

    // Defaults: hot cache is disabled unless MC_STORE_LOCAL_HOT_CACHE_SIZE is
    // set to a positive value; when enabled, default block size is 16MB and
    // thread_num is 2.
    size_t block_size = 16 * 1024 * 1024;  // 16MB default block size
    size_t thread_num = 2;

    // Read MC_STORE_LOCAL_HOT_CACHE_SIZE from environment
    size_t total_cache = GetLocalHotCacheSizeFromEnv();
    if (total_cache == 0) {
        // Environment variable not set or invalid, disable cache
        return ErrorCode::OK;
    }

    // Read MC_STORE_LOCAL_HOT_BLOCK_SIZE from environment
    block_size = GetLocalHotBlockSizeFromEnv(block_size);

    // MC_STORE_LOCAL_HOT_CACHE_USE_SHM: "1" enables memfd-backed shm (default
    // off). When enabled, hot cache is shareable with dummy clients via IPC.
    bool use_shm = false;
    if (const char* ev = std::getenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")) {
        use_shm = (std::string(ev) == "1");
    }

    // Enable hot cache
    {
        hot_cache_ =
            std::make_shared<LocalHotCache>(total_cache, block_size, use_shm);
        // Check if cache initialization was successful
        if (hot_cache_->GetCacheSize() == 0) {
            LOG(ERROR)
                << "Local hot cache creation failed: no blocks allocated. "
                << "total_cache=" << total_cache;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            return ErrorCode::INVALID_PARAMS;
        }

        int rc = transfer_engine_->registerLocalMemory(
            hot_cache_->GetBaseAddress(), hot_cache_->GetTotalSize(),
            kWildcardLocation, true, true);
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

        LOG(INFO) << "Local hot cache enabled with cache size=" << total_cache
                  << ", block size=" << block_size
                  << ", block amount=" << hot_cache_->GetCacheSize()
                  << ", shm=" << (use_shm ? "on" : "off")
                  << ", transfer engine registered=on";
        // Create async handler with 2 worker threads
        hot_cache_handler_ =
            std::make_unique<LocalHotCacheHandler>(hot_cache_, thread_num);
        admission_sketch_ = std::make_unique<CountMinSketch>();

        // MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD: minimum CMS count before a
        // key is admitted to hot cache (default 2).
        if (const char* ev =
                std::getenv("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD")) {
            std::string ev_str(ev);
            std::string error_msg =
                "Invalid MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD='" + ev_str +
                "', using default";
            try {
                unsigned long long v = std::stoull(ev_str, nullptr, 10);
                if (v > 0 && v <= 255) {
                    admission_threshold_ = static_cast<uint8_t>(v);
                } else {
                    LOG(WARNING) << error_msg;
                }
            } catch (const std::exception&) {
                LOG(WARNING) << error_msg;
            }
        }
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

void Client::ProcessSlicesAsync(const std::string& key,
                                const std::vector<Slice>& slices,
                                const Replica::Descriptor& replica) {
    if (!(hot_cache_ && replica.is_memory_replica())) {
        return;
    }

    // Skip local data unless hot cache is in shm mode (shared with dummy
    // clients who need local data cached for zero-copy access).
    if (!hot_cache_->IsShm() && IsReplicaOnLocalMemory(replica)) {
        return;
    }

    // Identify TE transfer slices (non-local) and submit async put tasks
    for (size_t i = 0; i < slices.size(); ++i) {
        if (!hot_cache_handler_->SubmitPutTask(key, slices[i])) {
            LOG(ERROR) << "Failed to submit hot cache put task for key=" << key
                       << " slice_idx=" << i;
            return;
        }
    }
}

bool Client::IsReplicaOnLocalMemory(const Replica::Descriptor& replica) {
    if (!replica.is_memory_replica()) {
        return false;
    }
    const auto replica_transfer_endpoint =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    if (metadata_connstring_ == P2PHANDSHAKE) {
        return replica_transfer_endpoint == GetTransportEndpoint();
    }
    return local_hostname_ == replica_transfer_endpoint;
}

}  // namespace mooncake
