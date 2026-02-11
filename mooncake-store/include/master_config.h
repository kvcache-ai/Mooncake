#pragma once

#include <optional>
#include <stdexcept>

#include <glog/logging.h>

#include "config_helper.h"
#include "types.h"

namespace mooncake {

// The configuration for the master server
struct MasterConfig {
    bool enable_metric_reporting;
    uint32_t metrics_port;
    uint32_t rpc_port;
    uint32_t rpc_thread_num;
    std::string rpc_address;
    int32_t rpc_conn_timeout_seconds;
    bool rpc_enable_tcp_no_delay;

    uint64_t default_kv_lease_ttl;
    uint64_t default_kv_soft_pin_ttl;
    bool allow_evict_soft_pinned_objects;
    double eviction_ratio;
    double eviction_high_watermark_ratio;
    int64_t client_live_ttl_sec;

    bool enable_ha;
    bool enable_offload;
    std::string etcd_endpoints;

    std::string cluster_id;
    std::string root_fs_dir;
    int64_t global_file_segment_size;
    std::string memory_allocator;
    std::string allocation_strategy;

    // HTTP metadata server configuration
    bool enable_http_metadata_server;
    uint32_t http_metadata_server_port;
    std::string http_metadata_server_host;

    uint64_t put_start_discard_timeout_sec;
    uint64_t put_start_release_timeout_sec;

    // Storage backend eviction configuration
    bool enable_disk_eviction;
    uint64_t quota_bytes;

    // Task manager configuration
    uint32_t max_total_finished_tasks;
    uint32_t max_total_pending_tasks;
    uint32_t max_total_processing_tasks;
    uint64_t pending_task_timeout_sec;
    uint64_t processing_task_timeout_sec;
    std::string cxl_path;
    size_t cxl_size;
    bool enable_cxl = false;
};

class MasterServiceSupervisorConfig {
   public:
    // no default values (required parameters) - using RequiredParam
    RequiredParam<bool> enable_metric_reporting{"enable_metric_reporting"};
    RequiredParam<int> metrics_port{"metrics_port"};
    RequiredParam<int64_t> default_kv_lease_ttl{"default_kv_lease_ttl"};
    RequiredParam<int64_t> default_kv_soft_pin_ttl{"default_kv_soft_pin_ttl"};
    RequiredParam<bool> allow_evict_soft_pinned_objects{
        "allow_evict_soft_pinned_objects"};
    RequiredParam<double> eviction_ratio{"eviction_ratio"};
    RequiredParam<double> eviction_high_watermark_ratio{
        "eviction_high_watermark_ratio"};
    RequiredParam<int64_t> client_live_ttl_sec{"client_live_ttl_sec"};
    RequiredParam<bool> enable_offload{"enable_offload"};
    RequiredParam<int> rpc_port{"rpc_port"};
    RequiredParam<size_t> rpc_thread_num{"rpc_thread_num"};

    // Parameters with default values (optional parameters)
    std::string rpc_address = "0.0.0.0";
    std::chrono::steady_clock::duration rpc_conn_timeout = std::chrono::seconds(
        0);  // Client connection timeout. 0 = no timeout (infinite)
    bool rpc_enable_tcp_no_delay = true;
    std::string etcd_endpoints = "0.0.0.0:2379";
    std::string local_hostname = "0.0.0.0:50051";
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    int64_t global_file_segment_size = DEFAULT_GLOBAL_FILE_SEGMENT_SIZE;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;
    uint64_t put_start_discard_timeout_sec = DEFAULT_PUT_START_DISCARD_TIMEOUT;
    uint64_t put_start_release_timeout_sec = DEFAULT_PUT_START_RELEASE_TIMEOUT;
    bool enable_disk_eviction = true;
    uint64_t quota_bytes = 0;
    uint32_t max_total_finished_tasks = DEFAULT_MAX_TOTAL_FINISHED_TASKS;
    uint32_t max_total_pending_tasks = DEFAULT_MAX_TOTAL_PENDING_TASKS;
    uint32_t max_total_processing_tasks = DEFAULT_MAX_TOTAL_PROCESSING_TASKS;
    uint64_t pending_task_timeout_sec =
        DEFAULT_PENDING_TASK_TIMEOUT_SEC;  // 0 = no timeout(infinite)
    uint64_t processing_task_timeout_sec =
        DEFAULT_PROCESSING_TASK_TIMEOUT_SEC;  // 0 = no timeout(infinite)

    std::string cxl_path = DEFAULT_CXL_PATH;
    size_t cxl_size = DEFAULT_CXL_SIZE;
    bool enable_cxl = false;
    MasterServiceSupervisorConfig() = default;

    // From MasterConfig
    MasterServiceSupervisorConfig(const MasterConfig& config) {
        // Set required parameters using RequiredParam
        enable_metric_reporting = config.enable_metric_reporting;
        metrics_port = static_cast<int>(config.metrics_port);
        default_kv_lease_ttl = config.default_kv_lease_ttl;
        default_kv_soft_pin_ttl = config.default_kv_soft_pin_ttl;
        allow_evict_soft_pinned_objects =
            config.allow_evict_soft_pinned_objects;
        eviction_ratio = config.eviction_ratio;
        eviction_high_watermark_ratio = config.eviction_high_watermark_ratio;
        client_live_ttl_sec = config.client_live_ttl_sec;
        enable_offload = config.enable_offload;
        rpc_port = static_cast<int>(config.rpc_port);
        rpc_thread_num = static_cast<size_t>(config.rpc_thread_num);

        // Set optional parameters (these have default values)
        rpc_address = config.rpc_address;
        rpc_conn_timeout =
            std::chrono::seconds(config.rpc_conn_timeout_seconds);
        rpc_enable_tcp_no_delay = config.rpc_enable_tcp_no_delay;
        etcd_endpoints = config.etcd_endpoints;
        local_hostname = rpc_address + ":" + std::to_string(rpc_port);
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        global_file_segment_size = config.global_file_segment_size;

        // Convert string memory_allocator to BufferAllocatorType enum
        if (config.memory_allocator == "cachelib") {
            memory_allocator = BufferAllocatorType::CACHELIB;
        } else {
            memory_allocator = BufferAllocatorType::OFFSET;
        }

        put_start_discard_timeout_sec = config.put_start_discard_timeout_sec;
        put_start_release_timeout_sec = config.put_start_release_timeout_sec;
        enable_disk_eviction = config.enable_disk_eviction;
        quota_bytes = config.quota_bytes;

        max_total_finished_tasks = config.max_total_finished_tasks;
        max_total_pending_tasks = config.max_total_pending_tasks;
        max_total_processing_tasks = config.max_total_processing_tasks;
        pending_task_timeout_sec = config.pending_task_timeout_sec;
        processing_task_timeout_sec = config.processing_task_timeout_sec;

        cxl_path = config.cxl_path;
        cxl_size = config.cxl_size;
        enable_cxl = config.enable_cxl;
        validate();
    }

    // Some of the parameters are not used in constructor but will be used in
    // the future. So we need to validate them at the beginning of the program
    // to avoid unexpected errors in the future.
    void validate() const {
        // Validate that all required parameters are set
        if (!enable_metric_reporting.IsSet()) {
            throw std::runtime_error("enable_metric_reporting is not set");
        }
        if (!metrics_port.IsSet()) {
            throw std::runtime_error("metrics_port is not set");
        }
        if (!default_kv_lease_ttl.IsSet()) {
            throw std::runtime_error("default_kv_lease_ttl is not set");
        }
        if (!default_kv_soft_pin_ttl.IsSet()) {
            throw std::runtime_error("default_kv_soft_pin_ttl is not set");
        }
        if (!allow_evict_soft_pinned_objects.IsSet()) {
            throw std::runtime_error(
                "allow_evict_soft_pinned_objects is not set");
        }
        if (!eviction_ratio.IsSet()) {
            throw std::runtime_error("eviction_ratio is not set");
        }
        if (!eviction_high_watermark_ratio.IsSet()) {
            throw std::runtime_error(
                "eviction_high_watermark_ratio is not set");
        }
        if (!client_live_ttl_sec.IsSet()) {
            throw std::runtime_error("client_live_ttl_sec is not set");
        }
        if (!rpc_port.IsSet()) {
            throw std::runtime_error("rpc_port is not set");
        }
        if (!rpc_thread_num.IsSet()) {
            throw std::runtime_error("rpc_thread_num is not set");
        }
    }
};

class WrappedMasterServiceConfig {
   public:
    // Required parameters (no default values) - using RequiredParam
    RequiredParam<uint64_t> default_kv_lease_ttl{"default_kv_lease_ttl"};

    // Optional parameters (with default values)
    uint64_t default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    bool allow_evict_soft_pinned_objects =
        DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS;
    bool enable_metric_reporting = true;
    uint16_t http_port = 9003;
    double eviction_ratio = DEFAULT_EVICTION_RATIO;
    double eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    ViewVersionId view_version = 0;
    int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    bool enable_ha = false;
    bool enable_offload = false;
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    int64_t global_file_segment_size = DEFAULT_GLOBAL_FILE_SEGMENT_SIZE;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;
    AllocationStrategyType allocation_strategy_type =
        AllocationStrategyType::RANDOM;
    uint64_t put_start_discard_timeout_sec = DEFAULT_PUT_START_DISCARD_TIMEOUT;
    uint64_t put_start_release_timeout_sec = DEFAULT_PUT_START_RELEASE_TIMEOUT;
    bool enable_disk_eviction = true;
    uint64_t quota_bytes = 0;

    uint32_t max_total_finished_tasks = DEFAULT_MAX_TOTAL_FINISHED_TASKS;
    uint32_t max_total_pending_tasks = DEFAULT_MAX_TOTAL_PENDING_TASKS;
    uint32_t max_total_processing_tasks = DEFAULT_MAX_TOTAL_PROCESSING_TASKS;
    uint64_t pending_task_timeout_sec =
        DEFAULT_PENDING_TASK_TIMEOUT_SEC;  // 0 = no timeout(infinite)
    uint64_t processing_task_timeout_sec =
        DEFAULT_PROCESSING_TASK_TIMEOUT_SEC;  // 0 = no timeout(infinite)

    std::string cxl_path = DEFAULT_CXL_PATH;
    size_t cxl_size = DEFAULT_CXL_SIZE;
    bool enable_cxl = false;
    WrappedMasterServiceConfig() = default;

    // From MasterConfig
    WrappedMasterServiceConfig(const MasterConfig& config,
                               ViewVersionId view_version_param) {
        // Set required parameters using RequiredParam
        default_kv_lease_ttl = config.default_kv_lease_ttl;

        // Set optional parameters (these have default values)
        default_kv_soft_pin_ttl = config.default_kv_soft_pin_ttl;
        allow_evict_soft_pinned_objects =
            config.allow_evict_soft_pinned_objects;
        enable_metric_reporting = config.enable_metric_reporting;
        http_port = static_cast<uint16_t>(config.metrics_port);
        eviction_ratio = config.eviction_ratio;
        eviction_high_watermark_ratio = config.eviction_high_watermark_ratio;
        view_version = view_version_param;
        client_live_ttl_sec = config.client_live_ttl_sec;
        enable_ha = config.enable_ha;
        enable_offload = config.enable_offload;
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        global_file_segment_size = config.global_file_segment_size;
        enable_disk_eviction = config.enable_disk_eviction;
        quota_bytes = config.quota_bytes;

        // Convert string memory_allocator to BufferAllocatorType enum
        if (config.memory_allocator == "cachelib") {
            memory_allocator = mooncake::BufferAllocatorType::CACHELIB;
        } else {
            memory_allocator = mooncake::BufferAllocatorType::OFFSET;
        }

        // Convert string allocation_strategy to AllocationStrategyType enum
        if (config.allocation_strategy == "best_of_n") {
            allocation_strategy_type = AllocationStrategyType::BEST_OF_N;
        } else if (config.allocation_strategy == "cxl") {
            allocation_strategy_type = AllocationStrategyType::CXL;
        } else if (config.allocation_strategy == "random") {
            allocation_strategy_type = AllocationStrategyType::RANDOM;
        } else {
            LOG(WARNING)
                << "Unrecognized allocation_strategy value: '"
                << config.allocation_strategy << "'. Defaulting to 'random'. "
                << "Valid options are: random, best_of_n, cxl (case-sensitive)";
            allocation_strategy_type = AllocationStrategyType::RANDOM;
        }

        put_start_discard_timeout_sec = config.put_start_discard_timeout_sec;
        put_start_release_timeout_sec = config.put_start_release_timeout_sec;

        max_total_finished_tasks = config.max_total_finished_tasks;
        max_total_pending_tasks = config.max_total_pending_tasks;
        max_total_processing_tasks = config.max_total_processing_tasks;
        pending_task_timeout_sec = config.pending_task_timeout_sec;
        processing_task_timeout_sec = config.processing_task_timeout_sec;
        cxl_path = config.cxl_path;
        cxl_size = config.cxl_size;
        enable_cxl = config.enable_cxl;
    }

    // From MasterServiceSupervisorConfig, enable_ha is set to true
    WrappedMasterServiceConfig(const MasterServiceSupervisorConfig& config,
                               ViewVersionId view_version_param)
        : WrappedMasterServiceConfig() {
        // Set required parameters using assignment operator
        default_kv_lease_ttl = config.default_kv_lease_ttl;

        // Set optional parameters (these have default values)
        default_kv_soft_pin_ttl = config.default_kv_soft_pin_ttl;
        allow_evict_soft_pinned_objects =
            config.allow_evict_soft_pinned_objects;
        enable_metric_reporting = config.enable_metric_reporting;
        http_port = static_cast<uint16_t>(config.metrics_port);
        eviction_ratio = config.eviction_ratio;
        eviction_high_watermark_ratio = config.eviction_high_watermark_ratio;
        view_version = view_version_param;
        client_live_ttl_sec = config.client_live_ttl_sec;
        enable_ha =
            true;  // This is used in HA mode, so enable_ha should be true
        enable_offload = config.enable_offload;
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        global_file_segment_size = config.global_file_segment_size;
        memory_allocator = config.memory_allocator;
        enable_disk_eviction = config.enable_disk_eviction;
        quota_bytes = config.quota_bytes;
        put_start_discard_timeout_sec = config.put_start_discard_timeout_sec;
        put_start_release_timeout_sec = config.put_start_release_timeout_sec;
        max_total_finished_tasks = config.max_total_finished_tasks;
        max_total_pending_tasks = config.max_total_pending_tasks;
        max_total_processing_tasks = config.max_total_processing_tasks;
        pending_task_timeout_sec = config.pending_task_timeout_sec;
        processing_task_timeout_sec = config.processing_task_timeout_sec;

        cxl_path = config.cxl_path;
        cxl_size = config.cxl_size;
        enable_cxl = config.enable_cxl;
    }
};

// Forward declarations
class MasterServiceConfig;

// Builder class for MasterServiceConfig
class MasterServiceConfigBuilder {
   private:
    uint64_t default_kv_lease_ttl_ = DEFAULT_DEFAULT_KV_LEASE_TTL;
    uint64_t default_kv_soft_pin_ttl_ = DEFAULT_KV_SOFT_PIN_TTL_MS;
    bool allow_evict_soft_pinned_objects_ =
        DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS;
    double eviction_ratio_ = DEFAULT_EVICTION_RATIO;
    double eviction_high_watermark_ratio_ =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    ViewVersionId view_version_ = 0;
    int64_t client_live_ttl_sec_ = DEFAULT_CLIENT_LIVE_TTL_SEC;
    bool enable_ha_ = false;
    bool enable_offload_ = false;
    std::string cluster_id_ = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir_ = DEFAULT_ROOT_FS_DIR;
    int64_t global_file_segment_size_ = DEFAULT_GLOBAL_FILE_SEGMENT_SIZE;
    BufferAllocatorType memory_allocator_ = BufferAllocatorType::OFFSET;
    AllocationStrategyType allocation_strategy_type_ =
        AllocationStrategyType::RANDOM;
    bool enable_disk_eviction_ = true;
    uint64_t quota_bytes_ = 0;
    uint64_t put_start_discard_timeout_sec_ = DEFAULT_PUT_START_DISCARD_TIMEOUT;
    uint64_t put_start_release_timeout_sec_ = DEFAULT_PUT_START_RELEASE_TIMEOUT;
    uint32_t max_total_finished_tasks_ = DEFAULT_MAX_TOTAL_FINISHED_TASKS;
    uint32_t max_total_pending_tasks_ = DEFAULT_MAX_TOTAL_PENDING_TASKS;
    uint32_t max_total_processing_tasks_ = DEFAULT_MAX_TOTAL_PROCESSING_TASKS;
    uint64_t pending_task_timeout_sec_ = DEFAULT_PENDING_TASK_TIMEOUT_SEC;
    uint64_t processing_task_timeout_sec_ = DEFAULT_PROCESSING_TASK_TIMEOUT_SEC;

    std::string cxl_path_ = DEFAULT_CXL_PATH;
    size_t cxl_size_ = DEFAULT_CXL_SIZE;
    bool enable_cxl_ = false;

   public:
    MasterServiceConfigBuilder() = default;

    MasterServiceConfigBuilder& set_default_kv_lease_ttl(uint64_t ttl) {
        default_kv_lease_ttl_ = ttl;
        return *this;
    }

    MasterServiceConfigBuilder& set_default_kv_soft_pin_ttl(uint64_t ttl) {
        default_kv_soft_pin_ttl_ = ttl;
        return *this;
    }

    MasterServiceConfigBuilder& set_allow_evict_soft_pinned_objects(
        bool allow) {
        allow_evict_soft_pinned_objects_ = allow;
        return *this;
    }

    MasterServiceConfigBuilder& set_eviction_ratio(double ratio) {
        eviction_ratio_ = ratio;
        return *this;
    }

    MasterServiceConfigBuilder& set_eviction_high_watermark_ratio(
        double ratio) {
        eviction_high_watermark_ratio_ = ratio;
        return *this;
    }

    MasterServiceConfigBuilder& set_view_version(ViewVersionId version) {
        view_version_ = version;
        return *this;
    }

    MasterServiceConfigBuilder& set_client_live_ttl_sec(int64_t ttl) {
        client_live_ttl_sec_ = ttl;
        return *this;
    }

    MasterServiceConfigBuilder& set_enable_ha(bool enable) {
        enable_ha_ = enable;
        return *this;
    }

    MasterServiceConfigBuilder& set_enable_offload(bool enable) {
        enable_offload_ = enable;
        return *this;
    }

    MasterServiceConfigBuilder& set_cluster_id(const std::string& id) {
        cluster_id_ = id;
        return *this;
    }

    MasterServiceConfigBuilder& set_root_fs_dir(const std::string& dir) {
        root_fs_dir_ = dir;
        return *this;
    }

    MasterServiceConfigBuilder& set_global_file_segment_size(
        int64_t segment_size) {
        global_file_segment_size_ = segment_size;
        return *this;
    }

    MasterServiceConfigBuilder& set_memory_allocator(
        BufferAllocatorType allocator) {
        memory_allocator_ = allocator;
        return *this;
    }

    MasterServiceConfigBuilder& set_allocation_strategy_type(
        AllocationStrategyType type) {
        allocation_strategy_type_ = type;
        return *this;
    }

    MasterServiceConfigBuilder& set_put_start_discard_timeout_sec(
        uint64_t put_start_discard_timeout_sec) {
        put_start_discard_timeout_sec_ = put_start_discard_timeout_sec;
        return *this;
    }

    MasterServiceConfigBuilder& set_put_start_release_timeout_sec(
        uint64_t put_start_release_timeout_sec) {
        put_start_release_timeout_sec_ = put_start_release_timeout_sec;
        return *this;
    }

    MasterServiceConfigBuilder& set_max_total_finished_tasks(
        uint32_t max_total_finished_tasks) {
        max_total_finished_tasks_ = max_total_finished_tasks;
        return *this;
    }

    MasterServiceConfigBuilder& set_max_total_pending_tasks(
        uint32_t max_total_pending_tasks) {
        max_total_pending_tasks_ = max_total_pending_tasks;
        return *this;
    }

    MasterServiceConfigBuilder& set_max_total_processing_tasks(
        uint32_t max_total_processing_tasks) {
        max_total_processing_tasks_ = max_total_processing_tasks;
        return *this;
    }

    MasterServiceConfigBuilder& set_pending_task_timeout_sec(uint64_t sec) {
        pending_task_timeout_sec_ = sec;
        return *this;
    }

    MasterServiceConfigBuilder& set_processing_task_timeout_sec(uint64_t sec) {
        processing_task_timeout_sec_ = sec;
        return *this;
    }

    MasterServiceConfigBuilder& set_cxl_path(const std::string& path) {
        cxl_path_ = path;
        return *this;
    }

    MasterServiceConfigBuilder& set_cxl_size(size_t size) {
        cxl_size_ = size;
        return *this;
    }

    MasterServiceConfigBuilder& set_enable_cxl(bool enable) {
        enable_cxl_ = enable;
        return *this;
    }

    MasterServiceConfig build() const;
};

// Configuration for Task manager
struct TaskManagerConfig {
    uint32_t max_total_finished_tasks;
    uint32_t max_total_pending_tasks;
    uint32_t max_total_processing_tasks;
    uint64_t pending_task_timeout_sec;
    uint64_t processing_task_timeout_sec;
};

class MasterServiceConfig {
   public:
    uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    uint64_t default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    bool allow_evict_soft_pinned_objects =
        DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS;
    double eviction_ratio = DEFAULT_EVICTION_RATIO;
    double eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    ViewVersionId view_version = 0;
    int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    bool enable_ha = false;
    bool enable_offload = false;
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    int64_t global_file_segment_size = DEFAULT_GLOBAL_FILE_SEGMENT_SIZE;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;
    AllocationStrategyType allocation_strategy_type =
        AllocationStrategyType::RANDOM;
    uint64_t put_start_discard_timeout_sec = DEFAULT_PUT_START_DISCARD_TIMEOUT;
    uint64_t put_start_release_timeout_sec = DEFAULT_PUT_START_RELEASE_TIMEOUT;
    bool enable_disk_eviction = true;
    uint64_t quota_bytes = 0;

    TaskManagerConfig task_manager_config = {
        .max_total_finished_tasks = DEFAULT_MAX_TOTAL_FINISHED_TASKS,
        .max_total_pending_tasks = DEFAULT_MAX_TOTAL_PENDING_TASKS,
        .max_total_processing_tasks = DEFAULT_MAX_TOTAL_PROCESSING_TASKS,
        .pending_task_timeout_sec = DEFAULT_PENDING_TASK_TIMEOUT_SEC,
        .processing_task_timeout_sec = DEFAULT_PROCESSING_TASK_TIMEOUT_SEC,
    };

    std::string cxl_path = DEFAULT_CXL_PATH;
    size_t cxl_size = DEFAULT_CXL_SIZE;
    bool enable_cxl = false;
    MasterServiceConfig() = default;

    // From WrappedMasterServiceConfig
    MasterServiceConfig(const WrappedMasterServiceConfig& config) {
        auto cxl_allocator_type = BufferAllocatorType::CACHELIB;

        default_kv_lease_ttl = config.default_kv_lease_ttl;
        default_kv_soft_pin_ttl = config.default_kv_soft_pin_ttl;
        allow_evict_soft_pinned_objects =
            config.allow_evict_soft_pinned_objects;
        eviction_ratio = config.eviction_ratio;
        eviction_high_watermark_ratio = config.eviction_high_watermark_ratio;
        view_version = config.view_version;
        client_live_ttl_sec = config.client_live_ttl_sec;
        enable_ha = config.enable_ha;
        enable_offload = config.enable_offload;
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        global_file_segment_size = config.global_file_segment_size;
        memory_allocator =
            config.enable_cxl ? cxl_allocator_type : config.memory_allocator;
        allocation_strategy_type = config.allocation_strategy_type;
        enable_disk_eviction = config.enable_disk_eviction;
        quota_bytes = config.quota_bytes;
        put_start_discard_timeout_sec = config.put_start_discard_timeout_sec;
        put_start_release_timeout_sec = config.put_start_release_timeout_sec;
        task_manager_config.max_total_finished_tasks =
            config.max_total_finished_tasks;
        task_manager_config.max_total_pending_tasks =
            config.max_total_pending_tasks;
        task_manager_config.max_total_processing_tasks =
            config.max_total_processing_tasks;
        task_manager_config.pending_task_timeout_sec =
            config.pending_task_timeout_sec;
        task_manager_config.processing_task_timeout_sec =
            config.processing_task_timeout_sec;
        cxl_path = config.cxl_path;
        cxl_size = config.cxl_size;
        enable_cxl = config.enable_cxl;
    }

    // Static factory method to create a builder
    static MasterServiceConfigBuilder builder();
};

// Implementation of MasterServiceConfigBuilder::build()
inline MasterServiceConfig MasterServiceConfigBuilder::build() const {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = default_kv_lease_ttl_;
    config.default_kv_soft_pin_ttl = default_kv_soft_pin_ttl_;
    config.allow_evict_soft_pinned_objects = allow_evict_soft_pinned_objects_;
    config.eviction_ratio = eviction_ratio_;
    config.eviction_high_watermark_ratio = eviction_high_watermark_ratio_;
    config.view_version = view_version_;
    config.client_live_ttl_sec = client_live_ttl_sec_;
    config.enable_ha = enable_ha_;
    config.enable_offload = enable_offload_;
    config.cluster_id = cluster_id_;
    config.root_fs_dir = root_fs_dir_;
    config.global_file_segment_size = global_file_segment_size_;
    config.memory_allocator = memory_allocator_;
    config.allocation_strategy_type = allocation_strategy_type_;
    config.put_start_discard_timeout_sec = put_start_discard_timeout_sec_;
    config.put_start_release_timeout_sec = put_start_release_timeout_sec_;
    config.enable_disk_eviction = enable_disk_eviction_;
    config.quota_bytes = quota_bytes_;
    config.task_manager_config.max_total_finished_tasks =
        max_total_finished_tasks_;
    config.task_manager_config.max_total_pending_tasks =
        max_total_pending_tasks_;
    config.task_manager_config.max_total_processing_tasks =
        max_total_processing_tasks_;
    config.task_manager_config.pending_task_timeout_sec =
        pending_task_timeout_sec_;
    config.task_manager_config.processing_task_timeout_sec =
        processing_task_timeout_sec_;
    config.cxl_path = cxl_path_;
    config.cxl_size = cxl_size_;
    config.enable_cxl = enable_cxl_;
    return config;
}

// Implementation of MasterServiceConfig::builder()
inline MasterServiceConfigBuilder MasterServiceConfig::builder() {
    return MasterServiceConfigBuilder();
}

// Configuration for InProcMaster (in-process master server for testing)
struct InProcMasterConfig {
    std::optional<int> rpc_port;
    std::optional<int> http_metrics_port;
    std::optional<int> http_metadata_port;
    std::optional<uint64_t> default_kv_lease_ttl;
    std::optional<bool> enable_cxl;
    std::optional<std::string> cxl_path;
    std::optional<size_t> cxl_size;
};

// Builder class for InProcMasterConfig
class InProcMasterConfigBuilder {
   private:
    std::optional<int> rpc_port_ = std::nullopt;
    std::optional<int> http_metrics_port_ = std::nullopt;
    std::optional<int> http_metadata_port_ = std::nullopt;
    std::optional<uint64_t> default_kv_lease_ttl_ = std::nullopt;
    std::optional<bool> enable_cxl_ = std::nullopt;
    std::optional<std::string> cxl_path_ = std::nullopt;
    std::optional<size_t> cxl_size_ = std::nullopt;

   public:
    InProcMasterConfigBuilder() = default;

    InProcMasterConfigBuilder& set_rpc_port(int port) {
        rpc_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_http_metrics_port(int port) {
        http_metrics_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_http_metadata_port(int port) {
        http_metadata_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_default_kv_lease_ttl(uint64_t ttl) {
        default_kv_lease_ttl_ = ttl;
        return *this;
    }

    InProcMasterConfigBuilder& set_enable_cxl(bool enable) {
        enable_cxl_ = enable;
        return *this;
    }

    InProcMasterConfigBuilder& set_cxl_path(const std::string& path) {
        cxl_path_ = path;
        return *this;
    }

    InProcMasterConfigBuilder& set_cxl_size(size_t size) {
        cxl_size_ = size;
        return *this;
    }

    InProcMasterConfig build() const;
};

// Implementation of InProcMasterConfigBuilder::build()
inline InProcMasterConfig InProcMasterConfigBuilder::build() const {
    InProcMasterConfig config;
    config.rpc_port = rpc_port_;
    config.http_metrics_port = http_metrics_port_;
    config.http_metadata_port = http_metadata_port_;
    config.default_kv_lease_ttl = default_kv_lease_ttl_;
    config.enable_cxl = enable_cxl_;
    config.cxl_path = cxl_path_;
    config.cxl_size = cxl_size_;
    return config;
}

}  // namespace mooncake
