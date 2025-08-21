#pragma once

#include <stdexcept>

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
    std::string etcd_endpoints;

    std::string cluster_id;
    std::string root_fs_dir;
    std::string memory_allocator;
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
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;

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

        // Convert string memory_allocator to BufferAllocatorType enum
        if (config.memory_allocator == "cachelib") {
            memory_allocator = BufferAllocatorType::CACHELIB;
        } else {
            memory_allocator = BufferAllocatorType::OFFSET;
        }

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
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;

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
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;

        // Convert string memory_allocator to BufferAllocatorType enum
        if (config.memory_allocator == "cachelib") {
            memory_allocator = mooncake::BufferAllocatorType::CACHELIB;
        } else {
            memory_allocator = mooncake::BufferAllocatorType::OFFSET;
        }
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
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        memory_allocator = config.memory_allocator;
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
    std::string cluster_id_ = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir_ = DEFAULT_ROOT_FS_DIR;
    BufferAllocatorType memory_allocator_ = BufferAllocatorType::OFFSET;

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

    MasterServiceConfigBuilder& set_cluster_id(const std::string& id) {
        cluster_id_ = id;
        return *this;
    }

    MasterServiceConfigBuilder& set_root_fs_dir(const std::string& dir) {
        root_fs_dir_ = dir;
        return *this;
    }

    MasterServiceConfigBuilder& set_memory_allocator(
        BufferAllocatorType allocator) {
        memory_allocator_ = allocator;
        return *this;
    }

    MasterServiceConfig build() const;
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
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;

    MasterServiceConfig() = default;

    // From WrappedMasterServiceConfig
    MasterServiceConfig(const WrappedMasterServiceConfig& config) {
        default_kv_lease_ttl = config.default_kv_lease_ttl;
        default_kv_soft_pin_ttl = config.default_kv_soft_pin_ttl;
        allow_evict_soft_pinned_objects =
            config.allow_evict_soft_pinned_objects;
        eviction_ratio = config.eviction_ratio;
        eviction_high_watermark_ratio = config.eviction_high_watermark_ratio;
        view_version = config.view_version;
        client_live_ttl_sec = config.client_live_ttl_sec;
        enable_ha = config.enable_ha;
        cluster_id = config.cluster_id;
        root_fs_dir = config.root_fs_dir;
        memory_allocator = config.memory_allocator;
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
    config.cluster_id = cluster_id_;
    config.root_fs_dir = root_fs_dir_;
    config.memory_allocator = memory_allocator_;
    return config;
}

// Implementation of MasterServiceConfig::builder()
inline MasterServiceConfigBuilder MasterServiceConfig::builder() {
    return MasterServiceConfigBuilder();
}

}  // namespace mooncake