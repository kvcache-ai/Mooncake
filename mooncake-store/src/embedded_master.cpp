#include "embedded_master.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <string_view>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "http_metadata_server.h"
#include "master_admin_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

EmbeddedMaster::~EmbeddedMaster() { Stop(); }

bool EmbeddedMaster::Start(InProcMasterConfig config) {
    if (server_) {
        LOG(ERROR) << "Embedded master is already running on port "
                   << rpc_port_;
        return false;
    }
    try {
        // Allocate all needed ports atomically to avoid collisions from
        // rapid sequential getFreeTcpPort() calls (TOCTOU).
        int needed = (!config.rpc_port.has_value()) +
                     (!config.http_metrics_port.has_value()) +
                     (!config.http_metadata_port.has_value());
        std::vector<int> available_ports;
        if (needed > 0) {
            std::vector<int> reserved_ports;
            if (config.rpc_port.has_value()) {
                reserved_ports.push_back(config.rpc_port.value());
            }
            if (config.http_metrics_port.has_value()) {
                reserved_ports.push_back(config.http_metrics_port.value());
            }
            if (config.http_metadata_port.has_value()) {
                reserved_ports.push_back(config.http_metadata_port.value());
            }
            auto free_ports = getFreeTcpPorts(
                needed + static_cast<int>(reserved_ports.size()));
            available_ports.reserve(needed);
            for (int port : free_ports) {
                if (std::find(reserved_ports.begin(), reserved_ports.end(),
                              port) == reserved_ports.end()) {
                    available_ports.push_back(port);
                    if (static_cast<int>(available_ports.size()) == needed) {
                        break;
                    }
                }
            }
            if (static_cast<int>(available_ports.size()) < needed) {
                LOG(ERROR) << "Failed to reserve " << needed
                           << " free TCP ports for embedded master";
                return false;
            }
        }
        int idx = 0;
        rpc_port_ = config.rpc_port.has_value() ? config.rpc_port.value()
                                                : available_ports[idx++];
        http_metrics_port_ = config.http_metrics_port.has_value()
                                 ? config.http_metrics_port.value()
                                 : available_ports[idx++];
        http_metadata_port_ = config.http_metadata_port.has_value()
                                  ? config.http_metadata_port.value()
                                  : available_ports[idx++];

        if (http_metadata_port_ > 0) {
            meta_server_ = std::make_unique<HttpMetadataServer>(
                static_cast<uint16_t>(http_metadata_port_), "127.0.0.1");
            if (!meta_server_->start()) {
                LOG(ERROR) << "Failed to start embedded HTTP metadata server "
                              "on port "
                           << http_metadata_port_;
                meta_server_.reset();
                return false;
            }
        }

        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"0.0.0.0",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server_->init_ibv();
        }

        uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
        if (config.default_kv_lease_ttl.has_value()) {
            default_kv_lease_ttl = config.default_kv_lease_ttl.value();
        } else if (const char* ttl_env = std::getenv("DEFAULT_KV_LEASE_TTL")) {
            char* endptr = nullptr;
            unsigned long parsed = std::strtoul(ttl_env, &endptr, 10);
            if (endptr != ttl_env && endptr && *endptr == '\0') {
                default_kv_lease_ttl = static_cast<uint64_t>(parsed);
            }
        }

        WrappedMasterServiceConfig wms_cfg;
        wms_cfg.default_kv_lease_ttl = default_kv_lease_ttl;
        wms_cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
        wms_cfg.allow_evict_soft_pinned_objects = true;
        wms_cfg.enable_metric_reporting = false;
        wms_cfg.enable_offload = config.enable_offload.has_value()
                                     ? config.enable_offload.value()
                                     : false;
        wms_cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
        wms_cfg.eviction_high_watermark_ratio =
            config.eviction_high_watermark_ratio.has_value()
                ? config.eviction_high_watermark_ratio.value()
                : DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
        wms_cfg.view_version = 0;
        wms_cfg.enable_ha = false;
        wms_cfg.http_port = static_cast<uint16_t>(http_metrics_port_);
        wms_cfg.cluster_id = DEFAULT_CLUSTER_ID;
        wms_cfg.root_fs_dir = config.root_fs_dir.has_value()
                                  ? config.root_fs_dir.value()
                                  : DEFAULT_ROOT_FS_DIR;
        wms_cfg.memory_allocator = BufferAllocatorType::OFFSET;
        if (config.enable_disk_eviction.has_value()) {
            wms_cfg.enable_disk_eviction = config.enable_disk_eviction.value();
        }
        if (config.quota_bytes.has_value()) {
            wms_cfg.quota_bytes = config.quota_bytes.value();
        }

        wms_cfg.enable_cxl =
            config.enable_cxl.has_value() ? config.enable_cxl.value() : false;
        if (config.cxl_path.has_value()) {
            wms_cfg.cxl_path = config.cxl_path.value();
        } else if (const char* cxl_path_env = std::getenv("MC_CXL_DEV_PATH")) {
            wms_cfg.cxl_path = cxl_path_env;
        }

        if (config.cxl_size.has_value()) {
            wms_cfg.cxl_size = config.cxl_size.value();
        } else if (const char* cxl_size_env = std::getenv("MC_CXL_DEV_SIZE")) {
            char* endptr = nullptr;
            unsigned long long val = std::strtoull(cxl_size_env, &endptr, 10);
            if (endptr != cxl_size_env && *endptr == '\0') {
                wms_cfg.cxl_size = static_cast<size_t>(val);
            }
        }

        wrapped_ = std::make_shared<WrappedMasterService>(wms_cfg);
        admin_server_ = std::make_unique<MasterAdminServer>(
            static_cast<uint16_t>(http_metrics_port_),
            /*enable_metric_reporting=*/false);
        if (!admin_server_->Start()) {
            LOG(ERROR) << "Failed to start embedded master admin server on "
                          "port "
                       << http_metrics_port_;
            admin_server_.reset();
            wrapped_.reset();
            server_.reset();
            return false;
        }
        admin_server_->SetRuntimeState(ha::MasterRuntimeState::kServing);
        admin_server_->SetServiceDelegate(wrapped_);
        admin_server_->SetServiceAvailable(true);
        RegisterRpcService(*server_, *wrapped_);

        auto ec = server_->async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start embedded master RPC server on port "
                       << rpc_port_;
            admin_server_->Stop();
            admin_server_.reset();
            wrapped_.reset();
            server_.reset();
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        LOG(INFO) << "Embedded master started on " << master_address()
                  << ", metrics_port=" << http_metrics_port_
                  << ", metadata_port=" << http_metadata_port_;
        return true;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start embedded master: " << e.what();
        Stop();
        return false;
    } catch (...) {
        LOG(ERROR) << "Failed to start embedded master";
        Stop();
        return false;
    }
}

void EmbeddedMaster::Stop() {
    if (admin_server_) {
        admin_server_->Stop();
        admin_server_.reset();
    }
    if (server_) {
        server_->stop();
        server_.reset();
        wrapped_.reset();
    }
    if (meta_server_) {
        meta_server_->stop();
        meta_server_.reset();
    }
    rpc_port_ = 0;
    http_metrics_port_ = 0;
    http_metadata_port_ = 0;
}

}  // namespace mooncake
