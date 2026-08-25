#pragma once

#include <memory>
#include <string>

#include "master_config.h"
#include "rpc_service.h"

namespace coro_rpc {
class coro_rpc_server;
}  // namespace coro_rpc

namespace mooncake {

class HttpMetadataServer;
class MasterAdminServer;

// In-process master used for standalone single-node deployments and tests.
// When enabled, a store client starts this master in the same process so no
// external mooncake_master is required.
class EmbeddedMaster {
   public:
    EmbeddedMaster() = default;
    ~EmbeddedMaster();

    EmbeddedMaster(const EmbeddedMaster&) = delete;
    EmbeddedMaster& operator=(const EmbeddedMaster&) = delete;
    EmbeddedMaster(EmbeddedMaster&&) = delete;
    EmbeddedMaster& operator=(EmbeddedMaster&&) = delete;

    bool Start(InProcMasterConfig config);
    void Stop();

    int rpc_port() const { return rpc_port_; }
    int http_metrics_port() const { return http_metrics_port_; }
    int http_metadata_port() const { return http_metadata_port_; }
    std::string master_address() const {
        return std::string("127.0.0.1:") + std::to_string(rpc_port_);
    }
    std::string metadata_url() const {
        if (http_metadata_port_ <= 0) return {};
        return std::string("http://127.0.0.1:") +
               std::to_string(http_metadata_port_) + "/metadata";
    }
    std::string http_metrics_base() const {
        return std::string("http://127.0.0.1:") +
               std::to_string(http_metrics_port_);
    }
    std::shared_ptr<WrappedMasterService> service() const { return wrapped_; }

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::shared_ptr<WrappedMasterService> wrapped_;
    std::unique_ptr<MasterAdminServer> admin_server_;
    std::unique_ptr<HttpMetadataServer> meta_server_;
    int rpc_port_ = 0;
    int http_metrics_port_ = 0;
    int http_metadata_port_ = 0;
};

}  // namespace mooncake
