#include <gflags/gflags.h>

#include <chrono>  // For std::chrono
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "rpc_service.h"
#include "ha_helper.h"
#include "types.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

DEFINE_int32(port, 50051, "Port for master service to listen on");
DEFINE_int32(max_threads, 4, "Maximum number of threads to use");
DEFINE_bool(enable_gc, false, "Enable garbage collection");
DEFINE_bool(enable_metric_reporting, true, "Enable periodic metric reporting");
DEFINE_int32(metrics_port, 9003, "Port for HTTP metrics server to listen on");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
            "Default lease time for kv objects");
DEFINE_double(eviction_ratio, mooncake::DEFAULT_EVICTION_RATIO, "Ratio of objects to evict when storage space is full");
DEFINE_validator(eviction_ratio, [](const char* flagname, double value) {
    if (value < 0.0 || value > 1.0) {
        LOG(FATAL) << "Eviction ratio must be between 0.0 and 1.0";
        return false;
    }
    return true;
});
DEFINE_bool(enable_ha, false, "Enable high availability, which depends on ETCD");
DEFINE_string(etcd_endpoints, "", "Endpoints of ETCD server, separated by semicolon");

int main(int argc, char* argv[]) {
    easylog::set_min_severity(easylog::Severity::WARN);
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Master service started on port " << FLAGS_port
              << ", enable_gc=" << FLAGS_enable_gc
              << ", max_threads=" << FLAGS_max_threads
              << ", enable_metric_reporting=" << FLAGS_enable_metric_reporting
              << ", metrics_port=" << FLAGS_metrics_port
              << ", default_kv_lease_ttl=" << FLAGS_default_kv_lease_ttl
              << ", eviction_ratio=" << FLAGS_eviction_ratio
              << ", enable_ha=" << FLAGS_enable_ha
              << ", etcd_endpoints=" << FLAGS_etcd_endpoints;

    int server_thread_num = std::min(
            FLAGS_max_threads,
            static_cast<int>(std::thread::hardware_concurrency()));

    if (FLAGS_enable_ha && FLAGS_etcd_endpoints.empty()) {
        LOG(FATAL) << "ETCD endpoints must be set when enable_ha is true";
        return 1;
    }
    if (!FLAGS_enable_ha && !FLAGS_etcd_endpoints.empty()) {
        LOG(WARNING) << "ETCD endpoints are set but will not be used because high availability is disabled";
    }

    if (FLAGS_enable_ha) {
        mooncake::MasterServiceSupervisor supervisor(
            FLAGS_port,
            server_thread_num,
            FLAGS_enable_gc,
            FLAGS_enable_metric_reporting,
            FLAGS_metrics_port,
            FLAGS_default_kv_lease_ttl,
            FLAGS_eviction_ratio,
            FLAGS_etcd_endpoints);

        return supervisor.Start();
    } else {
        // version is not used in non-HA mode, just pass a dummy value
        mooncake::ViewVersion version = 0;
        coro_rpc::coro_rpc_server server(server_thread_num, FLAGS_port);
        mooncake::WrappedMasterService wrapped_master_service(
            FLAGS_enable_gc, FLAGS_default_kv_lease_ttl,
            FLAGS_enable_metric_reporting, FLAGS_metrics_port,
            FLAGS_eviction_ratio, version);

        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}
