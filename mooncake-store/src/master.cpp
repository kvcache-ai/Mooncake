#include <chrono>  // For std::chrono
#include <memory>  // For std::unique_ptr
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "default_config.h"
#include "ha_helper.h"
#include "http_metadata_server.h"
#include "rpc_service.h"
#include "types.h"

#include "master_config.h"
#include "cli.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

// Function to start HTTP metadata server
std::unique_ptr<mooncake::HttpMetadataServer> StartHttpMetadataServer(
    int port, const std::string& host) {
    LOG(INFO) << "Starting C++ HTTP metadata server on " << host << ":" << port;

    try {
        auto server =
            std::make_unique<mooncake::HttpMetadataServer>(port, host);
        server->start();

        // Check if server started successfully
        if (server->is_running()) {
            LOG(INFO) << "C++ HTTP metadata server started successfully";
            return server;
        } else {
            LOG(ERROR) << "Failed to start C++ HTTP metadata server";
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start C++ HTTP metadata server: " << e.what();
        return nullptr;
    }
}

int main(int argc, char* argv[]) {
    // Initialize the master configuration
    mooncake::MasterConfig master_config;
    easylog::set_min_severity(easylog::Severity::WARN);

    Flag flag(
        argv[0],
        "Mooncake is the serving platform for Kimi, a leading LLM service "
        "provided by icon Moonshot AI.",
        "master.conf");

    flag.add_option(
            "--cluster_id", master_config.cluster_id,
            "Cluster ID for the master service, used for kvcache persistence "
            "in HA mode")
        ->configurable(false);

    flag.add_option("--max_threads", master_config.rpc_thread_num,
                    "Maximum number of threads to use")
        ->check(CLI::Range(
            uint32_t(1),
            static_cast<uint32_t>(std::thread::hardware_concurrency())));

    flag.add_option("--rpc_thread_num", master_config.rpc_thread_num,
                    "Number of threads for RPC server")
        ->check(CLI::Range(
            uint32_t(1),
            static_cast<uint32_t>(std::thread::hardware_concurrency())));
    flag.deprecate_option("--max_threads", "--rpc_thread_num");

    flag.add_option("--enable_metric_reporting",
                    master_config.enable_metric_reporting,
                    "Enable periodic metric reporting");
    flag.add_option("--metrics_port", master_config.metrics_port,
                    "Port for HTTP metrics server to listen on")
        ->check(CLI::Range(0, 65535));
    flag.add_option("--default_kv_lease_ttl",
                    master_config.default_kv_lease_ttl,
                    "Default lease time for kv objects in milliseconds");
    flag.add_option("--default_kv_soft_pin_ttl",
                    master_config.default_kv_soft_pin_ttl,
                    "Default soft pin ttl for kv objects in milliseconds");
    flag.add_option(
        "--allow_evict_soft_pinned_objects",
        master_config.allow_evict_soft_pinned_objects,
        "Whether to allow eviction of soft pinned objects during eviction");
    flag.add_option("--eviction_ratio", master_config.eviction_ratio,
                    "Ratio of objects to evict when storage space is full")
        ->check(CLI::Range(0.0, 1.0));
    flag.add_option("--eviction_high_watermark_ratio",
                    master_config.eviction_high_watermark_ratio,
                    "Ratio of high watermark trigger eviction")
        ->check(CLI::Range(0.0, 1.0));

    flag.add_option("--port", master_config.rpc_port,
                    "Port for master service to listen on")
        ->check(CLI::Range(0, 65535));
    flag.add_option("--rpc_port", master_config.rpc_port,
                    "Port for RPC server to listen on")
        ->check(CLI::Range(0, 65535));
    flag.deprecate_option("--port", "--rpc_port");

    flag.add_option("--rpc_address", master_config.rpc_address,
                    "Address for RPC server to bind to, required in HA mode");
    flag.add_option("--rpc_conn_timeout_seconds",
                    master_config.rpc_conn_timeout_seconds,
                    "Connection timeout in seconds (0 = no timeout)");
    flag.add_option("--rpc_enable_tcp_no_delay",
                    master_config.rpc_enable_tcp_no_delay,
                    "Enable TCP_NODELAY for RPC connections");

    bool enable_ha = true;
    flag.add_flag("--enable_ha", enable_ha,
                  "Enable high availability, which depends on etcd");
    flag.retire_option("--enable_ha");  // Deprecated

    flag.add_option(
        "--etcd_endpoints", master_config.etcd_endpoints,
        "Endpoints of ETCD server, separated by semicolon, required "
        "in HA mode");
    flag.add_option(
        "--client_ttl", master_config.client_live_ttl_sec,
        "How long a client is considered alive after the last ping, only "
        "used in HA mode");
    flag.add_option("--root_fs_dir", master_config.root_fs_dir,
                    "Root directory for storage backend, used in HA mode");
    flag.add_option("--global_file_segment_size",
                    master_config.global_file_segment_size,
                    "Size of global NFS/3FS segment in bytes");

    flag.add_option("--memory_allocator", master_config.memory_allocator,
                    "Memory allocator for global segments, cachelib | offset")
        ->check(CLI::IsMember({"cachelib", "offset"}));
    flag.add_option("--enable_http_metadata_server",
                    master_config.enable_http_metadata_server,
                    "Enable HTTP metadata server instead of etcd");
    flag.add_option("--http_metadata_server_port",
                    master_config.http_metadata_server_port,
                    "Port for HTTP metadata server to listen on");
    flag.add_option("--http_metadata_server_host",
                    master_config.http_metadata_server_host,
                    "Host for HTTP metadata server to bind to");
    flag.add_option("--put_start_discard_timeout_sec",
                    master_config.put_start_discard_timeout_sec,
                    "Timeout for discarding uncompleted PutStart operations");
    flag.add_option(
        "--put_start_release_timeout_sec",
        master_config.put_start_release_timeout_sec,
        "Timeout for releasing space allocated in uncompleted PutStart "
        "operations");
    flag.add_option(
        "--enable_disk_eviction", master_config.enable_disk_eviction,
        "Enable disk eviction feature for storage backend (default: true)");
    flag.add_option(
        "--quota_bytes", master_config.quota_bytes,
        "Quota for storage backend in bytes (0 = use default 90% of "
        "capacity)");

    flag.add_option("--log_dir", master_config.log_dir,
                    "Root directory for logs")
        ->configurable(false);
    std::unordered_map<std::string, mooncake::GlogLogLevel> log_level_map{
        {"info", mooncake::GlogLogLevel::INFO},
        {"warning", mooncake::GlogLogLevel::WARNING},
        {"error", mooncake::GlogLogLevel::ERROR},
        {"fatal", mooncake::GlogLogLevel::FATAL}};
    flag.add_option("--log_level", master_config.min_log_level,
                    "Log level for files")
        ->transform(CLI::CheckedTransformer(log_level_map, CLI::ignore_case))
        ->configurable(false);

    if (flag.parser(argc, argv)) {
        exit(1);
    }
    LOG(INFO) << master_config.stringify();

    master_config.InitLogging();

    // Start HTTP metadata server if enabled
    std::unique_ptr<mooncake::HttpMetadataServer> http_metadata_server;
    if (master_config.enable_http_metadata_server) {
        http_metadata_server =
            StartHttpMetadataServer(master_config.http_metadata_server_port,
                                    master_config.http_metadata_server_host);
        if (!http_metadata_server) {
            LOG(FATAL) << "Failed to start HTTP metadata server";
            return 1;
        }

        // Give the server some time to start
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (!master_config.etcd_endpoints.empty()) {
        mooncake::MasterServiceSupervisor supervisor(master_config);
        return supervisor.Start();
    } else {
        coro_rpc::coro_rpc_server server(
            master_config.rpc_thread_num, master_config.rpc_port,
            master_config.rpc_address,
            std::chrono::seconds(master_config.rpc_conn_timeout_seconds),
            master_config.rpc_enable_tcp_no_delay);
        mooncake::WrappedMasterService wrapped_master_service(master_config);
        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}
