#include <gflags/gflags.h>
#include <csignal>
#include <chrono>
#include <thread>
#include <asio/io_context.hpp>
#include <asio/signal_set.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common.h"
#include "config.h"
#include "real_client.h"
#include "version.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_string(local_buffer_size, "0", "Size of local buffer (e.g., 16MB, 1GB)");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_string(tenant_id, "default", "Tenant identifier");
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_bool(start_offload_rpc_server, true,
            "Expose TCP RPC for disk-tier reads "
            "(batch_get_offload_object / release_offload_buffer). "
            "Effective only when --enable_offload is true. "
            "Disable for a write-only owner.");
DEFINE_uint32(graceful_unmount_seconds, 0,
              "Grace period, in seconds, for unmounting segments on shutdown. "
              "0 (default) unmounts immediately. When greater than zero, a "
              "termination signal first asks the master to stop allocating on "
              "this client's segments while keeping them readable, and the "
              "process stays alive (holding its memory registrations) until "
              "the master released them, so peers that already hold segment "
              "information can finish their reads. Standalone client only.");
DECLARE_bool(enable_http_server);
DECLARE_int32(http_port);

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::batchRemove_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_put_from_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::batch_put_from_cuda_ipc_dummy_helper>(
        &real_client);
    server
        .register_handler<&RealClient::batch_upsert_from_cuda_ipc_dummy_helper>(
            &real_client);
    server.register_handler<
        &RealClient::batch_upsert_from_multi_buffers_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_dummy_helper>(&real_client);
    server.register_handler<&RealClient::upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_parts_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_batch_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_get_into_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::batch_get_into_cuda_ipc_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::get_into_range_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::get_into_ranges_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_ipc_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_unmap_shm_internal>(
        &real_client);
    server.register_handler<&RealClient::is_shm_mapped_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::release_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_release_hot_cache>(&real_client);
    server.register_handler<&RealClient::acquire_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::release_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::batch_acquire_buffer_dummy>(
        &real_client);
    server.register_handler<&RealClient::allocate_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
    server.register_handler<&RealClient::release_offload_buffer>(&real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    // This binary owns SIGINT/SIGTERM/SIGHUP through the asio::signal_set
    // below, so the shared tracker must not also run its sigwait consumer for
    // them: POSIX leaves it unspecified which one receives a signal that is
    // handled by sigaction and awaited by sigwait at the same time. Has to
    // happen before the first getInstance().
    mooncake::ResourceTracker::DisableSignalHandling();

    // Attention !!!
    // Initialization of ResourceTracker must be the most earliest.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    gflags::SetVersionString(mooncake::MOONCAKE_DISPLAY_VERSION);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    LOG(INFO) << "Mooncake real client version: "
              << mooncake::MOONCAKE_DISPLAY_VERSION;

    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);
    size_t local_buffer_size = string_to_byte_size(FLAGS_local_buffer_size);
#ifdef USE_ASCEND_DIRECT
    // just set to true, does not affect GPU process.
    globalConfig().ascend_agent_mode = true;
#endif

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        FLAGS_host, FLAGS_metadata_server, global_segment_size,
        local_buffer_size, FLAGS_protocol, FLAGS_device_names,
        FLAGS_master_server_address, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock", FLAGS_port,
        FLAGS_enable_offload, FLAGS_start_offload_rpc_server, "",
        FLAGS_tenant_id, FLAGS_enable_http_server, FLAGS_http_port);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    auto rpc_bind_host = getHostNameWithoutPort(FLAGS_host);
    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, rpc_bind_host);
    RegisterClientRpcService(server, *client_inst);

    // This entry point only owns signal registration and configuration; the
    // shutdown transition itself lives in RealClient::Close(), which every
    // frontend shares. ResourceTracker's own consumer was disabled above, so
    // asio is the single owner of these signals.
    asio::io_context signal_io;
    asio::signal_set signals(signal_io, SIGINT, SIGTERM, SIGHUP);
    signals.async_wait([&](const asio::error_code &ec, int sig) {
        if (ec) {
            return;
        }
        LOG(INFO) << "Received signal " << sig << ", shutting down";

        // Stop ingress before the graceful wait. Close() can block for the
        // grace period plus the confirmation budget, and while the server is
        // still serving, a handler could mount a segment that
        // GracefulUnmountAll() has already snapshotted past, or still be
        // running when Close() resets the client. stop() closes the acceptors,
        // closes the live connections and joins the handler pool, so no handler
        // is running by the time it returns.
        //
        // This does not shorten the grace period. What the grace period keeps
        // alive is peers reading these segments directly through the transfer
        // engine, which only needs the memory registrations and the master-side
        // segment state that Close() holds; it does not go through this RPC
        // port. The port only serves this host's own dummy clients, which are
        // going away with the process anyway.
        server.stop();

        CloseOptions options;
        options.grace_period =
            std::chrono::seconds(FLAGS_graceful_unmount_seconds);
        auto closed = client_inst->Close(options);
        if (!closed) {
            LOG(ERROR) << "Client shutdown reported an error: "
                       << toString(closed.error());
        }
    });

    std::thread signal_thread([&signal_io]() { signal_io.run(); });

    LOG(INFO) << "Starting real client service on " << rpc_bind_host << ":"
              << FLAGS_port;

    const auto rc = server.start();

    // Safe to call from any thread, and it does not interrupt a handler that is
    // already executing. If no signal arrived, this is what lets run() return
    // instead of blocking on the pending async_wait; if one did, the handler
    // keeps running Close() and this has no effect on it.
    signal_io.stop();

    // The handler calls server.stop() first, so start() returns while Close()
    // may still be running. This join is what actually waits for it, and it has
    // to happen before client_inst goes out of scope. `signals` is deliberately
    // left alone: asio's signal_set is not safe for concurrent access, and
    // ~signal_set cancels any operation that is still pending.
    if (signal_thread.joinable()) {
        signal_thread.join();
    }
    return rc;
}
