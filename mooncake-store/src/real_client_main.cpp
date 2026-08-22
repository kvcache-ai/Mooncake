#include <gflags/gflags.h>
#include <csignal>
#include <cstring>
#include <chrono>
#include <thread>
#include <asio/io_context.hpp>
#include <asio/signal_set.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common.h"
#include "config.h"
#include "real_client.h"

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
DEFINE_uint32(graceful_unmount_timeout_slack_seconds, 45,
              "Extra seconds added to --graceful_unmount_seconds to bound the "
              "shutdown wait. Covers the master-side scheduler delay and the "
              "client-side confirmation retries.");
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

namespace {

// Runs the graceful-unmount sequence for the standalone client.
//
// The standalone binary owns the termination-signal policy: it decides the
// grace period and the deadline, while Client only exposes the mechanism
// (initiate + await). RealClient::tearDownAll() keeps its immediate,
// deterministic semantics and is left to the regular teardown below.
void GracefulUnmountBeforeExit(RealClient &client) {
    if (FLAGS_graceful_unmount_seconds == 0 || !client.client_) {
        return;
    }
    const auto grace = std::chrono::seconds(FLAGS_graceful_unmount_seconds);
    const size_t requested = client.client_->GracefulUnmountAll(
        std::chrono::duration_cast<std::chrono::milliseconds>(grace).count());
    if (requested == 0) {
        return;
    }
    // The deadline must outlast the announced grace period: the master applies
    // it on its own scheduler and the client re-checks removal afterwards.
    const auto deadline =
        std::chrono::steady_clock::now() + grace +
        std::chrono::seconds(FLAGS_graceful_unmount_timeout_slack_seconds);
    client.client_->WaitForGracefulUnmountAll(deadline);
}

}  // namespace

int main(int argc, char *argv[]) {
    // Attention !!!
    // Initialization of ResourceTracker must be the most earliest.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

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

    // Termination signals are handled here, in the standalone entry point,
    // rather than in the shared ResourceTracker: signal disposition is
    // process-wide state and must not be claimed by a library that is also
    // linked into Python and other embedders.
    //
    // asio::signal_set installs a process-wide handler, so the signal is caught
    // no matter which thread the kernel picks. However, ResourceTracker blocks
    // these signals in the main thread and every thread that inherits its mask,
    // and a blocked signal is never delivered to a handler -- it stays pending
    // for that sigwait thread instead. Unblocking them only on the thread that
    // runs this io_context therefore makes delivery deterministic: it is the
    // only thread eligible to take them, so asio always wins the race.
    asio::io_context signal_io;
    asio::signal_set signals(signal_io, SIGINT, SIGTERM, SIGHUP);
    signals.async_wait([&](const asio::error_code &ec, int sig) {
        if (ec) {
            return;
        }
        LOG(INFO) << "Received signal " << sig << ", shutting down";
        GracefulUnmountBeforeExit(*client_inst);
        server.stop();
    });

    std::thread signal_thread([&signal_io]() {
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGINT);
        sigaddset(&set, SIGTERM);
        sigaddset(&set, SIGHUP);
        if (int rc = pthread_sigmask(SIG_UNBLOCK, &set, nullptr); rc != 0) {
            LOG(ERROR) << "Failed to unblock termination signals on the signal "
                          "thread: "
                       << strerror(rc);
            return;
        }
        signal_io.run();
    });

    LOG(INFO) << "Starting real client service on " << rpc_bind_host << ":"
              << FLAGS_port;

    const auto rc = server.start();

    signals.cancel();
    signal_io.stop();
    if (signal_thread.joinable()) {
        signal_thread.join();
    }
    return rc;
}
