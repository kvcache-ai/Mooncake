#include <gflags/gflags.h>

#include <chrono>  // For std::chrono
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "rpc_service.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

DEFINE_int32(port, 50051, "Port for master service to listen on");
DEFINE_int32(max_threads, 4, "Maximum number of threads to use");
DEFINE_bool(enable_gc, false, "Enable garbage collection");
DEFINE_bool(enable_metric_reporting, true, "Enable periodic metric reporting");
DEFINE_int32(metrics_port, 9003, "Port for HTTP metrics server to listen on");

int main(int argc, char* argv[]) {
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    easylog::set_min_severity(easylog::Severity::WARN);

    // init rpc server
    coro_rpc_server server(
        /*thread=*/std::min(
            FLAGS_max_threads,
            static_cast<int>(std::thread::hardware_concurrency())),
        /*port=*/FLAGS_port);
    LOG(INFO) << "Master service started on port " << FLAGS_port
              << ", enable_gc=" << FLAGS_enable_gc
              << ", max_threads=" << FLAGS_max_threads
              << ", enable_metric_reporting=" << FLAGS_enable_metric_reporting
              << ", metrics_port=" << FLAGS_metrics_port;

    mooncake::WrappedMasterService wrapped_master_service(
        FLAGS_enable_gc, FLAGS_enable_metric_reporting, FLAGS_metrics_port);
    server.register_handler<&mooncake::WrappedMasterService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetReplicaList>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);

    // Metric reporting is now handled by WrappedMasterService

    return !server.start();
}
