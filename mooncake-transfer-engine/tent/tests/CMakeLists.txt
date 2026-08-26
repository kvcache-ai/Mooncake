# TENT Tests and Examples

# TENT Metrics Example
add_executable(tent_metrics_example tent_metrics_example.cpp)
# asio_shared must be linked explicitly for executables:
# ASIO_SEPARATE_COMPILATION mode requires that any binary using ASIO symbols
# links against the shared asio implementation directly. Libraries
# (tent_metrics, tent_interface) get it transitively; executables need it
# explicit.
target_link_libraries(tent_metrics_example PRIVATE tent_metrics tent_common
                                                   asio_shared glog::glog ibverbs)
target_include_directories(tent_metrics_example
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# TENT Metrics Config Loader Unit Test
add_executable(metrics_config_loader_test metrics_config_loader_test.cpp)
target_link_libraries(metrics_config_loader_test
                      PRIVATE tent_metrics tent_common gtest gtest_main glog::glog)
target_include_directories(metrics_config_loader_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME metrics_config_loader_test COMMAND metrics_config_loader_test)

# TENT Metrics HTTP server test: validates that initialize() detects a busy
# metrics port and degrades to log-only metrics instead of falsely reporting a
# listening endpoint.
add_executable(tent_metrics_http_server_test metrics_http_server_test.cpp)
target_link_libraries(tent_metrics_http_server_test
                      PRIVATE tent_metrics tent_common gtest gtest_main)
if(TARGET asio_shared)
  target_link_libraries(tent_metrics_http_server_test PRIVATE asio_shared)
endif()
target_include_directories(tent_metrics_http_server_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_metrics_http_server_test
         COMMAND tent_metrics_http_server_test)

# TENT Metrics recording test: drives a real TransferEngineImpl with a
# FakeTransport and asserts that counters/histograms move correctly via the JSON
# output. Covers completed/failed/infeasible-deadline recording and histogram
# bucket invariants. Compiles to a stub test when metrics are disabled at
# compile time.
add_executable(tent_metrics_recording_test metrics_recording_test.cpp)
target_link_libraries(tent_metrics_recording_test PRIVATE gtest gtest_main
                                                          tent_link_group glog::glog)
if(TARGET asio_shared)
  target_link_libraries(tent_metrics_recording_test PRIVATE asio_shared)
endif()
target_include_directories(tent_metrics_recording_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_metrics_recording_test COMMAND tent_metrics_recording_test)

add_executable(admission_queue_test admission_queue_test.cpp
                                    ../src/runtime/admission_queue.cpp)
target_link_libraries(admission_queue_test PRIVATE tent_common gtest gtest_main)
target_include_directories(admission_queue_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME admission_queue_test COMMAND admission_queue_test)

add_executable(receiver_credit_test receiver_credit_test.cpp
                                    ../src/runtime/receiver_credit.cpp)
target_link_libraries(receiver_credit_test PRIVATE tent_common gtest gtest_main)
target_include_directories(receiver_credit_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME receiver_credit_test COMMAND receiver_credit_test)

add_executable(bw_arbitration_test bw_arbitration_test.cpp)
target_link_libraries(bw_arbitration_test PRIVATE tent_common gtest gtest_main)
target_include_directories(bw_arbitration_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME bw_arbitration_test COMMAND bw_arbitration_test)

# Reproducible hot-path microbenchmark; intentionally not registered with ctest.
# Run manually when changing deadline promotion partitioning.
add_executable(deadline_promotion_bench deadline_promotion_bench.cpp
                                        ../src/runtime/admission_queue.cpp)
target_link_libraries(deadline_promotion_bench PRIVATE tent_common)
target_include_directories(deadline_promotion_bench
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

add_executable(promotion_policy_test promotion_policy_test.cpp)
target_link_libraries(promotion_policy_test PRIVATE tent_common gtest
                                                    gtest_main)
target_include_directories(promotion_policy_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME promotion_policy_test COMMAND promotion_policy_test)

add_executable(rdma_cancel_test rdma_cancel_test.cpp ../src/runtime/slab.cpp)
target_link_libraries(rdma_cancel_test PRIVATE tent_common gtest gtest_main)
target_include_directories(rdma_cancel_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME rdma_cancel_test COMMAND rdma_cancel_test)

add_executable(thread_local_storage_test thread_local_storage_test.cpp)
target_link_libraries(thread_local_storage_test PRIVATE gtest gtest_main)
target_include_directories(thread_local_storage_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME thread_local_storage_test COMMAND thread_local_storage_test)

add_executable(tent_rw_spinlock_test rw_spinlock_test.cpp)
target_link_libraries(tent_rw_spinlock_test PRIVATE gtest gtest_main)
target_include_directories(tent_rw_spinlock_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_rw_spinlock_test COMMAND tent_rw_spinlock_test)

add_executable(tent_ip_utils_test ip_utils_test.cpp)
target_link_libraries(tent_ip_utils_test PRIVATE tent_common gtest gtest_main)
target_include_directories(tent_ip_utils_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_ip_utils_test COMMAND tent_ip_utils_test)

add_executable(tent_qp_pool_layout_test qp_pool_layout_test.cpp)
target_link_libraries(tent_qp_pool_layout_test PRIVATE tent_common gtest
                                                       gtest_main)
target_include_directories(tent_qp_pool_layout_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_qp_pool_layout_test COMMAND tent_qp_pool_layout_test)

add_executable(tent_coalesce_regions_test coalesce_regions_test.cpp)
target_link_libraries(tent_coalesce_regions_test PRIVATE tent_common gtest
                                                         gtest_main)
target_include_directories(tent_coalesce_regions_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_coalesce_regions_test COMMAND tent_coalesce_regions_test)

add_executable(segment_manager_test segment_manager_test.cpp)
target_link_libraries(segment_manager_test PRIVATE gtest gtest_main
                                                   tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(segment_manager_test PRIVATE asio_shared)
endif()
target_include_directories(segment_manager_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME segment_manager_test COMMAND segment_manager_test)

add_executable(request_merge_test request_merge_test.cpp)
target_link_libraries(request_merge_test PRIVATE gtest gtest_main
                                                 tent_link_group)
target_include_directories(request_merge_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME request_merge_test COMMAND request_merge_test)

add_executable(transfer_engine_config_override_test
               transfer_engine_config_override_test.cpp)
target_link_libraries(transfer_engine_config_override_test
                      PRIVATE gtest gtest_main tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(transfer_engine_config_override_test
                        PRIVATE asio_shared)
endif()
target_include_directories(transfer_engine_config_override_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME transfer_engine_config_override_test
         COMMAND transfer_engine_config_override_test)

add_executable(tent_tcp_transport_test tcp_transport_test.cpp)
target_link_libraries(tent_tcp_transport_test PRIVATE gtest gtest_main
                                                      tent_link_group)
target_include_directories(tent_tcp_transport_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_tcp_transport_test COMMAND tent_tcp_transport_test)

add_executable(tent_tcp_transport_config_test tcp_transport_config_test.cpp)
target_link_libraries(tent_tcp_transport_config_test PRIVATE gtest gtest_main
                                                        tent_link_group)
target_include_directories(tent_tcp_transport_config_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_tcp_transport_config_test COMMAND tent_tcp_transport_config_test)

add_executable(tent_high_performance_tcp_workers_test
               high_performance_tcp_workers_test.cpp)
target_link_libraries(tent_high_performance_tcp_workers_test PRIVATE gtest
                                                                    gtest_main
                                                                    tent_xport_tcp_hp_core)
target_include_directories(
  tent_high_performance_tcp_workers_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_workers_test
         COMMAND tent_high_performance_tcp_workers_test)

add_executable(tent_high_performance_tcp_protocol_test
               high_performance_tcp_protocol_test.cpp)
target_link_libraries(tent_high_performance_tcp_protocol_test PRIVATE gtest gtest_main
                                                                    tent_xport_tcp_hp_core)
target_include_directories(tent_high_performance_tcp_protocol_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_protocol_test
         COMMAND tent_high_performance_tcp_protocol_test)

add_executable(tent_high_performance_tcp_buffer_registry_test
               high_performance_tcp_buffer_registry_test.cpp)
target_link_libraries(tent_high_performance_tcp_buffer_registry_test PRIVATE
                      gtest gtest_main tent_xport_tcp_hp_core)
target_include_directories(
  tent_high_performance_tcp_buffer_registry_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_buffer_registry_test
         COMMAND tent_high_performance_tcp_buffer_registry_test)

add_executable(tent_high_performance_tcp_socket_test
               high_performance_tcp_socket_test.cpp)
target_link_libraries(tent_high_performance_tcp_socket_test PRIVATE gtest gtest_main
                                                                  tent_xport_tcp_hp_core)
target_include_directories(tent_high_performance_tcp_socket_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_socket_test
         COMMAND tent_high_performance_tcp_socket_test)

add_executable(tent_high_performance_tcp_transport_test
               high_performance_tcp_transport_test.cpp)
target_link_libraries(tent_high_performance_tcp_transport_test PRIVATE
                      gtest gtest_main tent_link_group)
target_include_directories(tent_high_performance_tcp_transport_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_transport_test
         COMMAND tent_high_performance_tcp_transport_test)

add_executable(tent_high_performance_tcp_rpc_gate_test
               high_performance_tcp_rpc_gate_test.cpp)
target_link_libraries(tent_high_performance_tcp_rpc_gate_test PRIVATE
                      gtest gtest_main tent_link_group)
target_include_directories(tent_high_performance_tcp_rpc_gate_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_rpc_gate_test
         COMMAND tent_high_performance_tcp_rpc_gate_test)

add_executable(tent_high_performance_tcp_e2e_test
               high_performance_tcp_e2e_test.cpp)
target_link_libraries(tent_high_performance_tcp_e2e_test PRIVATE
                      gtest gtest_main tent_link_group)
target_include_directories(tent_high_performance_tcp_e2e_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_high_performance_tcp_e2e_test_concurrency_1
         COMMAND tent_high_performance_tcp_e2e_test
                 --gtest_filter=HighPerformanceTcpE2eTest.WriteThenReadConcurrency1)
add_test(NAME tent_high_performance_tcp_e2e_test_concurrency_4
         COMMAND tent_high_performance_tcp_e2e_test
                 --gtest_filter=HighPerformanceTcpE2eTest.WriteThenReadConcurrency4)
add_test(NAME tent_high_performance_tcp_e2e_test_concurrency_16
         COMMAND tent_high_performance_tcp_e2e_test
                 --gtest_filter=HighPerformanceTcpE2eTest.WriteThenReadConcurrency16)

add_executable(tent_tcp_datapath_roundtrip_test
               tcp_datapath_roundtrip_test.cpp)
target_link_libraries(tent_tcp_datapath_roundtrip_test PRIVATE gtest gtest_main
                                                      tent_link_group)
target_include_directories(tent_tcp_datapath_roundtrip_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
# Each case runs in its own process: the test forks a server child, which
# must happen before the client process initializes any engine state.
add_test(NAME tent_tcp_datapath_roundtrip_test_single_threaded
         COMMAND tent_tcp_datapath_roundtrip_test
                 --gtest_filter=TcpDataPathRoundtripTest.WriteThenReadAcrossProcesses)
add_test(NAME tent_tcp_datapath_roundtrip_test_multi_threaded_rpc
         COMMAND tent_tcp_datapath_roundtrip_test
                 --gtest_filter=TcpDataPathRoundtripTest.WriteThenReadAcrossProcessesMultiThreadedRpc)

add_executable(tent_shm_transport_test shm_transport_test.cpp)
target_link_libraries(tent_shm_transport_test PRIVATE gtest gtest_main
                                                      tent_link_group)
target_include_directories(tent_shm_transport_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_shm_transport_test COMMAND tent_shm_transport_test)

add_executable(tent_failover_test failover_test.cpp)
target_link_libraries(tent_failover_test PRIVATE gtest gtest_main
                                                 tent_link_group)
target_include_directories(tent_failover_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_failover_test COMMAND tent_failover_test)

add_executable(tent_endpoint_lifecycle_test endpoint_lifecycle_test.cpp)
target_link_libraries(tent_endpoint_lifecycle_test PRIVATE gtest gtest_main
                                                           tent_link_group)
target_include_directories(tent_endpoint_lifecycle_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_endpoint_lifecycle_test COMMAND tent_endpoint_lifecycle_test)

add_executable(tent_endpoint_store_test endpoint_store_test.cpp)
target_link_libraries(tent_endpoint_store_test PRIVATE gtest gtest_main
                                                       tent_link_group)
target_include_directories(tent_endpoint_store_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_endpoint_store_test COMMAND tent_endpoint_store_test)

add_executable(tent_rdma_transport_test rdma_transport_test.cpp)
target_link_libraries(tent_rdma_transport_test PRIVATE gtest gtest_main
                                                       tent_link_group ibverbs)
target_include_directories(tent_rdma_transport_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_rdma_transport_test COMMAND tent_rdma_transport_test)

if(USE_HIP)
  find_package(HIP REQUIRED)
  add_executable(tent_rocm_platform_test rocm_platform_test.cpp)
  target_link_libraries(tent_rocm_platform_test
                        PRIVATE gtest gtest_main tent_link_group hip::host)
  target_include_directories(tent_rocm_platform_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
  add_test(NAME tent_rocm_platform_test COMMAND tent_rocm_platform_test)
endif()

if(TARGET tent_xport_gds)
  add_executable(tent_gds_transport_status_test gds_transport_status_test.cpp)
  target_link_libraries(tent_gds_transport_status_test PRIVATE gtest gtest_main
                                                               tent_link_group)
  target_link_options(
    tent_gds_transport_status_test PRIVATE "-Wl,--wrap=cuFileDriverOpen"
    "-Wl,--wrap=cuFileBatchIOGetStatus" "-Wl,--wrap=cuFileBatchIOCancel")
  target_include_directories(tent_gds_transport_status_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
  add_test(NAME tent_gds_transport_status_test
           COMMAND tent_gds_transport_status_test)
endif()

if(USE_SUNRISE)
  add_executable(tent_sunrise_link_transport_test
                 sunrise_link_transport_test.cpp)
  target_link_libraries(tent_sunrise_link_transport_test
                        PRIVATE gtest gtest_main tent_link_group)
  target_include_directories(
    tent_sunrise_link_transport_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
  add_test(NAME tent_sunrise_link_transport_test
           COMMAND tent_sunrise_link_transport_test)
endif()

if(USE_MPCOMM)
  add_executable(tent_mpcomm_transport_test mpcomm_transport_test.cpp)
  target_link_libraries(tent_mpcomm_transport_test PRIVATE gtest gtest_main
                                                           tent_link_group)
  target_include_directories(
    tent_mpcomm_transport_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
  add_test(NAME tent_mpcomm_transport_test COMMAND tent_mpcomm_transport_test)
endif()

# Boundary tests of the TENT side of the mpcomm transport, driven through an
# injected MpcommAdapter. Built unconditionally: nothing here includes mpcomm.h
# and nothing needs an RDMA device, so these run wherever the rest of the suite
# runs. The hardware data path stays in tent_mpcomm_transport_test above, which
# skips itself when no device is present.
add_executable(tent_mpcomm_boundary_test mpcomm_boundary_test.cpp)
target_link_libraries(tent_mpcomm_boundary_test PRIVATE gtest gtest_main
                                                        tent_link_group)
target_include_directories(
  tent_mpcomm_boundary_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
                                    ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
add_test(NAME tent_mpcomm_boundary_test COMMAND tent_mpcomm_boundary_test)

add_executable(tent_fault_proxy_test fault_proxy_test.cpp)
target_link_libraries(tent_fault_proxy_test PRIVATE gtest gtest_main
                                                    tent_link_group)
target_include_directories(tent_fault_proxy_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_fault_proxy_test COMMAND tent_fault_proxy_test)

add_executable(tent_rail_monitor_test rail_monitor_test.cpp)
target_link_libraries(tent_rail_monitor_test PRIVATE gtest gtest_main
                                                     tent_link_group)
target_include_directories(tent_rail_monitor_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_rail_monitor_test COMMAND tent_rail_monitor_test)

# QoS Contract Schema Unit Test
add_executable(tent_qos_contract_test qos_contract_test.cpp
                                      ../src/runtime/qos_contract.cpp)
target_link_libraries(tent_qos_contract_test PRIVATE gtest gtest_main
                                                     tent_common)
target_include_directories(tent_qos_contract_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_qos_contract_test COMMAND tent_qos_contract_test)

add_executable(tent_transport_selector_test transport_selector_test.cpp)
target_link_libraries(tent_transport_selector_test PRIVATE gtest gtest_main
                                                           tent_link_group)
target_include_directories(tent_transport_selector_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_transport_selector_test COMMAND tent_transport_selector_test)

add_executable(tent_topology_priority_matrix_test
               topology_priority_matrix_test.cpp)
target_link_libraries(tent_topology_priority_matrix_test PRIVATE gtest
                                                                 gtest_main
                                                                 tent_link_group)
target_include_directories(tent_topology_priority_matrix_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_topology_priority_matrix_test
         COMMAND tent_topology_priority_matrix_test)

if(USE_UB)
  add_executable(
    tent_ub_teardown_test
    ub_teardown_test.cpp ../src/transport/ub/buffers.cpp
    ../src/transport/ub/context.cpp ../src/transport/ub/jfc.cpp)
  target_link_libraries(tent_ub_teardown_test PRIVATE tent_common gtest
                                                      gtest_main)
  target_include_directories(tent_ub_teardown_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
  add_test(NAME tent_ub_teardown_test COMMAND tent_ub_teardown_test)
endif()

# End-to-end failover test: drives real TransferEngineImpl with
# FaultProxyTransport-wrapped fakes to exercise resubmitTransferTask.
add_executable(tent_engine_failover_e2e_test engine_failover_e2e_test.cpp)
target_link_libraries(tent_engine_failover_e2e_test PRIVATE gtest gtest_main
                                                            tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_engine_failover_e2e_test PRIVATE asio_shared)
endif()
target_include_directories(tent_engine_failover_e2e_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_engine_failover_e2e_test
         COMMAND tent_engine_failover_e2e_test)

# Per-request transport_hint: validates submitTransfer parameter, routing,
# disabled-transport rejection, out-of-range rejection, mixed-hint batches.
add_executable(tent_transport_hint_test transport_hint_test.cpp)
target_link_libraries(tent_transport_hint_test PRIVATE gtest gtest_main
                                                       tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_transport_hint_test PRIVATE asio_shared)
endif()
target_include_directories(tent_transport_hint_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_transport_hint_test COMMAND tent_transport_hint_test)

add_executable(tent_intent_type_test intent_type_test.cpp)
target_link_libraries(tent_intent_type_test PRIVATE gtest gtest_main
                                                    tent_common)
target_include_directories(tent_intent_type_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_intent_type_test COMMAND tent_intent_type_test)

# ProgressWorker skeleton test: covers default-off behavior, event-driven
# progress without poll-failover, and freeBatch races (issue #2116).
add_executable(tent_progress_worker_test progress_worker_test.cpp)
target_link_libraries(tent_progress_worker_test PRIVATE gtest gtest_main
                                                        tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_progress_worker_test PRIVATE asio_shared)
endif()
target_include_directories(tent_progress_worker_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
                                   ${CMAKE_CURRENT_SOURCE_DIR}/../src/runtime)
add_test(NAME tent_progress_worker_test COMMAND tent_progress_worker_test)

# stage_buffers_ is reached from the staging workers and the RPC thread. Drives
# a real ProxyManager concurrently; no RDMA device required.
add_executable(tent_stage_buffer_concurrency_test
               stage_buffer_concurrency_test.cpp)
target_link_libraries(tent_stage_buffer_concurrency_test
                      PRIVATE gtest gtest_main tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_stage_buffer_concurrency_test PRIVATE asio_shared)
endif()
target_include_directories(tent_stage_buffer_concurrency_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_stage_buffer_concurrency_test
         COMMAND tent_stage_buffer_concurrency_test)

# Regression test for PR #3526: a timed-out ProxyManager shutdown drain must
# hand undrained batches and their stage buffers to the engine for deferred
# teardown instead of freeing them. Pure CPU; no RDMA device required.
add_executable(tent_proxy_shutdown_timeout_test
               proxy_shutdown_timeout_test.cpp)
target_link_libraries(tent_proxy_shutdown_timeout_test
                      PRIVATE gtest gtest_main tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_proxy_shutdown_timeout_test PRIVATE asio_shared)
endif()
target_include_directories(tent_proxy_shutdown_timeout_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_proxy_shutdown_timeout_test
         COMMAND tent_proxy_shutdown_timeout_test)

# Backoff policy for the synchronous wait loops. Pure function, no engine
# required.
add_executable(tent_poll_backoff_test poll_backoff_test.cpp)
target_link_libraries(tent_poll_backoff_test PRIVATE gtest gtest_main
                                                     tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_poll_backoff_test PRIVATE asio_shared)
endif()
target_include_directories(tent_poll_backoff_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_poll_backoff_test COMMAND tent_poll_backoff_test)

# Delegate RPC end to end: ControlClient::delegate -> onDelegate ->
# transferSync, against a real engine on loopback with a FakeTransport.
add_executable(tent_delegate_rpc_test delegate_rpc_test.cpp)
target_link_libraries(tent_delegate_rpc_test PRIVATE gtest gtest_main
                                                     tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_delegate_rpc_test PRIVATE asio_shared)
endif()
target_include_directories(tent_delegate_rpc_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_delegate_rpc_test COMMAND tent_delegate_rpc_test)

# A slow handler must not stall the single-io_context RPC server. Starts a real
# CoroRpcAgent on loopback; no RDMA device required.
add_executable(tent_rpc_handler_isolation_test rpc_handler_isolation_test.cpp)
target_link_libraries(tent_rpc_handler_isolation_test PRIVATE gtest gtest_main
                                                              tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_rpc_handler_isolation_test PRIVATE asio_shared)
endif()
target_include_directories(tent_rpc_handler_isolation_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_rpc_handler_isolation_test
         COMMAND tent_rpc_handler_isolation_test)

add_executable(tent_runtime_queue_dispatch_test runtime_queue_dispatch_test.cpp)
target_link_libraries(tent_runtime_queue_dispatch_test PRIVATE gtest gtest_main
                                                               tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_runtime_queue_dispatch_test PRIVATE asio_shared)
endif()
target_include_directories(tent_runtime_queue_dispatch_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_runtime_queue_dispatch_test
         COMMAND tent_runtime_queue_dispatch_test)

# Causal chain stage decomposition: validates that dispatch_time and post_time
# timestamps are populated on both queue and direct-commit paths.
add_executable(causal_chain_test causal_chain_test.cpp)
target_link_libraries(causal_chain_test PRIVATE gtest gtest_main
                                                tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(causal_chain_test PRIVATE asio_shared)
endif()
target_include_directories(causal_chain_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME causal_chain_test COMMAND causal_chain_test)

# TPU PJRT shim test: exercises the dlopen'd adapter ABI (device-pointer
# classification, D2H/H2D copy, device topology) against an in-process mock
# adapter, so it runs on any Linux host without TPU hardware or a PJRT runtime.
if(USE_TPU)
  # Mock adapter satisfying the tpu_pjrt_abi.h C ABI. Built as a shared object
  # that the shim (and the test) load via dlopen.
  add_library(mock_tpu_pjrt SHARED tpu/mock_tpu_pjrt_adapter.cpp)
  target_include_directories(mock_tpu_pjrt
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

  add_executable(tent_tpu_pjrt_shim_test tpu/tpu_pjrt_shim_test.cpp)
  add_dependencies(tent_tpu_pjrt_shim_test mock_tpu_pjrt)
  target_link_libraries(
    tent_tpu_pjrt_shim_test PRIVATE gtest gtest_main tent_link_group
                                    ${CMAKE_DL_LIBS})
  target_include_directories(tent_tpu_pjrt_shim_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
  target_compile_definitions(
    tent_tpu_pjrt_shim_test
    PRIVATE MOCK_TPU_PJRT_LIB="$<TARGET_FILE:mock_tpu_pjrt>")
  add_test(NAME tent_tpu_pjrt_shim_test COMMAND tent_tpu_pjrt_shim_test)

  # Drives TpuTransport's staging hop (interior-offset chunks, and the
  # exactly-one-device-side guard) against the same mock adapter.
  add_executable(tent_tpu_transport_test tpu/tpu_transport_test.cpp)
  add_dependencies(tent_tpu_transport_test mock_tpu_pjrt)
  target_link_libraries(
    tent_tpu_transport_test PRIVATE gtest gtest_main tent_link_group
                                    ${CMAKE_DL_LIBS})
  target_include_directories(tent_tpu_transport_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
  target_compile_definitions(
    tent_tpu_transport_test
    PRIVATE MOCK_TPU_PJRT_LIB="$<TARGET_FILE:mock_tpu_pjrt>")
  add_test(NAME tent_tpu_transport_test COMMAND tent_tpu_transport_test)
endif()
