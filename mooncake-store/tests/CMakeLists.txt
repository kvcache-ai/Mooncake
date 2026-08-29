set(MOONCAKE_STORE_TESTS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

function(add_store_test name)
  add_executable(${name} ${ARGN})
  target_include_directories(${name} PRIVATE ${MOONCAKE_STORE_TESTS_SOURCE_DIR})
  target_link_libraries(
    ${name}
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog::glog
           ibverbs
           gtest
           gtest_main
           pthread)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

function(add_ha_test name)
  add_executable(${name} ${ARGN})
  target_include_directories(${name} PRIVATE ${MOONCAKE_STORE_TESTS_SOURCE_DIR})
  target_link_libraries(
    ${name}
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog::glog
           gflags::gflags
           ibverbs
           gtest
           gtest_main
           pthread)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

add_store_test(buffer_allocator_test buffer_allocator_test.cpp)
add_store_test(region_driver_test region_driver_test.cpp)
add_store_test(runtime_accelerator_test runtime_accelerator_test.cpp)
add_store_test(registered_pinned_memory_test registered_pinned_memory_test.cpp)
add_store_test(allocation_strategy_test allocation_strategy_test.cpp)
add_store_test(replica_selection_test replica_selection_test.cpp)
add_test(
  NAME replica_selection_env_opt_in_test
  COMMAND replica_selection_test
          --gtest_filter=ReplicaSelectionTest.EnvironmentOptInUsesBuiltinScorer)
set_tests_properties(replica_selection_env_opt_in_test
                     PROPERTIES ENVIRONMENT "MC_STORE_REPLICA_SCORING=1")
add_store_test(eviction_strategy_test eviction_strategy_test.cpp)
add_store_test(deadline_scheduler_test deadline_scheduler_test.cpp)
add_store_test(background_worker_test background_worker_test.cpp)
add_store_test(kv_event_publisher_test kv_event_publisher_test.cpp)
if(ENABLE_KV_EVENTS)
  target_compile_definitions(kv_event_publisher_test
                             PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
  target_link_libraries(kv_event_publisher_test PRIVATE Mooncake::libzmq)
endif()
add_subdirectory(master_service)

add_store_test(master_service_test master_service_test.cpp)
add_store_test(master_service_group_test master_service_group_test.cpp)
add_ha_test(master_service_evict_scenario_test
            master_service_evict_scenario_test.cpp)
target_link_libraries(master_service_evict_scenario_test
                      PRIVATE master_service_test_dsl)
add_store_test(master_service_scenario_test master_service_scenario_test.cpp)
target_link_libraries(master_service_scenario_test
                      PRIVATE master_service_test_dsl)
add_store_test(master_service_group_scenario_test
               master_service_group_scenario_test.cpp)
target_link_libraries(master_service_group_scenario_test
                      PRIVATE master_service_test_dsl)
add_store_test(master_service_tenant_scenario_test
               master_service_tenant_scenario_test.cpp)
target_link_libraries(master_service_tenant_scenario_test
                      PRIVATE master_service_test_dsl)
add_store_test(config_helper_test config_helper_test.cpp)
add_store_test(master_service_config_test master_service_config_test.cpp)
add_store_test(master_service_processing_key_double_erase_test
               master_service_processing_key_double_erase_test.cpp)
add_store_test(master_service_tenant_quota_test
               master_service_tenant_quota_test.cpp)
add_store_test(batch_remove_test batch_remove_test.cpp)
add_store_test(master_service_ssd_test master_service_ssd_test.cpp)
add_store_test(offload_on_evict_test offload_on_evict_test.cpp)
add_store_test(promotion_on_hit_test promotion_on_hit_test.cpp)
add_store_test(dynamic_replication_test dynamic_replication_test.cpp)
add_store_test(file_storage_promotion_test file_storage_promotion_test.cpp)
add_store_test(master_service_ssd_test_for_snapshot
               ha/snapshot/master_service_ssd_test_for_snapshot.cpp)
add_store_test(master_service_promotion_test_for_snapshot
               ha/snapshot/master_service_promotion_test_for_snapshot.cpp)
if(USE_NOF)
  add_store_test(nof_heartbeat_test nof_heartbeat_test.cpp)
endif()
add_store_test(client_integration_test client_integration_test.cpp)
add_test(
  NAME object_checksum_client_test
  COMMAND
    client_integration_test
    "--gtest_filter=ObjectChecksumTest.*:ClientIntegrationTest.ObjectChecksumRejectsCorruptedObject:ClientIntegrationTest.BatchPutPreservesObjectChecksumPairing"
)
set_tests_properties(object_checksum_client_test
                     PROPERTIES ENVIRONMENT "MOONCAKE_STORE_CHECKSUM=1")
add_store_test(rpc_timeout_test rpc_timeout_test.cpp)
if(USE_CXL)
  add_store_test(cxl_client_integration_test cxl_client_integration_test.cpp)
endif()
add_store_test(master_metrics_test master_metrics_test.cpp)
add_store_test(master_admin_server_test master_admin_server_test.cpp)
add_store_test(posix_file_test posix_file_test.cpp)
add_store_test(thread_pool_test thread_pool_test.cpp)
add_store_test(transfer_task_test transfer_task_test.cpp)
find_package(CUDAToolkit QUIET)
if(CUDAToolkit_FOUND)
  target_compile_definitions(transfer_task_test PRIVATE MOONCAKE_TEST_CUDA_H2D)
  target_link_libraries(transfer_task_test PRIVATE CUDA::cudart)
endif()
if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
  target_compile_definitions(transfer_task_test PRIVATE USE_CUDA)
  if(USE_TENT)
    add_test(
      NAME transfer_scatter_tent_test
      COMMAND
        transfer_task_test
        --gtest_filter=TransferTaskTest.TransferScatterWritesGpuDestinationDirectly
    )
    set_tests_properties(transfer_scatter_tent_test PROPERTIES ENVIRONMENT
                                                               "MC_USE_TENT=1")
  endif()
endif()
add_store_test(tenant_quota_test tenant_quota_test.cpp)
add_store_test(tenant_quota_ledger_test tenant_quota_ledger_test.cpp)
add_store_test(tenant_id_test tenant_id_test.cpp)
add_store_test(segment_test segment_test.cpp)
add_executable(local_ssd_test local_ssd/local_ssd_test.cpp)
target_include_directories(local_ssd_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(local_ssd_test PRIVATE mooncake_local_ssd gtest
                                             gtest_main)
add_test(NAME local_ssd_test COMMAND local_ssd_test)
add_store_test(local_ssd_codec_test ha/snapshot/local_ssd_codec_test.cpp)
add_store_test(offset_allocator_test offset_allocator_test.cpp)
add_store_test(utils_test utils_test.cpp)
add_store_test(client_buffer_test client_buffer_test.cpp)
add_store_test(client_auto_port_config_test client_auto_port_config_test.cpp)
add_store_test(client_local_hot_cache_test client_local_hot_cache_test.cpp)
add_store_test(client_tcp_local_memcpy_test client_tcp_local_memcpy_test.cpp)
add_store_test(pybind_client_test pybind_client_test.cpp)
if(CUDAToolkit_FOUND)
  target_compile_definitions(pybind_client_test PRIVATE MOONCAKE_TEST_CUDA_H2D)
  target_link_libraries(pybind_client_test PRIVATE CUDA::cudart)
endif()
add_store_test(ipv6_client_test ipv6_client_test.cpp)
add_store_test(host_port_fix_test host_port_fix_test.cpp)
add_store_test(client_metrics_test client_metrics_test.cpp)
add_store_test(client_metric_config_test client_metric_config_test.cpp)
add_store_test(ssd_metrics_test ssd_metrics_test.cpp)
add_store_test(serializer_test serializer_test.cpp)
add_store_test(dfs_posix_test dfs_posix_test.cpp)
add_store_test(dfs_sync_client_test dfs_sync_client_test.cpp)
add_test(NAME dfs_batch_read_checksum_test
         COMMAND dfs_sync_client_test
                 --gtest_filter=DfsSyncClientTest.BatchGetVerifiesDfsChecksum)
set_tests_properties(dfs_batch_read_checksum_test
                     PROPERTIES ENVIRONMENT "MOONCAKE_STORE_CHECKSUM=1")
if(USE_3FS)
  add_store_test(dfs_hf3fs_test dfs_hf3fs_test.cpp)
endif()
add_store_test(
  embedded_snapshot_catalog_store_test
  ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store_test.cpp
)
add_store_test(zstd_util_test zstd_util_test.cpp)
add_store_test(
  local_file_snapshot_object_store_test
  ha/snapshot/object/backends/local/local_file_snapshot_object_store_test.cpp)
add_store_test(file_util_test file_util_test.cpp)
add_store_test(snapshot_child_process_test
               ha/snapshot/snapshot_child_process_test.cpp)
add_store_test(master_snapshot_codec_test
               ha/snapshot/master_snapshot_codec_test.cpp)
add_ha_test(batch_oplog_snapshot_types_test
            ha/snapshot/batch_oplog/metadata_test.cpp)
add_ha_test(batch_oplog_snapshot_codec_test
            ha/snapshot/batch_oplog/codec_test.cpp)
add_ha_test(batch_oplog_snapshot_writer_test
            ha/snapshot/batch_oplog/writer_test.cpp)
add_ha_test(batch_oplog_snapshot_provider_test
            ha/snapshot/batch_oplog/provider_test.cpp)
add_ha_test(batch_oplog_snapshot_publisher_test
            ha/snapshot/batch_oplog/publisher_test.cpp)
add_ha_test(batch_oplog_snapshot_coordinator_test
            ha/snapshot/batch_oplog/coordinator_test.cpp)
add_store_test(master_service_test_for_snapshot
               ha/snapshot/master_service_test_for_snapshot.cpp)
add_store_test(non_ha_reconnect_test non_ha_reconnect_test.cpp)
add_store_test(nvme_kv_storage_backend_test nvme_kv_storage_backend_test.cpp)
add_store_test(storage_backend_test storage_backend_test.cpp)
add_store_test(client_storage_backend_test client_storage_backend_test.cpp)
add_store_test(object_storage_adapter_test object_storage_adapter_test.cpp)
add_store_test(mutex_test mutex_test.cpp)
add_store_test(file_storage_test file_storage_test.cpp)
add_store_test(file_storage_config_test file_storage_config_test.cpp)
add_store_test(task_manager_test task_manager_test.cpp)
add_store_test(task_executor_test task_executor_test.cpp)
add_store_test(task_integration_test task_integration_test.cpp)
add_store_test(dummy_client_get_buffer_test dummy_client_get_buffer_test.cpp)
add_test(
  NAME dummy_client_object_checksum_test
  COMMAND
    dummy_client_get_buffer_test
    "--gtest_filter=DummyClientGetBufferTest.GetIntoRejectsCorruptedObjectWithChecksum:DummyClientGetBufferTest.BatchQueryPreservesObjectChecksum"
)
set_tests_properties(dummy_client_object_checksum_test
                     PROPERTIES ENVIRONMENT "MOONCAKE_STORE_CHECKSUM=1")
add_store_test(uds_transport_test uds_transport_test.cpp)
add_store_test(health_check_test health_check_test.cpp)
add_store_test(http_metadata_server_test http_metadata_server_test.cpp)
add_store_test(mmap_arena_test mmap_arena_test.cpp)
add_store_test(mmap_arena_fallback_test mmap_arena_fallback_test.cpp)
add_store_test(object_data_type_test object_data_type_test.cpp)
add_subdirectory(e2e)

add_executable(high_availability_test ha/leadership/high_availability_test.cpp)
target_include_directories(high_availability_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(STORE_USE_REDIS)
  target_sources(
    high_availability_test
    PRIVATE ha/leadership/backends/redis/high_availability_redis_test.cpp)
  target_link_libraries(high_availability_test PRIVATE Mooncake::hiredis)

  add_executable(
    redis_snapshot_catalog_store_test
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store_test.cpp)
  target_include_directories(redis_snapshot_catalog_store_test
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(
    redis_snapshot_catalog_store_test
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog::glog
           gflags::gflags
           ibverbs
           gtest
           pthread
    PRIVATE Mooncake::hiredis)
  add_test(NAME redis_snapshot_catalog_store_test
           COMMAND redis_snapshot_catalog_store_test)
endif()
if(STORE_USE_K8S_LEASE)
  target_sources(
    high_availability_test
    PRIVATE ha/leadership/backends/k8s/high_availability_k8s_test.cpp)
endif()
target_link_libraries(
  high_availability_test
  PUBLIC mooncake_store
         transfer_engine
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         glog::glog
         gtest
         gtest_main
         pthread)
if(STORE_USE_K8S_LEASE)
  target_link_libraries(high_availability_test PRIVATE ${K8S_LEASE_WRAPPER_LIB})
endif()
if(STORE_USE_ETCD
   OR STORE_USE_REDIS
   OR STORE_USE_K8S_LEASE)
  add_test(NAME high_availability_test COMMAND high_availability_test)
endif()
add_ha_test(ha_backend_availability_test
            ha/leadership/ha_backend_availability_test.cpp)
add_ha_test(leader_label_reconciler_test
            ha/leadership/leader_label_reconciler_test.cpp)
add_ha_test(oplog_batch_storage_test ha/oplog/oplog_batch_storage_test.cpp)
add_ha_test(oplog_batch_auditor_test ha/oplog/oplog_batch_auditor_test.cpp)
target_sources(oplog_batch_auditor_test
               PRIVATE ../tools/oplog_batch_auditor.cpp)
target_include_directories(oplog_batch_auditor_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(
  stress_workload_test
  PUBLIC mooncake_store
         transfer_engine
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         mooncake_common
         glog::glog
         gflags::gflags
         pthread)

# HA tests
add_ha_test(standby_state_machine_test
            ha/standby/standby_state_machine_test.cpp)
add_ha_test(ha_metric_manager_test ha/standby/ha_metric_manager_test.cpp)
add_ha_test(hot_standby_service_test ha/standby/hot_standby_service_test.cpp)
add_ha_test(standby_metadata_store_test
            ha/standby/standby_metadata_store_test.cpp)
add_ha_test(hot_standby_snapshot_bootstrap_test
            ha/standby/hot_standby_snapshot_bootstrap_test.cpp)
add_ha_test(oplog_applier_test ha/oplog/oplog_applier_test.cpp)
add_ha_test(oplog_types_test ha/oplog/oplog_types_test.cpp)
add_ha_test(oplog_batch_codec_test ha/oplog/oplog_batch_codec_test.cpp)
add_ha_test(oplog_batch_standby_reader_test
            ha/oplog/oplog_batch_standby_reader_test.cpp)
add_ha_test(ordered_oplog_writer_test ha/oplog/ordered_oplog_writer_test.cpp)
if(MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(ha_metric_manager_test
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(ordered_oplog_writer_test
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
endif()
add_executable(
  oplog_test_failpoint_test ha/oplog/oplog_test_failpoint_test.cpp
                            ../src/ha/oplog/oplog_test_failpoint.cpp)
target_include_directories(oplog_test_failpoint_test
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_ROOT})
target_compile_definitions(oplog_test_failpoint_test
                           PRIVATE MOONCAKE_ENABLE_TEST_FAILPOINTS)
target_link_libraries(oplog_test_failpoint_test PRIVATE glog::glog gtest
                                                        gtest_main pthread)
add_test(NAME oplog_test_failpoint_test COMMAND oplog_test_failpoint_test)
add_ha_test(master_service_ha_test ha/master_service_ha_test.cpp)
add_ha_test(catalog_backed_snapshot_provider_test
            ha/snapshot/catalog_backed_snapshot_provider_test.cpp)
if(STORE_USE_REDIS)
  foreach(target catalog_backed_snapshot_provider_test
                 hot_standby_snapshot_bootstrap_test)
    target_link_libraries(${target} PRIVATE Mooncake::hiredis)
  endforeach()
endif()

foreach(
  target
  hot_standby_service_test oplog_applier_test oplog_batch_codec_test
  oplog_batch_standby_reader_test oplog_batch_storage_test
  ordered_oplog_writer_test)
  target_link_libraries(${target} PRIVATE Mooncake::xxhash)
endforeach()
