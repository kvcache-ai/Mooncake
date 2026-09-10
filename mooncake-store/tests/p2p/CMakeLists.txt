# P2P client tests use the aggregate until the client facade is split.
function(add_p2p_store_executable name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PUBLIC
        ${MOONCAKE_STORE_LIBS}
        transfer_engine
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog
        ibverbs
        gtest
        gtest_main
        pthread
    )
endfunction()

function(add_p2p_store_test name)
    add_p2p_store_executable(${name} ${ARGN})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES LABELS "p2p;client;unit")
endfunction()

function(add_p2p_master_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE mooncake_p2p_master_core gtest gtest_main)
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES LABELS "p2p;master;unit")
endfunction()

# master side
add_p2p_master_test(p2p_route_table_test p2p_route_table_test.cpp)
add_p2p_master_test(p2p_rpc_types_test p2p_rpc_types_test.cpp)
add_p2p_master_test(p2p_master_rpc_test
    p2p_master_rpc_test.cpp
    ../../src/p2p/master/p2p_master_client.cpp)
set_tests_properties(p2p_master_rpc_test PROPERTIES TIMEOUT 60)
add_p2p_master_test(p2p_master_service_test p2p_master_service_test.cpp)
add_p2p_master_test(p2p_master_http_endpoints_test p2p_master_http_endpoints_test.cpp)
add_p2p_master_test(p2p_master_metrics_test p2p_master_metrics_test.cpp)
add_p2p_master_test(p2p_segment_manager_test p2p_segment_manager_test.cpp)
add_p2p_master_test(p2p_client_manager_test p2p_client_manager_test.cpp)
add_p2p_master_test(p2p_client_meta_test p2p_client_meta_test.cpp)
set_tests_properties(p2p_master_rpc_test p2p_master_http_endpoints_test
    PROPERTIES LABELS "p2p;master;integration")

add_executable(stress_workload_test_p2p stress_workload_test.cpp)
target_link_libraries(stress_workload_test_p2p PUBLIC
    ${MOONCAKE_STORE_LIBS}
    transfer_engine
    cachelib_memory_allocator
    ${ETCD_WRAPPER_LIB}
    mooncake_common
    glog
    gflags
    pthread
)

# client side
add_p2p_store_test(p2p_client_integration_test p2p_client_integration_test.cpp)
add_p2p_store_test(p2p_client_http_endpoints_test p2p_client_http_endpoints_test.cpp)
add_p2p_store_test(p2p_register_concurrency_test p2p_register_concurrency_test.cpp)
add_p2p_store_test(peer_client_test peer_client_test.cpp)
# Explicit benchmark binary: never count it as a default functional CTest.
add_p2p_store_executable(peer_client_perf_test peer_client_perf_test.cpp)
add_p2p_store_test(client_rpc_service_test client_rpc_service_test.cpp)
add_p2p_store_test(data_manager_v1_test data_manager_v1_test.cpp)
add_p2p_store_test(route_cache_test route_cache_test.cpp)
add_p2p_store_test(inflight_tracker_test inflight_tracker_test.cpp)
add_p2p_store_test(async_metadata_notifier_test async_metadata_notifier_test.cpp)
add_p2p_store_test(runtime_config_store_test runtime_config_store_test.cpp)
add_p2p_store_test(client_metrics_aggregator_test client_metrics_aggregator_test.cpp)
add_p2p_store_test(p2p_client_metrics_test p2p_client_metrics_test.cpp)
add_p2p_store_test(client_http_metrics_test client_http_metrics_test.cpp)

# tiered cache
add_p2p_store_test(tiered_backend_test tiered_backend_test.cpp)
add_p2p_store_test(storage_tier_test storage_tier_test.cpp)
add_p2p_store_test(scheduler_integration_test scheduler_integration_test.cpp)
add_p2p_store_test(multi_lru_test multi_lru_test.cpp)
add_p2p_store_test(event_driven_policy_test event_driven_policy_test.cpp)
add_p2p_store_test(event_driven_scheduler_test event_driven_scheduler_test.cpp)
add_p2p_store_test(bounded_dedup_queue_test bounded_dedup_queue_test.cpp)
add_p2p_store_test(tinylfu_sketch_test tinylfu_sketch_test.cpp)
add_p2p_store_test(tier_role_resolver_test tier_role_resolver_test.cpp)
if(USE_ASCEND_CACHE_TIER)
    add_p2p_store_test(ascend_tier_test ascend_tier_test.cpp)
endif()

# Normal RPC connection/heartbeat coverage also applies without HA.
add_p2p_store_test(heartbeat_dedicated_port_test
                   heartbeat_dedicated_port_test.cpp)
set_tests_properties(p2p_client_integration_test p2p_client_http_endpoints_test
    p2p_register_concurrency_test client_rpc_service_test
    runtime_config_store_test client_http_metrics_test
    heartbeat_dedicated_port_test scheduler_integration_test
    PROPERTIES LABELS "p2p;client;integration")
set_tests_properties(p2p_register_concurrency_test heartbeat_dedicated_port_test
    p2p_master_service_test p2p_client_manager_test p2p_client_meta_test
    p2p_segment_manager_test
    PROPERTIES TIMEOUT 180)
if(USE_ASCEND_CACHE_TIER)
    set_tests_properties(ascend_tier_test PROPERTIES LABELS "p2p;client;hardware")
endif()

# HA
add_p2p_store_test(p2p_client_ha_test p2p_client_ha_test.cpp)
add_p2p_store_test(ha_integration_test ha_integration_test.cpp)
add_p2p_store_test(ha_recovery_manager_test ha_recovery_manager_test.cpp)
add_p2p_store_test(oplog_manager_test ha/oplog/oplog_manager_test.cpp)
add_p2p_store_test(oplog_serializer_test ha/oplog/oplog_serializer_test.cpp)
add_p2p_store_test(localfs_oplog_store_test ha/oplog/localfs_oplog_store_test.cpp)
add_p2p_store_test(standby_state_machine_test ha/oplog/standby_state_machine_test.cpp)
add_p2p_store_test(ha_metric_manager_test ha/oplog/ha_metric_manager_test.cpp)
add_p2p_store_test(oplog_applier_test ha/oplog/oplog_applier_test.cpp)
add_p2p_store_test(oplog_replicator_test ha/oplog/oplog_replicator_test.cpp)
add_p2p_store_test(p2p_oplog_test
    ha/oplog/p2p_oplog_test.cpp
    ha/oplog/p2p_oplog_applier_test.cpp
    ha/oplog/p2p_record_oplog_test.cpp
    ha/oplog/p2p_standby_metadata_store_test.cpp
    ha/oplog/p2p_hot_standby_service_test.cpp
)

set_tests_properties(p2p_client_ha_test ha_integration_test ha_recovery_manager_test
    oplog_manager_test oplog_serializer_test localfs_oplog_store_test
    standby_state_machine_test ha_metric_manager_test oplog_applier_test
    oplog_replicator_test p2p_oplog_test PROPERTIES LABELS "p2p;ha")

# redis-dependent entries (registered only when STORE_USE_REDIS, as before)
add_executable(redis_election_helper_test redis_election_helper_test.cpp)
target_link_libraries(redis_election_helper_test PUBLIC ${MOONCAKE_STORE_LIBS} transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
if(STORE_USE_REDIS)
    add_test(NAME redis_election_helper_test COMMAND redis_election_helper_test)
    add_p2p_store_test(redis_oplog_store_test ha/oplog/redis_oplog_store_test.cpp)
    set_tests_properties(redis_election_helper_test redis_oplog_store_test
        PROPERTIES LABELS "p2p;ha;redis")
endif()
