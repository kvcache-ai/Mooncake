add_executable(transfer_engine_bench transfer_engine_bench.cpp)
target_link_libraries(transfer_engine_bench PUBLIC transfer_engine)

add_executable(transfer_engine_validator transfer_engine_validator.cpp)
target_link_libraries(transfer_engine_validator PUBLIC transfer_engine)

add_executable(transfer_engine_bench_with_notify transfer_engine_bench_with_notify.cpp)
target_link_libraries(transfer_engine_bench_with_notify PUBLIC transfer_engine)

add_executable(memory_pool memory_pool.cpp)
target_link_libraries(memory_pool PUBLIC transfer_engine)

if (USE_ASCEND)
    add_executable(transfer_engine_ascend_one_sided transfer_engine_ascend_one_sided.cpp)
    target_link_libraries(transfer_engine_ascend_one_sided PUBLIC transfer_engine)

    add_executable(transfer_engine_ascend_perf transfer_engine_ascend_perf.cpp)
    target_link_libraries(transfer_engine_ascend_perf PUBLIC transfer_engine)
endif()

if (USE_ASCEND_DIRECT)
    add_executable(transfer_engine_ascend_direct_perf transfer_engine_ascend_direct_perf.cpp)
    target_link_libraries(transfer_engine_ascend_direct_perf PUBLIC transfer_engine)
endif()

if (USE_ASCEND_HETEROGENEOUS)
    add_executable(transfer_engine_heterogeneous_ascend_perf_initiator transfer_engine_heterogeneous_ascend_perf_initiator.cpp)
    target_link_libraries(transfer_engine_heterogeneous_ascend_perf_initiator PUBLIC transfer_engine)
endif()
