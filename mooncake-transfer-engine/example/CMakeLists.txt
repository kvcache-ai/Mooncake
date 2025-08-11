file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")

link_directories(${ASCEND_LIB_DIR})

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