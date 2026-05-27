set(WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}")

if (USE_HIP)
  file(GLOB EXAMPLE_SOURCES "*.cpp")
  hipify_files(EXAMPLE_SOURCES)

  file(RELATIVE_PATH EXAMPLE_REL_PATH "${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
  set(WORKSPACE "${CMAKE_BINARY_DIR}/${EXAMPLE_REL_PATH}")
endif()

add_executable(transfer_engine_bench ${WORKSPACE}/transfer_engine_bench.cpp)
target_link_libraries(transfer_engine_bench PUBLIC transfer_engine)
if (USE_TENT)
    target_link_libraries(transfer_engine_bench PUBLIC tent)
    target_compile_definitions(transfer_engine_bench PRIVATE USE_TENT)
endif()

add_executable(transfer_engine_validator ${WORKSPACE}/transfer_engine_validator.cpp)
target_link_libraries(transfer_engine_validator PUBLIC transfer_engine)

add_executable(transfer_engine_bench_with_notify ${WORKSPACE}/transfer_engine_bench_with_notify.cpp)
target_link_libraries(transfer_engine_bench_with_notify PUBLIC transfer_engine)

add_executable(memory_pool ${WORKSPACE}/memory_pool.cpp)
target_link_libraries(memory_pool PUBLIC transfer_engine)

if (USE_ASCEND)
    add_executable(transfer_engine_ascend_one_sided ${WORKSPACE}/transfer_engine_ascend_one_sided.cpp)
    target_link_libraries(transfer_engine_ascend_one_sided PUBLIC transfer_engine)

    add_executable(transfer_engine_ascend_perf ${WORKSPACE}/transfer_engine_ascend_perf.cpp)
    target_link_libraries(transfer_engine_ascend_perf PUBLIC transfer_engine)
endif()

if (USE_ASCEND_DIRECT)
    add_executable(transfer_engine_ascend_direct_perf ${WORKSPACE}/transfer_engine_ascend_direct_perf.cpp)
    target_link_libraries(transfer_engine_ascend_direct_perf PUBLIC ascendcl transfer_engine)
endif()

if (USE_ASCEND_HETEROGENEOUS)
    add_executable(transfer_engine_heterogeneous_ascend_perf_initiator ${WORKSPACE}/transfer_engine_heterogeneous_ascend_perf_initiator.cpp)
    target_link_libraries(transfer_engine_heterogeneous_ascend_perf_initiator PUBLIC transfer_engine)
endif()

if (USE_UBSHMEM)
    target_link_libraries(transfer_engine_bench PUBLIC transfer_engine)
endif()