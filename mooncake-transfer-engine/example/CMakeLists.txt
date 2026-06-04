set(WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}")

if(USE_HIP)
  file(GLOB EXAMPLE_SOURCES "*.cpp")
  hipify_files(EXAMPLE_SOURCES)

  file(RELATIVE_PATH EXAMPLE_REL_PATH "${CMAKE_SOURCE_DIR}"
       "${CMAKE_CURRENT_SOURCE_DIR}")
  set(WORKSPACE "${CMAKE_BINARY_DIR}/${EXAMPLE_REL_PATH}")
endif()

add_executable(transfer_engine_bench ${WORKSPACE}/transfer_engine_bench.cpp)
target_link_libraries(transfer_engine_bench PUBLIC transfer_engine)
if(USE_TENT)
  target_link_libraries(transfer_engine_bench PUBLIC tent_link_group)
  target_compile_definitions(transfer_engine_bench PRIVATE USE_TENT)
endif()

add_executable(transfer_engine_validator
               ${WORKSPACE}/transfer_engine_validator.cpp)
target_link_libraries(transfer_engine_validator PUBLIC transfer_engine)

add_executable(transfer_engine_bench_with_notify
               ${WORKSPACE}/transfer_engine_bench_with_notify.cpp)
target_link_libraries(transfer_engine_bench_with_notify PUBLIC transfer_engine)

if(USE_EFA)
  add_executable(efa_first_submit_probe
                 ${WORKSPACE}/efa_first_submit_probe.cpp)
  target_link_libraries(efa_first_submit_probe PUBLIC transfer_engine)
endif()

add_executable(memory_pool ${WORKSPACE}/memory_pool.cpp)
target_link_libraries(memory_pool PUBLIC transfer_engine)

if(USE_ASCEND)
  add_executable(transfer_engine_ascend_one_sided
                 ${WORKSPACE}/transfer_engine_ascend_one_sided.cpp)
  target_link_libraries(transfer_engine_ascend_one_sided PUBLIC transfer_engine)

  add_executable(transfer_engine_ascend_perf
                 ${WORKSPACE}/transfer_engine_ascend_perf.cpp)
  target_link_libraries(transfer_engine_ascend_perf PUBLIC transfer_engine)
endif()

if(USE_ASCEND_DIRECT)
  add_executable(transfer_engine_ascend_direct_perf
                 ${WORKSPACE}/transfer_engine_ascend_direct_perf.cpp)
  target_link_libraries(transfer_engine_ascend_direct_perf
                        PUBLIC ascendcl transfer_engine)
endif()

if(USE_ASCEND_HETEROGENEOUS)
  add_executable(
    transfer_engine_heterogeneous_ascend_perf_initiator
    ${WORKSPACE}/transfer_engine_heterogeneous_ascend_perf_initiator.cpp)
  target_link_libraries(transfer_engine_heterogeneous_ascend_perf_initiator
                        PUBLIC transfer_engine)
endif()

if(USE_UBSHMEM)
  target_link_libraries(transfer_engine_bench PUBLIC transfer_engine)
endif()

# Device Transport Example — two-rank P2P write + signal via Device API.
# Requires 2 GPUs with P2P access.  Built but not registered with CTest
# (manual execution only, like rdma_transport_test).
if(USE_CUDA)
  enable_language(CUDA)
  add_executable(device_transport_example
                 ${WORKSPACE}/device_transport_example.cu)
  set_source_files_properties(
    ${WORKSPACE}/device_transport_example.cu
    PROPERTIES LANGUAGE CUDA)
  target_include_directories(device_transport_example PRIVATE
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include)
  target_link_libraries(device_transport_example PUBLIC
    transfer_engine gflags::gflags glog::glog)
  if(TORCH_CUDA_ARCH_LIST)
    set_target_properties(device_transport_example PROPERTIES
      CUDA_ARCHITECTURES "${TORCH_CUDA_ARCH_LIST}")
  else()
    set_target_properties(device_transport_example PROPERTIES
      CUDA_ARCHITECTURES "80;90")
  endif()
endif()
