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

if(USE_FLAGCX)
  add_executable(flagcx_demo ${WORKSPACE}/flagcx_demo.cpp)
  target_link_libraries(flagcx_demo PUBLIC transfer_engine)
endif()

# Device Transport Example — two-rank P2P write + signal via Device API.
# Requires 2 GPUs with P2P access.  Built but not registered with CTest
# (manual execution only, like rdma_transport_test).
#
# Off by default: this .cu target pulls in transfer_engine.h, whose common.h
# uses C++20 std::string APIs (e.g. starts_with), so it needs the CUDA20
# dialect.  Older CMake (such as the CI Docker image) cannot enable CUDA20 and
# fails at generate time.  Opt in explicitly with -DBUILD_DEVICE_TRANSPORT_EXAMPLE=ON
# when building manually on a host with a recent CMake and 2 GPUs.
option(BUILD_DEVICE_TRANSPORT_EXAMPLE
       "Build the two-rank Device API P2P example (requires CUDA20-capable CMake and 2 GPUs)"
       OFF)
if(USE_CUDA AND BUILD_DEVICE_TRANSPORT_EXAMPLE)
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
  # common.h (pulled in via transfer_engine.h) uses C++20 std::string APIs
  # such as starts_with; nvcc does not inherit CMAKE_CXX_STANDARD, so set the
  # CUDA standard explicitly on this target.  CUDA_EXTENSIONS OFF requests
  # plain -std=c++20 (nvcc has no gnu++20 dialect mapping).
  set_target_properties(device_transport_example PROPERTIES
    CUDA_STANDARD 20 CUDA_STANDARD_REQUIRED ON CUDA_EXTENSIONS OFF)
  # Convert torch-style arch list (e.g. "8.0;9.0") to CMake CUDA format
  # (e.g. "80;90") by stripping the dot.
  if(TORCH_CUDA_ARCH_LIST)
    set(_cuda_arch_list "")
    foreach(_arch IN LISTS TORCH_CUDA_ARCH_LIST)
      string(REPLACE "." "" _arch_clean "${_arch}")
      list(APPEND _cuda_arch_list "${_arch_clean}")
    endforeach()
    set_target_properties(device_transport_example PROPERTIES
      CUDA_ARCHITECTURES "${_cuda_arch_list}")
  else()
    set_target_properties(device_transport_example PROPERTIES
      CUDA_ARCHITECTURES "80;90")
  endif()
endif()
