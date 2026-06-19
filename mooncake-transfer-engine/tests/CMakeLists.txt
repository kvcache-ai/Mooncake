set(WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}")

if(USE_HIP)
  file(GLOB TEST_SOURCES "*.cpp")
  hipify_files(TEST_SOURCES)

  file(RELATIVE_PATH EXAMPLE_REL_PATH "${CMAKE_SOURCE_DIR}"
       "${CMAKE_CURRENT_SOURCE_DIR}")
  set(WORKSPACE "${CMAKE_BINARY_DIR}/${EXAMPLE_REL_PATH}")
endif()

add_executable(rdma_transport_test ${WORKSPACE}/rdma_transport_test.cpp)
target_link_libraries(rdma_transport_test PUBLIC transfer_engine gflags::gflags
                                                 glog::glog)
# add_test(NAME rdma_transport_test COMMAND rdma_transport_test)

add_executable(transport_uint_test ${WORKSPACE}/transport_uint_test.cpp)
target_link_libraries(transport_uint_test PUBLIC transfer_engine gtest
                                                 gtest_main)
add_test(NAME transport_uint_test COMMAND transport_uint_test)

add_executable(endpoint_store_test ${WORKSPACE}/endpoint_store_test.cpp)
target_link_libraries(endpoint_store_test PUBLIC transfer_engine gtest
                                                 gtest_main)
add_test(NAME endpoint_store_test COMMAND endpoint_store_test)

# Integration test for the monitorWorker reclaim tick (issue #1845). Self-skips
# when no RDMA device is present, so safe to register with ctest.
add_executable(endpoint_store_integration_test
               ${WORKSPACE}/endpoint_store_integration_test.cpp)
target_link_libraries(endpoint_store_integration_test PUBLIC transfer_engine
                                                             gtest gtest_main)
add_test(NAME endpoint_store_integration_test
         COMMAND endpoint_store_integration_test)
set_tests_properties(endpoint_store_integration_test PROPERTIES LABELS "rdma")

add_executable(rdma_transport_test2 ${WORKSPACE}/rdma_transport_test2.cpp)
target_link_libraries(rdma_transport_test2 PUBLIC transfer_engine gtest
                                                  gtest_main)
# add_test(NAME rdma_transport_test2 COMMAND rdma_transport_test2)

add_executable(rdma_loopback_test ${WORKSPACE}/rdma_loopback_test.cpp)
target_link_libraries(rdma_loopback_test PUBLIC transfer_engine gtest
                                                gtest_main)
# add_test(NAME rdma_loopback_test COMMAND rdma_loopback_test)

# This test verifies endpoint re-establishment in RDMATransport.
add_executable(rdma_endpoint_reestablish_test
               ${WORKSPACE}/rdma_endpoint_reestablish_test.cpp)
target_link_libraries(rdma_endpoint_reestablish_test PUBLIC transfer_engine
                                                            gtest gtest_main)
if(UNIX AND NOT APPLE)
  target_link_options(
    rdma_endpoint_reestablish_test PRIVATE "-Wl,--wrap=ibv_modify_qp"
    "-Wl,--wrap=ibv_query_gid" "-Wl,--wrap=_ibv_query_gid_ex")
endif()
add_test(NAME rdma_endpoint_reestablish_test
         COMMAND rdma_endpoint_reestablish_test)
set_tests_properties(rdma_endpoint_reestablish_test PROPERTIES LABELS "rdma")

if(USE_CXL)
  add_executable(cxl_transport_test ${WORKSPACE}/cxl_transport_test.cpp)
  target_link_libraries(cxl_transport_test PUBLIC transfer_engine gtest
                                                  gtest_main)
  add_test(NAME cxl_transport_test COMMAND cxl_transport_test)
endif()

if(USE_NVMEOF)
  add_executable(nvmeof_transport_test ${WORKSPACE}/nvmeof_transport_test.cpp)
  target_link_libraries(nvmeof_transport_test PUBLIC transfer_engine gtest
                                                     gtest_main)
  # add_test(NAME nvmeof_transport_test COMMAND nvmeof_transport_test)
endif()

if(USE_TCP)
  add_executable(tcp_transport_test ${WORKSPACE}/tcp_transport_test.cpp)
  target_link_libraries(tcp_transport_test PUBLIC transfer_engine gtest
                                                  gtest_main)
  add_test(NAME tcp_transport_test COMMAND tcp_transport_test)
endif()

add_executable(tcp_address_validation_test
               ${WORKSPACE}/tcp_address_validation_test.cpp)
target_link_libraries(tcp_address_validation_test PUBLIC gtest gtest_main)
add_test(NAME tcp_address_validation_test COMMAND tcp_address_validation_test)

if(USE_MNNVL)
  add_executable(nvlink_transport_test ${WORKSPACE}/nvlink_transport_test.cpp)
  target_link_libraries(nvlink_transport_test PUBLIC transfer_engine gtest
                                                     gtest_main)
  add_test(NAME nvlink_transport_test COMMAND nvlink_transport_test)
endif()

if(USE_UBSHMEM)
  add_executable(ubshmem_transport_test ${WORKSPACE}/ubshmem_transport_test.cpp)
  target_link_libraries(ubshmem_transport_test PUBLIC transfer_engine gtest
                                                      gtest_main ascendcl)
  add_test(NAME ubshmem_transport_test COMMAND ubshmem_transport_test)
endif()

if(USE_EFA)
  add_executable(efa_transport_test ${WORKSPACE}/efa_transport_test.cpp)
  target_link_libraries(efa_transport_test PUBLIC transfer_engine gtest
                                                  gtest_main)
  add_test(NAME efa_transport_test COMMAND efa_transport_test)

  add_executable(efa_c_api_test ${WORKSPACE}/efa_c_api_test.cpp)
  target_link_libraries(efa_c_api_test PUBLIC transfer_engine gtest gtest_main)
  add_test(NAME efa_c_api_test COMMAND efa_c_api_test)

  add_executable(efa_single_nic_large_mr_test
                 ${WORKSPACE}/efa_single_nic_large_mr_test.cpp)
  target_link_libraries(efa_single_nic_large_mr_test
                        PUBLIC transfer_engine gflags::gflags glog::glog)

  add_executable(efa_transfer_test ${WORKSPACE}/efa_transfer_test.cpp)
  target_link_libraries(efa_transfer_test PUBLIC transfer_engine gflags::gflags
                                                 glog::glog)

  # GPU (CUDA device memory) loopback test — reproduces the EFA SHM intra-node
  # segfault on FI_HMEM_CUDA buffers (ofiwg/libfabric#12328) and validates
  # EfaContext::tryLoopbackCopy.  Needs CUDA headers/libs.
  if(USE_CUDA)
    add_executable(efa_gpu_loopback_test ${WORKSPACE}/efa_gpu_loopback_test.cpp)
    # Resolve CUDA include dirs / runtime via CUDAToolkit instead of a hardcoded
    # /usr/local/cuda/include, so the test builds wherever CUDA lives (e.g. the
    # DLAMI pip venv layout).  find_package is idempotent and may not have run
    # yet in this scope (top-level only calls it under WITH_EP), so request it
    # here; fall back to the legacy path if the module variant is unavailable.
    find_package(CUDAToolkit QUIET)
    if(CUDAToolkit_FOUND)
      target_include_directories(efa_gpu_loopback_test
                                 PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
      target_link_libraries(
        efa_gpu_loopback_test PUBLIC transfer_engine gtest gtest_main
                                     CUDA::cudart)
    else()
      target_include_directories(efa_gpu_loopback_test
                                 PRIVATE /usr/local/cuda/include)
      target_link_libraries(efa_gpu_loopback_test PUBLIC transfer_engine gtest
                                                         gtest_main cudart)
    endif()
    add_test(NAME efa_gpu_loopback_test COMMAND efa_gpu_loopback_test)
  endif()
endif()

if(USE_CXI)
  add_executable(cxi_transport_test ${WORKSPACE}/cxi_transport_test.cpp)
  add_executable(cxi_transfer_test ${WORKSPACE}/cxi_transfer_test.cpp)
  target_link_libraries(cxi_transport_test PUBLIC transfer_engine gtest
                                                  gtest_main)
  target_link_libraries(cxi_transfer_test PUBLIC transfer_engine gtest
                                                 gtest_main)
  add_test(NAME cxi_transport_test COMMAND cxi_transport_test)
endif()

# UB transport test with URMA endpoint and mock support
if(USE_UB)
  add_executable(ub_transport_test ${WORKSPACE}/ub_transport_test.cpp)
  target_link_libraries(ub_transport_test PUBLIC transfer_engine gtest
                                                 gtest_main glog::glog pthread)
  target_include_directories(ub_transport_test PRIVATE ${urma_INCLUDE_DIR})
  # Built but not registered with ctest: this test may still have race
  # conditions and other stability issues, so keep it out of CI for now. Run
  # manually with ./ub_transport_test. add_test(NAME ub_transport_test COMMAND
  # ub_transport_test)
endif()

if(USE_SUNRISE)
  add_executable(sunrise_link_transport_test
                 ${WORKSPACE}/sunrise_link_transport_test.cpp)
  target_include_directories(sunrise_link_transport_test
                             PRIVATE ${MC_TANGRT_ROOT}/include)
  target_link_libraries(sunrise_link_transport_test PUBLIC transfer_engine
                                                           gtest gtest_main)
  add_test(NAME sunrise_link_transport_test COMMAND sunrise_link_transport_test)

  add_executable(sunrise_link_transport_runtime_test
                 ${WORKSPACE}/sunrise_link_transport_runtime_test.cpp)
  target_include_directories(sunrise_link_transport_runtime_test
                             PRIVATE ${MC_TANGRT_ROOT}/include)
  target_link_libraries(sunrise_link_transport_runtime_test
                        PUBLIC transfer_engine gtest gtest_main)
  add_test(NAME sunrise_link_transport_runtime_test
           COMMAND sunrise_link_transport_runtime_test)

  add_executable(sunrise_link_transport_unit_test
                 ${WORKSPACE}/sunrise_link_transport_unit_test.cpp)
  target_include_directories(sunrise_link_transport_unit_test
                             PRIVATE ${MC_TANGRT_ROOT}/include)
  target_link_libraries(sunrise_link_transport_unit_test
                        PUBLIC transfer_engine gtest gtest_main)
  add_test(NAME sunrise_link_transport_unit_test
           COMMAND sunrise_link_transport_unit_test)
endif()

add_executable(transfer_metadata_test ${WORKSPACE}/transfer_metadata_test.cpp)
target_link_libraries(transfer_metadata_test PUBLIC transfer_engine gtest
                                                    gtest_main)
add_test(NAME transfer_metadata_test COMMAND transfer_metadata_test)

add_executable(config_test ${WORKSPACE}/config_test.cpp)
target_link_libraries(config_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME config_test COMMAND config_test)

add_executable(rdma_gid_probe_test ${WORKSPACE}/rdma_gid_probe_test.cpp)
target_link_libraries(rdma_gid_probe_test PUBLIC transfer_engine gtest
                                                 gtest_main)
add_test(NAME rdma_gid_probe_test COMMAND rdma_gid_probe_test)

add_executable(rdma_context_reprobe_test
               ${WORKSPACE}/rdma_context_reprobe_test.cpp)
target_link_libraries(rdma_context_reprobe_test PUBLIC transfer_engine gtest
                                                       gtest_main)
add_test(NAME rdma_context_reprobe_test COMMAND rdma_context_reprobe_test)

add_executable(topology_test ${WORKSPACE}/topology_test.cpp)
target_link_libraries(topology_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME topology_test COMMAND topology_test)

add_executable(memory_location_test ${WORKSPACE}/memory_location_test.cpp)
target_link_libraries(memory_location_test PUBLIC transfer_engine gtest
                                                  gtest_main)
add_test(NAME memory_location_test COMMAND memory_location_test)

add_executable(common_test ${WORKSPACE}/common_test.cpp)
target_link_libraries(common_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME common_test COMMAND common_test)

if(USE_ASCEND_DIRECT)
  # AscendDirectTransport unit test with mock implementations Mock
  # implementations are included in the test file via anonymous namespace
  add_executable(ascend_direct_transport_test
                 ${WORKSPACE}/ascend_direct_transport_test.cpp)
  target_link_libraries(ascend_direct_transport_test
                        PUBLIC transfer_engine gtest gtest_main metadef)
  # Allow shared library undefined symbols (Ascend libraries have circular deps)
  set_target_properties(ascend_direct_transport_test
                        PROPERTIES LINK_FLAGS "-Wl,--allow-shlib-undefined")

  add_test(NAME ascend_direct_transport_test
           COMMAND ascend_direct_transport_test)
endif()

# Multi-protocol transport test (only when ENABLE_MULTI_PROTOCOL is ON)
if(ENABLE_MULTI_PROTOCOL)
  add_executable(mp_transport_test ${WORKSPACE}/mp_transport_test.cpp)
  target_link_libraries(mp_transport_test PUBLIC transfer_engine gtest
                                                 gtest_main)
  add_test(NAME mp_transport_test COMMAND mp_transport_test)
endif()
