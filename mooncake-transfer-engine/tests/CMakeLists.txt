set(WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}")

if (USE_HIP)
  file(GLOB TEST_SOURCES "*.cpp")
  hipify_files(TEST_SOURCES)

  file(RELATIVE_PATH EXAMPLE_REL_PATH "${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
  set(WORKSPACE "${CMAKE_BINARY_DIR}/${EXAMPLE_REL_PATH}")
endif()

add_executable(rdma_transport_test ${WORKSPACE}/rdma_transport_test.cpp)
target_link_libraries(rdma_transport_test PUBLIC transfer_engine)
# add_test(NAME rdma_transport_test COMMAND rdma_transport_test)

add_executable(transport_uint_test ${WORKSPACE}/transport_uint_test.cpp)
target_link_libraries(transport_uint_test PUBLIC transfer_engine gtest gtest_main )
add_test(NAME transport_uint_test COMMAND transport_uint_test)

add_executable(rdma_transport_test2 ${WORKSPACE}/rdma_transport_test2.cpp)
target_link_libraries(rdma_transport_test2 PUBLIC transfer_engine gtest gtest_main )
# add_test(NAME rdma_transport_test2 COMMAND rdma_transport_test2)

add_executable(rdma_loopback_test ${WORKSPACE}/rdma_loopback_test.cpp)
target_link_libraries(rdma_loopback_test PUBLIC transfer_engine gtest gtest_main )
# add_test(NAME rdma_loopback_test COMMAND rdma_loopback_test)

if (USE_CXL)
    add_executable(cxl_transport_test ${WORKSPACE}/cxl_transport_test.cpp)
    target_link_libraries(cxl_transport_test PUBLIC transfer_engine gtest gtest_main )
    add_test(NAME cxl_transport_test COMMAND cxl_transport_test)
endif()

if (USE_NVMEOF)
    add_executable(nvmeof_transport_test ${WORKSPACE}/nvmeof_transport_test.cpp)
    target_link_libraries(nvmeof_transport_test PUBLIC transfer_engine gtest gtest_main )
    # add_test(NAME nvmeof_transport_test COMMAND nvmeof_transport_test)
endif()

if (USE_TCP)
add_executable(tcp_transport_test ${WORKSPACE}/tcp_transport_test.cpp)
target_link_libraries(tcp_transport_test PUBLIC transfer_engine gtest gtest_main )
add_test(NAME tcp_transport_test COMMAND tcp_transport_test)
endif()

if (USE_MNNVL)
    add_executable(nvlink_transport_test ${WORKSPACE}/nvlink_transport_test.cpp)
    target_link_libraries(nvlink_transport_test PUBLIC transfer_engine gtest gtest_main )
    add_test(NAME nvlink_transport_test COMMAND nvlink_transport_test)
endif()

if (USE_UBSHMEM)
    add_executable(ubshmem_transport_test ${WORKSPACE}/ubshmem_transport_test.cpp)
    target_link_libraries(ubshmem_transport_test PUBLIC transfer_engine gtest gtest_main ascendcl)
    add_test(NAME ubshmem_transport_test COMMAND ubshmem_transport_test)
endif()

if (USE_EFA)
    add_executable(efa_transport_test ${WORKSPACE}/efa_transport_test.cpp)
    target_link_libraries(efa_transport_test PUBLIC transfer_engine gtest gtest_main)
    add_test(NAME efa_transport_test COMMAND efa_transport_test)
endif()

add_executable(transfer_metadata_test ${WORKSPACE}/transfer_metadata_test.cpp)
target_link_libraries(transfer_metadata_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME transfer_metadata_test COMMAND transfer_metadata_test)

add_executable(topology_test ${WORKSPACE}/topology_test.cpp)
target_link_libraries(topology_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME topology_test COMMAND topology_test)

add_executable(memory_location_test ${WORKSPACE}/memory_location_test.cpp)
target_link_libraries(memory_location_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME memory_location_test COMMAND memory_location_test)

add_executable(common_test ${WORKSPACE}/common_test.cpp)
target_link_libraries(common_test PUBLIC transfer_engine gtest gtest_main)
add_test(NAME common_test COMMAND common_test)

add_executable(zmq_communicator_test zmq_communicator_test.cpp)
target_link_libraries(zmq_communicator_test PUBLIC transfer_engine zmq_communicator_lib gtest gtest_main)
target_include_directories(zmq_communicator_test PRIVATE ../src ../include)
add_test(NAME zmq_communicator_test COMMAND zmq_communicator_test)
