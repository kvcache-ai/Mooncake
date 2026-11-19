cmake_minimum_required(VERSION 3.16)
project(mooncake_ssd_offload_core_tests LANGUAGES CXX)

# Standalone build of the exact sources used by mooncake_store. No RNIC, CANN,
# Linux io_uring, network downloads, or master process is required.
get_filename_component(MOONCAKE_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.."
                       ABSOLUTE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

option(SSD_OFFLOAD_SANITIZERS "Enable address and undefined behavior checks"
       OFF)

find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
  set(SSD_OFFLOAD_GTEST_SOURCE
      "${MOONCAKE_ROOT}/extern/yaml-cpp/test/googletest-1.16.0"
      CACHE PATH "Existing GoogleTest source directory")
  if(NOT EXISTS "${SSD_OFFLOAD_GTEST_SOURCE}/CMakeLists.txt")
    message(
      FATAL_ERROR
        "Provide GTest or set SSD_OFFLOAD_GTEST_SOURCE to a local checkout")
  endif()
  set(BUILD_GMOCK
      OFF
      CACHE BOOL "Build only GoogleTest for core tests")
  add_subdirectory("${SSD_OFFLOAD_GTEST_SOURCE}"
                   "${CMAKE_CURRENT_BINARY_DIR}/googletest" EXCLUDE_FROM_ALL)
endif()

add_executable(
  ssd_read_pipeline_test
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_pipeline.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_source.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_pipeline_io.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_uring_pipeline.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_destination_registry.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_destination_registration.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_protocol.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_control.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_direct_read.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_fixed_l2_requester.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_control_wire.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_owner_read_worker.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_push_requester.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_read_pipeline_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_read_source_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_pipeline_io_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_uring_pipeline_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_streaming_config_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_destination_registry_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_destination_registration_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/secure_random_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_read_protocol_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_read_control_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_direct_read_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_fixed_l2_requester_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_owner_read_worker_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_push_requester_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_control_wire_test.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_registered_read_test.cpp")
option(SSD_OFFLOAD_RPC_CODEC_TESTS
       "Build RPC codec tests with existing yalantinglibs headers" OFF)
if(SSD_OFFLOAD_RPC_CODEC_TESTS)
  set(SSD_OFFLOAD_YLT_INCLUDE
      "${MOONCAKE_ROOT}/extern/yalantinglibs/include"
      CACHE PATH "Local yalantinglibs include directory")
  if(NOT EXISTS "${SSD_OFFLOAD_YLT_INCLUDE}/ylt/struct_pack.hpp")
    message(FATAL_ERROR "Provide SSD_OFFLOAD_YLT_INCLUDE for RPC codec tests")
  endif()
  target_sources(
    ssd_read_pipeline_test
    PRIVATE
      "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_read_protocol_codec_test.cpp")
  target_include_directories(ssd_read_pipeline_test SYSTEM
                             PRIVATE "${SSD_OFFLOAD_YLT_INCLUDE}")
endif()

target_include_directories(ssd_read_pipeline_test
                           PRIVATE "${MOONCAKE_ROOT}/mooncake-store/include")
target_link_libraries(ssd_read_pipeline_test PRIVATE GTest::gtest_main)
option(SSD_OFFLOAD_PEER_DIRECTORY_TESTS
       "Build trusted bootstrap tests with local JsonCpp" OFF)
if(SSD_OFFLOAD_PEER_DIRECTORY_TESTS)
  include("${MOONCAKE_ROOT}/mooncake-common/FindJsonCpp.cmake")
  if(NOT TARGET JsonCpp::JsonCpp)
    message(FATAL_ERROR "Provide local JsonCpp for peer directory tests")
  endif()
  target_sources(
    ssd_read_pipeline_test
    PRIVATE "${MOONCAKE_ROOT}/mooncake-store/src/ssd_peer_directory.cpp"
            "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_peer_directory_test.cpp")
  target_link_libraries(ssd_read_pipeline_test PRIVATE JsonCpp::JsonCpp)
  add_executable(
    ssd_peer_directory_allocation_test
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_peer_directory.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_peer_directory_allocation_test.cpp"
  )
  target_include_directories(ssd_peer_directory_allocation_test
                             PRIVATE "${MOONCAKE_ROOT}/mooncake-store/include")
  target_link_libraries(
    ssd_peer_directory_allocation_test
    PRIVATE GTest::gtest_main JsonCpp::JsonCpp ${CMAKE_DL_LIBS})
  target_compile_options(ssd_peer_directory_allocation_test
                         PRIVATE -Wall -Wextra -Werror)
  if(SSD_OFFLOAD_SANITIZERS)
    target_compile_options(
      ssd_peer_directory_allocation_test PRIVATE -fsanitize=address,undefined
                                                 -fno-omit-frame-pointer)
    target_link_options(ssd_peer_directory_allocation_test PRIVATE
                        -fsanitize=address,undefined)
  endif()
  add_test(NAME ssd_peer_directory_allocation_test
           COMMAND ssd_peer_directory_allocation_test)
endif()
option(SSD_OFFLOAD_URING_TESTS "Enable native Linux io_uring tests" OFF)
if(SSD_OFFLOAD_URING_TESTS)
  find_path(SSD_OFFLOAD_LIBURING_INCLUDE liburing.h REQUIRED)
  find_library(SSD_OFFLOAD_LIBURING uring REQUIRED)
  target_include_directories(ssd_read_pipeline_test
                             PRIVATE ${SSD_OFFLOAD_LIBURING_INCLUDE})
  target_link_libraries(ssd_read_pipeline_test PRIVATE ${SSD_OFFLOAD_LIBURING})
  target_compile_definitions(ssd_read_pipeline_test PRIVATE USE_URING)
endif()
target_compile_options(ssd_read_pipeline_test PRIVATE -Wall -Wextra -Werror)
if(SSD_OFFLOAD_SANITIZERS)
  target_compile_options(
    ssd_read_pipeline_test PRIVATE -fsanitize=address,undefined
                                   -fno-omit-frame-pointer)
  target_link_options(ssd_read_pipeline_test PRIVATE
                      -fsanitize=address,undefined)
endif()

include(GoogleTest)
gtest_discover_tests(ssd_read_pipeline_test)
add_executable(
  ssd_requester_allocation_test
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_fixed_l2_requester.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_control_wire.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_push_requester.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_destination_registry.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_destination_registration.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_protocol.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_control.cpp"
  "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_requester_allocation_test.cpp")
target_include_directories(ssd_requester_allocation_test
                           PRIVATE "${MOONCAKE_ROOT}/mooncake-store/include")
target_link_libraries(ssd_requester_allocation_test PRIVATE GTest::gtest_main)
target_compile_options(ssd_requester_allocation_test PRIVATE -Wall -Wextra
                                                             -Werror)
if(SSD_OFFLOAD_SANITIZERS)
  target_compile_options(
    ssd_requester_allocation_test PRIVATE -fsanitize=address,undefined
                                          -fno-omit-frame-pointer)
  target_link_options(ssd_requester_allocation_test PRIVATE
                      -fsanitize=address,undefined)
endif()
gtest_discover_tests(ssd_requester_allocation_test)
if(SSD_OFFLOAD_URING_TESTS)
  add_executable(
    ssd_uring_shutdown_test
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_source.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_pipeline_io.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_uring_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_uring_shutdown_test.cpp")
  target_include_directories(
    ssd_uring_shutdown_test PRIVATE "${MOONCAKE_ROOT}/mooncake-store/include"
                                    ${SSD_OFFLOAD_LIBURING_INCLUDE})
  target_compile_definitions(ssd_uring_shutdown_test PRIVATE USE_URING)
  target_link_libraries(ssd_uring_shutdown_test PRIVATE GTest::gtest_main
                                                        ${SSD_OFFLOAD_LIBURING})
  if(SSD_OFFLOAD_SANITIZERS)
    target_compile_options(
      ssd_uring_shutdown_test PRIVATE -fsanitize=address,undefined
                                      -fno-omit-frame-pointer)
    target_link_options(ssd_uring_shutdown_test PRIVATE
                        -fsanitize=address,undefined)
  endif()
  gtest_discover_tests(ssd_uring_shutdown_test)
endif()

if(SSD_OFFLOAD_URING_TESTS)
  add_executable(
    ssd_uring_read_executor_contract_test
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_source.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_pipeline_io.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_uring_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_uring_read_executor_contract_test.cpp"
  )
  target_include_directories(
    ssd_uring_read_executor_contract_test
    PRIVATE "${MOONCAKE_ROOT}/mooncake-store/include"
            ${SSD_OFFLOAD_LIBURING_INCLUDE})
  target_compile_definitions(ssd_uring_read_executor_contract_test
                             PRIVATE USE_URING)
  target_compile_options(ssd_uring_read_executor_contract_test
                         PRIVATE -Wall -Wextra -Werror)
  target_link_libraries(
    ssd_uring_read_executor_contract_test
    PRIVATE GTest::gtest_main ${SSD_OFFLOAD_LIBURING} pthread)
  target_link_options(
    ssd_uring_read_executor_contract_test
    PRIVATE
    -Wl,--wrap=io_uring_queue_init
    -Wl,--wrap=io_uring_queue_exit
    -Wl,--wrap=io_uring_submit
    -Wl,--wrap=io_uring_register_buffers
    -Wl,--wrap=eventfd)
  if(SSD_OFFLOAD_SANITIZERS)
    target_compile_options(
      ssd_uring_read_executor_contract_test PRIVATE -fsanitize=address,undefined
                                                    -fno-omit-frame-pointer)
    target_link_options(ssd_uring_read_executor_contract_test PRIVATE
                        -fsanitize=address,undefined)
  endif()
  gtest_discover_tests(
    ssd_uring_read_executor_contract_test
    PROPERTIES
    TIMEOUT 30)
  add_executable(
    ssd_uring_read_executor_fault_test
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_read_source.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_pipeline_io.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/src/ssd_uring_pipeline.cpp"
    "${MOONCAKE_ROOT}/mooncake-store/tests/ssd_uring_read_executor_fault_test.cpp"
  )
  target_include_directories(
    ssd_uring_read_executor_fault_test BEFORE
    PRIVATE "${MOONCAKE_ROOT}/mooncake-store/tests/fixtures/uring_read_executor"
            "${MOONCAKE_ROOT}/mooncake-store/include")
  target_compile_definitions(ssd_uring_read_executor_fault_test
                             PRIVATE USE_URING)
  target_compile_options(ssd_uring_read_executor_fault_test
                         PRIVATE -Wall -Wextra -Werror)
  target_link_libraries(ssd_uring_read_executor_fault_test
                        PRIVATE GTest::gtest_main pthread)
  if(SSD_OFFLOAD_SANITIZERS)
    target_compile_options(
      ssd_uring_read_executor_fault_test PRIVATE -fsanitize=address,undefined
                                                 -fno-omit-frame-pointer)
    target_link_options(ssd_uring_read_executor_fault_test PRIVATE
                        -fsanitize=address,undefined)
  endif()
  gtest_discover_tests(
    ssd_uring_read_executor_fault_test
    PROPERTIES
    TIMEOUT 30)
endif()
