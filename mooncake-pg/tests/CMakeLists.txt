# These tests intentionally do not link the PG runtime or require a GPU.
add_library(pg_tree_geometry_cxx17 OBJECT tree_geometry_cxx17_test.cpp)
set_target_properties(
  pg_tree_geometry_cxx17
  PROPERTIES CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON
             CXX_EXTENSIONS OFF)
target_include_directories(pg_tree_geometry_cxx17
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

if(NOT TARGET gtest_main)
  include(${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/FindGTest.cmake)
endif()

add_executable(
  pg_tree_host_test
  tree_topology_test.cpp tree_layout_test.cpp tree_policy_test.cpp
  ../src/device_comm/device_collective/algorithms/tree/tree_topology.cpp)
target_compile_features(pg_tree_host_test PRIVATE cxx_std_20)
target_include_directories(pg_tree_host_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(pg_tree_host_test PRIVATE gtest_main pthread)
add_dependencies(pg_tree_host_test pg_tree_geometry_cxx17)

if(TARGET yalantinglibs::yalantinglibs)
  target_link_libraries(pg_tree_host_test PRIVATE yalantinglibs::yalantinglibs)
else()
  # Standalone host tests need only the existing PGResult header dependency.
  # Reuse headers from the pinned yalantinglibs dependency of a Mooncake build.
  find_path(MOONCAKE_PG_YLT_INCLUDE_DIR ylt/util/expected.hpp)
  if(NOT MOONCAKE_PG_YLT_INCLUDE_DIR)
    message(FATAL_ERROR "Set MOONCAKE_PG_YLT_INCLUDE_DIR to the pinned "
                        "yalantinglibs include directory")
  endif()
  target_include_directories(pg_tree_host_test SYSTEM
                             PRIVATE ${MOONCAKE_PG_YLT_INCLUDE_DIR})
endif()

include(GoogleTest)
gtest_discover_tests(pg_tree_host_test)

if(NOT MOONCAKE_PG_HOST_TESTS_ONLY AND USE_CUDA)
  add_executable(pg_tree_runtime_host_test tree_coordinator_test.cpp)
  target_compile_features(pg_tree_runtime_host_test PRIVATE cxx_std_20)
  target_include_directories(
    pg_tree_runtime_host_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)
  target_link_libraries(
    pg_tree_runtime_host_test
    PRIVATE mooncake_pg gtest_main asio_shared yalantinglibs::yalantinglibs
            CUDA::cudart CUDA::cuda_driver)
  gtest_discover_tests(pg_tree_runtime_host_test)
endif()
