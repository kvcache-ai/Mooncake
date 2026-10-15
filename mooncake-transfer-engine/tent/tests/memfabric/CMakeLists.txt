# These targets reuse production sources without enabling their native CMake
# targets. Keep ACL declarations and symbols inside the test executables; never
# link this support target into tent_link_group or an installed library.
add_library(tent_test_acl INTERFACE)
target_include_directories(tent_test_acl
                           INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/mock_acl)
target_compile_definitions(tent_test_acl INTERFACE TENT_ACL_TEST_ONLY=1)

add_executable(
  tent_memfabric_transport_test
  ../memfabric_transport_test.cpp
  ../../src/transport/memfabric/memfabric_transport.cpp
  ../../src/transport/memfabric/memfabric_under_api.cpp
  ../../src/transport/memfabric/memfabric_allocator.cpp)
target_include_directories(
  tent_memfabric_transport_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include
          ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
target_link_libraries(
  tent_memfabric_transport_test PRIVATE gtest gtest_main tent_link_group
                                        tent_test_acl ${CMAKE_DL_LIBS})
add_test(NAME tent_memfabric_transport_test
         COMMAND tent_memfabric_transport_test)

add_executable(
  tent_ascend_platform_mock_test
  ascend_platform_mock_test.cpp ../../src/platform/ascend/ascend_probe.cpp
  ../../src/platform/ascend/ascend_allocator.cpp)
# Enables CHECK_ASCEND in these translation units only. The CPU runtime and its
# Platform::getLoader() are unchanged; tests construct AscendPlatform directly.
target_compile_definitions(tent_ascend_platform_mock_test PRIVATE USE_ASCEND)
target_link_libraries(tent_ascend_platform_mock_test
                      PRIVATE gtest gtest_main tent_link_group tent_test_acl)
add_test(NAME tent_ascend_platform_mock_test
         COMMAND tent_ascend_platform_mock_test)

set_tests_properties(tent_memfabric_transport_test
                     tent_ascend_platform_mock_test PROPERTIES TIMEOUT 120)
