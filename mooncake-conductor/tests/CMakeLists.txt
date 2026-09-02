# Unit tests for mooncake-conductor (gtest, BUILD_UNIT_TESTS).

if(TARGET gtest_main)
  set(CONDUCTOR_GTEST_LIBS gtest gtest_main)
else()
  find_package(GTest REQUIRED)
  set(CONDUCTOR_GTEST_LIBS GTest::gtest GTest::gtest_main)
endif()

add_executable(conductor_test common_utils_test.cpp json_uint64_test.cpp
                              model_context_test.cpp compute_hash_test.cpp)

target_link_libraries(conductor_test PRIVATE conductor_cpp_core
                                             ${CONDUCTOR_GTEST_LIBS})

# Tests load golden vectors relative to this directory.
target_compile_definitions(
  conductor_test
  PRIVATE CONDUCTOR_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")

add_test(NAME conductor_test COMMAND conductor_test)
