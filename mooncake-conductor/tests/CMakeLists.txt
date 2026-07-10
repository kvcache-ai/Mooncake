# Unit tests for mooncake-conductor (gtest, BUILD_UNIT_TESTS).

find_package(GTest REQUIRED)

add_executable(conductor_test
  common_utils_test.cpp
  model_context_test.cpp
  compute_hash_test.cpp
  prefix_indexer_test.cpp
  json_uint64_test.cpp
  msg_decoder_test.cpp
  zmq_client_test.cpp
  event_manager_test.cpp
  concurrency_test.cpp
)

target_link_libraries(conductor_test PRIVATE
  conductor_cpp_core
  GTest::gtest
  GTest::gtest_main
)

# Tests load golden vectors relative to this directory.
target_compile_definitions(conductor_test PRIVATE
  CONDUCTOR_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
)

add_test(NAME conductor_test COMMAND conductor_test)
