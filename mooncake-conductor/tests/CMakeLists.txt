# Unit tests for mooncake-conductor (gtest, BUILD_UNIT_TESTS).

find_package(GTest REQUIRED)

add_executable(conductor_test common_utils_test.cpp json_uint64_test.cpp)

target_link_libraries(conductor_test PRIVATE conductor_cpp_core GTest::gtest
                                             GTest::gtest_main)

add_test(NAME conductor_test COMMAND conductor_test)
