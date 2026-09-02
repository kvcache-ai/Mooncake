# Unit tests for mooncake-conductor (gtest, BUILD_UNIT_TESTS).

if(TARGET gtest_main)
  set(CONDUCTOR_GTEST_LIBS gtest gtest_main)
else()
  find_package(GTest REQUIRED)
  set(CONDUCTOR_GTEST_LIBS GTest::gtest GTest::gtest_main)
endif()

add_executable(conductor_test common_utils_test.cpp json_uint64_test.cpp)

target_link_libraries(conductor_test PRIVATE conductor_cpp_core
                                             ${CONDUCTOR_GTEST_LIBS})

add_test(NAME conductor_test COMMAND conductor_test)
