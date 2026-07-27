add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC mooncake_common gtest pthread)
add_test(NAME default_config_test COMMAND default_config_test)

add_executable(environ_test environ_test.cpp)
target_link_libraries(environ_test PUBLIC mooncake_common gtest pthread)
add_test(NAME environ_test COMMAND environ_test)

add_executable(crc_checksum_test crc_checksum_test.cpp)
target_link_libraries(crc_checksum_test PUBLIC mooncake_common gtest gtest_main
                                               pthread)
add_test(NAME crc_checksum_test COMMAND crc_checksum_test)
