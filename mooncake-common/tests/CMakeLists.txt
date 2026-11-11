add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC
    mooncake_common 
    gtest
    pthread PUBLIC
)
add_test(NAME default_config_test COMMAND default_config_test)

add_executable(environ_test environ_test.cpp)
target_link_libraries(environ_test PUBLIC mooncake_common gtest pthread)
add_test(NAME environ_test COMMAND environ_test)

foreach(parser_test ascii_string bool_parser environment_value_parser
                    integer_parser)
  add_executable(${parser_test}_test ${parser_test}_test.cpp)
  target_link_libraries(${parser_test}_test PUBLIC mooncake_common gtest
                                                   gtest_main pthread)
  add_test(NAME ${parser_test}_test COMMAND ${parser_test}_test)
endforeach()
