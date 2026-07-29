add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC mooncake_common gtest pthread)
add_test(NAME default_config_test COMMAND default_config_test)

add_executable(environ_test environ_test.cpp)
target_link_libraries(environ_test PUBLIC mooncake_common gtest pthread)
add_test(NAME environ_test COMMAND environ_test)

add_executable(rpc_client_io_threads_test rpc_client_io_threads_test.cpp)
target_link_libraries(rpc_client_io_threads_test PUBLIC mooncake_common gtest
                                                        pthread)
add_test(NAME rpc_client_io_threads_test COMMAND rpc_client_io_threads_test)

add_executable(rpc_client_io_context_test rpc_client_io_context_test.cpp)
target_link_libraries(rpc_client_io_context_test PUBLIC mooncake_common gtest
                                                        ibverbs pthread)
add_test(NAME rpc_client_io_context_test COMMAND rpc_client_io_context_test)
