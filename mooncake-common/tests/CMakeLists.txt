add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC mooncake_common gtest pthread)
add_test(NAME default_config_test COMMAND default_config_test)

add_executable(environ_test environ_test.cpp)
target_link_libraries(environ_test PUBLIC mooncake_common gtest pthread)
add_test(NAME environ_test COMMAND environ_test)

add_executable(rpc_client_io_threads_test rpc_client_io_threads_test.cpp)
target_link_libraries(rpc_client_io_threads_test PUBLIC mooncake_common gtest
                                                        pthread)
add_test(NAME rpc_client_io_threads_default_test
         COMMAND rpc_client_io_threads_test
                 --gtest_filter=RpcClientIoThreadsTest.UsesConservativeDefault)
set_tests_properties(
  rpc_client_io_threads_default_test
  PROPERTIES
    ENVIRONMENT
    "MC_RPC_CLIENT_IO_THREADS=0;MC_STORE_RPC_CLIENT_IO_THREADS=0;MC_TE_RPC_CLIENT_IO_THREADS=0"
)
add_test(
  NAME rpc_client_io_threads_malformed_test
  COMMAND
    rpc_client_io_threads_test
    --gtest_filter=RpcClientIoThreadsTest.InvalidCommonValueUsesConservativeDefault
)
set_tests_properties(rpc_client_io_threads_malformed_test
                     PROPERTIES ENVIRONMENT "MC_RPC_CLIENT_IO_THREADS=invalid")
add_test(
  NAME rpc_client_io_threads_out_of_range_test
  COMMAND
    rpc_client_io_threads_test
    --gtest_filter=RpcClientIoThreadsTest.InvalidCommonValueUsesConservativeDefault
)
set_tests_properties(
  rpc_client_io_threads_out_of_range_test
  PROPERTIES ENVIRONMENT "MC_RPC_CLIENT_IO_THREADS=99999999999999999999")
add_test(
  NAME rpc_client_io_threads_override_test
  COMMAND
    rpc_client_io_threads_test
    --gtest_filter=RpcClientIoThreadsTest.UsesComponentOverridesAndFreezesValues
)
set_tests_properties(
  rpc_client_io_threads_override_test
  PROPERTIES
    ENVIRONMENT
    "MC_RPC_CLIENT_IO_THREADS=8;MC_STORE_RPC_CLIENT_IO_THREADS=4;MC_TE_RPC_CLIENT_IO_THREADS=6"
)
add_test(
  NAME rpc_client_io_threads_fallback_test
  COMMAND
    rpc_client_io_threads_test
    --gtest_filter=RpcClientIoThreadsTest.InvalidComponentValuesUseCommonFallback
)
set_tests_properties(
  rpc_client_io_threads_fallback_test
  PROPERTIES
    ENVIRONMENT
    "MC_RPC_CLIENT_IO_THREADS=8;MC_STORE_RPC_CLIENT_IO_THREADS=0;MC_TE_RPC_CLIENT_IO_THREADS=invalid"
)

add_executable(rpc_client_io_context_test rpc_client_io_context_test.cpp)
target_link_libraries(rpc_client_io_context_test PUBLIC mooncake_common gtest
                                                        ibverbs pthread)
add_test(NAME rpc_client_io_context_test COMMAND rpc_client_io_context_test)
