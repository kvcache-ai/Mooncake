set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(easylog_test
        test_easylog.cpp
        main.cpp
        )
add_test(NAME easylog_test COMMAND easylog_test)
target_compile_definitions(easylog_test PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE)

