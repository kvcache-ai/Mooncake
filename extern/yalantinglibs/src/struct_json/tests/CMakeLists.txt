set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(test_json
        test_json.cpp
        test_some.cpp
        main.cpp
        )
add_test(NAME test_json COMMAND test_json)
