set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(test_yaml
        test_yaml.cpp
        )
add_test(NAME test_yaml COMMAND test_yaml)
