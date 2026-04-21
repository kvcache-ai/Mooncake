set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(reflection_test
        test_reflection.cpp
        )
add_test(NAME reflection_test COMMAND reflection_test)
