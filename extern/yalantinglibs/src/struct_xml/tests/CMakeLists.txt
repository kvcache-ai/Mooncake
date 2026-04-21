set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_definitions(-DTHROW_UNKNOWN_KEY)
add_executable(test_xml
        test_xml.cpp
        test_xml_nothrow.cpp
        main.cpp
        )
add_test(NAME test_xml COMMAND test_xml)
